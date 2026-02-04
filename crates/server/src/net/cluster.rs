//! Cluster Server - Handles inter-node communication
//!
//! Implements:
//! - Heartbeat/health checking between nodes
//! - Simple leader election
//! - Command replication to followers
//! - Cluster membership management

use super::protocol::{
    AppendEntriesMessage, AppendEntriesResponseMessage, ClusterNode, ClusterStateMessage,
    JoinRequestMessage, JoinResponseMessage, LogEntry, MessageType, NodeRole, NodeState,
    PingMessage, PongMessage, ReplicateAckMessage, ReplicateMessage, ReplicationCommand,
    RequestVoteMessage, VoteResponseMessage, decode_message, encode_message,
};
use crate::config::cluster::ClusterConfig;
use anyhow::Result;
use bytes::BytesMut;
use eagle_core::store::Store;
use hashbrown::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tokio::time::{interval, timeout};
use tracing::{debug, error, info};

/// Cluster node state
pub struct ClusterState {
    /// Current node ID
    pub node_id: String,
    /// Current role
    pub role: RwLock<NodeRole>,
    /// Current term
    pub term: AtomicU64,
    /// Current leader ID
    pub leader_id: RwLock<Option<String>>,
    /// Known peers
    pub peers: RwLock<HashMap<String, PeerInfo>>,
    /// Last heartbeat from leader
    pub last_heartbeat: RwLock<Instant>,
    /// Voted for in current term
    pub voted_for: RwLock<Option<String>>,
    /// Commit index
    pub commit_index: AtomicU64,
    /// Replication log
    pub log: RwLock<Vec<LogEntry>>,
    /// Start time
    pub start_time: Instant,
}

/// Information about a peer node
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: String,
    pub address: SocketAddr,
    pub role: NodeRole,
    pub is_alive: bool,
    pub last_seen: Instant,
    pub match_index: u64,
    #[allow(dead_code)]
    pub next_index: u64,
}

impl ClusterState {
    pub fn new(config: &ClusterConfig) -> Self {
        let mut peers = HashMap::new();
        for (id, addr) in &config.peers {
            peers.insert(
                id.clone(),
                PeerInfo {
                    node_id: id.clone(),
                    address: *addr,
                    role: NodeRole::Follower,
                    is_alive: false,
                    last_seen: Instant::now(),
                    match_index: 0,
                    next_index: 1,
                },
            );
        }

        Self {
            node_id: config.node_id.clone(),
            role: RwLock::new(NodeRole::Follower),
            term: AtomicU64::new(0),
            leader_id: RwLock::new(None),
            peers: RwLock::new(peers),
            last_heartbeat: RwLock::new(Instant::now()),
            voted_for: RwLock::new(None),
            commit_index: AtomicU64::new(0),
            log: RwLock::new(Vec::new()),
            start_time: Instant::now(),
        }
    }

    pub fn node_state(&self) -> NodeState {
        NodeState {
            node_id: self.node_id.clone(),
            role: *self.role.blocking_read(),
            term: self.term.load(Ordering::Acquire),
            leader_id: self.leader_id.blocking_read().clone(),
            entry_count: self.log.blocking_read().len() as u64,
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }

    pub async fn become_follower(&self, term: u64, leader_id: Option<String>) {
        *self.role.write().await = NodeRole::Follower;
        self.term.store(term, Ordering::Release);
        *self.leader_id.write().await = leader_id;
        *self.voted_for.write().await = None;
    }

    pub async fn become_candidate(&self) {
        let new_term = self.term.fetch_add(1, Ordering::AcqRel) + 1;
        *self.role.write().await = NodeRole::Candidate;
        *self.voted_for.write().await = Some(self.node_id.clone());
        info!("Became candidate for term {}", new_term);
    }

    pub async fn become_leader(&self) {
        *self.role.write().await = NodeRole::Leader;
        *self.leader_id.write().await = Some(self.node_id.clone());
        info!(
            "Became leader for term {}",
            self.term.load(Ordering::Acquire)
        );
    }

    pub async fn is_leader(&self) -> bool {
        *self.role.read().await == NodeRole::Leader
    }
}

/// Start the cluster server
pub async fn start_cluster_server(
    addr: SocketAddr,
    store: Arc<Store>,
    config: ClusterConfig,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Cluster server listening on {}", addr);

    let state = Arc::new(ClusterState::new(&config));
    let (tx, rx) = mpsc::channel::<ReplicationCommand>(1000);

    // Start background tasks
    let state_clone = state.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        heartbeat_task(state_clone, config_clone).await;
    });

    let state_clone = state.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        election_task(state_clone, config_clone).await;
    });

    let state_clone = state.clone();
    let store_clone = store.clone();
    tokio::spawn(async move {
        replication_task(state_clone, store_clone, rx).await;
    });

    // Accept connections
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        info!("Accepted cluster connection from {}", peer_addr);

        let state = state.clone();
        let store = store.clone();
        let tx = tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_cluster_connection(socket, peer_addr, state, store, tx).await {
                error!("Cluster connection error from {}: {}", peer_addr, e);
            }
        });
    }
}

/// Handle a cluster connection
async fn handle_cluster_connection(
    socket: TcpStream,
    peer_addr: SocketAddr,
    state: Arc<ClusterState>,
    store: Arc<Store>,
    tx: mpsc::Sender<ReplicationCommand>,
) -> Result<()> {
    let (reader, writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    let mut buf = BytesMut::with_capacity(4096);

    loop {
        // Read message length prefix
        let len = match reader.read_u32().await {
            Ok(l) => l as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };

        if len > 10 * 1024 * 1024 {
            // 10MB max message size
            error!("Message too large: {} bytes", len);
            break;
        }

        buf.resize(len, 0);
        reader.read_exact(&mut buf).await?;

        // Decode and handle message
        let (header, payload) = decode_message(&buf)?;

        let response = match header.msg_type {
            MessageType::Ping => {
                let ping: PingMessage = serde_json::from_slice(&payload)?;
                handle_ping(&state, ping).await?
            }
            MessageType::RequestVote => {
                let req: RequestVoteMessage = serde_json::from_slice(&payload)?;
                handle_request_vote(&state, req).await?
            }
            MessageType::AppendEntries => {
                let req: AppendEntriesMessage = serde_json::from_slice(&payload)?;
                handle_append_entries(&state, &store, req).await?
            }
            MessageType::Replicate => {
                let req: ReplicateMessage = serde_json::from_slice(&payload)?;
                handle_replicate(&state, &store, req, &tx).await?
            }
            MessageType::JoinRequest => {
                let req: JoinRequestMessage = serde_json::from_slice(&payload)?;
                handle_join_request(&state, req).await?
            }
            MessageType::ClusterStateRequest => handle_cluster_state_request(&state).await?,
            _ => {
                debug!("Unhandled message type: {:?}", header.msg_type);
                continue;
            }
        };

        // Send response
        writer.write_all(&response).await?;
        writer.flush().await?;
    }

    info!("Cluster connection closed from {}", peer_addr);
    Ok(())
}

/// Handle ping message
async fn handle_ping(state: &ClusterState, ping: PingMessage) -> Result<bytes::Bytes> {
    // Update peer info
    {
        let mut peers = state.peers.write().await;
        if let Some(peer) = peers.get_mut(&ping.node_state.node_id) {
            peer.is_alive = true;
            peer.last_seen = Instant::now();
            peer.role = ping.node_state.role;
        }
    }

    let pong = PongMessage {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        request_timestamp: ping.timestamp,
        node_state: state.node_state(),
    };

    encode_message(
        MessageType::Pong,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &pong,
    )
    .map_err(Into::into)
}

/// Handle vote request
async fn handle_request_vote(
    state: &ClusterState,
    req: RequestVoteMessage,
) -> Result<bytes::Bytes> {
    let current_term = state.term.load(Ordering::Acquire);
    let mut vote_granted = false;

    if req.term > current_term {
        state.become_follower(req.term, None).await;
    }

    if req.term >= current_term {
        let voted_for = state.voted_for.read().await;
        if voted_for.is_none() || voted_for.as_ref() == Some(&req.candidate_id) {
            // Check log is at least as up-to-date
            let log = state.log.read().await;
            let last_log_index = log.len() as u64;
            let last_log_term = log.last().map(|e| e.term).unwrap_or(0);

            if req.last_log_term > last_log_term
                || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index)
            {
                vote_granted = true;
                *state.voted_for.write().await = Some(req.candidate_id.clone());
            }
        }
    }

    let response = VoteResponseMessage {
        term: state.term.load(Ordering::Acquire),
        vote_granted,
    };

    encode_message(
        MessageType::VoteResponse,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &response,
    )
    .map_err(Into::into)
}

/// Handle append entries (heartbeat and log replication)
async fn handle_append_entries(
    state: &ClusterState,
    store: &Store,
    req: AppendEntriesMessage,
) -> Result<bytes::Bytes> {
    let current_term = state.term.load(Ordering::Acquire);
    let mut success = false;
    let mut match_index = 0;

    if req.term >= current_term {
        state
            .become_follower(req.term, Some(req.leader_id.clone()))
            .await;
        *state.last_heartbeat.write().await = Instant::now();

        // Check prev log entry
        let mut log = state.log.write().await;
        let prev_ok = if req.prev_log_index == 0 {
            true
        } else if let Some(entry) = log.get((req.prev_log_index - 1) as usize) {
            entry.term == req.prev_log_term
        } else {
            false
        };

        if prev_ok {
            // Append entries
            for entry in req.entries {
                let idx = entry.index as usize;
                if idx <= log.len() {
                    // Overwrite conflicting entry
                    if idx < log.len() && log[idx - 1].term != entry.term {
                        log.truncate(idx - 1);
                    }
                    if idx > log.len() || log.get(idx - 1).is_none() {
                        // Apply command to store
                        apply_command(store, &entry.command).await?;
                        log.push(entry);
                    }
                } else {
                    apply_command(store, &entry.command).await?;
                    log.push(entry);
                }
            }

            // Update commit index
            if req.leader_commit > state.commit_index.load(Ordering::Acquire) {
                let new_commit = req.leader_commit.min(log.len() as u64);
                state.commit_index.store(new_commit, Ordering::Release);
            }

            success = true;
            match_index = log.len() as u64;
        }
    }

    let response = AppendEntriesResponseMessage {
        term: state.term.load(Ordering::Acquire),
        success,
        match_index,
    };

    encode_message(
        MessageType::AppendEntriesResponse,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &response,
    )
    .map_err(Into::into)
}

/// Handle simple replication
async fn handle_replicate(
    state: &ClusterState,
    store: &Store,
    req: ReplicateMessage,
    _tx: &mpsc::Sender<ReplicationCommand>,
) -> Result<bytes::Bytes> {
    let mut success = true;

    for cmd in req.commands {
        if let Err(e) = apply_command(store, &cmd).await {
            error!("Failed to apply replicated command: {}", e);
            success = false;
            break;
        }
    }

    let response = ReplicateAckMessage {
        sequence: req.sequence,
        success,
    };

    encode_message(
        MessageType::ReplicateAck,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &response,
    )
    .map_err(Into::into)
}

/// Handle join request
async fn handle_join_request(
    state: &ClusterState,
    req: JoinRequestMessage,
) -> Result<bytes::Bytes> {
    let is_leader = state.is_leader().await;
    let leader_id = state.leader_id.read().await.clone();

    let mut cluster_nodes = Vec::new();
    for (id, peer) in state.peers.read().await.iter() {
        cluster_nodes.push(ClusterNode {
            node_id: id.clone(),
            address: peer.address.to_string(),
            role: peer.role,
            is_alive: peer.is_alive,
        });
    }

    // Add self
    cluster_nodes.push(ClusterNode {
        node_id: state.node_id.clone(),
        address: String::new(), // Would need to track our own address
        role: *state.role.read().await,
        is_alive: true,
    });

    let response = if is_leader {
        // Add the new node to our peers
        let addr: SocketAddr = req
            .address
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap());
        state.peers.write().await.insert(
            req.node_id.clone(),
            PeerInfo {
                node_id: req.node_id,
                address: addr,
                role: NodeRole::Follower,
                is_alive: true,
                last_seen: Instant::now(),
                match_index: 0,
                next_index: 1,
            },
        );

        JoinResponseMessage {
            success: true,
            leader_id,
            leader_address: None,
            cluster_nodes,
        }
    } else {
        JoinResponseMessage {
            success: false,
            leader_id,
            leader_address: None, // Could look up leader address
            cluster_nodes,
        }
    };

    encode_message(
        MessageType::JoinResponse,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &response,
    )
    .map_err(Into::into)
}

/// Handle cluster state request
async fn handle_cluster_state_request(state: &ClusterState) -> Result<bytes::Bytes> {
    let mut nodes = Vec::new();
    for (id, peer) in state.peers.read().await.iter() {
        nodes.push(ClusterNode {
            node_id: id.clone(),
            address: peer.address.to_string(),
            role: peer.role,
            is_alive: peer.is_alive,
        });
    }

    let response = ClusterStateMessage {
        term: state.term.load(Ordering::Acquire),
        leader_id: state.leader_id.read().await.clone(),
        nodes,
        commit_index: state.commit_index.load(Ordering::Acquire),
    };

    encode_message(
        MessageType::ClusterStateResponse,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &response,
    )
    .map_err(Into::into)
}

/// Apply a replication command to the store
async fn apply_command(store: &Store, cmd: &ReplicationCommand) -> Result<()> {
    match cmd {
        ReplicationCommand::Set { key, value } => {
            store.set(key.clone(), value.clone())?;
        }
        ReplicationCommand::Delete { key } => {
            store.delete(key);
        }
        ReplicationCommand::HSet { key, field, value } => {
            store.hset(key, field, value.clone())?;
        }
        ReplicationCommand::HDelete { key, field } => {
            store.hdel(key, field)?;
        }
    }
    Ok(())
}

/// Background heartbeat task
async fn heartbeat_task(state: Arc<ClusterState>, _config: ClusterConfig) {
    let mut interval = interval(Duration::from_millis(100));

    loop {
        interval.tick().await;

        if !state.is_leader().await {
            continue;
        }

        let peers: Vec<_> = state.peers.read().await.values().cloned().collect();

        for peer in peers {
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = send_heartbeat(&state, &peer).await {
                    debug!("Failed to send heartbeat to {}: {}", peer.node_id, e);
                }
            });
        }
    }
}

/// Send heartbeat to a peer
async fn send_heartbeat(state: &ClusterState, peer: &PeerInfo) -> Result<()> {
    let socket = timeout(Duration::from_secs(1), TcpStream::connect(peer.address)).await??;
    let (_reader, mut writer) = socket.into_split();

    let entries = AppendEntriesMessage {
        term: state.term.load(Ordering::Acquire),
        leader_id: state.node_id.clone(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: state.commit_index.load(Ordering::Acquire),
    };

    let msg = encode_message(
        MessageType::AppendEntries,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &entries,
    )?;

    writer.write_u32(msg.len() as u32).await?;
    writer.write_all(&msg).await?;
    writer.flush().await?;

    Ok(())
}

/// Background election task
async fn election_task(state: Arc<ClusterState>, config: ClusterConfig) {
    let election_timeout = config.election_timeout;

    loop {
        tokio::time::sleep(election_timeout).await;

        let role = *state.role.read().await;
        if role == NodeRole::Leader {
            continue;
        }

        let last_heartbeat = *state.last_heartbeat.read().await;
        if last_heartbeat.elapsed() > election_timeout {
            info!("Election timeout, starting election");
            start_election(&state).await;
        }
    }
}

/// Start a leader election
async fn start_election(state: &ClusterState) {
    state.become_candidate().await;

    let term = state.term.load(Ordering::Acquire);
    let log = state.log.read().await;
    let last_log_index = log.len() as u64;
    let last_log_term = log.last().map(|e| e.term).unwrap_or(0);
    drop(log);

    let req = RequestVoteMessage {
        candidate_id: state.node_id.clone(),
        term,
        last_log_index,
        last_log_term,
    };

    let peers: Vec<_> = state.peers.read().await.values().cloned().collect();
    let total_nodes = peers.len() + 1;
    let votes_needed = total_nodes / 2 + 1;
    let mut votes = 1; // Vote for self

    for peer in peers {
        match request_vote(&state.node_id, term, &peer, &req).await {
            Ok(response) => {
                if response.vote_granted {
                    votes += 1;
                    if votes >= votes_needed {
                        state.become_leader().await;
                        return;
                    }
                }
            }
            Err(e) => {
                debug!("Failed to request vote from {}: {}", peer.node_id, e);
            }
        }
    }

    // Not enough votes, stay as follower
    state.become_follower(term, None).await;
}

/// Request vote from a peer
async fn request_vote(
    node_id: &str,
    term: u64,
    peer: &PeerInfo,
    req: &RequestVoteMessage,
) -> Result<VoteResponseMessage> {
    let socket = timeout(Duration::from_secs(1), TcpStream::connect(peer.address)).await??;
    let (mut reader, mut writer) = socket.into_split();

    let msg = encode_message(MessageType::RequestVote, node_id, term, req)?;

    writer.write_u32(msg.len() as u32).await?;
    writer.write_all(&msg).await?;
    writer.flush().await?;

    // Read response
    let len = reader.read_u32().await? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;

    let (_, payload) = decode_message(&buf)?;
    let response: VoteResponseMessage = serde_json::from_slice(&payload)?;

    Ok(response)
}

/// Background replication task
async fn replication_task(
    state: Arc<ClusterState>,
    _store: Arc<Store>,
    mut rx: mpsc::Receiver<ReplicationCommand>,
) {
    while let Some(cmd) = rx.recv().await {
        if !state.is_leader().await {
            continue;
        }

        // Add to log
        let entry = {
            let mut log = state.log.write().await;
            let index = log.len() as u64 + 1;
            let term = state.term.load(Ordering::Acquire);
            let entry = LogEntry {
                index,
                term,
                command: cmd,
            };
            log.push(entry.clone());
            entry
        };

        // Replicate to peers
        let peers: Vec<_> = state.peers.read().await.values().cloned().collect();
        for peer in peers {
            let entry = entry.clone();
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = replicate_to_peer(&state, &peer, vec![entry]).await {
                    debug!("Failed to replicate to {}: {}", peer.node_id, e);
                }
            });
        }
    }
}

/// Replicate entries to a peer
async fn replicate_to_peer(
    state: &ClusterState,
    peer: &PeerInfo,
    entries: Vec<LogEntry>,
) -> Result<()> {
    let socket = timeout(Duration::from_secs(1), TcpStream::connect(peer.address)).await??;
    let (_reader, mut writer) = socket.into_split();

    let msg = AppendEntriesMessage {
        term: state.term.load(Ordering::Acquire),
        leader_id: state.node_id.clone(),
        prev_log_index: peer.match_index,
        prev_log_term: 0, // Would need to track this
        entries,
        leader_commit: state.commit_index.load(Ordering::Acquire),
    };

    let encoded = encode_message(
        MessageType::AppendEntries,
        &state.node_id,
        state.term.load(Ordering::Acquire),
        &msg,
    )?;

    writer.write_u32(encoded.len() as u32).await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;

    Ok(())
}
