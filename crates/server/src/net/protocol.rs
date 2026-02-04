//! Cluster Protocol Messages
//!
//! Defines the wire protocol for cluster communication including:
//! - Heartbeat/ping messages
//! - Replication commands
//! - Cluster state synchronization
//! - Raft-style leader election (simplified)

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor};

/// Protocol version
pub const PROTOCOL_VERSION: u8 = 1;

/// Magic number for protocol validation
pub const PROTOCOL_MAGIC: u32 = 0x4547414C; // "EGAL"

/// Message types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    /// Heartbeat/ping
    Ping = 0,
    /// Heartbeat response
    Pong = 1,
    /// Request vote (leader election)
    RequestVote = 2,
    /// Vote response
    VoteResponse = 3,
    /// Append entries (replication)
    AppendEntries = 4,
    /// Append entries response
    AppendEntriesResponse = 5,
    /// Cluster state request
    ClusterStateRequest = 6,
    /// Cluster state response
    ClusterStateResponse = 7,
    /// Key-value replication
    Replicate = 8,
    /// Replication acknowledgment
    ReplicateAck = 9,
    /// Join cluster request
    JoinRequest = 10,
    /// Join cluster response
    JoinResponse = 11,
}

impl From<u8> for MessageType {
    fn from(v: u8) -> Self {
        match v {
            0 => MessageType::Ping,
            1 => MessageType::Pong,
            2 => MessageType::RequestVote,
            3 => MessageType::VoteResponse,
            4 => MessageType::AppendEntries,
            5 => MessageType::AppendEntriesResponse,
            6 => MessageType::ClusterStateRequest,
            7 => MessageType::ClusterStateResponse,
            8 => MessageType::Replicate,
            9 => MessageType::ReplicateAck,
            10 => MessageType::JoinRequest,
            11 => MessageType::JoinResponse,
            _ => MessageType::Ping,
        }
    }
}

/// Header for all cluster messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub magic: u32,
    pub version: u8,
    pub msg_type: MessageType,
    pub payload_len: u32,
    pub sender_id: String,
    pub term: u64,
}

impl MessageHeader {
    pub fn new(msg_type: MessageType, sender_id: String, term: u64, payload_len: u32) -> Self {
        Self {
            magic: PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            msg_type,
            payload_len,
            sender_id,
            term,
        }
    }

    pub fn validate(&self) -> bool {
        self.magic == PROTOCOL_MAGIC && self.version == PROTOCOL_VERSION
    }
}

/// Ping message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingMessage {
    pub timestamp: u64,
    pub node_state: NodeState,
}

/// Pong response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongMessage {
    pub timestamp: u64,
    pub request_timestamp: u64,
    pub node_state: NodeState,
}

/// Node state for heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub node_id: String,
    pub role: NodeRole,
    pub term: u64,
    pub leader_id: Option<String>,
    pub entry_count: u64,
    pub uptime_secs: u64,
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

/// Request vote message (Raft)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteMessage {
    pub candidate_id: String,
    pub term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Vote response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponseMessage {
    pub term: u64,
    pub vote_granted: bool,
}

/// Append entries message (log replication)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesMessage {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// Append entries response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponseMessage {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

/// Log entry for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: ReplicationCommand,
}

/// Command to replicate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationCommand {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    HSet {
        key: Vec<u8>,
        field: Vec<u8>,
        value: Vec<u8>,
    },
    HDelete {
        key: Vec<u8>,
        field: Vec<u8>,
    },
}

/// Replicate message (simple replication without full Raft)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateMessage {
    pub sequence: u64,
    pub commands: Vec<ReplicationCommand>,
}

/// Replicate acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateAckMessage {
    pub sequence: u64,
    pub success: bool,
}

/// Join request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequestMessage {
    pub node_id: String,
    pub address: String,
}

/// Join response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponseMessage {
    pub success: bool,
    pub leader_id: Option<String>,
    pub leader_address: Option<String>,
    pub cluster_nodes: Vec<ClusterNode>,
}

/// Cluster node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: String,
    pub address: String,
    pub role: NodeRole,
    pub is_alive: bool,
}

/// Cluster state message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateMessage {
    pub term: u64,
    pub leader_id: Option<String>,
    pub nodes: Vec<ClusterNode>,
    pub commit_index: u64,
}

/// Encode a message to bytes
pub fn encode_message<T: Serialize>(
    msg_type: MessageType,
    sender_id: &str,
    term: u64,
    payload: &T,
) -> io::Result<Bytes> {
    let payload_bytes =
        serde_json::to_vec(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let header = MessageHeader::new(
        msg_type,
        sender_id.to_string(),
        term,
        payload_bytes.len() as u32,
    );

    let header_bytes =
        serde_json::to_vec(&header).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut buf = BytesMut::with_capacity(4 + header_bytes.len() + payload_bytes.len());
    buf.put_u32(header_bytes.len() as u32);
    buf.put_slice(&header_bytes);
    buf.put_slice(&payload_bytes);

    Ok(buf.freeze())
}

/// Decode a message from bytes
pub fn decode_message(data: &[u8]) -> io::Result<(MessageHeader, Bytes)> {
    if data.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Message too short",
        ));
    }

    let mut cursor = Cursor::new(data);
    let header_len = cursor.get_u32() as usize;

    if data.len() < 4 + header_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Incomplete header",
        ));
    }

    let header: MessageHeader = serde_json::from_slice(&data[4..4 + header_len])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if !header.validate() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid message header",
        ));
    }

    let payload_start = 4 + header_len;
    let payload_end = payload_start + header.payload_len as usize;

    if data.len() < payload_end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Incomplete payload",
        ));
    }

    let payload = Bytes::copy_from_slice(&data[payload_start..payload_end]);

    Ok((header, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_ping() {
        let ping = PingMessage {
            timestamp: 12345,
            node_state: NodeState {
                node_id: "node1".to_string(),
                role: NodeRole::Follower,
                term: 1,
                leader_id: None,
                entry_count: 100,
                uptime_secs: 3600,
            },
        };

        let encoded = encode_message(MessageType::Ping, "node1", 1, &ping).unwrap();
        let (header, payload) = decode_message(&encoded).unwrap();

        assert_eq!(header.msg_type, MessageType::Ping);
        assert_eq!(header.sender_id, "node1");
        assert_eq!(header.term, 1);

        let decoded: PingMessage = serde_json::from_slice(&payload).unwrap();
        assert_eq!(decoded.timestamp, 12345);
    }

    #[test]
    fn test_replication_command() {
        let cmd = ReplicationCommand::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: ReplicationCommand = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            ReplicationCommand::Set { key, value } => {
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            }
            _ => panic!("Wrong command type"),
        }
    }
}
