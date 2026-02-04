use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterInfo {
    node_id: String,
    role: String,
    state: String,
    slots: Vec<(u16, u16)>, // (start, end) ranges
}

impl ClusterInfo {
    pub fn new() -> Self {
        ClusterInfo {
            node_id: "node1".to_string(),
            role: "master".to_string(),
            state: "online".to_string(),
            slots: vec![(0, 16383)], // Full slot range
        }
    }
}

impl Default for ClusterInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ClusterInfo {
    fn name(&self) -> &'static str {
        "CLUSTER INFO"
    }

    async fn execute(&self, _store: &Store) -> Result<RespValue> {
        // CLUSTER INFO returns multi-line output, must use BulkString (SimpleString cannot contain \r\n)
        Ok(RespValue::BulkString(Bytes::from(
            "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n",
        )))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterNodes {
    nodes: Vec<ClusterInfo>,
}

impl ClusterNodes {
    pub fn new() -> Self {
        ClusterNodes {
            nodes: vec![ClusterInfo::new()],
        }
    }
}

impl Default for ClusterNodes {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ClusterNodes {
    fn name(&self) -> &'static str {
        "CLUSTER NODES"
    }

    async fn execute(&self, _store: &Store) -> Result<RespValue> {
        // Format: <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot ranges>
        Ok(RespValue::SimpleString(format!(
            "{}:0 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383",
            self.nodes[0].node_id
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_info() -> Result<()> {
        let store = Store::new()?;
        let cmd = ClusterInfo::new();
        let result = cmd.execute(&store).await?;

        match result {
            RespValue::BulkString(bytes) => {
                let s = std::str::from_utf8(&bytes)?;
                assert!(s.contains("cluster_state:ok"));
                assert!(s.contains("cluster_size:1"));
            }
            _ => panic!("Unexpected response type"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_nodes() -> Result<()> {
        let store = Store::new()?;
        let cmd = ClusterNodes::new();
        let result = cmd.execute(&store).await?;

        match result {
            RespValue::SimpleString(s) => {
                assert!(s.contains("myself,master"));
                assert!(s.contains("0-16383"));
            }
            _ => panic!("Unexpected response type"),
        }

        Ok(())
    }
}
