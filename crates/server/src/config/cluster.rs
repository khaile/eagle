use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::{collections::HashMap, net::SocketAddr, time::Duration};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    #[serde(default = "default_node_id")]
    pub node_id: String,

    #[serde(default)]
    pub peers: HashMap<String, SocketAddr>,

    #[serde(default = "default_cluster_port")]
    pub cluster_port: u16,

    #[serde(default = "default_raft_timeout")]
    pub election_timeout: Duration,
}

fn default_node_id() -> String {
    use uuid::Uuid;
    Uuid::new_v4().to_string()
}

fn default_cluster_port() -> u16 {
    16379
}

fn default_raft_timeout() -> Duration {
    Duration::from_secs(1)
}

impl ClusterConfig {
    pub fn load(path: &str) -> Result<Self> {
        let contents = fs::read_to_string(path)?;
        let config = serde_json::from_str(&contents)?;
        Ok(config)
    }
}
