use crate::scheduler::SchedulingStrategy;
use omnipaxos::{
    util::{FlexibleQuorum, NodeId},
    ClusterConfig, OmniPaxosConfig, ServerConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosServerConfig {
    pub location: String,
    pub server_id: NodeId,
    pub num_clients: usize,
    pub summary_filepath: String,
    pub output_filepath: String,
    pub in_scheduling_strategy: SchedulingStrategy,
    pub out_scheduling_strategy: SchedulingStrategy,
    pub worker_num : Option<usize>,
    // Cluster-wide settings
    pub local_deployment: Option<bool>,
    pub cluster_name: String,
    pub nodes: Vec<NodeId>,
    pub initial_leader: u64,
    pub initial_flexible_quorum: Option<FlexibleQuorum>,
    pub num_partitions: u64,
    pub partition_size: u64,
}

impl Into<OmniPaxosConfig> for OmniPaxosServerConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: self.nodes,
            flexible_quorum: self.initial_flexible_quorum,
        };
        let server_config = ServerConfig {
            pid: self.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl OmniPaxosServerConfig {
    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
    }
}
