use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
use serde::{Deserialize, Serialize};

use omnipaxos_kv::common::kv::{Command, CommandId, KVCommand, Key, Value};
use omnipaxos_kv::common::utils::Timestamp;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use omnipaxos::messages::{Message, sequence_paxos::PaxosMsg};
use omnipaxos_kv::common::messages::ClusterMessage;


/// Struct to track micro-profiling metrics for post-quorum messages and queue lengths
pub struct PartitionMetrics {
    // Total messages processed per partition
    total_messages: AtomicUsize,
    
    // Post-quorum messages processed per partition
    post_quorum_messages: AtomicUsize,
    
    // Maximum queue length observed for the partition
    max_queue_length: AtomicUsize,
}

impl PartitionMetrics {
    pub fn new() -> Self {
        Self {
            total_messages: AtomicUsize::new(0),
            post_quorum_messages: AtomicUsize::new(0),
            max_queue_length: AtomicUsize::new(0),
        }
    }
    
    /// Record a message being processed
    pub fn record_message(&self, is_post_quorum: bool) {
        self.total_messages.fetch_add(1, Ordering::SeqCst);
        if is_post_quorum {
            self.post_quorum_messages.fetch_add(1, Ordering::SeqCst);
        }
    }
    
    /// Update the maximum queue length if the current length is larger
    pub fn update_max_queue_length(&self, queue_length: usize) {
        let current_max = self.max_queue_length.load(Ordering::SeqCst);
        if queue_length > current_max {
            self.max_queue_length.store(queue_length, Ordering::SeqCst);
        }
    }
    
    /// Get the total number of messages processed
    pub fn total_messages(&self) -> usize {
        self.total_messages.load(Ordering::SeqCst)
    }
    
    /// Get the number of post-quorum messages processed
    pub fn post_quorum_messages(&self) -> usize {
        self.post_quorum_messages.load(Ordering::SeqCst)
    }
    
    /// Get the maximum queue length observed
    pub fn max_queue_length(&self) -> usize {
        self.max_queue_length.load(Ordering::SeqCst)
    }
    
    /// Calculate the percentage of post-quorum messages
    pub fn post_quorum_percentage(&self) -> f64 {
        let total = self.total_messages();
        if total > 0 {
            (self.post_quorum_messages() as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Manager for all partition metrics
pub struct MetricsManager {
    // Metrics for each partition key range
    partition_metrics: HashMap<Key, PartitionMetrics>,
}

impl MetricsManager {
    pub fn new() -> Self {
        Self {
            partition_metrics: HashMap::new(),
        }
    }
    
    /// Register a new partition for metrics tracking
    pub fn register_partition(&mut self, key: Key) {
        self.partition_metrics.entry(key).or_insert_with(PartitionMetrics::new);
    }
    
    /// Get metrics for a specific partition
    pub fn get_partition_metrics(&self, key: &Key) -> Option<&PartitionMetrics> {
        self.partition_metrics.get(key)
    }
    
    /// Record a message being processed for a partition
    pub fn record_message(&self, key: &Key, msg: &ClusterMessage) {
        if let Some(metrics) = self.partition_metrics.get(key) {
            if let ClusterMessage::OmniPaxosMessage((_, omnipaxos_msg)) = msg {
                let is_post_quorum = Self::is_post_quorum_message(omnipaxos_msg);
                metrics.record_message(is_post_quorum);
            }
        }
    }
    
    /// Update the maximum queue length for a partition
    pub fn update_max_queue_length(&self, key: &Key, queue_length: usize) {
        if let Some(metrics) = self.partition_metrics.get(key) {
            metrics.update_max_queue_length(queue_length);
        }
    }
    
    /// Check if a message is post-quorum
    fn is_post_quorum_message(msg: &Message<Command>) -> bool {
        match msg {
            Message::SequencePaxos(paxos_msg) => {
                match &paxos_msg.msg {
                    // Decide messages are post-quorum
                    PaxosMsg::Decide(_) => true,
                    
                    // Accepted messages indicate that a quorum has been reached for this proposal
                    PaxosMsg::Accepted(_) => true,
                    
                    // These messages are usually sent after a quorum of promises has been received
                    PaxosMsg::AcceptSync(_) => true,
                    PaxosMsg::AcceptDecide(_) => true,
                    
                    // All other messages are pre-quorum or unrelated to quorum
                    _ => false,
                }
            },
            // BLE messages are not directly related to quorum
            Message::BLE(_) => false,
        }
    }
    
    /// Get all partition keys being tracked
    pub fn get_all_partition_keys(&self) -> Vec<Key> {
        self.partition_metrics.keys().cloned().collect()
    }
}