use omnipaxos::util::NodeId;
use omnipaxos_kv::common::messages::ClusterMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SchedulingStrategy {
    FCFS,
    // TODO: Add more strategies here
    LIFO
}

// NOTE: Message buffer is already fcfs
pub fn fcfs(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    ()
}

pub fn lifo(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    _msg_buffer.reverse()
}

// TODO: Add more strategies here
