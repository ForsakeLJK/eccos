use omnipaxos::util::NodeId;
use omnipaxos_kv::common::messages::ClusterMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SchedulingStrategy {
    FCFS,
    LIFO,
    RR,
    EARLY,
    FAIR,
}

// NOTE: Message buffer is already fcfs
pub fn fcfs(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    ()
}

pub fn lifo(msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    msg_buffer.reverse()
}

pub fn rr(msg_buffer: &mut Vec<(NodeId, ClusterMessage)>, partition_size: u64) {
    let mut start_signals = Vec::new();
    let mut stop_signals = Vec::new();
    let mut omnipaxos_messages = Vec::new();

    for (node_id, cluster_msg) in msg_buffer.drain(..) {
        match cluster_msg {
            ClusterMessage::LeaderStartSignal(_) => start_signals.push((node_id, cluster_msg)),
            ClusterMessage::LeaderStopSignal => stop_signals.push((node_id, cluster_msg)),
            ClusterMessage::OmniPaxosMessage(_) => omnipaxos_messages.push((node_id, cluster_msg)),
        }
    }

    let mut partition_groups: HashMap<usize, Vec<(NodeId, ClusterMessage)>> = HashMap::new();

    for (node_id, cluster_msg) in omnipaxos_messages {
        if let ClusterMessage::OmniPaxosMessage((key, _)) = &cluster_msg {
            let partition_id = key / partition_size as usize;
            partition_groups
                .entry(partition_id)
                .or_default()
                .push((node_id, cluster_msg));
        }
    }

    let mut partition_keys: Vec<_> = partition_groups.keys().cloned().collect();
    partition_keys.sort();

    let mut result = Vec::new();

    result.extend(start_signals);

    let mut has_more = true;
    let mut round = 0;

    while has_more {
        has_more = false;

        for &partition_id in &partition_keys {
            if let Some(messages) = partition_groups.get_mut(&partition_id) {
                if round < messages.len() {
                    result.push(messages[round].clone());
                    has_more = true;
                }
            }
        }

        round += 1;
    }

    result.extend(stop_signals);

    *msg_buffer = result;
}

pub fn early(
    msg_buffer: &Vec<(NodeId, ClusterMessage)>,
    partition_size: u64,
    num_threads: usize,
) -> HashMap<usize, usize> {
    let mut result = HashMap::new();

    // Keep track of which thread handles which partition_id
    let mut partition_to_thread = HashMap::new();

    for (idx, (_node_id, message)) in msg_buffer.iter().enumerate() {
        if let ClusterMessage::OmniPaxosMessage((key, _)) = message {
            // Calculate partition_id
            let partition_id = *key / partition_size as usize;

            // If this partition hasn't been assigned to a thread yet, assign it
            let thread_id = *partition_to_thread
                .entry(partition_id)
                .or_insert_with(|| partition_id % num_threads);

            // Add this message's idx and thread assignment to the result
            result.insert(idx, thread_id);
        }
        // Skip LeaderStopSignal and LeaderStartSignal as specified
    }

    result
}

pub fn fair(
    msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
    waiting_map: &mut HashMap<usize, Vec<ClusterMessage>>,
    partition_size: u64,
) {
    let mut leader_start: Option<(NodeId, ClusterMessage)> = None;
    let mut leader_stop: Option<(NodeId, ClusterMessage)> = None;

    // Step 1: Drain msg_buffer and categorize messages
    let mut temp_buffer: Vec<(NodeId, ClusterMessage)> = Vec::new();

    while let Some((node_id, msg)) = msg_buffer.pop() {
        match msg {
            ClusterMessage::LeaderStartSignal(_) => leader_start = Some((node_id, msg)),
            ClusterMessage::LeaderStopSignal => leader_stop = Some((node_id, msg)),
            ClusterMessage::OmniPaxosMessage((key, _)) => {
                let partition_id = key / partition_size as usize;
                waiting_map.entry(partition_id).or_default().push(msg);
            }
        }
    }

    // Step 2: Poll waiting_map in round-robin order
    if !waiting_map.is_empty() {
        let least_len = waiting_map.values().map(Vec::len).min().unwrap_or(0);
        let mut keys: Vec<_> = waiting_map.keys().cloned().collect();
        keys.sort(); // Ensure a deterministic order

        for _ in 0..least_len {
            for &key in &keys {
                if let Some(vec) = waiting_map.get_mut(&key) {
                    if let Some(msg) = vec.drain(..1).next() {
                        temp_buffer.push((0, msg)); // NodeId 0 as placeholder
                    }
                }
            }
        }
    }

    // Step 3: Reconstruct msg_buffer with correct order
    if let Some(start_msg) = leader_start.take() {
        msg_buffer.push(start_msg);
    }
    msg_buffer.append(&mut temp_buffer);
    if let Some(stop_msg) = leader_stop.take() {
        msg_buffer.push(stop_msg);
    }
}
