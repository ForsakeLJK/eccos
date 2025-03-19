use omnipaxos::util::NodeId;
use omnipaxos_kv::common::messages::ClusterMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use std::fs;
use toml;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SchedulingStrategy {
    FCFS,
    LIFO,
    RR,
    WRR,
    EARLY,
    WMRR,
    WMWRR
}

// NOTE: Message buffer is already fcfs
pub fn fcfs(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    ()
}

// LIFO
pub fn lifo(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    _msg_buffer.reverse()
}

// WRR help functions
fn load_client_weights() -> Vec<usize> {
    let config_path = "../build_scripts/client-1-config.toml";
    let config_string = match fs::read_to_string(config_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read client config: {}", e);
            return vec![];
        }
    };
    let config: toml::Value = match toml::from_str(&config_string) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("Failed to parse client config: {}", e);
            return vec![];
        }
    };

    let mut result = Vec::new();

    if let Some(request_config) = config.get("request_config") {
        if let Some(skew) = request_config.get("skew") {
            if let Some(skew_type) = skew.get("type").and_then(|v| v.as_str()) {
                if skew_type == "Weighted" {
                    if let Some(weights) = skew.get("weights").and_then(|v| v.as_array()) {
                        let weight_values: Vec<usize> = weights
                            .iter()
                            .filter_map(|w| w.as_integer().map(|i| i as usize))
                            .collect();
                        if !weight_values.is_empty() {
                            result = weight_values;
                        }
                    }
                }
            }
        }
    }
    result
}

lazy_static! {
    static ref CURRENT_WRR_PARTITION: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    static ref CURRENT_WRR_WEIGHT_COUNTER: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
}

fn extract_partition_key(msg: &ClusterMessage) -> Option<omnipaxos_kv::common::kv::Key> {
    match msg {
        ClusterMessage::OmniPaxosMessage((key, _)) => Some(*key),
        _ => None,
    }
}

pub fn rr_basic(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>, weights: HashMap<u64, usize>, default_weight: usize) {
    if _msg_buffer.is_empty() {
        return;
    }

    let mut partition_messages: HashMap<u64, Vec<(NodeId, ClusterMessage)>> = HashMap::new();
    let mut control_messages = Vec::new();
    for (node_id, msg) in _msg_buffer.drain(..) {
        if let Some(key) = extract_partition_key(&msg) {
            let key_u64: u64 = key as u64;
            partition_messages
                .entry(key_u64)
                .or_insert(Vec::new())
                .push((node_id, msg));
        } else {
            control_messages.push((node_id, msg));
        }
    }

    let mut partition_keys: Vec<u64> = partition_messages.keys().cloned().collect();
    partition_keys.sort();
    if partition_keys.is_empty() {
        *_msg_buffer = control_messages;
        return;
    }

    let mut new_buffer = Vec::new();
    new_buffer.extend(control_messages);

    let mut current_partition_idx = CURRENT_WRR_PARTITION.lock().unwrap();
    let mut weight_counter = CURRENT_WRR_WEIGHT_COUNTER.lock().unwrap();
    if *current_partition_idx >= partition_keys.len() {
        *current_partition_idx = 0;
        *weight_counter = 0;
    }
    let mut processed_partitions = vec![false; partition_keys.len()];
    let mut all_processed = false;
    let max_iterations = 1000;
    let mut iteration_count = 0;

    while !all_processed && iteration_count < max_iterations {
        iteration_count += 1;
        if *current_partition_idx >= partition_keys.len() {
            *current_partition_idx = 0;
        }
        let partition_id = partition_keys[*current_partition_idx];
        let partition_weight = *weights.get(&partition_id).unwrap_or(&default_weight);
        // println!("partition_id: {:?} weights: {:?} partition_weight: {:?}", partition_id, weights, partition_weight);
        if let Some(messages) = partition_messages.get_mut(&partition_id) {
            if !messages.is_empty() && *weight_counter < partition_weight {
                if let Some(msg) = messages.pop() {
                    new_buffer.push(msg);
                }
                *weight_counter += 1;
                if *weight_counter >= partition_weight {
                    *weight_counter = 0;
                    *current_partition_idx = (*current_partition_idx + 1) % partition_keys.len();
                }
            } else {
                *weight_counter = 0;
                *current_partition_idx = (*current_partition_idx + 1) % partition_keys.len();
                if messages.is_empty() {
                    if *current_partition_idx < processed_partitions.len() {
                        processed_partitions[*current_partition_idx] = true;
                    }
                }
            }
        } else {
            *weight_counter = 0;
            *current_partition_idx = (*current_partition_idx + 1) % partition_keys.len();
            if *current_partition_idx < processed_partitions.len() {
                processed_partitions[*current_partition_idx] = true;
            }
        }
        all_processed = processed_partitions.iter().all(|&processed| processed);
        if partition_messages.values().all(|msgs| msgs.is_empty()) {
            break;
        }
    }
    for messages in partition_messages.values() {
        for msg in messages {
            new_buffer.push(msg.clone());
        }
    }

    *_msg_buffer = new_buffer;
}

// Round Robin
pub fn rr(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    let uniform_weights: HashMap<u64, usize> = HashMap::new();
    // Special WRR -> weight is equal
    rr_basic(_msg_buffer, uniform_weights, 1);
}

// Weighted Round Robin
pub fn wrr(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    const DEFAULT_WEIGHT: usize = 1;
    let weights = load_client_weights();
    let weights_map: HashMap<u64, usize> = weights.iter()
        .enumerate()
        .map(|(idx, &weight)| ((idx * 500) as u64, weight))
        .collect();
    rr_basic(_msg_buffer, weights_map, DEFAULT_WEIGHT);
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

Waiting-Map Weighted Round Robin
pub fn wmwrr(
    msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
    waiting_map: &mut HashMap<usize, Vec<ClusterMessage>>,
    partition_size: u64,
    prioritized_partition_id: usize,
) {
    let mut leader_start: Option<(NodeId, ClusterMessage)> = None;
    let mut leader_stop: Option<(NodeId, ClusterMessage)> = None;

    // Step 1: Drain msg_buffer and categorize messages
    let mut temp_buffer: Vec<(NodeId, ClusterMessage)> = Vec::new();
    let mut non_priority_messages: Vec<(NodeId, ClusterMessage)> = Vec::new();

    while let Some((node_id, msg)) = msg_buffer.pop() {
        match msg {
            ClusterMessage::LeaderStartSignal(_) => leader_start = Some((node_id, msg)),
            ClusterMessage::LeaderStopSignal => leader_stop = Some((node_id, msg)),
            ClusterMessage::OmniPaxosMessage((key, _)) => {
                let partition_id = key / partition_size as usize;

                if partition_id == prioritized_partition_id {
                    temp_buffer.push((node_id, msg));
                } else {
                    non_priority_messages.push((node_id, msg));
                }
            }
        }
    }

    // Step 2: Dealing with non priority messages
    let mut processed_count = 0;
    for (node_id, msg) in non_priority_messages {
        if processed_count < 3 {
            temp_buffer.push((node_id, msg));
            processed_count += 1;
        } else {
            let partition_id = match msg {
                ClusterMessage::OmniPaxosMessage((key, _)) => key / partition_size as usize,
                _ => unreachable!(),
            };
            waiting_map.entry(partition_id).or_default().push(msg);
        }
    }

    // Step 3: Poll waiting_map in round-robin order
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

    // Step 4: Reconstruct msg_buffer with correct order
    if let Some(start_msg) = leader_start.take() {
        msg_buffer.push(start_msg);
    }
    msg_buffer.append(&mut temp_buffer);
    if let Some(stop_msg) = leader_stop.take() {
        msg_buffer.push(stop_msg);
    }
}