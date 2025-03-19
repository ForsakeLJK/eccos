use omnipaxos::util::NodeId;
use omnipaxos_kv::common::messages::ClusterMessage;
use omnipaxos::messages::Message;
use omnipaxos_kv::common::kv::Command;
use omnipaxos::messages::sequence_paxos::PaxosMsg;
use omnipaxos::messages::ballot_leader_election::HeartbeatMsg;
use serde::{Deserialize, Serialize};
use omnipaxos_kv::common::utils::Timestamp;
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SchedulingStrategy {
    FCFS,
    LIFO,
    MP,
    HB_LF_FF,
    TS
    // TODO: Add more strategies here
}

// NOTE: Message buffer is already fcfs
pub fn fcfs(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    ()
}

// TODO: Add more strategies here
pub fn lifo(msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    msg_buffer.reverse();
}

pub fn mp(msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    fn message_prioritization(messages: &mut Vec<(NodeId, ClusterMessage)>) {
        messages.sort_by(|a, b| {
            let priority_a = get_message_type_priority(&a.1);
            let priority_b = get_message_type_priority(&b.1);
            priority_a.cmp(&priority_b)
        });
    }

    fn get_message_type_priority(msg: &ClusterMessage) -> i32 {
        match msg {
            ClusterMessage::OmniPaxosMessage((_, inner_msg)) => {
                match inner_msg {
                    // Handling the SequencePaxos (log replication) message
                    Message::SequencePaxos(paxos_msg) => match &paxos_msg.msg {
                        PaxosMsg::Accepted(_) => 10,  // Highest priority for Accept request
                        PaxosMsg::PrepareReq(_) => 8,  // "PrepareReq" is the equivalent of "Promise"
                        PaxosMsg::Prepare(_) => 7,  // You might need to adjust this based on your exact types
                        PaxosMsg::Decide(_) => 6,   // Decide message (typically lower priority)
                        _ => 5,  // Default priority for other Paxos message types
                    },
    
                    // Handling the BLE (Ballot Leader Election) message
                    Message::BLE(ble_msg) => match &ble_msg.msg {
                        HeartbeatMsg::Request(_) => 2,  // Lower priority for heartbeat request
                        HeartbeatMsg::Reply(_) => 3,    // Higher priority for heartbeat reply
                    },
    
                    _ => 1,  // Default case for unknown message types
                }
            },
    
            _ => 1,  // Default priority for other types of cluster messages (if any)
        }
    }
    
    


    // Now call the message prioritization function to sort the messages in buffer
    message_prioritization(msg_buffer);
}

pub fn hybrid_lifo_fifo(msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    // Take ownership of the buffer contents for processing
    let buffer_contents = std::mem::take(msg_buffer);
    
    // Separate messages based on priority
    let (high_priority_msgs, low_priority_msgs): (Vec<_>, Vec<_>) = buffer_contents
        .into_iter() // Use into_iter() to consume the vector
        .partition(|(_, msg)| match msg {
            ClusterMessage::OmniPaxosMessage((_, inner_msg)) => {
                match inner_msg {
                    Message::SequencePaxos(_) => true, // Assume SequencePaxos messages are high priority
                    _ => false, // Otherwise, treat as low priority
                }
            },
            _ => false, // Default to low priority
        });
    
    // Process high-priority messages in reverse order (LIFO)
    let mut high_priority_msgs = high_priority_msgs;
    high_priority_msgs.reverse();
    
    // Merge high priority (LIFO) and low priority (FIFO)
    let mut merged_msgs = Vec::new();
    merged_msgs.extend(high_priority_msgs);
    merged_msgs.extend(low_priority_msgs);
    
    // Now update the original buffer
    *msg_buffer = merged_msgs;
}


pub fn ts(msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    // Utility function to get the timestamp from a message
    fn get_timestamp_from_omnipaxos_message(msg: &Message<Command>) -> u64 {
        match msg {
            // For SequencePaxos messages
            Message::SequencePaxos(paxos_msg) => {
                match &paxos_msg.msg {
                    // For messages with a ballot, use the ballot number
                    PaxosMsg::Prepare(prepare) => prepare.n.n as u64,
                    PaxosMsg::Promise(promise) => promise.n.n as u64,
                    PaxosMsg::AcceptSync(accept_sync) => accept_sync.n.n as u64,
                    PaxosMsg::AcceptDecide(accept_decide) => accept_decide.n.n as u64,
                    PaxosMsg::Accepted(accepted) => accepted.n.n as u64,
                    PaxosMsg::NotAccepted(not_accepted) => not_accepted.n.n as u64,
                    PaxosMsg::Decide(decide) => decide.n.n as u64,
                    PaxosMsg::AcceptStopSign(accept_stop_sign) => accept_stop_sign.n.n as u64,
                    // Messages with ballot in PrepareReq
                    PaxosMsg::PrepareReq(prepare_req) => prepare_req.n.n as u64,
                    // For messages without ballot numbers, use a priority offset
                    PaxosMsg::ProposalForward(_) => 0, // Lowest priority
                    PaxosMsg::Compaction(_) => 1, // Low priority
                    PaxosMsg::ForwardStopSign(_) => 2, // Medium-low priority
                }
            },
            // For BLE messages, add a large offset to the round number
            Message::BLE(ble_msg) => {
                // Use 1000 as an offset to separate from SequencePaxos messages
                let ble_offset = 1000;
                match &ble_msg.msg {
                    HeartbeatMsg::Request(req) => ble_offset + (req.round as u64),
                    HeartbeatMsg::Reply(reply) => ble_offset + (reply.round as u64)
                }
            }
        }
    }

    // Helper function to extract timestamp based on ClusterMessage type
    fn get_cluster_message_timestamp(msg: &ClusterMessage) -> u64 {
        match msg {
            ClusterMessage::OmniPaxosMessage(payload) => {
                let (_, omnipaxos_msg) = payload;
                get_timestamp_from_omnipaxos_message(omnipaxos_msg)
            },
            ClusterMessage::LeaderStartSignal(timestamp) => *timestamp as u64,
            ClusterMessage::LeaderStopSignal => 0, // Assign lowest priority
        }
    }

    // Create a temporary vector to hold the sorted messages to avoid borrow issues
    let mut sorted_messages = Vec::with_capacity(msg_buffer.len());
    
    // Step 1: Create a vector of indices sorted by timestamp
    let mut indices: Vec<usize> = (0..msg_buffer.len()).collect();
    
    indices.sort_by(|&a, &b| {
        let timestamp_a = get_cluster_message_timestamp(&msg_buffer[a].1);
        let timestamp_b = get_cluster_message_timestamp(&msg_buffer[b].1);
        
        // Sort descending by timestamp (higher timestamps first)
        timestamp_b.cmp(&timestamp_a)
    });
    
    // Step 2: Create a new vector with messages in the sorted order
    for &idx in &indices {
        sorted_messages.push(msg_buffer[idx].clone());
    }
    
    // Step 3: Replace the original buffer with the sorted messages
    msg_buffer.clear();
    msg_buffer.append(&mut sorted_messages);
}