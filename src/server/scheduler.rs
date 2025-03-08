use omnipaxos::util::NodeId;
use omnipaxos_kv::common::messages::ClusterMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SchedulingStrategy {
    FCFS,
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
