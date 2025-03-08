use omnipaxos::util::NodeId;
use omnipaxos_kv::common::messages::ClusterMessage;
use omnipaxos::messages::Message;
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
    fn get_timestamp_from_message(msg: &ClusterMessage) -> Timestamp {
            // For OmniPaxosMessages that contain SequencePaxos or BLE
            ClusterMessage::OmniPaxosMessage((_, inner_msg)) => {
                match inner_msg {
                    // Handling the SequencePaxos messages
                    Message::SequencePaxos(paxos_msg) => match &paxos_msg.msg {
                        PaxosMsg::Prepare(prepare) => prepare.n.n as Timestamp,
                        PaxosMsg::Promise(promise) => promise.n.n as Timestamp,
                        PaxosMsg::AcceptSync(accept_sync) => accept_sync.n.n as Timestamp,
                        PaxosMsg::AcceptDecide(accept_decide) => accept_decide.n.n as Timestamp,
                        PaxosMsg::Accepted(accepted) => accepted.n.n as Timestamp,
                        PaxosMsg::NotAccepted(not_accepted) => not_accepted.n.n as Timestamp,
                        PaxosMsg::Decide(decide) => decide.n.n as Timestamp,
                        PaxosMsg::AcceptStopSign(accept_stop_sign) => accept_stop_sign.n.n as Timestamp,
                        // Messages with ballot in PrepareReq
                        PaxosMsg::PrepareReq(prepare_req) => prepare_req.n.n as Timestamp,
                        _ => 0, // Default timestamp for unhandled Paxos message types
                    },
                    // Handling the BLE (Ballot Leader Election) messages
                    Message::BLE(ble_msg) => {
                        let ble_offset = 1000; // Adding an offset to BLE messages
                        match &ble_msg.msg {
                            HeartbeatMsg::Request(req) => ble_offset + (req.round as Timestamp),
                            HeartbeatMsg::Reply(reply) => ble_offset + (reply.round as Timestamp),
                        }
                    }
                }
            }
    }
    

    // Step 1: Sort messages first by timestamp (descending order)
    let mut indexed_messages: Vec<_> = msg_buffer
        .iter()
        .enumerate()
        .collect(); // Collect (index, message) pairs

    indexed_messages.sort_by(|a, b| {
        let timestamp_a = get_timestamp_from_message(&a.1);
        let timestamp_b = get_timestamp_from_message(&b.1);

        // Sort in descending order by timestamp (latest first)
        timestamp_b.cmp(&timestamp_a)
            // If timestamps are the same, preserve original order (FIFO effect)
            .then_with(|| a.0.cmp(&b.0))
    });

    msg_buffer.clear();
    // Collect the mapped result into a Vec by dereferencing the references
    msg_buffer.extend(indexed_messages.into_iter().map(|(_, msg)| (*msg).clone()));
}


