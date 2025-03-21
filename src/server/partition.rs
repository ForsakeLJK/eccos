use crate::data_collection::DataCollection;
use log::{debug, info};
use omnipaxos::{
    ballot_leader_election::Ballot, messages::Message, storage::Storage, util::LogEntry, OmniPaxos,
    OmniPaxosConfig,
};

use omnipaxos_kv::common::{kv::*, messages::*};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::sync::atomic::{AtomicUsize, Ordering};
use omnipaxos::messages::sequence_paxos::PaxosMsg;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct Partition {
    server_id: NodeId,
    key_range: KeyRange,
    omnipaxos: OmniPaxosInstance,
    msg_buffer: Vec<Message<Command>>,
    current_decided_idx: usize,
    initial_leader: NodeId,
    data_collection: DataCollection,
    
    // Metrics for profiling
    total_messages: AtomicUsize,
    post_quorum_messages: AtomicUsize,
    max_queue_length: AtomicUsize,
}

impl Partition {
    pub fn new(
        key_range: KeyRange,
        initial_leader: NodeId,
        op_config: OmniPaxosConfig,
        server_id: NodeId,
    ) -> Self {
        // Initialize OmniPaxos instance
        let mut storage: MemoryStorage<Command> = MemoryStorage::default();
        let init_leader_ballot = Ballot {
            config_id: 0,
            n: 1,
            priority: 0,
            pid: initial_leader,
        };
        storage
            .set_promise(init_leader_ballot)
            .expect("Failed to write to storage");
        let omnipaxos = op_config.build(storage).unwrap();
        let msg_buffer = vec![];
        let current_decided_idx = 0;
        let initial_leader = initial_leader;
        let data_collection = DataCollection::new();
        
        // Initialize metrics
        let total_messages = AtomicUsize::new(0);
        let post_quorum_messages = AtomicUsize::new(0);
        let max_queue_length = AtomicUsize::new(0);
        
        Partition {
            server_id,
            key_range,
            omnipaxos,
            msg_buffer,
            current_decided_idx,
            initial_leader,
            data_collection,
            total_messages,
            post_quorum_messages,
            max_queue_length,
        }
    }

    pub fn initial_leader(&self) -> NodeId {
        self.initial_leader
    }

    pub fn tick(&mut self) {
        self.omnipaxos.tick();
    }

    pub fn get_outgoing_msgs(&mut self) -> Vec<(NodeId, ClusterMessage)> {
        self.omnipaxos.take_outgoing_messages(&mut self.msg_buffer);
        let key = self.key_range().start_key().clone();
        let mut outgoing_msgs = vec![];

        for msg in self.msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage((key.clone(), msg));
            outgoing_msgs.push((to, cluster_msg));
        }

        outgoing_msgs
    }

    pub fn is_init_leader_elected(&self) -> bool {
        if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader() {
            if curr_leader == self.server_id && is_accept_phase {
                info!("{}: Leader fully initialized", self.server_id);
                return true;
            }
        }
        return false;
    }

    pub fn leader_takeover(&mut self) {
        info!(
            "{} {}-{}: Attempting to take leadership",
            self.server_id,
            self.key_range.start_key(),
            self.key_range.end_key()
        );
        self.omnipaxos.try_become_leader();
    }

    pub fn get_decided_cmds(&mut self) -> Vec<Command> {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();

        if self.current_decided_idx >= new_decided_idx {
            return vec![];
        }

        let decided_entries = self
            .omnipaxos
            .read_decided_suffix(self.current_decided_idx)
            .unwrap();
        let decided_commands: Vec<Command> = decided_entries
            .into_iter()
            .filter_map(|e| match e {
                LogEntry::Decided(cmd) => {
                    self.data_collection.commit_entry();
                    Some(cmd)
                }
                _ => unreachable!(),
            })
            .collect();

        self.current_decided_idx = new_decided_idx;
        debug!("Decided {new_decided_idx}");

        decided_commands
    }

    pub fn append_to_log(&mut self, cmd: Command) {
        self.omnipaxos
            .append(cmd)
            .expect("Append to Omnipaxos log failed");
    }

    pub fn key_range(&self) -> &KeyRange {
        &self.key_range
    }

    pub fn handle_incoming(&mut self, msg: Message<Command>) {
        // Record metrics before handling the message
        self.record_message(&msg);
        self.omnipaxos.handle_incoming(msg);
    }

    pub fn count_committed_entries(&self) -> usize {
        self.data_collection.count_committed_entries()
    }
    
    
    
    /// Record a message being processed
    fn record_message(&self, msg: &Message<Command>) {
        self.total_messages.fetch_add(1, Ordering::SeqCst);
        if self.is_post_quorum_message(msg) {
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
    
    /// Check if a message is post-quorum
    fn is_post_quorum_message(&self, msg: &Message<Command>) -> bool {
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