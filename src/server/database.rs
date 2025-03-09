use omnipaxos_kv::common::kv::{KVCommand, Key, Value};
use std::collections::HashMap;
use std::thread::{sleep};
use std::time::Duration;

pub struct Database {
    db: HashMap<Key, Value>,
}

impl Database {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    pub fn handle_command(&mut self, command: KVCommand) -> Option<Option<Value>> {
        match command {
            KVCommand::Put(key, value) => {
                sleep(Duration::from_millis(10));
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                sleep(Duration::from_millis(10));
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => {
                sleep(Duration::from_millis(3));
                Some(self.db.get(&key).map(|v| v.clone()))
            },
        }
    }
}
