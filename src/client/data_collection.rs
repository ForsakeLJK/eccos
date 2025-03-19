use std::{fs::File, io, io::Write};
use std::fs::OpenOptions;
use chrono::Utc;
use csv::Writer;
use omnipaxos_kv::common::{kv::CommandId, utils::Timestamp};
use serde::Serialize;

use crate::client::ClientConfig;

#[derive(Debug, Serialize, Clone, Copy)]
struct RequestData {
    request_time: Timestamp,
    write: bool,
    response_time: Option<Timestamp>,
}

pub struct ClientData {
    request_data: Vec<RequestData>,
}

impl ClientData {
    pub fn new() -> Self {
        ClientData {
            request_data: Vec::new(),
        }
    }

    pub fn new_request(&mut self, is_write: bool) {
        let data = RequestData {
            request_time: Utc::now().timestamp_millis(),
            write: is_write,
            response_time: None,
        };
        self.request_data.push(data);
    }

    pub fn new_response(&mut self, command_id: CommandId) {
        let response_time = Utc::now().timestamp_millis();
        self.request_data[command_id].response_time = Some(response_time);
    }

    pub fn response_count(&self) -> usize {
        self.request_data
            .iter()
            .filter(|data| data.response_time.is_some())
            .count()
    }

    pub fn request_count(&self) -> usize {
        self.request_data.len()
    }

    pub fn avg_latency_ms(&self) -> f64 {
        let completed_requests: Vec<&RequestData> = self
            .request_data
            .iter()
            .filter(|x| x.response_time.is_some())
            .collect();

        let summed_latency = completed_requests
            .iter()
            .map(|x| {
                (
                    x.request_time,
                    x.response_time.expect("None is filtered out"),
                )
            })
            .fold(0, |acc, e| acc + (e.1 - e.0));

        summed_latency as f64 / completed_requests.len() as f64
    }

    pub fn save_summary(&self, config: ClientConfig) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&config)?;
        let mut summary_file = File::create(config.summary_filepath)?;
        summary_file.write_all(config_json.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    pub fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for data in &self.request_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }

    pub fn save_throughput(&self, total_req_cnt: usize) -> Result<(), io::Error> {
        let path = "../benchmarks/logs/local-run/client-throughput.csv";

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        writeln!(
            file,
            "{},{},{}",
            self.response_count(),
            self.avg_latency_ms(),
            total_req_cnt
        )?;

        Ok(())
    }
}
