use crate::{data_collection::ClientData, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use rand::{
    distributions::Distribution, distributions::WeightedIndex, rngs::SmallRng, Rng, SeedableRng,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration, Instant},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: NodeId,
    pub request_config: RequestConfig,
    pub local_deployment: Option<bool>,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
}

const NETWORK_BATCH_SIZE: usize = 1000;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let local_deployment = config.local_deployment.unwrap_or(false);
        let network = Network::new(
            config.cluster_name.clone(),
            vec![config.server_id],
            local_deployment,
            NETWORK_BATCH_SIZE,
        )
        .await;
        Client {
            id: config.server_id,
            network,
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            final_request_count: None,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                let now = Utc::now();
                let time_until_scheduled_start_ms = start_time - now.timestamp_millis();
                if time_until_scheduled_start_ms > 0 {
                    sleep(Duration::from_millis(time_until_scheduled_start_ms as u64)).await;
                } else {
                    warn!("Started after synchronization point!");
                }
            }
            _ => panic!("Error waiting for start signal"),
        };
        info!("{}: Start sending requests", self.id);

        let mut rng = SmallRng::seed_from_u64(self.id);
        let request_config = self.config.request_config;

        let (tx_wakeup, mut rx_wakeup) = mpsc::channel(1);
        let (tx_end, mut rx_end) = mpsc::channel(1);

        let experiment_start_time = Instant::now();
        let _wake_up_task = tokio::spawn({
            let mut rng = rng.clone();
            let request_config = request_config.clone();
            async move {
                loop {
                    let wakeup_duration = request_config.compute_inter_arrival_time(&mut rng);
                    sleep(wakeup_duration).await;
                    tx_wakeup.send(true).await.unwrap_or(());
                    if experiment_start_time.elapsed() > request_config.get_experiment_duration() {
                        tx_end.send(true).await.unwrap_or(());
                    }
                }
            }
        });

        // Main event loop
        info!("{}: Starting requests", self.id);
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                },
                _ = rx_wakeup.recv() => {
                    let is_write = rng.gen::<f64>() > request_config.read_ratio;
                    let (key, val) = request_config.generate_kv_request(&mut rng);
                    self.send_request(key, val, is_write).await;
                },
                _ = rx_end.recv() => {
                    break;
                }
            }
        }

        info!(
            "{}: Client finished: collected {} responses with avg latency of {}ms. sent {} requests",
            self.id,
            self.client_data.response_count(),
            self.client_data.avg_latency_ms(),
            self.next_request_id
        );
        self.network
            .send(self.active_server, ClientMessage::EndExperiment)
            .await;
        self.network.shutdown().await;
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        debug!("Received {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let cmd_id = server_response.command_id();
                self.client_data.new_response(cmd_id);
            }
        }
    }

    async fn send_request(&mut self, key: Key, val: Value, is_write: bool) {
        let cmd = match is_write {
            true => KVCommand::Put(key, val),
            false => KVCommand::Get(key),
        };
        let request = ClientMessage::Append(self.next_request_id, cmd);
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        return false;
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data.save_summary(self.config.clone())?;
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        self.client_data.save_throughput(self.next_request_id)?;
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone, Serialize, Copy)]
#[serde(tag = "type", content = "weights")]
enum SkewType {
    Uniform,
    Weighted([i32; 3]),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestConfig {
    duration_sec: u64,
    requests_per_sec: f64,
    read_ratio: f64,
    key_range: [Key; 2],
    skew: SkewType,
}

impl RequestConfig {
    fn get_experiment_duration(self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    // INFO: taken from https://stackoverflow.com/questions/5148635/how-to-simulate-poisson-arrival
    fn compute_inter_arrival_time(&self, rng: &mut SmallRng) -> Duration {
        let uniform_random: f64 = rng.gen(); // Random number between 0 and 1

        // Exponential distribution formula: -ln(1 - U) / λ
        let inter_arrival_time = -(((1.0 - uniform_random) as f64).ln()) / self.requests_per_sec;
        Duration::from_secs_f64(inter_arrival_time)
    }

    fn generate_kv_request(&self, rng: &mut SmallRng) -> (Key, Value) {
        let val = rng.gen_range(0..=10);
        let key = match &self.skew {
            // 0, partition_size * partition_cnt

            SkewType::Uniform => rng.gen_range(self.key_range[0]..=self.key_range[1]),
            SkewType::Weighted(weights) => {
                let dist = WeightedIndex::new(weights).unwrap();
                dist.sample(rng) * 1000 + 3
            }
        };
        // println!("{}", key);
        (key, val)
    }
}
