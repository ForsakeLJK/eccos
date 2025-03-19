#!/bin/bash

client1_id=1
rust_log="info"

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Clients' output is saved into bencharking directory
local_experiment_dir="../benchmarks/logs/local-run"
mkdir -p "${local_experiment_dir}"

# Run clients
client1_config_path="./client-${client1_id}-config.toml"
RUST_LOG=$rust_log CONFIG_FILE="$client1_config_path"  cargo run --release --manifest-path="../Cargo.toml" --bin client &
#wait