use std::net::SocketAddr;

use clap::{command, Parser};

mod block_handler;
mod block_manager;
pub mod commit_interpreter;
mod committee;
pub mod committer;
mod config;
mod core;
mod data;
#[cfg(test)]
#[cfg(feature = "simulator")]
mod future_simulator;
mod net_sync;
mod network;
mod prometheus;
mod runtime;
#[cfg(test)]
#[cfg(feature = "simulator")]
mod simulated_network;
#[cfg(test)]
mod simulator;
mod syncer;
#[cfg(test)]
mod test_util;
mod threshold_clock;
mod types;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    operation: Operation,
}

#[derive(Parser)]
enum Operation {
    /// Generate a committee file, parameters files and the private config files of all validators
    /// from a list of initial peers. This is only suitable for benchmarks as it exposes all keys.
    BenchmarkGenesis {
        #[clap(long, value_name = "[ADDR]", value_delimiter = ',', num_args(4..))]
        ips: Vec<SocketAddr>,
    },
    /// Run a validator node.
    Run {
        /// Path to the file holding the public committee information.
        #[clap(long, value_name = "FILE", global = true)]
        committee_path: String,

        /// Path to the file holding the public validator parameters (such as network addresses).
        #[clap(long, value_name = "FILE", global = true)]
        parameters_path: String,

        /// Path to the file holding the private validator configurations (including keys).
        #[clap(long, value_name = "FILE", global = true)]
        config_path: String,
    },
}

fn main() {
    println!("Hello, world!");
}
