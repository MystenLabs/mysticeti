use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::Arc,
};

use ::prometheus::default_registry;
use clap::{command, Parser};
use eyre::{eyre, Context, Result};

pub mod block_handler;
mod block_manager;
mod commit_interpreter;
pub mod committee;
mod committer;
pub mod config;
pub mod core;
mod data;
#[cfg(test)]
#[cfg(feature = "simulator")]
mod future_simulator;
pub mod metrics;
pub mod net_sync;
pub mod network;
pub mod prometheus;
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
pub mod types;
#[allow(dead_code)]
mod wal;

mod block_store;
mod stat;

use crate::{
    block_handler::{TestBlockHandler, TestCommitHandler},
    committee::Committee,
    config::{Parameters, Print, PrivateConfig},
    core::Core,
    metrics::Metrics,
    net_sync::NetworkSyncer,
    network::Network,
    types::AuthorityIndex,
};

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
        /// The list of ip addresses of the all validators.
        #[clap(long, value_name = "ADDR", value_delimiter = ' ', num_args(4..))]
        ips: Vec<IpAddr>,

        /// The working directory where the files will be generated.
        #[clap(long, value_name = "FILE", default_value = "genesis")]
        working_directory: PathBuf,
    },
    /// Run a validator node.
    Run {
        /// The authority index of this node.
        #[clap(long, value_name = "INT")]
        authority: AuthorityIndex,

        /// Path to the file holding the public committee information.
        #[clap(long, value_name = "FILE")]
        committee_path: String,

        /// Path to the file holding the public validator parameters (such as network addresses).
        #[clap(long, value_name = "FILE")]
        parameters_path: String,

        /// Path to the file holding the private validator configurations (including keys).
        #[clap(long, value_name = "FILE")]
        private_configs_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Nice colored error messages.
    color_eyre::install()?;

    // Parse the command line arguments.
    match Args::parse().operation {
        Operation::BenchmarkGenesis {
            ips,
            working_directory,
        } => benchmark_genesis(ips, working_directory)?,
        Operation::Run {
            authority,
            committee_path,
            parameters_path,
            private_configs_path,
        } => {
            run(
                authority,
                committee_path,
                parameters_path,
                private_configs_path,
            )
            .await?
        }
    }

    Ok(())
}

/// Generate all the genesis files required for benchmarks.
fn benchmark_genesis(ips: Vec<IpAddr>, working_directory: PathBuf) -> Result<()> {
    tracing::info!("Generating benchmark genesis files");
    fs::create_dir_all(&working_directory).wrap_err(format!(
        "Failed to create directory '{}'",
        working_directory.display()
    ))?;

    let committee_size = ips.len();
    let mut committee_path = working_directory.clone();
    committee_path.push(Committee::DEFAULT_FILENAME);
    Committee::new_for_benchmarks(committee_size)
        .print(&committee_path)
        .wrap_err("Failed to print committee file")?;
    tracing::info!("Generated committee file: {}", committee_path.display());

    let mut parameters_path = working_directory.clone();
    parameters_path.push(Parameters::DEFAULT_FILENAME);
    Parameters::new_for_benchmarks(ips)
        .print(&parameters_path)
        .wrap_err("Failed to print parameters file")?;
    tracing::info!(
        "Generated (public) parameters file: {}",
        parameters_path.display()
    );

    for i in 0..committee_size {
        let mut path = working_directory.clone();
        path.push(PrivateConfig::default_filename(i as AuthorityIndex));
        let parent_directory = path.parent().unwrap();
        fs::create_dir_all(parent_directory).wrap_err(format!(
            "Failed to create directory '{}'",
            parent_directory.display()
        ))?;
        PrivateConfig::new_for_benchmarks(i as AuthorityIndex)
            .print(&path)
            .wrap_err("Failed to print private config file")?;
        tracing::info!("Generated private config file: {}", path.display());
    }

    Ok(())
}

/// Boot a single validator node.
async fn run(
    authority: AuthorityIndex,
    committee_path: String,
    parameters_path: String,
    private_configs_path: String,
) -> Result<()> {
    tracing::info!("Starting validator {authority}");

    let committee = Committee::load(&committee_path)
        .wrap_err(format!("Failed to load committee file '{committee_path}'"))?;
    let parameters = Parameters::load(&parameters_path).wrap_err(format!(
        "Failed to load parameters file '{parameters_path}'"
    ))?;
    let _private = PrivateConfig::load(&private_configs_path).wrap_err(format!(
        "Failed to load private configuration file '{private_configs_path}'"
    ))?;

    let committee = Arc::new(committee);

    let network_address = parameters
        .network_address(authority)
        .ok_or(eyre!("No network address for authority {authority}"))
        .wrap_err("Unknown authority")?;
    let mut binding_network_address = network_address;
    binding_network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    let metrics_address = parameters
        .metrics_address(authority)
        .ok_or(eyre!("No metrics address for authority {authority}"))
        .wrap_err("Unknown authority")?;
    let mut binding_metrics_address = metrics_address;
    binding_metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    // Boot the prometheus server.
    let registry = default_registry();
    let metrics = Arc::new(Metrics::new(registry));
    let _handle = prometheus::start_prometheus_server(binding_metrics_address, registry);

    // Boot the validator node.
    let last_transaction = 0;
    let block_handler = TestBlockHandler::new(last_transaction, committee.clone(), authority);
    let commit_handler =
        TestCommitHandler::new(committee.clone(), block_handler.transaction_time.clone());
    let core = Core::new(block_handler, authority, committee.clone())
        .with_metrics(metrics)
        .with_genesis();
    let network = Network::load(&parameters, authority, binding_network_address).await;
    let network_synchronizer =
        NetworkSyncer::start(network, core, parameters.wave_length(), commit_handler);

    tracing::info!("Validator {authority} listening on {network_address}");
    tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

    // Await the completion of the network synchronizer.
    network_synchronizer.await_completion().await;
    tracing::info!("Validator {authority} shutdown");
    Ok(())
}
