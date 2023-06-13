use std::{net::IpAddr, path::PathBuf, sync::Arc};

use ::prometheus::default_registry;
use clap::{command, Parser};
use eyre::{eyre, Context, Result};

use mysticeti::{
    block_handler::{TestBlockHandler, TestCommitHandler},
    committee::Committee,
    config::{Parameters, Print, PrivateConfig},
    core::Core,
    metrics::Metrics,
    net_sync::NetworkSyncer,
    network::Network,
    prometheus,
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
        #[clap(long, value_name = "[ADDR]", value_delimiter = ',', num_args(4..))]
        ips: Vec<IpAddr>,
    },
    /// Run a validator node.
    Run {
        /// The authority index of this node.
        #[clap(long, value_name = "INT", global = true)]
        authority: AuthorityIndex,

        /// Path to the file holding the public committee information.
        #[clap(long, value_name = "FILE", global = true)]
        committee_path: String,

        /// Path to the file holding the public validator parameters (such as network addresses).
        #[clap(long, value_name = "FILE", global = true)]
        parameters_path: String,

        /// Path to the file holding the private validator configurations (including keys).
        #[clap(long, value_name = "FILE", global = true)]
        private_configs_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Nice colored error messages.
    color_eyre::install()?;

    // Parse the command line arguments.
    match Args::parse().operation {
        Operation::BenchmarkGenesis { ips } => benchmark_genesis(ips)?,
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
fn benchmark_genesis(ips: Vec<IpAddr>) -> Result<()> {
    tracing::info!("Generating benchmark genesis files");

    let committee_size = ips.len();
    let committee_path = Committee::DEFAULT_FILENAME;
    Committee::new_for_benchmarks(committee_size).print(committee_path)?;
    tracing::info!("Generated committee file: {committee_path}");

    let parameters_path = Parameters::DEFAULT_FILENAME;
    Parameters::new_for_benchmarks(ips).print(parameters_path)?;
    tracing::info!("Generated (public) parameters file: {parameters_path}");

    for i in 0..committee_size {
        let path = PrivateConfig::DEFAULT_FILENAME;
        let filename = [path, &i.to_string()].iter().collect::<PathBuf>();
        PrivateConfig::new_for_benchmarks(i as AuthorityIndex).print(filename.clone())?;
        tracing::info!("Generated private config file: {}", filename.display());
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
    binding_network_address.set_ip("0.0.0.0".parse().unwrap());

    let metrics_address = parameters
        .metrics_address(authority)
        .ok_or(eyre!("No metrics address for authority {authority}"))
        .wrap_err("Unknown authority")?;
    let mut binding_metrics_address = metrics_address;
    binding_metrics_address.set_ip("0.0.0.0".parse().unwrap());

    // Boot the prometheus server.
    let registry = default_registry();
    let metrics = Arc::new(Metrics::new(&registry));
    let _handle = prometheus::start_prometheus_server(binding_metrics_address, registry);

    // Boot the validator node.
    let last_transaction = 0;
    let block_handler = TestBlockHandler::new(last_transaction, committee.clone(), authority);
    let core = Core::new(block_handler, authority, committee.clone()).with_metrics(metrics);
    let network = Network::load(&parameters, authority, binding_network_address).await;
    let _network_synchronizer = NetworkSyncer::start(
        network,
        core,
        parameters.wave_length(),
        TestCommitHandler::new(committee),
    );

    tracing::info!("Validator {authority} listening on {network_address}");
    tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

    Ok(())
}
