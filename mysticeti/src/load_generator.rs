// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use eyre::{Context, Result};
use mysticeti_core::{
    config::{Parameters, Print},
    load_generator::LoadGenerator,
};
use tracing_subscriber::{filter::LevelFilter, fmt, EnvFilter};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The index of the target node.
    #[clap(short, long, value_name = "INT")]
    target: u64,

    /// The rate of transactions per second.
    #[clap(short, long, value_name = "INT")]
    rate: u64,

    /// The size of each transaction in bytes.
    #[clap(short, long, value_name = "INT")]
    size: usize,

    /// Path to the file holding the public validator parameters (such as network addresses).
    #[clap(long, value_name = "FILE")]
    parameters_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    fmt().with_env_filter(filter).init();

    let args = Args::parse();
    let parameters = Parameters::load(&args.parameters_path).wrap_err(format!(
        "Failed to load parameters file '{}'",
        args.parameters_path
    ))?;

    let generator = LoadGenerator::new(args.target, args.rate, args.size, &parameters)?;
    generator.wait().await;
    generator.run().await?;

    Ok(())
}
