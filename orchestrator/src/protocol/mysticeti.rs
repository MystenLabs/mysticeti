// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    fmt::{Debug, Display},
    net::IpAddr,
    path::PathBuf,
    str::FromStr,
};

use mysticeti_core::{
    committee::Committee,
    config::{self, Parameters, PrivateConfig},
    types::AuthorityIndex,
};
use serde::{Deserialize, Serialize};

use crate::{
    benchmark::{BenchmarkParameters, BenchmarkType},
    client::Instance,
    settings::Settings,
};

use super::{ProtocolCommands, ProtocolMetrics};

const CARGO_FLAGS: &str = "--release";
const RUST_FLAGS: &str = "RUSTFLAGS=-C\\ target-cpu=native";

/// The type of benchmarks supported by Mysticeti.
/// Note that all transactions are interpreted as both owned and shared.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MysticetiBenchmarkType {
    /// The transaction size in bytes.
    transaction_size: usize,
}

impl Debug for MysticetiBenchmarkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.transaction_size)
    }
}

impl Display for MysticetiBenchmarkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}B transactions", self.transaction_size)
    }
}

impl FromStr for MysticetiBenchmarkType {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            transaction_size: s.parse::<usize>()?,
        })
    }
}

impl BenchmarkType for MysticetiBenchmarkType {}

/// All configurations information to run a Mysticeti client or validator.
pub struct MysticetiProtocol {
    working_dir: PathBuf,
}

impl ProtocolCommands<MysticetiBenchmarkType> for MysticetiProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec![]
    }

    fn db_directories(&self) -> Vec<PathBuf> {
        let storage = self.working_dir.join("private/val-*/*");
        vec![storage]
    }

    fn cleanup_commands(&self) -> Vec<String> {
        vec!["killall mysticeti".to_string()]
    }

    fn genesis_command<'a, I>(&self, instances: I) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        let ips = instances
            .map(|x| x.main_ip.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let working_directory = self.working_dir.display();

        let genesis = [
            &format!("{RUST_FLAGS} cargo run {CARGO_FLAGS} --bin mysticeti --"),
            "benchmark-genesis",
            &format!("--ips {ips} --working-directory {working_directory}"),
        ]
        .join(" ");

        ["source $HOME/.cargo/env", &genesis].join(" && ")
    }

    fn monitor_command<I>(&self, instances: I) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .map(|i| {
                (
                    i,
                    "tail -f --pid=$(pidof mysticeti) -f /dev/null; tail -100 node.log".to_string(),
                )
            })
            .collect()
    }

    fn node_command<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters<MysticetiBenchmarkType>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .enumerate()
            .map(|(i, instance)| {
                let authority = i as AuthorityIndex;
                let committee_path: PathBuf =
                    [&self.working_dir, &Committee::DEFAULT_FILENAME.into()]
                        .iter()
                        .collect();
                let parameters_path: PathBuf =
                    [&self.working_dir, &Parameters::DEFAULT_FILENAME.into()]
                        .iter()
                        .collect();
                let private_configs_path: PathBuf = [
                    &self.working_dir,
                    &PrivateConfig::default_filename(authority),
                ]
                .iter()
                .collect();

                let env = env::var("ENV").unwrap_or_default();
                let run = [
                    &env,
                    &format!("{RUST_FLAGS} cargo run {CARGO_FLAGS} --bin mysticeti --"),
                    "run",
                    &format!(
                        "--authority {authority} --committee-path {}",
                        committee_path.display()
                    ),
                    &format!(
                        "--parameters-path {} --private-config-path {}",
                        parameters_path.display(),
                        private_configs_path.display()
                    ),
                ]
                .join(" ");
                let tps = format!("export TPS={}", parameters.load / parameters.nodes);
                let tx_size = format!("export TRANSACTION_SIZE={}", parameters.benchmark_type.transaction_size);
                // let consensus_only = format!("export CONSENSUS_ONLY={}", 1);
                let syncer = format!("export USE_SYNCER={}", 1);
                let command = ["#!/bin/bash -e", "source $HOME/.cargo/env", &tps, &tx_size, &syncer, &run].join("\\n");
                let command = format!("echo -e '{command}' > mysticeti-start.sh && chmod +x mysticeti-start.sh && ./mysticeti-start.sh");

                (instance, command)
            })
            .collect()
    }

    fn client_command<I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters<MysticetiBenchmarkType>,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // TODO
        vec![]
    }
}

impl MysticetiProtocol {
    /// Make a new instance of the Mysticeti protocol commands generator.
    pub fn new(settings: &Settings) -> Self {
        Self {
            working_dir: settings.working_dir.clone(),
        }
    }
}

impl ProtocolMetrics for MysticetiProtocol {
    const BENCHMARK_DURATION: &'static str = "benchmark_duration";
    const TOTAL_TRANSACTIONS: &'static str = "latency_s_count";
    const LATENCY_BUCKETS: &'static str = "latency_s";
    const LATENCY_SUM: &'static str = "latency_s_sum";
    const LATENCY_SQUARED_SUM: &'static str = "latency_squared_s";

    fn nodes_metrics_path<I>(&self, instances: I) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        let (ips, instances): (_, Vec<_>) = instances
            .into_iter()
            .map(|x| (IpAddr::V4(x.main_ip), x))
            .unzip();
        let parameters = config::Parameters::new_for_benchmarks(ips);
        let metrics_paths = parameters
            .all_metric_addresses()
            .map(|x| format!("{x}{}", mysticeti_core::prometheus::METRICS_ROUTE));

        instances.into_iter().zip(metrics_paths).collect()
    }

    fn clients_metrics_path<I>(&self, instances: I) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // TODO: hack until we have benchmark clients.
        self.nodes_metrics_path(instances)
    }
}
