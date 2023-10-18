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
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MysticetiBenchmarkType {
    /// The transaction size in bytes.
    transaction_size: usize,
    /// Consensus only mode.
    consensus_only: bool,
    /// Whether to disable the pipeline within the universal committer.
    disable_pipeline: bool,
    /// The number of leaders to use.
    number_of_leaders: usize,
}

impl Default for MysticetiBenchmarkType {
    fn default() -> Self {
        Self {
            transaction_size: 512,
            consensus_only: false,
            disable_pipeline: false,
            number_of_leaders: 2,
        }
    }
}

impl Debug for MysticetiBenchmarkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.transaction_size)
    }
}

impl Display for MysticetiBenchmarkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consensus_only = if self.consensus_only {
            " (consensus-only)"
        } else {
            ""
        };
        write!(f, "{}B transactions{consensus_only}", self.transaction_size)
    }
}

impl FromStr for MysticetiBenchmarkType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(Self::default());
        }

        let parameters = s.split("-").collect::<Vec<_>>();
        Ok(Self {
            transaction_size: parameters[0].parse::<usize>().map_err(|e| e.to_string())?,
            consensus_only: parameters[1].parse::<bool>().map_err(|e| e.to_string())?,
            disable_pipeline: parameters[2].parse::<bool>().map_err(|e| e.to_string())?,
            number_of_leaders: parameters[3].parse::<usize>().map_err(|e| e.to_string())?,
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

    fn genesis_command<'a, I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters<MysticetiBenchmarkType>,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        let ips = instances
            .map(|x| x.main_ip.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let working_directory = self.working_dir.display();

        let disable_pipeline = if parameters.benchmark_type.disable_pipeline {
            "--disable-pipeline"
        } else {
            ""
        };
        let number_of_leaders = parameters.benchmark_type.number_of_leaders;

        let genesis = [
            &format!("{RUST_FLAGS} cargo run {CARGO_FLAGS} --bin mysticeti --"),
            "benchmark-genesis",
            &format!("--ips {ips} --working-directory {working_directory} {disable_pipeline} --number-of-leaders {number_of_leaders}"),
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
                let consensus_only = if parameters.benchmark_type.consensus_only {
                    format!("export CONSENSUS_ONLY={}", 1)
                } else {
                    "".to_string()
                };
                let syncer = format!("export USE_SYNCER={}", 1);
                let command = ["#!/bin/bash -e", "source $HOME/.cargo/env", &tps, &tx_size, &consensus_only, &syncer, &run].join("\\n");
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
