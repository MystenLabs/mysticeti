// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;
use std::{
    env,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use ::prometheus::Registry;
use eyre::{eyre, Context, Result};

use crate::block_handler::TransactionGenerator;
use crate::core::CoreOptions;
use crate::log::TransactionLog;
use crate::{
    block_handler::{RealBlockHandler, TestCommitHandler},
    committee::Committee,
    config::{Parameters, PrivateConfig},
    core::Core,
    metrics::Metrics,
    net_sync::NetworkSyncer,
    network::Network,
    prometheus,
    runtime::{JoinError, JoinHandle},
    types::AuthorityIndex,
    wal,
};

pub struct Validator {
    network_synchronizer: NetworkSyncer<RealBlockHandler, TestCommitHandler<TransactionLog>>,
    metrics_handle: JoinHandle<Result<(), hyper::Error>>,
}

impl Validator {
    pub async fn start(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        parameters: &Parameters,
        config: PrivateConfig,
    ) -> Result<Self> {
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
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry, Some(&committee));
        reporter.start();

        let metrics_handle =
            prometheus::start_prometheus_server(binding_metrics_address, &registry);

        // Boot the validator node.
        let (block_handler, block_sender) = RealBlockHandler::new(
            committee.clone(),
            authority,
            config.storage(),
            metrics.clone(),
        );
        let tps = env::var("TPS");
        let tps = tps.map(|t| t.parse::<usize>().expect("Failed to parse TPS variable"));
        let tps = tps.unwrap_or(10);
        let transactions_per_100ms = (tps + 9) / 10;
        let initial_delay = env::var("INITIAL_DELAY");
        let initial_delay = initial_delay.map(|t| {
            t.parse::<u64>()
                .expect("Failed to parse INITIAL_DELAY variable")
        });
        let initial_delay = initial_delay.unwrap_or(10);
        tracing::info!("Starting generator with {transactions_per_100ms} transactions per 100ms, initial delay {initial_delay} sec");
        let initial_delay = Duration::from_secs(initial_delay);
        TransactionGenerator::start(
            block_sender,
            authority,
            transactions_per_100ms,
            initial_delay,
        );
        let committed_transaction_log =
            TransactionLog::start(config.storage().committed_transactions_log())
                .expect("Failed to open committed transaction log for write");
        let commit_handler = TestCommitHandler::new_with_handler(
            committee.clone(),
            block_handler.transaction_time.clone(),
            metrics.clone(),
            committed_transaction_log,
        );
        let wal_file =
            wal::open_file_for_wal(config.storage().wal()).expect("Failed to open wal file");
        let core = Core::open(
            block_handler,
            authority,
            committee.clone(),
            metrics.clone(),
            wal_file,
            CoreOptions::test(),
        );
        let network = Network::load(
            parameters,
            authority,
            binding_network_address,
            metrics.clone(),
        )
        .await;
        let network_synchronizer = NetworkSyncer::start(
            network,
            core,
            parameters.wave_length(),
            commit_handler,
            metrics,
        );

        tracing::info!("Validator {authority} listening on {network_address}");
        tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

        Ok(Self {
            network_synchronizer,
            metrics_handle,
        })
    }

    pub async fn await_completion(
        self,
    ) -> (
        Result<(), JoinError>,
        Result<Result<(), hyper::Error>, JoinError>,
    ) {
        tokio::join!(
            self.network_synchronizer.await_completion(),
            self.metrics_handle
        )
    }

    pub async fn stop(self) {
        self.network_synchronizer.shutdown().await;
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::VecDeque,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use tempdir::TempDir;

    use tokio::time;

    use crate::{
        committee::Committee,
        config::{Parameters, PrivateConfig},
        prometheus,
        types::AuthorityIndex,
    };

    use super::Validator;

    /// Check whether the validator specified by its metrics address has committed at least once.
    async fn check_commit(address: &SocketAddr) -> Result<bool, reqwest::Error> {
        let route = prometheus::METRICS_ROUTE;
        let res = reqwest::get(format! {"http://{address}{route}"}).await?;
        let string = res.text().await?;
        let commit = string.contains("committed_leaders_total");
        Ok(commit)
    }

    /// Await for all the validators specified by their metrics addresses to commit.
    async fn await_for_commits(addresses: Vec<SocketAddr>) {
        let mut queue = VecDeque::from(addresses);
        while let Some(address) = queue.pop_front() {
            time::sleep(Duration::from_millis(100)).await;
            match check_commit(&address).await {
                Ok(commits) if commits => (),
                _ => queue.push_back(address),
            }
        }
    }

    /// Ensure that a committee of honest validators commits.
    #[tokio::test]
    async fn validator_smoke_test() {
        let committee_size = 4;
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];

        let committee = Committee::new_for_benchmarks(committee_size);
        let parameters = Parameters::new_for_benchmarks(ips);

        let mut handles = Vec::new();
        let tempdir = TempDir::new("validator_smoke_test").unwrap();
        for i in 0..committee_size {
            let authority = i as AuthorityIndex;
            let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);

            let validator = Validator::start(authority, committee.clone(), &parameters, private)
                .await
                .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = parameters
            .all_metric_addresses()
            .map(|address| address.to_owned())
            .collect();
        let timeout = Parameters::DEFAULT_LEADER_TIMEOUT * 5;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    /// Ensure validators can sync missing blocks
    #[tokio::test]
    async fn validator_sync() {
        let committee_size = 4;
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];

        let committee = Committee::new_for_benchmarks(committee_size);
        let parameters = Parameters::new_for_benchmarks(ips);

        let mut handles = Vec::new();
        let tempdir = TempDir::new("validator_sync").unwrap();

        // Boot all validators but one.
        for i in 1..committee_size {
            let authority = i as AuthorityIndex;
            let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);

            let validator = Validator::start(authority, committee.clone(), &parameters, private)
                .await
                .unwrap();
            handles.push(validator.await_completion());
        }

        // Boot the last validator after they others commit.
        let addresses = parameters
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = Parameters::DEFAULT_LEADER_TIMEOUT * 5;
        println!("addresses: {:?}", addresses);
        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }

        // Boot the last validator.
        let authority = 0 as AuthorityIndex;
        let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);
        let validator = Validator::start(authority, committee.clone(), &parameters, private)
            .await
            .unwrap();
        handles.push(validator.await_completion());

        // Ensure the last validator commits.
        let address = parameters
            .all_metric_addresses()
            .next()
            .map(|address| address.to_owned())
            .unwrap();
        let timeout = Parameters::DEFAULT_LEADER_TIMEOUT * 5;
        tokio::select! {
            _ = await_for_commits(vec![address]) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }
}
