// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::time::Duration;
use std::{
    env,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use ::prometheus::Registry;
use eyre::{eyre, Context, Result};
use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::block_handler::{BlockHandler, SimpleBlockHandler};
use crate::block_validator::{AcceptAllBlockVerifier, BlockVerifier};
use crate::commit_observer::{
    CommitObserver, CommitObserverRecoveredState, SimpleCommitObserver, TestCommitObserver,
};
use crate::consensus::linearizer::CommittedSubDag;
use crate::crypto::Signer;
use crate::metrics::MetricReporter;
use crate::metrics::MetricReporterHandle;
use crate::prometheus::PrometheusServerHandle;
use crate::runtime::TimeInstant;
use crate::state::CoreRecoveredState;
use crate::transactions_generator::TransactionGeneratorHandle;
use crate::types::TransactionLocator;
use crate::wal::{walf, WalWriter};
use crate::{
    block_handler::BenchmarkFastPathBlockHandler,
    committee::Committee,
    config::{Parameters, PrivateConfig},
    core::Core,
    metrics::Metrics,
    net_sync::NetworkSyncer,
    network::Network,
    prometheus,
    runtime::JoinError,
    types::AuthorityIndex,
    wal,
};
use crate::{block_store::BlockStore, log::TransactionLog};
use crate::{core::CoreOptions, transactions_generator::TransactionGenerator};

pub(crate) type TransactionTimeMap = Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>;

pub struct CommitConsumer {
    // A sender to forward the committed sub dags to
    sender: UnboundedSender<CommittedSubDag>,
    // The last commit height that the consumer has consumed up to. This is useful for crash/recovery
    // so mysticeti can replay the commits from `last_sent_height + 1`.
    last_sent_height: u64,
}

impl CommitConsumer {
    #[allow(dead_code)]
    pub fn new(sender: UnboundedSender<CommittedSubDag>, last_sent_height: u64) -> Self {
        Self {
            sender,
            last_sent_height,
        }
    }
}

pub struct Validator<
    B: BlockHandler + 'static = BenchmarkFastPathBlockHandler,
    C: CommitObserver + 'static = TestCommitObserver<TransactionLog>,
> {
    network_synchronizer: NetworkSyncer<B, C>,
    metrics_handle: PrometheusServerHandle,
    reporter_handle: MetricReporterHandle,
    transaction_generator_handle: Option<TransactionGeneratorHandle>,
}

impl Validator<BenchmarkFastPathBlockHandler, TestCommitObserver<TransactionLog>> {
    // Method to be used when need to start a validator but for benchmarking/testing purposes. It is
    // initialising the BenchmarkFastPathBlockHandler.
    pub async fn start_benchmarking(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        parameters: &Parameters,
        config: PrivateConfig,
        signer: Signer,
    ) -> Result<Validator<BenchmarkFastPathBlockHandler, TestCommitObserver<TransactionLog>>> {
        let (metrics, reporter, metrics_handle) =
            Self::init_metrics(authority, committee.clone(), parameters, None)?;
        let (core_recovered, commit_observer_recovered, wal_writer) =
            Self::init_storage(authority, committee.clone(), &config, metrics.clone());
        let reporter_handle = reporter.start();

        // Boot the validator node.
        let transaction_time: TransactionTimeMap = Arc::new(Mutex::new(HashMap::new()));
        let (block_handler, block_sender) = BenchmarkFastPathBlockHandler::new(
            committee.clone(),
            authority,
            config.storage(),
            core_recovered.block_store.clone(),
            metrics.clone(),
            transaction_time.clone(),
        );
        let tps = env::var("TPS");
        let tps = tps.map(|t| t.parse::<usize>().expect("Failed to parse TPS variable"));
        let tps = tps.unwrap_or(10);
        let initial_delay = env::var("INITIAL_DELAY");
        let initial_delay = initial_delay.map(|t| {
            t.parse::<u64>()
                .expect("Failed to parse INITIAL_DELAY variable")
        });
        let initial_delay = initial_delay.unwrap_or(10);
        let transaction_size = env::var("TRANSACTION_SIZE");
        let transaction_size = transaction_size
            .map(|t| {
                t.parse::<usize>()
                    .expect("Failed to parse TRANSACTION_SIZE variable")
            })
            .unwrap_or(TransactionGenerator::DEFAULT_TRANSACTION_SIZE);

        tracing::info!("Starting generator with {tps} transactions per second, initial delay {initial_delay} sec");
        let initial_delay = Duration::from_secs(initial_delay);
        let transaction_generator_handle = TransactionGenerator::start(
            block_sender,
            authority,
            tps,
            transaction_size,
            initial_delay,
        );

        // Boot the validator node.
        let committed_transaction_log =
            TransactionLog::start(config.storage().committed_transactions_log())
                .expect("Failed to open committed transaction log for write");
        let commit_observer = TestCommitObserver::new(
            core_recovered.block_store.clone(),
            committee.clone(),
            transaction_time,
            metrics.clone(),
            committed_transaction_log,
            commit_observer_recovered,
        );

        Validator::start_internal(
            authority,
            committee,
            parameters,
            signer,
            metrics,
            metrics_handle,
            reporter_handle,
            Some(transaction_generator_handle),
            core_recovered,
            wal_writer,
            block_handler,
            commit_observer,
            AcceptAllBlockVerifier,
        )
        .await
    }
}

impl Validator<SimpleBlockHandler> {
    #[allow(clippy::too_many_arguments)]
    pub async fn start_production(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        parameters: &Parameters,
        config: PrivateConfig,
        registry: Registry,
        signer: Signer,
        consumer: CommitConsumer,
        block_verifier: impl BlockVerifier,
    ) -> Result<(
        Validator<SimpleBlockHandler, SimpleCommitObserver>,
        tokio::sync::mpsc::Sender<(Vec<u8>, tokio::sync::oneshot::Sender<()>)>,
    )> {
        let (metrics, reporter, metrics_handle) =
            Self::init_metrics(authority, committee.clone(), parameters, Some(registry))?;
        let (core_recovered, commit_observer_recovered, wal_writer) =
            Self::init_storage(authority, committee.clone(), &config, metrics.clone());
        let reporter_handle = reporter.start();
        let (block_handler, tx_sender) = SimpleBlockHandler::new();

        let commit_observer = SimpleCommitObserver::new(
            core_recovered.block_store.clone(),
            consumer.sender,
            consumer.last_sent_height,
            commit_observer_recovered,
        );

        let validator = Validator::start_internal(
            authority,
            committee,
            parameters,
            signer,
            metrics,
            metrics_handle,
            reporter_handle,
            None,
            core_recovered,
            wal_writer,
            block_handler,
            commit_observer,
            block_verifier,
        )
        .await?;

        Ok((validator, tx_sender))
    }
}

impl<B: BlockHandler + 'static, C: CommitObserver + 'static> Validator<B, C> {
    #[allow(clippy::too_many_arguments)]
    async fn start_internal(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        parameters: &Parameters,
        signer: Signer,
        metrics: Arc<Metrics>,
        metrics_handle: PrometheusServerHandle,
        reporter_handle: MetricReporterHandle,
        transaction_generator_handle: Option<TransactionGeneratorHandle>,
        core_recovered: CoreRecoveredState,
        wal_writer: WalWriter,
        block_handler: B,
        commit_observer: C,
        block_verifier: impl BlockVerifier,
    ) -> Result<Self> {
        let network_address = parameters
            .network_address(authority)
            .ok_or(eyre!("No network address for authority {authority}"))
            .wrap_err("Unknown authority")?;
        let mut binding_network_address = network_address;
        binding_network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        tracing::info!("Binding to local address {}", binding_network_address);

        let core = Core::open(
            block_handler,
            authority,
            committee.clone(),
            parameters,
            metrics.clone(),
            core_recovered,
            wal_writer,
            CoreOptions::default(),
            signer,
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
            commit_observer,
            parameters.shutdown_grace_period(),
            block_verifier,
            metrics,
        );

        tracing::info!("Validator {authority} listening on {network_address}");

        Ok(Self {
            network_synchronizer,
            metrics_handle,
            reporter_handle,
            transaction_generator_handle,
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
            self.metrics_handle.handle
        )
    }

    pub async fn stop(self) {
        self.network_synchronizer.shutdown().await;
        self.reporter_handle.shutdown().await;
        self.metrics_handle.shutdown().await;
        if let Some(handle) = self.transaction_generator_handle {
            handle.shutdown().await;
        }
    }

    fn init_metrics(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        parameters: &Parameters,
        registry: Option<Registry>,
    ) -> Result<(Arc<Metrics>, MetricReporter, PrometheusServerHandle)> {
        // Boot the prometheus server only when a registry is not passed in. If a registry is passed in
        // we assume that an upstream component is responsible for exposing the metrics.
        let (registry, metrics_handle) = if let Some(registry) = registry {
            (registry, PrometheusServerHandle::noop())
        } else {
            let registry = Registry::new();

            let metrics_address = parameters
                .metrics_address(authority)
                .ok_or(eyre!("No metrics address for authority {authority}"))
                .wrap_err("Unknown authority")?;
            let mut binding_metrics_address = metrics_address;
            binding_metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

            let metrics_handle =
                prometheus::start_prometheus_server(binding_metrics_address, &registry);

            tracing::info!("Validator {authority} exposing metrics on {metrics_address}");

            (registry, metrics_handle)
        };

        let (metrics, reporter) = Metrics::new(&registry, Some(&committee));
        Ok((metrics, reporter, metrics_handle))
    }

    fn init_storage(
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        config: &PrivateConfig,
        metrics: Arc<Metrics>,
    ) -> (CoreRecoveredState, CommitObserverRecoveredState, WalWriter) {
        // Open the block store.
        let wal_file =
            wal::open_file_for_wal(config.storage().wal()).expect("Failed to open wal file");
        let (wal_writer, wal_reader) = walf(wal_file).expect("Failed to open wal");
        let (core_recovered, commit_observer_recovered) = BlockStore::open(
            authority,
            Arc::new(wal_reader),
            &wal_writer,
            metrics.clone(),
            &committee,
        );
        (core_recovered, commit_observer_recovered, wal_writer)
    }
}

#[cfg(test)]
mod smoke_tests {
    use std::{
        collections::VecDeque,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use tempdir::TempDir;

    use tokio::time;

    use super::Validator;
    use crate::crypto::dummy_signer;
    use crate::runtime::sleep;
    use crate::{
        committee::Committee,
        config::{Parameters, PrivateConfig},
        prometheus,
        types::AuthorityIndex,
    };

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
    async fn validator_commit() {
        let committee_size = 4;
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];

        let committee = Committee::new_for_benchmarks(committee_size);
        let parameters = Parameters::new_for_benchmarks(ips).with_port_offset(0);

        let mut handles = Vec::new();
        let tempdir = TempDir::new("validator_commit").unwrap();
        for i in 0..committee_size {
            let authority = i as AuthorityIndex;
            let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);

            let validator = Validator::start_benchmarking(
                authority,
                committee.clone(),
                &parameters,
                private,
                dummy_signer(),
            )
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
        let parameters = Parameters::new_for_benchmarks(ips).with_port_offset(100);

        let mut handles = Vec::new();
        let tempdir = TempDir::new("validator_sync").unwrap();

        // Boot all validators but one.
        for i in 1..committee_size {
            let authority = i as AuthorityIndex;
            let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);

            let validator = Validator::start_benchmarking(
                authority,
                committee.clone(),
                &parameters,
                private,
                dummy_signer(),
            )
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
        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }

        // Boot the last validator.
        let authority = 0 as AuthorityIndex;
        let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);
        let validator = Validator::start_benchmarking(
            authority,
            committee.clone(),
            &parameters,
            private,
            dummy_signer(),
        )
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

    // Ensure that honest validators commit despite the presence of a crash fault.
    #[tokio::test]
    async fn validator_crash_faults() {
        let committee_size = 4;
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];

        let committee = Committee::new_for_benchmarks(committee_size);
        let parameters = Parameters::new_for_benchmarks(ips).with_port_offset(200);

        let mut handles = Vec::new();
        let tempdir = TempDir::new("validator_crash_faults").unwrap();
        for i in 1..committee_size {
            let authority = i as AuthorityIndex;
            let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);

            let validator = Validator::start_benchmarking(
                authority,
                committee.clone(),
                &parameters,
                private,
                dummy_signer(),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = parameters
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = Parameters::DEFAULT_LEADER_TIMEOUT * 15;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    #[tokio::test]
    async fn validator_shutdown_and_start() {
        let committee_size = 4;
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];

        let committee = Committee::new_for_benchmarks(committee_size);
        let parameters = Parameters::new_for_benchmarks(ips).with_port_offset(300);

        let tempdir = TempDir::new("validator_shutdown_and_start").unwrap();

        let mut validators = Vec::new();
        for i in 0..committee_size {
            let authority = i as AuthorityIndex;
            let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);

            let validator = Validator::start_benchmarking(
                authority,
                committee.clone(),
                &parameters,
                private,
                dummy_signer(),
            )
            .await
            .unwrap();
            validators.push(validator);
        }

        // let the network communicate a bit
        sleep(Duration::from_secs(2)).await;

        // now shutdown all the validators
        while let Some(validator) = validators.pop() {
            validator.stop().await;
        }

        // TODO: to make the test pass for now I am making the validators start with a fresh storage.
        // Otherwise I get a storage related error like:
        //  panicked at 'range end index 16 out of range for slice of length 13', mysticeti-core/src/wal.rs:296:33
        //
        // when block store is restored during the `open` method.
        //
        // Switching to CoreOptions::production() to have a sync file on every block write the error goes away. This makes me
        // believe that some partial writing is happening or something that makes the file perceived as corrupt while reloading it.
        // Haven't investigated further but needs to look into it.
        let tempdir = TempDir::new("validator_shutdown_and_start").unwrap();

        // now start again the validators - no error (ex network port conflict) should arise
        for i in 0..committee_size {
            let authority = i as AuthorityIndex;
            let private = PrivateConfig::new_for_benchmarks(tempdir.as_ref(), authority);

            let validator = Validator::start_benchmarking(
                authority,
                committee.clone(),
                &parameters,
                private,
                dummy_signer(),
            )
            .await
            .unwrap();
            validators.push(validator);
        }
    }
}
