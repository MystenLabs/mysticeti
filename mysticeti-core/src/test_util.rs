// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_handler::{BlockHandler, TestBlockHandler};
use crate::block_store::{BlockStore, BlockWriter, OwnBlockData, WAL_ENTRY_BLOCK};
use crate::block_validator::AcceptAllBlockVerifier;
use crate::commit_observer::TestCommitObserver;
use crate::committee::Committee;
use crate::config::Parameters;
use crate::core::{Core, CoreOptions};
use crate::crypto::dummy_signer;
use crate::data::Data;
#[cfg(feature = "simulator")]
use crate::future_simulator::OverrideNodeContext;
use crate::metrics::MetricReporter;
use crate::metrics::Metrics;
use crate::net_sync::NetworkSyncer;
use crate::network::Network;
#[cfg(feature = "simulator")]
use crate::simulated_network::SimulatedNetwork;
use crate::syncer::{Syncer, SyncerSignals};
use crate::types::{
    format_authority_index, AuthorityIndex, BlockReference, RoundNumber, StatementBlock,
};
use crate::wal::{open_file_for_wal, walf, WalPosition, WalWriter};
use futures::future::join_all;
use prometheus::Registry;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;

pub fn test_metrics() -> Arc<Metrics> {
    Metrics::new(&Registry::new(), None).0
}

pub fn committee(n: usize) -> Arc<Committee> {
    Committee::new_test(vec![1; n])
}

pub fn committee_and_cores(
    n: usize,
) -> (
    Arc<Committee>,
    Vec<Core<TestBlockHandler>>,
    Vec<TestCommitObserver>,
    Vec<MetricReporter>,
) {
    committee_and_cores_persisted_epoch_duration(n, None, &Parameters::default())
}

pub fn committee_and_cores_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> (
    Arc<Committee>,
    Vec<Core<TestBlockHandler>>,
    Vec<TestCommitObserver>,
    Vec<MetricReporter>,
) {
    let parameters = Parameters {
        rounds_in_epoch,
        ..Default::default()
    };
    committee_and_cores_persisted_epoch_duration(n, None, &parameters)
}

pub fn committee_and_cores_persisted(
    n: usize,
    path: Option<&Path>,
) -> (
    Arc<Committee>,
    Vec<Core<TestBlockHandler>>,
    Vec<TestCommitObserver>,
    Vec<MetricReporter>,
) {
    committee_and_cores_persisted_epoch_duration(n, path, &Parameters::default())
}

pub fn committee_and_cores_persisted_epoch_duration(
    n: usize,
    path: Option<&Path>,
    parameters: &Parameters,
) -> (
    Arc<Committee>,
    Vec<Core<TestBlockHandler>>,
    Vec<TestCommitObserver>,
    Vec<MetricReporter>,
) {
    let committee = committee(n);
    let cores: Vec<_> = committee
        .authorities()
        .map(|authority| {
            let last_transaction = first_transaction_for_authority(authority);
            let (metrics, reporter) = Metrics::new(&Registry::new(), Some(&committee));
            let block_handler = TestBlockHandler::new(
                last_transaction,
                committee.clone(),
                authority,
                metrics.clone(),
            );
            let wal_file = if let Some(path) = path {
                let wal_path = path.join(format!("{:03}.wal", authority));
                open_file_for_wal(&wal_path).unwrap()
            } else {
                tempfile::tempfile().unwrap()
            };
            let (wal_writer, wal_reader) = walf(wal_file).expect("Failed to open wal");
            let (core_recovered, commit_observer_recovered) = BlockStore::open(
                authority,
                Arc::new(wal_reader),
                &wal_writer,
                metrics.clone(),
                &committee,
            );

            println!("Opening core {authority}");
            let core = Core::open(
                block_handler,
                authority,
                committee.clone(),
                parameters,
                metrics,
                core_recovered,
                wal_writer,
                CoreOptions::test(),
                dummy_signer(),
            );
            let commit_observer = TestCommitObserver::new(
                core.block_store().clone(),
                committee.clone(),
                core.block_handler().transaction_time.clone(),
                test_metrics(),
                Default::default(),
                commit_observer_recovered,
            );
            (core, commit_observer, reporter)
        })
        .collect();
    let (cores, commit_observers, reporters) = itertools::multiunzip(cores);
    (committee, cores, commit_observers, reporters)
}

fn first_transaction_for_authority(authority: AuthorityIndex) -> u64 {
    authority * 1_000_000
}

pub fn committee_and_syncers(
    n: usize,
) -> (
    Arc<Committee>,
    Vec<Syncer<TestBlockHandler, bool, TestCommitObserver>>,
) {
    let (committee, cores, commit_observers, _) = committee_and_cores(n);
    (
        committee.clone(),
        cores
            .into_iter()
            .zip(commit_observers)
            .map(|(core, commit_observer)| {
                Syncer::new(core, 3, Default::default(), commit_observer, test_metrics())
            })
            .collect(),
    )
}

pub async fn networks_and_addresses(metrics: &[Arc<Metrics>]) -> (Vec<Network>, Vec<SocketAddr>) {
    let host = Ipv4Addr::LOCALHOST;
    let addresses: Vec<_> = (0..metrics.len())
        .map(|i| SocketAddr::V4(SocketAddrV4::new(host, 5001 + i as u16)))
        .collect();
    let networks =
        addresses
            .iter()
            .zip(metrics.iter())
            .enumerate()
            .map(|(i, (address, metrics))| {
                Network::from_socket_addresses(&addresses, i, *address, metrics.clone())
            });
    let networks = join_all(networks).await;
    (networks, addresses)
}

#[cfg(feature = "simulator")]
pub fn simulated_network_syncers(
    n: usize,
) -> (
    SimulatedNetwork,
    Vec<NetworkSyncer<TestBlockHandler, TestCommitObserver>>,
    Vec<MetricReporter>,
) {
    simulated_network_syncers_with_epoch_duration(n, Parameters::DEFAULT_ROUNDS_IN_EPOCH)
}

#[cfg(feature = "simulator")]
pub fn simulated_network_syncers_with_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> (
    SimulatedNetwork,
    Vec<NetworkSyncer<TestBlockHandler, TestCommitObserver>>,
    Vec<MetricReporter>,
) {
    let (committee, cores, commit_observers, reporters) =
        committee_and_cores_epoch_duration(n, rounds_in_epoch);
    let (simulated_network, networks) = SimulatedNetwork::new(&committee);
    let mut network_syncers = vec![];
    for ((network, core), commit_observer) in networks.into_iter().zip(cores).zip(commit_observers)
    {
        let node_context = OverrideNodeContext::enter(Some(core.authority()));
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            3,
            commit_observer,
            Parameters::DEFAULT_SHUTDOWN_GRACE_PERIOD,
            AcceptAllBlockVerifier,
            test_metrics(),
        );
        drop(node_context);
        network_syncers.push(network_syncer);
    }
    (simulated_network, network_syncers, reporters)
}

pub async fn network_syncers(n: usize) -> Vec<NetworkSyncer<TestBlockHandler, TestCommitObserver>> {
    network_syncers_with_epoch_duration(n, Parameters::DEFAULT_ROUNDS_IN_EPOCH).await
}

pub async fn network_syncers_with_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> Vec<NetworkSyncer<TestBlockHandler, TestCommitObserver>> {
    let (_, cores, commit_observers, _) = committee_and_cores_epoch_duration(n, rounds_in_epoch);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics).await;
    let mut network_syncers = vec![];
    for ((network, core), commit_observer) in networks.into_iter().zip(cores).zip(commit_observers)
    {
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            3,
            commit_observer,
            Parameters::DEFAULT_SHUTDOWN_GRACE_PERIOD,
            AcceptAllBlockVerifier,
            test_metrics(),
        );
        network_syncers.push(network_syncer);
    }
    network_syncers
}

pub fn rng_at_seed(seed: u64) -> StdRng {
    let bytes = seed.to_le_bytes();
    let mut seed = [0u8; 32];
    seed[..bytes.len()].copy_from_slice(&bytes);
    StdRng::from_seed(seed)
}

pub fn check_commits<H: BlockHandler, S: SyncerSignals>(
    syncers: &[Syncer<H, S, TestCommitObserver>],
) {
    let commits = syncers
        .iter()
        .map(|state| state.commit_observer().committed_leaders());
    let zero_commit = vec![];
    let mut max_commit = &zero_commit;
    for commit in commits {
        if commit.len() >= max_commit.len() {
            if is_prefix(&max_commit, commit) {
                max_commit = commit;
            } else {
                panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
            }
        } else {
            if !is_prefix(&commit, &max_commit) {
                panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
            }
        }
    }
    eprintln!("Max commit sequence: {max_commit:?}");
}

#[allow(dead_code)]
pub fn print_stats<S: SyncerSignals>(
    syncers: &[Syncer<TestBlockHandler, S, TestCommitObserver>],
    reporters: &mut [MetricReporter],
) {
    assert_eq!(syncers.len(), reporters.len());
    eprintln!("val ||    cert(ms)   ||cert commit(ms)|| tx commit(ms) |");
    eprintln!("    ||  p90  |  avg  ||  p90  |  avg  ||  p90  |  avg  |");
    syncers.iter().zip(reporters.iter_mut()).for_each(|(s, r)| {
        r.clear_receive_all();
        eprintln!(
            "  {} || {:05} | {:05} || {:05} | {:05} || {:05} | {:05} |",
            format_authority_index(s.core().authority()),
            r.transaction_certified_latency
                .histogram
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            r.transaction_certified_latency
                .histogram
                .avg()
                .unwrap_or_default()
                .as_millis(),
            r.certificate_committed_latency
                .histogram
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            r.certificate_committed_latency
                .histogram
                .avg()
                .unwrap_or_default()
                .as_millis(),
            r.transaction_committed_latency
                .histogram
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            r.transaction_committed_latency
                .histogram
                .avg()
                .unwrap_or_default()
                .as_millis(),
        )
    });
}

fn is_prefix(short: &[BlockReference], long: &[BlockReference]) -> bool {
    assert!(short.len() <= long.len());
    for (a, b) in short.iter().zip(long.iter().take(short.len())) {
        if a != b {
            return false;
        }
    }
    return true;
}

pub struct TestBlockWriter {
    block_store: BlockStore,
    wal_writer: WalWriter,
}

impl TestBlockWriter {
    pub fn new(committee: &Committee) -> Self {
        let file = tempfile::tempfile().unwrap();
        let (wal_writer, wal_reader) = walf(file).unwrap();
        let (core_state, _commit_observer_state) = BlockStore::open(
            0,
            Arc::new(wal_reader),
            &wal_writer,
            test_metrics(),
            committee,
        );
        let block_store = core_state.block_store;
        Self {
            block_store,
            wal_writer,
        }
    }

    pub fn add_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        let pos = self
            .wal_writer
            .write(WAL_ENTRY_BLOCK, &bincode::serialize(&block).unwrap())
            .unwrap();
        self.block_store.insert_block(block, pos);
        pos
    }

    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) {
        for block in blocks {
            self.add_block(block);
        }
    }

    pub fn into_block_store(self) -> BlockStore {
        self.block_store
    }

    pub fn block_store(&self) -> BlockStore {
        self.block_store.clone()
    }
}

impl BlockWriter for TestBlockWriter {
    fn insert_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        (&mut self.wal_writer, &self.block_store).insert_block(block)
    }

    fn insert_own_block(&mut self, block: &OwnBlockData) {
        (&mut self.wal_writer, &self.block_store).insert_own_block(block)
    }
}

/// Build a fully interconnected dag up to the specified round. This function starts building the
/// dag from the specified [`start`] references or from genesis if none are specified.
pub fn build_dag(
    committee: &Committee,
    block_writer: &mut TestBlockWriter,
    start: Option<Vec<BlockReference>>,
    stop: RoundNumber,
) -> Vec<BlockReference> {
    let mut includes = match start {
        Some(start) => {
            assert!(!start.is_empty());
            assert_eq!(
                start.iter().map(|x| x.round).max(),
                start.iter().map(|x| x.round).min()
            );
            start
        }
        None => {
            let (references, genesis): (Vec<_>, Vec<_>) = committee
                .authorities()
                .map(|index| StatementBlock::new_genesis(index))
                .map(|block| (*block.reference(), block))
                .unzip();
            block_writer.add_blocks(genesis);
            references
        }
    };

    let starting_round = includes.first().unwrap().round + 1;
    for round in starting_round..=stop {
        let (references, blocks): (Vec<_>, Vec<_>) = committee
            .authorities()
            .map(|authority| {
                let block = Data::new(StatementBlock::new(
                    authority,
                    round,
                    includes.clone(),
                    vec![],
                    0,
                    false,
                    Default::default(),
                ));
                (*block.reference(), block)
            })
            .unzip();
        block_writer.add_blocks(blocks);
        includes = references;
    }

    includes
}

pub fn build_dag_layer(
    // A list of (authority, parents) pairs. For each authority, we add a block linking to the
    // specified parents.
    connections: Vec<(AuthorityIndex, Vec<BlockReference>)>,
    block_writer: &mut TestBlockWriter,
) -> Vec<BlockReference> {
    let mut references = Vec::new();
    for (authority, parents) in connections {
        let round = parents.first().unwrap().round + 1;
        let block = Data::new(StatementBlock::new(
            authority,
            round,
            parents,
            vec![],
            0,
            false,
            Default::default(),
        ));

        references.push(*block.reference());
        block_writer.add_block(block);
    }
    references
}
