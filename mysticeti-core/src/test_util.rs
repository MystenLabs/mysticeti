// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_handler::{BlockHandler, TestBlockHandler};
use crate::block_store::{BlockStore, BlockWriter, OwnBlockData, WAL_ENTRY_BLOCK};
use crate::committee::Committee;
use crate::core::{Core, CoreOptions};
use crate::data::Data;
#[cfg(feature = "simulator")]
use crate::future_simulator::OverrideNodeContext;
use crate::net_sync::NetworkSyncer;
use crate::network::Network;
#[cfg(feature = "simulator")]
use crate::simulated_network::SimulatedNetwork;
use crate::syncer::{Syncer, SyncerSignals};
use crate::types::{
    format_authority_index, AuthorityIndex, BlockReference, StatementBlock, TransactionId,
};
use crate::wal::{open_file_for_wal, walf, WalPosition, WalWriter};
use crate::{block_handler::TestCommitHandler, metrics::Metrics};
use futures::future::join_all;
use prometheus::Registry;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;

pub fn test_metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new(&Registry::new()))
}

pub fn committee(n: usize) -> Arc<Committee> {
    Committee::new(vec![1; n])
}

pub fn committee_and_cores(n: usize) -> (Arc<Committee>, Vec<Core<TestBlockHandler>>) {
    committee_and_cores_persisted(n, None, None)
}

pub fn committee_and_cores_persisted(
    n: usize,
    path: Option<&Path>,
    block_handlers: Option<Vec<TestBlockHandler>>,
) -> (Arc<Committee>, Vec<Core<TestBlockHandler>>) {
    let committee = committee(n);
    let mut block_handler_iter = block_handlers.map(Vec::into_iter);
    let cores: Vec<_> = committee
        .authorities()
        .map(|authority| {
            let last_transaction = first_transaction_for_authority(authority);
            let block_handler = if let Some(block_handler_iter) = block_handler_iter.as_mut() {
                block_handler_iter.next().unwrap()
            } else {
                TestBlockHandler::new(last_transaction, committee.clone(), authority)
            };
            let wal_file = if let Some(path) = path {
                let wal_path = path.join(format!("{:03}.wal", authority));
                open_file_for_wal(&wal_path).unwrap()
            } else {
                tempfile::tempfile().unwrap()
            };
            Core::open(
                block_handler,
                authority,
                committee.clone(),
                test_metrics(),
                wal_file,
                CoreOptions::test(),
            )
        })
        .collect();
    (committee, cores)
}

fn first_transaction_for_authority(authority: AuthorityIndex) -> TransactionId {
    authority * 1_000_000
}

pub fn first_n_transactions(committee: &Committee, n: u64) -> Vec<TransactionId> {
    let mut result = vec![];
    for authority in committee.authorities() {
        let first = first_transaction_for_authority(authority);
        result.extend(first + 1..first + n + 1)
    }
    result
}

pub fn committee_and_syncers(
    n: usize,
) -> (
    Arc<Committee>,
    Vec<Syncer<TestBlockHandler, bool, TestCommitHandler>>,
) {
    let (committee, cores) = committee_and_cores(n);
    (
        committee.clone(),
        cores
            .into_iter()
            .map(|core| {
                let commit_handler = TestCommitHandler::new(
                    committee.clone(),
                    core.block_handler().transaction_time.clone(),
                );
                Syncer::new(core, 3, Default::default(), commit_handler, test_metrics())
            })
            .collect(),
    )
}

pub async fn networks_and_addresses(n: usize) -> (Vec<Network>, Vec<SocketAddr>) {
    let host = Ipv4Addr::LOCALHOST;
    let addresses: Vec<_> = (0..n)
        .map(|i| SocketAddr::V4(SocketAddrV4::new(host, 5001 + i as u16)))
        .collect();
    let networks = addresses
        .iter()
        .enumerate()
        .map(|(i, address)| Network::from_socket_addresses(&addresses, i, *address));
    let networks = join_all(networks).await;
    (networks, addresses)
}

#[cfg(feature = "simulator")]
pub fn simulated_network_syncers(
    n: usize,
) -> (
    SimulatedNetwork,
    Vec<NetworkSyncer<TestBlockHandler, TestCommitHandler>>,
) {
    let (committee, cores) = committee_and_cores(n);
    let (simulated_network, networks) = SimulatedNetwork::new(&committee);
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = TestCommitHandler::new(
            committee.clone(),
            core.block_handler().transaction_time.clone(),
        );
        let node_context = OverrideNodeContext::enter(Some(core.authority()));
        let network_syncer = NetworkSyncer::start(network, core, 3, commit_handler, test_metrics());
        drop(node_context);
        network_syncers.push(network_syncer);
    }
    (simulated_network, network_syncers)
}

pub async fn network_syncers(n: usize) -> Vec<NetworkSyncer<TestBlockHandler, TestCommitHandler>> {
    let (committee, cores) = committee_and_cores(n);
    let (networks, _) = networks_and_addresses(cores.len()).await;
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = TestCommitHandler::new(
            committee.clone(),
            core.block_handler().transaction_time.clone(),
        );
        let network_syncer = NetworkSyncer::start(network, core, 3, commit_handler, test_metrics());
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
    syncers: &[Syncer<H, S, TestCommitHandler>],
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
pub fn print_stats<S: SyncerSignals>(syncers: &[Syncer<TestBlockHandler, S, TestCommitHandler>]) {
    eprintln!("val ||    cert(ms)   ||cert commit(ms)|| tx commit(ms) |");
    eprintln!("    ||  p90  |  avg  ||  p90  |  avg  ||  p90  |  avg  |");
    syncers.iter().for_each(|s| {
        let b = s.core().block_handler();
        let c = s.commit_observer();
        eprintln!(
            "  {} || {:05} | {:05} || {:05} | {:05} || {:05} | {:05} |",
            format_authority_index(s.core().authority()),
            b.transaction_certified_latency
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            b.transaction_certified_latency
                .avg()
                .unwrap_or_default()
                .as_millis(),
            c.certificate_committed_latency
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            c.certificate_committed_latency
                .avg()
                .unwrap_or_default()
                .as_millis(),
            c.transaction_committed_latency
                .pct(900)
                .unwrap_or_default()
                .as_millis(),
            c.transaction_committed_latency
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
    pub fn new() -> Self {
        let file = tempfile::tempfile().unwrap();
        let (wal_writer, wal_reader) = walf(file).unwrap();
        let (block_store, _, _) = BlockStore::open(Arc::new(wal_reader), &wal_writer);
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

impl Default for TestBlockWriter {
    fn default() -> Self {
        Self::new()
    }
}
