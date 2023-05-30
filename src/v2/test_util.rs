// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::v2::block_handler::{BlockHandler, TestBlockHandler};
use crate::v2::committee::Committee;
use crate::v2::core::Core;
use crate::v2::net_sync::NetworkSyncer;
use crate::v2::network::Network;
use crate::v2::syncer::{Syncer, SyncerSignals};
use crate::v2::types::{AuthorityIndex, BlockReference, TransactionId};
use futures::future::join_all;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

pub fn committee_and_cores() -> (Arc<Committee>, Vec<Core<TestBlockHandler>>) {
    let committee = Committee::new(vec![1, 1, 1, 1]);
    let cores: Vec<_> = committee
        .authorities()
        .map(|authority| {
            let last_transaction = first_transaction_for_authority(authority);
            let block_handler =
                TestBlockHandler::new(last_transaction, committee.clone(), authority);
            let mut core = Core::new(block_handler, authority, committee.clone());
            core.add_blocks(committee.genesis_blocks(core.authority()));
            core
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

pub fn committee_and_syncers() -> (
    Arc<Committee>,
    Vec<Syncer<TestBlockHandler, bool, Vec<BlockReference>>>,
) {
    let (committee, cores) = committee_and_cores();
    (
        committee,
        cores
            .into_iter()
            .map(|core| Syncer::new(core, 3, Default::default(), Default::default()))
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

pub async fn network_syncers() -> Vec<NetworkSyncer<TestBlockHandler, Vec<BlockReference>>> {
    let (_committee, cores) = committee_and_cores();
    let (networks, _) = networks_and_addresses(cores.len()).await;
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let network_syncer = NetworkSyncer::start(network, core, 3, vec![]);
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
    syncers: &[Syncer<H, S, Vec<BlockReference>>],
) {
    let commits = syncers.iter().map(|state| state.commit_observer());
    let zero_commit = vec![];
    let mut max_commit = &zero_commit;
    for commit in commits {
        if commit.len() >= max_commit.len() {
            if is_prefix(&max_commit, &commit) {
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

fn is_prefix(short: &[BlockReference], long: &[BlockReference]) -> bool {
    assert!(short.len() <= long.len());
    for (a, b) in short.iter().zip(long.iter().take(short.len())) {
        if a != b {
            return false;
        }
    }
    return true;
}
