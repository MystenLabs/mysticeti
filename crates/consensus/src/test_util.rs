// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, sync::Arc};

use dag::{
    authority::Authority,
    committee::Committee,
    context::Ctx,
    core::{Core, block_handler::RealBlockHandler},
    crypto::CryptoEngine,
    metrics::Metrics,
    storage::Storage,
    test_util::committee,
};

use crate::{committer::Committer, protocol::ConsensusProtocol};

fn open_core<C: Ctx>(
    authority: Authority,
    committee: &Arc<Committee>,
    path: Option<&Path>,
) -> Core<C, Committer> {
    let metrics = Metrics::new_for_test(committee.len());
    let (storage, recovered) = if let Some(path) = path {
        let wal_path = path.join(format!("{:03}.wal", authority));
        Storage::open(authority, &wal_path, metrics.clone(), committee)
            .expect("Failed to open storage")
    } else {
        Storage::new_for_tests(authority, metrics.clone(), committee)
    };
    let protocol = ConsensusProtocol::default().to_protocol(committee.total_stake());
    let committer = Committer::new(committee.clone(), storage.block_reader().clone(), protocol);
    let (block_handler, _tx_sender) = RealBlockHandler::new(metrics.clone());
    let crypto = CryptoEngine::disabled();
    Core::open(
        block_handler,
        authority,
        committee.clone(),
        metrics,
        storage,
        recovered,
        false,
        committer,
        crypto,
    )
}

pub fn committee_and_cores<C: Ctx>(n: usize) -> (Arc<Committee>, Vec<Core<C, Committer>>) {
    committee_and_cores_persisted(n, None)
}

pub fn committee_and_cores_persisted<C: Ctx>(
    n: usize,
    path: Option<&Path>,
) -> (Arc<Committee>, Vec<Core<C, Committer>>) {
    let committee = committee(n);
    let cores = committee
        .authorities()
        .map(|authority| open_core(authority, &committee, path))
        .collect();
    (committee, cores)
}
