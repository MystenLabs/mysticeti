// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::v2::block_handler::TestBlockHandler;
use crate::v2::committee::Committee;
use crate::v2::core::Core;
use crate::v2::syncer::Syncer;
use crate::v2::types::{AuthorityIndex, BlockReference, TransactionId};
use rand::rngs::StdRng;
use rand::SeedableRng;
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

pub fn rng_at_seed(seed: u64) -> StdRng {
    let bytes = seed.to_le_bytes();
    let mut seed = [0u8; 32];
    seed[..bytes.len()].copy_from_slice(&bytes);
    StdRng::from_seed(seed)
}
