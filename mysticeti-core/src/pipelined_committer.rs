// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use crate::metrics::Metrics;
use crate::{
    block_store::BlockStore,
    committer::{Committer, LeaderStatus},
};
use crate::{committee::Committee, types::RoundNumber};

pub struct PipelinedCommitter {
    committers: Vec<Committer>,
}

impl PipelinedCommitter {
    /// Create a new [`Committer`] interpreting the dag using the provided committee and wave length.
    pub fn new(
        committee: Arc<Committee>,
        block_store: BlockStore,
        wave_length: u64,
        metrics: Arc<Metrics>,
    ) -> Self {
        assert!(wave_length >= 3);

        let committers = (0..wave_length)
            .map(|i| {
                Committer::new(
                    committee.clone(),
                    block_store.clone(),
                    wave_length,
                    metrics.clone(),
                )
                .with_offset(i)
            })
            .collect();

        Self { committers }
    }

    pub fn try_commit(&self, last_committer_round: RoundNumber) -> Vec<LeaderStatus> {
        let mut pending_queue = HashMap::new();

        for committer in &self.committers {
            for leader in committer.try_commit(last_committer_round) {
                let round = leader.round();
                pending_queue.insert(round, leader);
            }
        }

        let mut r = last_committer_round + 1;
        let mut sequence = Vec::new();
        loop {
            match pending_queue.remove(&r) {
                Some(leader) => sequence.push(leader),
                None => break,
            }
            r += 1;
        }

        sequence
    }
}
