// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp::max, collections::HashMap, fmt::Display, sync::Arc};

use crate::{block_store::BlockStore, committee::Committee, metrics::Metrics, types::RoundNumber};

use super::{
    base_committer::{BaseCommitter, BaseCommitterOptions},
    Committer, LeaderStatus, DEFAULT_WAVE_LENGTH,
};

pub struct MultiCommitterBuilder {
    committee: Arc<Committee>,
    block_store: BlockStore,
    metrics: Arc<Metrics>,
    wave_length: RoundNumber,
    number_of_leaders: usize,
    round_offset: RoundNumber,
}

impl MultiCommitterBuilder {
    pub fn new(committee: Arc<Committee>, block_store: BlockStore, metrics: Arc<Metrics>) -> Self {
        Self {
            committee,
            block_store,
            metrics,
            wave_length: DEFAULT_WAVE_LENGTH,
            number_of_leaders: 1,
            round_offset: 0,
        }
    }

    pub fn with_wave_length(mut self, wave_length: RoundNumber) -> Self {
        self.wave_length = wave_length;
        self
    }

    pub fn with_number_of_leaders(mut self, number_of_leaders: usize) -> Self {
        self.number_of_leaders = number_of_leaders;
        self
    }

    pub fn with_round_offset(mut self, round_offset: RoundNumber) -> Self {
        self.round_offset = round_offset;
        self
    }

    pub fn build(self) -> MultiCommitter {
        let committers: Vec<_> = (0..self.number_of_leaders)
            .map(|i| {
                let options = BaseCommitterOptions {
                    wave_length: self.wave_length,
                    leader_offset: i as u64,
                    round_offset: self.round_offset,
                };
                BaseCommitter::new(
                    self.committee.clone(),
                    self.block_store.clone(),
                    self.metrics.clone(),
                )
                .with_options(options)
            })
            .collect();

        MultiCommitter {
            wave_length: self.wave_length,
            round_offset: self.round_offset,
            committers,
        }
    }
}

pub struct MultiCommitter {
    wave_length: RoundNumber,
    round_offset: RoundNumber,
    committers: Vec<BaseCommitter>,
}

impl Committer for MultiCommitter {
    fn try_commit(&self, last_committed_round: RoundNumber) -> Vec<LeaderStatus> {
        // Run all committers and collect their output.
        let mut pending_queue = HashMap::new();
        for committer in &self.committers {
            for leader in committer.try_commit(last_committed_round) {
                let round = leader.round();
                tracing::debug!("{committer} decided {leader:?}");
                let key = (round, committer.leader_offset());
                pending_queue.insert(key, leader);
            }
        }

        // The very first leader to commit has round = wave_length.
        let mut r = max(self.wave_length, last_committed_round + 1);

        // Collect all leaders in order, and stop when we find a gap.
        let mut sequence = Vec::new();
        'main: loop {
            println!("r: {pending_queue:?}");
            // Ensure we can commit the entire round.
            // TODO: We should be able to commit partial rounds, but then this function would
            // need more granular information about the last committed state.
            for i in 0..self.committers.len() {
                let key = (r, i as u64);
                if !pending_queue.contains_key(&key) {
                    break 'main;
                }
            }
            // Commit the entire round, in order by leader.
            for i in 0..self.committers.len() {
                let key = (r, i as u64);
                let leader = pending_queue.remove(&key).unwrap();
                sequence.push(leader);
            }
            r += 1;
        }
        sequence
    }
}

impl Display for MultiCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiCommitter({})", self.round_offset)
    }
}
