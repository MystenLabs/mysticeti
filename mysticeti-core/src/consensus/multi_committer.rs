use std::{cmp::max, collections::HashMap, sync::Arc};

use futures::future::pending;

use crate::{block_store::BlockStore, committee::Committee, metrics::Metrics, types::RoundNumber};

use super::{base_committer::BaseCommitter, Committer, LeaderStatus};

pub struct MultiCommitter {
    wave_length: RoundNumber,
    number_of_leaders: usize,
    committers: Vec<BaseCommitter>,
}

impl MultiCommitter {
    pub fn new(
        wave_length: RoundNumber,
        number_of_leaders: usize,
        committee: Arc<Committee>,
        block_store: BlockStore,
        metrics: Arc<Metrics>,
    ) -> Self {
        let committers: Vec<_> = (0..number_of_leaders)
            .map(|i| {
                BaseCommitter::new(committee.clone(), block_store.clone(), metrics.clone())
                    .with_wave_length(wave_length)
                    .with_leader_number(i)
            })
            .collect();

        Self {
            wave_length,
            number_of_leaders,
            committers,
        }
    }
}

impl Committer for MultiCommitter {
    fn try_commit(&self, last_committer_round: RoundNumber) -> Vec<LeaderStatus> {
        let mut pending_queue = HashMap::new();

        for committer in &self.committers {
            for leader in committer.try_commit(last_committer_round) {
                let round = leader.round();
                tracing::debug!("{committer} decided {leader:?}");
                let key = (round, committer.leader_number());
                pending_queue.insert(key, leader);
            }
        }

        // The very first leader to commit has round = wave_length.
        let mut r = max(self.wave_length, last_committer_round + 1);

        // Collect all leaders in order, and stop when we find a gap.
        let mut sequence = Vec::new();
        'main: loop {
            for i in 0..self.number_of_leaders {
                let key = (r, i);
                if !pending_queue.contains_key(&key) {
                    break 'main;
                }
            }
            for i in 0..self.number_of_leaders {
                let key = (r, i);
                let leader = pending_queue.remove(&key).unwrap();
                sequence.push(leader);
            }
            r += 1;
        }
        sequence
    }
}
