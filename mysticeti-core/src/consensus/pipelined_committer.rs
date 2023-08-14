// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp::max, collections::HashMap, sync::Arc};

use crate::metrics::Metrics;
use crate::{block_store::BlockStore, consensus::base_committer::BaseCommitter};
use crate::{committee::Committee, types::RoundNumber};

use super::{Committer, LeaderStatus};

pub struct PipelinedCommitter {
    committers: Vec<BaseCommitter>,
    wave_length: RoundNumber,
}

impl PipelinedCommitter {
    /// Create a new [`BaseCommitter`] interpreting the dag using the provided committee and wave length.
    pub fn new(
        committee: Arc<Committee>,
        block_store: BlockStore,
        wave_length: u64,
        metrics: Arc<Metrics>,
    ) -> Self {
        let committers = (0..wave_length)
            .map(|i| {
                BaseCommitter::new(committee.clone(), block_store.clone(), metrics.clone())
                    .with_wave_length(wave_length)
                    .with_offset(i)
            })
            .collect();

        Self {
            committers,
            wave_length,
        }
    }
}

impl Committer for PipelinedCommitter {
    fn try_commit(&self, last_committer_round: RoundNumber) -> Vec<LeaderStatus> {
        let mut pending_queue = HashMap::new();

        for committer in &self.committers {
            for leader in committer.try_commit(last_committer_round) {
                let round = leader.round();
                tracing::debug!("{committer} decided {leader:?}");
                pending_queue.insert(round, leader);
            }
        }

        // The first leader to commit has round = wave_length.
        let mut r = max(self.wave_length, last_committer_round + 1);

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

#[cfg(test)]
mod test {
    use crate::{
        consensus::{
            pipelined_committer::PipelinedCommitter, Committer, LeaderStatus, DEFAULT_WAVE_LENGTH,
        },
        test_util::{build_dag, committee, test_metrics, TestBlockWriter},
    };

    /// Commit one leader.
    #[test]
    #[tracing_test::traced_test]
    fn commit_one() {
        let committee = committee(4);

        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            DEFAULT_WAVE_LENGTH,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), 1);
        if let LeaderStatus::Commit(ref block) = sequence[0] {
            assert_eq!(block.author(), committee.elect_leader(3))
        } else {
            panic!("Expected a committed leader")
        };
    }

    /// Ensure idempotent replies.
    #[test]
    #[tracing_test::traced_test]
    fn idempotence() {
        let committee = committee(4);

        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            DEFAULT_WAVE_LENGTH,
            test_metrics(),
        );

        let last_committed_round = 3;
        let sequence = committer.try_commit(last_committed_round);
        assert!(sequence.is_empty());
    }

    /// Commit 10 leaders in a row (9 of them are committed recursively).
    #[test]
    #[tracing_test::traced_test]
    fn commit_10() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let n = 10;
        let enough_blocks = (wave_length - 1) + n + (wave_length - 1);
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, enough_blocks);

        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), n as usize);
        for (i, leader_block) in sequence.iter().enumerate() {
            let leader_round = wave_length + i as u64;
            if let LeaderStatus::Commit(ref block) = leader_block {
                assert_eq!(block.author(), committee.elect_leader(leader_round));
            } else {
                panic!("Expected a committed leader")
            };
        }
    }

    /// Do not commit anything if we are still in the first wave. Remember that the first leader
    /// has round = wave_length.
    #[test]
    #[tracing_test::traced_test]
    fn commit_none() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let first_commit_round = 2 * wave_length - 1;
        for r in 0..first_commit_round {
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, r);

            let committer = PipelinedCommitter::new(
                committee.clone(),
                block_writer.into_block_store(),
                wave_length,
                test_metrics(),
            );

            let last_committed_round = 0;
            let sequence = committer.try_commit(last_committed_round);
            assert!(sequence.is_empty());
        }
    }

    /// Commit one by one each leader as the dag progresses in ideal conditions.
    #[test]
    #[tracing_test::traced_test]
    fn commit_incremental() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut last_committed_round = 0;
        for n in 1..=10 {
            let enough_blocks = (wave_length - 1) + n + (wave_length - 1);
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, enough_blocks);

            let committer = PipelinedCommitter::new(
                committee.clone(),
                block_writer.into_block_store(),
                wave_length,
                test_metrics(),
            );

            let sequence = committer.try_commit(last_committed_round);

            assert_eq!(sequence.len(), 1);
            let leader_round = (wave_length - 1) + n as u64;
            if let LeaderStatus::Commit(ref block) = sequence[0] {
                assert_eq!(block.author(), committee.elect_leader(leader_round));
            } else {
                panic!("Expected a committed leader")
            }

            last_committed_round = leader_round;
        }
    }
}
