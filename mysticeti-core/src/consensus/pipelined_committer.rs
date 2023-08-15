// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp::max, collections::HashMap, sync::Arc};

use crate::metrics::Metrics;
use crate::{block_store::BlockStore, consensus::base_committer::BaseCommitter};
use crate::{committee::Committee, types::RoundNumber};

use super::{base_committer::BaseCommitterOptions, Committer, LeaderStatus};

/// The [`PipelinedCommitter`] uses three [`BaseCommitter`] instances, each shifted by one round,
/// to commit every dag round (in the ideal case).
pub struct PipelinedCommitter {
    committers: Vec<BaseCommitter>,
    wave_length: RoundNumber,
}

impl PipelinedCommitter {
    pub fn new(
        committee: Arc<Committee>,
        block_store: BlockStore,
        wave_length: u64,
        metrics: Arc<Metrics>,
    ) -> Self {
        let committers = (0..wave_length)
            .map(|i| {
                let options = BaseCommitterOptions {
                    round_offset: i as u64,
                    wave_length,
                    ..Default::default()
                };
                BaseCommitter::new(committee.clone(), block_store.clone(), metrics.clone())
                    .with_options(options)
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

        // Try to commit the leaders of a all pipelines.
        for committer in &self.committers {
            for leader in committer.try_commit(last_committer_round) {
                let round = leader.round();
                tracing::debug!("{committer} decided {leader:?}");
                pending_queue.insert(round, leader);
            }
        }

        // The very first leader to commit has round = wave_length.
        let mut r = max(self.wave_length, last_committer_round + 1);

        // Collect all leaders in order, and stop when we find a gap.
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
        data::Data,
        test_util::{build_dag, build_dag_layer, committee, test_metrics, TestBlockWriter},
        types::StatementBlock,
    };

    /// Commit one leader.
    #[test]
    #[tracing_test::traced_test]
    fn direct_commit() {
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
        tracing::info!("Commit sequence: {sequence:?}");

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
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }

    /// Commit one by one each leader as the dag progresses in ideal conditions.
    #[test]
    #[tracing_test::traced_test]
    fn multiple_direct_commit() {
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
            tracing::info!("Commit sequence: {sequence:?}");

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

    /// Commit 10 leaders in a row (9 of them are committed recursively).
    #[test]
    #[tracing_test::traced_test]
    fn indirect_commit() {
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
        tracing::info!("Commit sequence: {sequence:?}");

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
    fn no_genesis_commit() {
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
            tracing::info!("Commit sequence: {sequence:?}");
            assert!(sequence.is_empty());
        }
    }

    /// We do not commit anything if we miss the first leader.
    #[test]
    #[tracing_test::traced_test]
    fn no_leader() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to finish wave 0.
        let decision_round_0 = wave_length - 1;
        let references = build_dag(&committee, &mut block_writer, None, decision_round_0);

        // Add enough blocks to reach the decision round of wave 1 (but without it leader),
        // as viewed by the first pipeline.
        let leader_round_1 = wave_length;
        let leader_1 = committee.elect_leader(leader_round_1);

        let (references, blocks): (Vec<_>, Vec<_>) = committee
            .authorities()
            .filter(|&authority| authority != leader_1)
            .map(|authority| {
                let block = Data::new(StatementBlock::new(
                    authority,
                    leader_round_1,
                    references.clone(),
                    vec![],
                    0,
                    false,
                    Default::default(),
                ));
                (*block.reference(), block)
            })
            .unzip();

        block_writer.add_blocks(blocks);

        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            decision_round_1,
        );

        // Ensure no blocks are committed.
        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        if let LeaderStatus::Skip(round) = sequence[0] {
            assert_eq!(round, leader_round_1);
        } else {
            panic!("Expected to directly skip the leader");
        }
    }

    /// We directly skip the leader if it has enough blame.
    #[test]
    #[tracing_test::traced_test]
    fn direct_skip() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the first leader, as seen by the first pipeline.
        let leader_round_1 = wave_length;
        let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

        // Filter out that leader.
        let references_1_without_leader: Vec<_> = references_1
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(leader_round_1))
            .collect();

        // Add enough blocks to reach the decision round of the first wave, as seen by the
        // first pipeline.
        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_1_without_leader),
            decision_round_1,
        );

        // Ensure no blocks are committed.
        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        if let LeaderStatus::Skip(round) = sequence[0] {
            assert_eq!(round, leader_round_1);
        } else {
            panic!("Expected to directly skip the leader");
        }
    }

    /// Commit the leaders of wave 1 and 3 while the leader of wave 2 is missing, as seen by the
    /// first pipeline.
    #[test]
    #[tracing_test::traced_test]
    fn indirect_skip() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 2, as seen by the first pipeline.
        let leader_round_2 = 2 * wave_length;
        let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

        // Filter out that leader.
        let references_2_without_leader: Vec<_> = references_2
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(leader_round_2))
            .collect();

        // Add enough blocks to reach the decision round of wave 3, as seen by the first pipeline.
        let decision_round_3 = 4 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_2_without_leader),
            decision_round_3,
        );

        // Ensure we commit the leaders of wave 1 and 3, as seen by the first pipeline.
        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), 7);

        // Ensure we commit the leaders of wave 1.
        for i in 0..=2 {
            let leader_round_1 = wave_length + i;
            let leader_1 = committee.elect_leader(leader_round_1);
            if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
                assert_eq!(block.author(), leader_1);
            } else {
                panic!("Expected a committed leader")
            };
        }

        // Ensure we skip the leader of wave 2 (first pipeline) but commit the others.
        let leader_round_2 = 2 * wave_length;
        if let LeaderStatus::Skip(round) = sequence[3] {
            assert_eq!(round, leader_round_2);
        } else {
            panic!("Expected a skipped leader")
        }

        for i in 4..=5 {
            let leader_round_2 = wave_length + i;
            let leader_2 = committee.elect_leader(leader_round_2);
            if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
                assert_eq!(block.author(), leader_2);
            } else {
                panic!("Expected a committed leader")
            };
        }

        // Ensure we commit the leader of wave 3.
        let leader_round_3 = 3 * wave_length;
        let leader_3 = committee.elect_leader(leader_round_3);
        if let LeaderStatus::Commit(ref block) = sequence[6] {
            assert_eq!(block.author(), leader_3);
        } else {
            panic!("Expected a committed leader")
        }
    }

    /// Commit all leaders of wave 1; then miss the first leader of wave 2 (first pipeline) and
    /// ensure the other leaders of wave 2 are not committed.
    #[test]
    #[tracing_test::traced_test]
    fn unblocked_pipeline_through_direct_skip() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 2, as seen by the first pipeline.
        let leader_round_2 = 2 * wave_length;
        let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

        // Filter out the first leader of wave 2.
        let references_2_without_leader: Vec<_> = references_2
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(leader_round_2))
            .collect();

        // Add enough blocks to reach the voting round of wave 3, as seen by the first pipeline.
        let voting_round_3 = (4 * wave_length - 1) - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_2_without_leader),
            voting_round_3,
        );

        // Ensure we commit the leaders of waves 1 and 2, except the leader we skipped.
        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), 6);

        // Ensure we commit the leaders of wave 1.
        for i in 0..=2 {
            let leader_round_1 = wave_length + i;
            let leader_1 = committee.elect_leader(leader_round_1);
            if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
                assert_eq!(block.author(), leader_1);
            } else {
                panic!("Expected a committed leader")
            };
        }

        // Ensure we skip the first leader of wave 2.
        let leader_round_2 = 2 * wave_length;
        if let LeaderStatus::Skip(round) = sequence[3] {
            assert_eq!(round, leader_round_2);
        } else {
            panic!("Expected to skip the leader")
        };

        // Ensure we commit the leaders of wave 2.
        for i in 4..=5 {
            let leader_round_2 = wave_length + i;
            let leader_2 = committee.elect_leader(leader_round_2);
            if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
                assert_eq!(block.author(), leader_2);
            } else {
                panic!("Expected a committed leader")
            };
        }
    }

    /// Commit all leaders of wave 1; then miss the first leader of wave 2 (first pipeline) and
    /// ensure the other leaders of wave 2 are not committed.
    #[test]
    #[tracing_test::traced_test]
    fn blocked_pipeline() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 2, as seen by the first pipeline.
        let leader_round_2 = 2 * wave_length;
        let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

        // Filter out the first leader of wave 2.
        let references_2_without_leader: Vec<_> = references_2
            .iter()
            .cloned()
            .filter(|x| x.authority != committee.elect_leader(leader_round_2))
            .collect();

        // Create a dag layer where only one authority votes for that leader.
        let mut authorities = committee.authorities();
        let leader_connection = vec![(authorities.next().unwrap(), references_2)];
        let non_leader_connections: Vec<_> = authorities
            .take((committee.quorum_threshold() - 1) as usize)
            .map(|authority| (authority, references_2_without_leader.clone()))
            .collect();

        let connections = leader_connection.into_iter().chain(non_leader_connections);
        let references = build_dag_layer(connections.collect(), &mut block_writer);

        // Add enough blocks to reach the voting round of wave 3, as seen by the first pipeline.
        let voting_round_3 = (4 * wave_length - 1) - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            voting_round_3,
        );

        // Ensure we commit the leaders of wave 1 and 3, as seen by the first pipeline.
        let committer = PipelinedCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), 3);

        // Ensure we commit the leaders of wave 1.
        for i in 0..=2 {
            let leader_round_1 = wave_length + i;
            let leader_1 = committee.elect_leader(leader_round_1);
            if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
                assert_eq!(block.author(), leader_1);
            } else {
                panic!("Expected a committed leader")
            };
        }
    }
}
