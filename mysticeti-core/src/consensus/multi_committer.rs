// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Display, sync::Arc};

use crate::{
    block_store::BlockStore,
    committee::Committee,
    metrics::Metrics,
    types::{AuthorityIndex, RoundNumber},
};

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
            round_offset: self.round_offset,
            committers,
        }
    }
}

pub struct MultiCommitter {
    round_offset: RoundNumber,
    committers: Vec<BaseCommitter>,
}

impl Committer for MultiCommitter {
    fn try_commit(&self, last_committed_round: RoundNumber) -> Vec<LeaderStatus> {
        // Run all committers and collect their output.
        let mut pending_queue = BTreeMap::new();
        for committer in &self.committers {
            for leader in committer.try_commit(last_committed_round) {
                tracing::debug!("[{self}] {committer} decided {leader:?}");
                pending_queue
                    .entry(leader.round())
                    .or_insert_with(Vec::new)
                    .push(leader);
            }
        }

        // Collect all leaders in order, as long as we have all leaders.
        let mut sequence = Vec::new();
        for leaders in pending_queue.into_values() {
            if leaders.len() == self.committers.len() {
                sequence.extend(leaders);
            }
        }
        sequence
    }

    fn leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        let mut leaders = vec![];
        // todo - this can be slightly optimized as all base committers here either return one value or zero depending on whether current round is a leader round for the wave
        for committer in &self.committers {
            leaders.append(&mut committer.leaders(round));
        }
        leaders
    }
}

impl Display for MultiCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiCommitter(R{})", self.round_offset)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        consensus::{
            multi_committer::MultiCommitterBuilder, Committer, LeaderStatus, DEFAULT_WAVE_LENGTH,
        },
        test_util::{build_dag, build_dag_layer, committee, test_metrics, TestBlockWriter},
    };

    /// Commit the leaders of the first wave.
    #[test]
    #[tracing_test::traced_test]
    fn direct_commit() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        for number_of_leaders in 1..committee.len() {
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, 5);

            let committer = MultiCommitterBuilder::new(
                committee.clone(),
                block_writer.into_block_store(),
                test_metrics(),
            )
            .with_wave_length(wave_length)
            .with_number_of_leaders(number_of_leaders)
            .build();

            let last_committed_round = 0;
            let sequence = committer.try_commit(last_committed_round);
            tracing::info!("Commit sequence: {sequence:?}");

            assert_eq!(sequence.len(), number_of_leaders);
            for (i, leader) in sequence.iter().enumerate() {
                if let LeaderStatus::Commit(block) = leader {
                    let leader_round = wave_length;
                    let leader_offset = i as u64;
                    let expected = committee.elect_leader(leader_round + leader_offset);
                    assert_eq!(block.author(), expected);
                } else {
                    panic!("Expected a committed leader")
                };
            }
        }
    }

    /// Ensure idempotent replies.
    #[test]
    #[tracing_test::traced_test]
    fn idempotence() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        for number_of_leaders in 1..committee.len() {
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, 5);

            let committer = MultiCommitterBuilder::new(
                committee.clone(),
                block_writer.into_block_store(),
                test_metrics(),
            )
            .with_wave_length(wave_length)
            .with_number_of_leaders(number_of_leaders)
            .build();

            let last_committed_round = DEFAULT_WAVE_LENGTH;
            let sequence = committer.try_commit(last_committed_round);
            tracing::info!("Commit sequence: {sequence:?}");
            assert!(sequence.is_empty());
        }
    }

    /// Commit one by one each wave as the dag progresses in ideal conditions.
    #[test]
    #[tracing_test::traced_test]
    fn multiple_direct_commit() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        let number_of_leaders = committee.quorum_threshold() as usize;

        let mut last_committed_round = 0;
        for n in 1..=10 {
            let enough_blocks = wave_length * (n + 1) - 1;
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, enough_blocks);

            let committer = MultiCommitterBuilder::new(
                committee.clone(),
                block_writer.into_block_store(),
                test_metrics(),
            )
            .with_wave_length(wave_length)
            .with_number_of_leaders(number_of_leaders)
            .build();

            let sequence = committer.try_commit(last_committed_round);

            assert_eq!(sequence.len(), number_of_leaders);
            tracing::info!("Commit sequence: {sequence:?}");

            let leader_round = n as u64 * wave_length;
            for (i, leader) in sequence.iter().enumerate() {
                if let LeaderStatus::Commit(block) = leader {
                    let leader_offset = i as u64;
                    let expected = committee.elect_leader(leader_round + leader_offset);
                    assert_eq!(block.author(), expected);
                } else {
                    panic!("Expected a committed leader")
                };
            }

            last_committed_round = leader_round;
        }
    }

    /// Commit 10 waves in a row (9 of them are committed recursively).
    #[test]
    #[tracing_test::traced_test]
    fn indirect_commit() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        let number_of_leaders = committee.quorum_threshold() as usize;

        let n = 10;
        let enough_blocks = wave_length * (n + 1) - 1;
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, enough_blocks);

        let committer = MultiCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_number_of_leaders(number_of_leaders)
        .build();

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), number_of_leaders * n as usize);
        for (i, leaders) in sequence.chunks(number_of_leaders).enumerate() {
            let leader_round = (i as u64 + 1) * wave_length;
            for (j, leader) in leaders.iter().enumerate() {
                if let LeaderStatus::Commit(block) = leader {
                    let leader_offset = j as u64;
                    let expected = committee.elect_leader(leader_round + leader_offset);
                    assert_eq!(block.author(), expected);
                } else {
                    panic!("Expected a committed leader")
                }
            }
        }
    }

    /// Do not commit anything if we are still in the first wave.
    #[test]
    #[tracing_test::traced_test]
    fn no_genesis_commit() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        let number_of_leaders = committee.quorum_threshold() as usize;

        let first_commit_round = 2 * wave_length - 1;
        for r in 0..first_commit_round {
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, r);

            let committer = MultiCommitterBuilder::new(
                committee.clone(),
                block_writer.into_block_store(),
                test_metrics(),
            )
            .with_wave_length(wave_length)
            .with_number_of_leaders(number_of_leaders)
            .build();

            let last_committed_round = 0;
            let sequence = committer.try_commit(last_committed_round);
            tracing::info!("Commit sequence: {sequence:?}");
            assert!(sequence.is_empty());
        }
    }

    /// We directly skip a leader that has enough blames and commit the others
    #[test]
    #[tracing_test::traced_test]
    fn no_leader() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        let number_of_leaders = committee.quorum_threshold() as usize;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to finish wave 0.
        let decision_round_0 = wave_length - 1;
        let references = build_dag(&committee, &mut block_writer, None, decision_round_0);

        // Add enough blocks to reach the decision round of wave 1 (but without its leader).
        let leader_round_1 = wave_length;
        let leader_1 = committee.elect_leader(leader_round_1);

        let connections = committee
            .authorities()
            .filter(|&authority| authority != leader_1)
            .map(|authority| (authority, references.clone()));
        let references = build_dag_layer(connections.collect(), &mut block_writer);

        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            decision_round_1,
        );

        // Ensure the omitted leader is skipped and the others are committed.
        let committer = MultiCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_number_of_leaders(number_of_leaders)
        .build();

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), number_of_leaders);
        for (i, leader) in sequence.iter().enumerate() {
            let leader_round = wave_length;
            let leader_offset = i as u64;
            let expected_leader = committee.elect_leader(leader_round + leader_offset);
            if i == 0 {
                if let LeaderStatus::Skip(leader, round) = sequence[i] {
                    assert_eq!(leader, expected_leader);
                    assert_eq!(round, leader_round_1);
                } else {
                    panic!("Expected to directly skip the leader");
                }
            } else {
                if let LeaderStatus::Commit(block) = leader {
                    assert_eq!(block.author(), expected_leader);
                } else {
                    panic!("Expected a committed leader")
                }
            }
        }
    }

    /// We directly skip the leader if it has enough blame.
    #[test]
    #[tracing_test::traced_test]
    fn direct_skip() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        let number_of_leaders = committee.quorum_threshold() as usize;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the first leader of wave 1.
        let leader_round_1 = wave_length;
        let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

        // Filter out that leader.
        let references_1_without_leader: Vec<_> = references_1
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(leader_round_1))
            .collect();

        // Add enough blocks to reach the decision round of wave 1.
        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_1_without_leader),
            decision_round_1,
        );

        // Ensure that the first leader of wave 1 is skipped.
        let committer = MultiCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_number_of_leaders(number_of_leaders)
        .build();

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), number_of_leaders);
        for (i, leader) in sequence.iter().enumerate() {
            let leader_round = wave_length;
            let leader_offset = i as u64;
            let expected_leader = committee.elect_leader(leader_round + leader_offset);
            if i == 0 {
                if let LeaderStatus::Skip(leader, round) = sequence[i] {
                    assert_eq!(leader, expected_leader);
                    assert_eq!(round, leader_round_1);
                } else {
                    panic!("Expected to directly skip the leader");
                }
            } else {
                if let LeaderStatus::Commit(block) = leader {
                    assert_eq!(block.author(), expected_leader);
                } else {
                    panic!("Expected a committed leader")
                }
            }
        }
    }

    /// Commit the leaders of wave 1 and 3 while the leader of wave 2 is missing.
    #[test]
    #[tracing_test::traced_test]
    fn indirect_skip() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        let number_of_leaders = committee.quorum_threshold() as usize;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leaders of wave 2.
        let leader_round_2 = 2 * wave_length;
        let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

        // Filter out the first leader of wave 2.
        let leader_2 = committee.elect_leader(leader_round_2);
        let references_2_without_leader: Vec<_> = references_2
            .iter()
            .cloned()
            .filter(|x| x.authority != leader_2)
            .collect();

        // Create a dag layer where only one authority votes for the leader of wave 2.
        let mut authorities = committee.authorities();
        let leader_connection = vec![(authorities.next().unwrap(), references_2)];
        let non_leader_connections: Vec<_> = authorities
            .take((committee.quorum_threshold() - 1) as usize)
            .map(|authority| (authority, references_2_without_leader.clone()))
            .collect();

        let connections = leader_connection.into_iter().chain(non_leader_connections);
        let references = build_dag_layer(connections.collect(), &mut block_writer);

        // Add enough blocks to reach the decision round of wave 3.
        let decision_round_3 = 4 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            decision_round_3,
        );

        // Ensure we commit the leaders of wave 1 and 3
        let committer = MultiCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_number_of_leaders(number_of_leaders)
        .build();

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), 3 * number_of_leaders);

        // Ensure we commit the leader of wave 1.
        let leader_round_1 = wave_length;
        for i in 0..number_of_leaders {
            if let LeaderStatus::Commit(ref block) = sequence[i] {
                let leader_offset = i as u64;
                let expected = committee.elect_leader(leader_round_1 + leader_offset);
                assert_eq!(block.author(), expected);
            } else {
                panic!("Expected a committed leader")
            }
        }

        // Ensure we skip the first leader of wave 2 but commit the others.
        let leader_round_2 = 2 * wave_length;
        for i in number_of_leaders..2 * number_of_leaders {
            let leader_offset = (i % number_of_leaders) as u64;
            let expected_leader = committee.elect_leader(leader_round_2 + leader_offset);
            if i == number_of_leaders {
                if let LeaderStatus::Skip(leader, round) = sequence[number_of_leaders] {
                    assert_eq!(leader, expected_leader);
                    assert_eq!(round, leader_round_2);
                } else {
                    panic!("Expected a skipped leader")
                }
            } else {
                if let LeaderStatus::Commit(ref block) = sequence[i] {
                    assert_eq!(block.author(), expected_leader);
                } else {
                    panic!("Expected a committed leader")
                }
            }
        }

        // Ensure we commit the leader of wave 3.
        let leader_round_3 = 3 * wave_length;
        for i in 2 * number_of_leaders..3 * number_of_leaders {
            if let LeaderStatus::Commit(ref block) = sequence[i] {
                let leader_offset = (i % number_of_leaders) as u64;
                let expected = committee.elect_leader(leader_round_3 + leader_offset);
                assert_eq!(block.author(), expected);
            } else {
                panic!("Expected a committed leader")
            }
        }
    }

    /// If there is no leader with enough support nor blame, we commit nothing.
    #[test]
    #[tracing_test::traced_test]
    fn undecided() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;
        let number_of_leaders = committee.quorum_threshold() as usize;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leaders of wave 1.
        let leader_round_1 = wave_length;
        let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

        // Filter out the first leader of wave 1.
        let references_1_without_leader: Vec<_> = references_1
            .iter()
            .cloned()
            .filter(|x| x.authority != committee.elect_leader(leader_round_1))
            .collect();

        // Create a dag layer where only one authority votes for that leader.
        let mut authorities = committee.authorities();
        let leader_connection = vec![(authorities.next().unwrap(), references_1)];
        let non_leader_connections: Vec<_> = authorities
            .take((committee.quorum_threshold() - 1) as usize)
            .map(|authority| (authority, references_1_without_leader.clone()))
            .collect();

        let connections = leader_connection.into_iter().chain(non_leader_connections);
        let references = build_dag_layer(connections.collect(), &mut block_writer);

        // Add enough blocks to reach the decision round of wave 1.
        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            decision_round_1,
        );

        // Ensure no blocks are committed.
        let committer = MultiCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_number_of_leaders(number_of_leaders)
        .build();

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }
}
