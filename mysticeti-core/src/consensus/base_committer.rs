// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, sync::Arc};

use crate::{block_store::BlockStore, consensus::DEFAULT_WAVE_LENGTH};
use crate::{
    committee::{Committee, QuorumThreshold, StakeAggregator},
    data::Data,
    types::{RoundNumber, StatementBlock},
};
use crate::{
    metrics::Metrics,
    types::{AuthorityIndex, BlockReference},
};

use super::{Committer, LeaderStatus};

/// The consensus protocol operates in 'waves'. Each wave is composed of a leader round, at least one
/// voting round, and one decision round.
type WaveNumber = u64;

/// The [`BaseCommitter`] contains all the commit logic. Once instantiated, the method [`try_commit`] can
/// be called at any time and any number of times (it is idempotent) to return extension to the commit
/// sequence. This structure is parametrized with a `wave length`, which must be at least 3 rounds: we
/// need one leader round, at least one round to vote for the leader, and one round to collect 2f+1
/// certificates for the leader. A longer wave_length increases the chance of committing the leader
/// under asynchrony at the cost of latency in the common case.
pub struct BaseCommitter {
    /// The committee information
    committee: Arc<Committee>,
    /// Keep all block data
    block_store: BlockStore,
    /// The length of a wave (minimum 3)
    wave_length: u64,
    /// The offset of the first wave. This is used in the pipelined committer to ensure that each
    /// [`BaseCommitter`] operates on a different view of the dag.
    offset: u64,
    /// Metrics information
    metrics: Arc<Metrics>,
}

impl BaseCommitter {
    /// We need at least one leader round, one voting round, and one decision round.
    pub const MINIMUM_WAVE_LENGTH: u64 = 3;

    /// Create a new [`BaseCommitter`] interpreting the dag using the provided committee and wave length.
    pub fn new(committee: Arc<Committee>, block_store: BlockStore, metrics: Arc<Metrics>) -> Self {
        Self {
            committee,
            block_store,
            wave_length: DEFAULT_WAVE_LENGTH,
            offset: 0,
            metrics,
        }
    }

    pub fn with_wave_length(mut self, wave_length: u64) -> Self {
        assert!(wave_length >= Self::MINIMUM_WAVE_LENGTH);
        assert!(wave_length > self.offset);
        self.wave_length = wave_length;
        self
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        assert!(offset < self.wave_length);
        self.offset = offset;
        self
    }

    /// Return the wave in which the specified round belongs.
    fn wave_number(&self, round: RoundNumber) -> WaveNumber {
        round.saturating_sub(self.offset) / self.wave_length
    }

    /// Return the highest wave number that can be committed given the highest round number.
    fn hightest_committable_wave(&self, highest_round: RoundNumber) -> WaveNumber {
        let wave = self.wave_number(highest_round);
        if highest_round == self.decision_round(wave) {
            wave
        } else {
            wave.saturating_sub(1)
        }
    }

    /// Return the leader round of the specified wave number. The leader round is always the first
    /// round of the wave.
    fn leader_round(&self, wave: WaveNumber) -> RoundNumber {
        wave * self.wave_length + self.offset
    }

    /// Return the decision round of the specified wave. The decision round is always the last
    /// round of the wave.
    fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        wave * self.wave_length + self.wave_length - 1 + self.offset
    }

    /// Check whether the specified block (`potential_certificate`) is a vote for
    /// the specified leader (`leader_block`).
    fn is_vote(
        &self,
        potential_vote: &Data<StatementBlock>,
        leader_block: &Data<StatementBlock>,
    ) -> bool {
        let (author, round) = leader_block.author_round();
        self.find_support((author, round), potential_vote) == Some(*leader_block.reference())
    }

    /// Find which block is supported at (author, round) by the given block.
    /// Block can indirectly reference multiple blocks at (author, round), but only one block at
    /// (author, round)  will be supported by the given block. If block A supports B at (author, round),
    /// it is guaranteed that any processed block by the same author that directly or indirectly includes
    /// A will also support B at (author, round).
    fn find_support(
        &self,
        (author, round): (AuthorityIndex, RoundNumber),
        from: &Data<StatementBlock>,
    ) -> Option<BlockReference> {
        if from.round() < round {
            return None;
        }
        for include in from.includes() {
            // Weak links may point to blocks with lower round numbers than strong links.
            if include.round() < round {
                continue;
            }
            if include.author_round() == (author, round) {
                return Some(*include);
            }
            let include = self
                .block_store
                .get_block(*include)
                .expect("We should have the whole sub-dag by now");
            if let Some(support) = self.find_support((author, round), &include) {
                return Some(support);
            }
        }
        None
    }

    /// Check whether the specified block (`potential_certificate`) is a certificate for
    /// the specified leader (`leader_block`).
    fn is_certificate(
        &self,
        potential_certificate: &Data<StatementBlock>,
        leader_block: &Data<StatementBlock>,
    ) -> bool {
        let mut votes_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for reference in potential_certificate.includes() {
            let potential_vote = self
                .block_store
                .get_block(*reference)
                .expect("We should have the whole sub-dag by now");

            if self.is_vote(&potential_vote, leader_block) {
                tracing::debug!("{potential_vote:?} is a vote for {leader_block:?}");
                if votes_stake_aggregator.add(reference.authority, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Check whether the specified leader has enough support (that is, 2f+1 certificates)
    /// at the specified round.
    fn enough_leader_support(
        &self,
        decision_round: RoundNumber,
        leader_block: &Data<StatementBlock>,
    ) -> bool {
        let decision_blocks = self.block_store.get_blocks_by_round(decision_round);

        let mut certificate_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for decision_block in &decision_blocks {
            let authority = decision_block.reference().authority;
            if self.is_certificate(decision_block, leader_block) {
                tracing::debug!("{decision_block:?} is a certificate for leader {leader_block:?}");
                if certificate_stake_aggregator.add(authority, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Output an ordered list of leader blocks (that we should commit in order).
    fn order_leaders(
        &self,
        last_committed_wave: WaveNumber,
        latest_leader_block: Data<StatementBlock>,
    ) -> Vec<LeaderStatus> {
        let latest_wave = self.wave_number(latest_leader_block.round());
        let mut to_commit = vec![LeaderStatus::Commit(latest_leader_block.clone())];
        let mut current_leader_block = latest_leader_block;

        let earliest_wave = last_committed_wave + 1;
        for w in (earliest_wave..latest_wave).rev() {
            // Get the block(s) proposed by the previous leader. There could be more than one
            // leader block per round (produced by a Byzantine leader).
            let leader_round = self.leader_round(w);
            let leader = self.committee.elect_leader(leader_round);
            let leader_blocks = self
                .block_store
                .get_blocks_at_authority_round(leader, leader_round);

            // Get all blocks that could be potential certificates for these leader blocks, if
            // they are ancestors of the current leader.
            let decision_round = self.decision_round(w);
            let decision_blocks = self.block_store.get_blocks_by_round(decision_round);
            let potential_certificates: Vec<_> = decision_blocks
                .iter()
                .filter(|block| self.block_store.linked(&current_leader_block, block))
                .collect();

            // Use those potential certificates to determine which (if any) of the previous leader
            // blocks can be committed.
            let mut certified_leader_blocks: Vec<_> = leader_blocks
                .into_iter()
                .filter(|leader_block| {
                    potential_certificates.iter().any(|potential_certificate| {
                        self.is_certificate(potential_certificate, leader_block)
                    })
                })
                .collect();

            // There can be at most one certified leader.
            if certified_leader_blocks.len() > 1 {
                panic!("More than one certified block at wave {w} from leader {leader}")
            }

            // We skip the previous leader if it has no certificates that are ancestors of current
            // leader. Otherwise we proceed to the next iteration using the previous leader as anchor.
            match certified_leader_blocks.pop() {
                Some(certified_leader_block) => {
                    to_commit.push(LeaderStatus::Commit(certified_leader_block.clone()));
                    current_leader_block = certified_leader_block;
                }
                None => {
                    to_commit.push(LeaderStatus::Skip(leader_round));
                }
            }
        }
        to_commit
    }

    /// Commit the specified leader block as well as any eligible past leader (recursively)
    /// that we did not already commit.
    fn commit(
        &self,
        last_committed_wave: WaveNumber,
        leader_block: Data<StatementBlock>,
    ) -> Vec<LeaderStatus> {
        let sequence: Vec<_> = self
            .order_leaders(last_committed_wave, leader_block)
            .into_iter()
            .rev()
            .collect();

        self.update_metrics(&sequence);

        sequence
    }

    /// Update metrics.
    fn update_metrics(&self, sequence: &[LeaderStatus]) {
        for (i, leader) in sequence.iter().rev().enumerate() {
            if let LeaderStatus::Commit(block) = leader {
                let commit_type = if i == 0 { "direct" } else { "indirect" };
                self.metrics
                    .committed_leaders_total
                    .with_label_values(&[&block.author().to_string(), commit_type])
                    .inc();
            }
        }
    }
}

impl Committer for BaseCommitter {
    fn try_commit(&self, last_committer_round: RoundNumber) -> Vec<LeaderStatus> {
        let mut last_committed_wave = self.wave_number(last_committer_round);
        let highest_round = self.block_store.highest_round();
        let highest_wave = self.hightest_committable_wave(highest_round);

        let mut sequence = Vec::new();
        for wave in (last_committed_wave + 1)..=highest_wave {
            let leader_round = self.leader_round(wave);
            let decision_round = self.decision_round(wave);

            tracing::debug!(
                "{self} trying to commit ( \
                    wave: {wave}, \
                    leader_round: {leader_round}, \
                    decision round: {decision_round} \
                )"
            );

            // Check whether the leader(s) has enough support. That is, whether there are 2f+1
            // certificates over the leader. Note that there could be more than one leader block
            // (created by Byzantine leaders).
            let leader = self.committee.elect_leader(leader_round);
            let leader_blocks = self
                .block_store
                .get_blocks_at_authority_round(leader, leader_round);
            let mut leaders_with_enough_support: Vec<_> = leader_blocks
                .into_iter()
                .filter(|l| self.enough_leader_support(decision_round, l))
                .collect();

            // There can be at most one leader with enough support for each round.
            if leaders_with_enough_support.len() > 1 {
                panic!("More than one certified block at wave {wave} from leader {leader}")
            }

            // If a leader has enough support, we commit it along with its linked predecessors.
            let new_commits = match leaders_with_enough_support.pop() {
                Some(leader_block) => {
                    tracing::debug!("leader {leader_block} has enough support to be committed");
                    let commits = self.commit(last_committed_wave, leader_block);
                    last_committed_wave = wave;
                    commits
                }
                None => vec![],
            };
            sequence.extend(new_commits);
        }
        sequence
    }
}

impl Display for BaseCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Committer-{}", self.offset)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::test_util::{build_dag, TestBlockWriter};
    use crate::{
        data::Data,
        test_util::{committee, test_metrics},
        types::StatementBlock,
    };

    /// Commit one leader.
    #[test]
    #[tracing_test::traced_test]
    fn commit_one() {
        let committee = committee(4);

        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
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

        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
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
        let enough_blocks = wave_length * (n + 1) - 1;
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, enough_blocks);

        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), n as usize);
        for (i, leader_block) in sequence.iter().enumerate() {
            let leader_round = (i as u64 + 1) * wave_length;
            if let LeaderStatus::Commit(ref block) = leader_block {
                assert_eq!(block.author(), committee.elect_leader(leader_round));
            } else {
                panic!("Expected a committed leader")
            };
        }
    }

    /// Do not commit anything if we are still in the first wave.
    #[test]
    #[tracing_test::traced_test]
    fn commit_none() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let first_commit_round = 2 * wave_length - 1;
        for r in 0..first_commit_round {
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, r);

            let committer = BaseCommitter::new(
                committee.clone(),
                block_writer.into_block_store(),
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
            let enough_blocks = wave_length * (n + 1) - 1;
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, enough_blocks);

            let committer = BaseCommitter::new(
                committee.clone(),
                block_writer.into_block_store(),
                test_metrics(),
            );

            let sequence = committer.try_commit(last_committed_round);

            assert_eq!(sequence.len(), 1);
            let leader_round = n as u64 * wave_length;
            if let LeaderStatus::Commit(ref block) = sequence[0] {
                assert_eq!(block.author(), committee.elect_leader(leader_round));
            } else {
                panic!("Expected a committed leader")
            }

            last_committed_round = leader_round;
        }
    }

    /// We do not commit anything if there is no leader.
    #[test]
    #[tracing_test::traced_test]
    fn no_leader() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to finish wave 0.
        let decision_round_0 = wave_length - 1;
        let references = build_dag(&committee, &mut block_writer, None, decision_round_0);

        // Add enough blocks to reach the decision round of wave 1 (but without its leader).
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
        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert!(sequence.is_empty());
    }

    /// If there is no leader with enough support, we commit nothing.
    #[test]
    #[tracing_test::traced_test]
    fn not_enough_support() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 1.
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

        // Ensure no blocks are committed.
        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert!(sequence.is_empty());
    }

    /// Commit the leaders of wave 1 and 3 while the leader of wave 2 is missing.
    #[test]
    #[tracing_test::traced_test]
    fn skip_leader() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 2.
        let leader_round_2 = 2 * wave_length;
        let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

        // Filter out that leader.
        let references_2_without_leader: Vec<_> = references_2
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(leader_round_2))
            .collect();

        // Add enough blocks to reach the decision round of wave 3.
        let decision_round_3 = 4 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_2_without_leader),
            decision_round_3,
        );

        // Ensure we commit the leaders of wave 1 and 3
        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), 3);

        // Ensure we commit the leader of wave 1.
        let leader_round_1 = wave_length;
        let leader_1 = committee.elect_leader(leader_round_1);
        if let LeaderStatus::Commit(ref block) = sequence[0] {
            assert_eq!(block.author(), leader_1);
        } else {
            panic!("Expected a committed leader")
        };

        // Ensure we skip the leader of wave 2.
        let leader_round_2 = 2 * wave_length;
        if let LeaderStatus::Skip(round) = sequence[1] {
            assert_eq!(round, leader_round_2);
        } else {
            panic!("Expected a skipped leader")
        }

        // Ensure we commit the leader of wave 3.
        let leader_round_3 = 3 * wave_length;
        let leader_3 = committee.elect_leader(leader_round_3);
        if let LeaderStatus::Commit(ref block) = sequence[2] {
            assert_eq!(block.author(), leader_3);
        } else {
            panic!("Expected a committed leader")
        }
    }
}
