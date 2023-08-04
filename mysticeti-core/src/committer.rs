// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::block_store::BlockStore;
use crate::{
    committee::{Committee, QuorumThreshold, StakeAggregator},
    data::Data,
    types::{RoundNumber, StatementBlock},
};
use crate::{
    metrics::Metrics,
    types::{AuthorityIndex, BlockReference},
};

/// The consensus protocol operates in 'waves'. Each wave is composed of a leader round, at least one
/// voting round, and one decision round.
type WaveNumber = u64;

/// The [`Committer`] contains all the commit logic. Once instantiated, the method [`try_commit`] can
/// be called at any time and any number of times (it is idempotent) to return extension to the commit
/// sequence. This structure is parametrized with a `wave length`, which must be at least 3 rounds: we
/// need one leader round, at least one round to vote for the leader, and one round to collect 2f+1
/// certificates for the leader. A longer wave_length increases the chance of committing the leader
/// under asynchrony at the cost of latency in the common case.
pub struct Committer {
    /// The committee information
    committee: Arc<Committee>,
    /// Keep all block data
    block_store: BlockStore,
    /// The length of a wave (minimum 3)
    wave_length: u64,
    /// Metrics information
    metrics: Arc<Metrics>,
}

impl Committer {
    /// Create a new [`Committer`] interpreting the dag using the provided committee and wave length.
    pub fn new(
        committee: Arc<Committee>,
        block_store: BlockStore,
        wave_length: u64,
        metrics: Arc<Metrics>,
    ) -> Self {
        assert!(wave_length >= 3);

        Self {
            committee,
            block_store,
            wave_length,
            metrics,
        }
    }

    /// Return the wave in which the specified round belongs.
    fn wave_number(&self, round: RoundNumber) -> WaveNumber {
        round / self.wave_length
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
        wave * self.wave_length
    }

    /// Return the decision round of the specified wave. The decision round is always the last
    /// round of the wave.
    fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        wave * self.wave_length + self.wave_length - 1
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
    ) -> Vec<Data<StatementBlock>> {
        let latest_wave = self.wave_number(latest_leader_block.round());
        let mut to_commit = vec![latest_leader_block.clone()];
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
            if let Some(certified_leader_block) = certified_leader_blocks.pop() {
                to_commit.push(certified_leader_block.clone());
                current_leader_block = certified_leader_block;
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
    ) -> Vec<Data<StatementBlock>> {
        let sequence: Vec<_> = self
            .order_leaders(last_committed_wave, leader_block)
            .into_iter()
            .rev()
            .collect();

        self.update_metrics(&sequence);

        sequence
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered committed sub-dags.
    pub fn try_commit(&self, last_committer_round: RoundNumber) -> Vec<Data<StatementBlock>> {
        let last_committed_wave = self.wave_number(last_committer_round);
        assert_eq!(
            last_committer_round,
            self.leader_round(last_committed_wave),
            "Last committed round is always a leader round"
        );

        let highest_round = self.block_store.highest_round();
        let highest_wave = self.hightest_committable_wave(highest_round);
        let leader_round = self.leader_round(highest_wave);
        let decision_round = self.decision_round(highest_wave);

        tracing::debug!(
            "Trying to commit ( \
                highest_round: {highest_round} \
                leader_round: {leader_round}, \
                decision round: {decision_round} \
            )"
        );

        // Ensure we commit each leader at most once (and skip genesis).
        if highest_wave <= last_committed_wave {
            tracing::debug!("Wave {highest_wave} already committed");
            return vec![];
        }

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
            panic!("More than one certified block at wave {highest_wave} from leader {leader}")
        }

        // If a leader has enough support, we commit it along with its linked predecessors.
        match leaders_with_enough_support.pop() {
            Some(leader_block) => {
                tracing::debug!("leader {leader_block} has enough support to be committed");
                self.commit(last_committed_wave, leader_block)
            }
            None => vec![],
        }
    }

    /// Update metrics.
    fn update_metrics(&self, sequence: &[Data<StatementBlock>]) {
        for (i, block) in sequence.iter().rev().enumerate() {
            let commit_type = if i == 0 { "direct" } else { "indirect" };
            self.metrics
                .committed_leaders_total
                .with_label_values(&[&block.author().to_string(), commit_type])
                .inc();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use prometheus::default_registry;

    use crate::test_util::TestBlockWriter;
    use crate::types::EpochStatus;
    use crate::{
        committee::Committee,
        data::Data,
        metrics::Metrics,
        test_util::{committee, test_metrics},
        types::{BlockReference, RoundNumber, StatementBlock},
    };

    /// Build a fully interconnected dag up to the specified round. This function starts building the
    /// dag from the specified [`start`] references or from genesis if none are specified.
    fn build_dag(
        committee: &Committee,
        block_writer: &mut TestBlockWriter,
        start: Option<Vec<BlockReference>>,
        stop: RoundNumber,
    ) -> Vec<BlockReference> {
        let mut includes = match start {
            Some(start) => {
                assert!(!start.is_empty());
                assert_eq!(
                    start.iter().map(|x| x.round).max(),
                    start.iter().map(|x| x.round).min()
                );
                start
            }
            None => {
                let (references, genesis): (Vec<_>, Vec<_>) = committee
                    .authorities()
                    .map(|index| StatementBlock::new_genesis(index))
                    .map(|block| (*block.reference(), block))
                    .unzip();
                block_writer.add_blocks(genesis);
                references
            }
        };

        let starting_round = includes.first().unwrap().round + 1;
        for round in starting_round..=stop {
            let (references, blocks): (Vec<_>, Vec<_>) = committee
                .authorities()
                .map(|authority| {
                    let block = Data::new(StatementBlock::new(
                        authority,
                        round,
                        includes.clone(),
                        vec![],
                        0,
                        EpochStatus::Open,
                        Default::default(),
                    ));
                    (*block.reference(), block)
                })
                .unzip();
            block_writer.add_blocks(blocks);
            includes = references;
        }

        includes
    }

    /// Commit one leader.
    #[test]
    #[tracing_test::traced_test]
    fn commit_one() {
        let committee = committee(4);
        let wave_length = 3;

        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = Committer::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), 1);
        assert_eq!(sequence[0].author(), committee.elect_leader(3))
    }

    /// Ensure idempotent replies.
    #[test]
    #[tracing_test::traced_test]
    fn idempotence() {
        let committee = committee(4);
        let wave_length = 3;

        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = Committer::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
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
        let (metrics, _) = Metrics::new(default_registry(), None);
        let committee = committee(4);
        let wave_length = 3;

        let n = 10;
        let enough_blocks = wave_length * (n + 1) - 1;
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, enough_blocks);

        let committer = Committer::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            metrics,
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), n as usize);
        for (i, leader_block) in sequence.iter().enumerate() {
            let leader_round = (i as u64 + 1) * wave_length;
            assert_eq!(leader_block.author(), committee.elect_leader(leader_round));
        }
    }

    /// Do not commit anything if we are still in the first wave.
    #[test]
    #[tracing_test::traced_test]
    fn commit_none() {
        let committee = committee(4);
        let wave_length = 3;

        let first_commit_round = 2 * wave_length - 1;
        for r in 0..first_commit_round - 1 {
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, r);

            let committer = Committer::new(
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
        let wave_length = 3;

        let mut last_committed_round = 0;
        for n in 1..=10 {
            let enough_blocks = wave_length * (n + 1) - 1;
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, enough_blocks);

            let committer = Committer::new(
                committee.clone(),
                block_writer.into_block_store(),
                wave_length,
                test_metrics(),
            );

            let sequence = committer.try_commit(last_committed_round);
            assert_eq!(sequence.len(), 1);
            for leader_block in sequence {
                let leader_round = n as u64 * wave_length;
                assert_eq!(leader_block.author(), committee.elect_leader(leader_round));
                last_committed_round = leader_round;
            }
        }
    }

    /// We do not commit anything if there is no leader.
    #[test]
    #[tracing_test::traced_test]
    fn no_leader() {
        let committee = committee(4);
        let wave_length = 3;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to finish the first wave.
        let round_decision_1 = wave_length - 1;
        let references = build_dag(&committee, &mut block_writer, None, round_decision_1);

        // Add enough blocks to reach the second decision round (but without the second leader).
        let round_leader_2 = wave_length;
        let leader_2 = committee.elect_leader(round_leader_2);

        let (references, blocks): (Vec<_>, Vec<_>) = committee
            .authorities()
            .filter(|&authority| authority != leader_2)
            .map(|authority| {
                let block = Data::new(StatementBlock::new(
                    authority,
                    round_leader_2,
                    references.clone(),
                    vec![],
                    0,
                    EpochStatus::Open,
                    Default::default(),
                ));
                (*block.reference(), block)
            })
            .unzip();

        block_writer.add_blocks(blocks);

        let round_decision_2 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            round_decision_2,
        );

        // Ensure no blocks are committed.
        let committer = Committer::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
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
        let wave_length = 3;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the second leader. Remember that he second leader is part of
        // the genesis.
        let round_leader_2 = wave_length;
        let references_2 = build_dag(&committee, &mut block_writer, None, round_leader_2);

        // Filter out the leader.
        let references_2_without_leader: Vec<_> = references_2
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(round_leader_2))
            .collect();

        // Add enough blocks to reach the second decision round.
        let round_decision_2 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_2_without_leader),
            round_decision_2,
        );

        // Ensure no blocks are committed.
        let committer = Committer::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert!(sequence.is_empty());
    }

    /// Commit the second and fourth leader while the third is missing.
    #[test]
    #[tracing_test::traced_test]
    fn skip_leader() {
        let committee = committee(4);
        let wave_length = 3;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the third leader.
        let round_leader_3 = 2 * wave_length;
        let references_3 = build_dag(&committee, &mut block_writer, None, round_leader_3);

        // Filter out the third leader.
        let references_3_without_leader: Vec<_> = references_3
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(round_leader_3))
            .collect();

        // Add enough blocks to reach the fourth decision round.
        let round_decision_4 = 4 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_3_without_leader),
            round_decision_4,
        );

        // Ensure we commit the second and fourth leaders.
        let committer = Committer::new(
            committee.clone(),
            block_writer.into_block_store(),
            wave_length,
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), 2);

        let round_leader_2 = wave_length;
        let leader_2 = committee.elect_leader(round_leader_2);
        assert_eq!(sequence[0].author(), leader_2);
        let round_leader_4 = 3 * wave_length;
        let leader_4 = committee.elect_leader(round_leader_4);
        assert_eq!(sequence[1].author(), leader_4);
    }
}
