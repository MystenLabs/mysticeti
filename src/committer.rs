// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::{
    block_manager::BlockManager,
    committee::{Committee, QuorumThreshold, StakeAggregator},
    data::Data,
    types::{RoundNumber, StatementBlock},
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
pub struct Committer<'a> {
    /// The committee information
    committee: Arc<Committee>,
    /// Keep all block data
    block_manager: &'a BlockManager,
    /// The length of a wave (minimum 3)
    wave_length: u64,
}

impl<'a> Committer<'a> {
    /// Create a new [`Committer`] interpreting the dag using the provided committee and wave length.
    pub fn new(
        committee: Arc<Committee>,
        block_manager: &'a BlockManager,
        wave_length: u64,
    ) -> Self {
        assert!(wave_length >= 3);

        Self {
            committee,
            block_manager,
            wave_length,
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

    /// Check whether `earlier_block` is an ancestor of `later_block`.
    fn linked(
        &self,
        later_block: &Data<StatementBlock>,
        earlier_block: &Data<StatementBlock>,
    ) -> bool {
        let mut parents = vec![later_block];
        for r in (earlier_block.round()..later_block.round()).rev() {
            parents = self
                .block_manager
                .get_blocks_by_round(r)
                .into_iter()
                .filter(|&block| {
                    parents
                        .iter()
                        .any(|x| x.includes().contains(block.reference()))
                })
                .collect();
        }
        parents.contains(&earlier_block)
    }

    /// Check whether the specified block (`potential_certificate`) is a certificate for
    /// the specified leader (`leader_block`).
    fn is_certificate(
        &self,
        leader_block: &Data<StatementBlock>,
        potential_certificate: &Data<StatementBlock>,
    ) -> bool {
        let mut votes_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for reference in potential_certificate.includes() {
            let potential_vote = self
                .block_manager
                .get_processed_block(reference)
                .expect("We should have the whole sub-dag by now");

            if self.linked(potential_vote, leader_block) {
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
        let decision_blocks = self.block_manager.get_blocks_by_round(decision_round);

        let mut certificate_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for decision_block in &decision_blocks {
            let authority = decision_block.reference().authority;
            if self.is_certificate(leader_block, &decision_block) {
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
        latest_leader_block: &'a Data<StatementBlock>,
    ) -> Vec<&'a Data<StatementBlock>> {
        let mut to_commit = vec![latest_leader_block];
        let mut current_leader_block = latest_leader_block;
        let latest_wave = self.wave_number(latest_leader_block.round());

        let earliest_wave = last_committed_wave + 1;
        for w in (earliest_wave..latest_wave).rev() {
            // Get the block(s) proposed by the previous leader. There could be more than one
            // leader block per round (produced by a Byzantine leader).
            let leader_round = self.leader_round(w);
            let leader = self.committee.elect_leader(leader_round);
            let leader_blocks = self
                .block_manager
                .get_blocks_at_authority_round(leader, leader_round);

            // Get all blocks that could be potential certificates for these leader blocks, if
            // they are ancestors of the current leader.
            let decision_round = self.decision_round(w);
            let decision_blocks = self.block_manager.get_blocks_by_round(decision_round);
            let potential_certificates: Vec<_> = decision_blocks
                .iter()
                .filter(|block| self.linked(block, current_leader_block))
                .collect();

            // Use those potential certificates to determine which (if any) of the previous leader
            // blocks can be committed.
            let mut certified_leader_blocks: Vec<_> = leader_blocks
                .iter()
                .filter(|leader_block| {
                    potential_certificates.iter().any(|potential_certificate| {
                        self.is_certificate(leader_block, potential_certificate)
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
                to_commit.push(certified_leader_block);
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
        leader_block: &Data<StatementBlock>,
    ) -> Vec<Data<StatementBlock>> {
        self.order_leaders(last_committed_wave, leader_block)
            .into_iter()
            .cloned()
            .rev()
            .collect()
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

        let highest_round = self.block_manager.highest_round;
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
            .block_manager
            .get_blocks_at_authority_round(leader, leader_round);
        let mut leaders_with_enough_support: Vec<_> = leader_blocks
            .iter()
            .filter(|l| self.enough_leader_support(decision_round, l))
            .collect();

        // There can be at most one leader with enough support for each round.
        if leaders_with_enough_support.len() > 1 {
            panic!("More than one certified block at wave {highest_wave} from leader {leader}")
        }

        // If a leader has enough support, we commit it along with its linked predecessors.
        match leaders_with_enough_support.pop() {
            Some(leader_block) => {
                tracing::debug!("leader {leader} has enough support to be committed");
                self.commit(last_committed_wave, leader_block)
            }
            None => vec![],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        block_manager::BlockManager,
        committee::Committee,
        data::Data,
        test_util::committee,
        types::{BlockReference, RoundNumber, StatementBlock},
    };

    use super::Committer;

    /// Build a fully interconnected dag starting from genesis and up to the specified round.
    fn build_dag(committee: &Committee, block_manager: &mut BlockManager, round: RoundNumber) {
        let genesis: Vec<_> = committee
            .authorities()
            .map(|index| StatementBlock::new_genesis(index))
            .collect();
        let mut includes: Vec<_> = genesis.iter().map(|x| *x.reference()).collect();
        block_manager.add_blocks(genesis);

        for round in 1..=round {
            let blocks: Vec<_> = committee
                .authorities()
                .map(|authority| {
                    let reference = BlockReference {
                        authority,
                        round,
                        digest: 0,
                    };
                    Data::new(StatementBlock::new(reference, includes.clone(), vec![]))
                })
                .collect();
            includes = blocks.iter().map(|x| *x.reference()).collect();
            block_manager.add_blocks(blocks);
        }
    }

    #[test]
    #[tracing_test::traced_test]
    fn commit_one() {
        let committee = committee(4);
        let wave_length = 3;

        let mut block_manager = BlockManager::default();
        build_dag(&committee, &mut block_manager, 5);

        let committer = Committer::new(committee.clone(), &block_manager, wave_length);

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), 1);
        assert_eq!(sequence[0].author(), committee.elect_leader(3))
    }

    #[test]
    #[tracing_test::traced_test]
    fn idempotence() {
        let committee = committee(4);
        let wave_length = 3;

        let mut block_manager = BlockManager::default();
        build_dag(&committee, &mut block_manager, 5);

        let committer = Committer::new(committee.clone(), &block_manager, wave_length);

        let last_committed_round = 3;
        let sequence = committer.try_commit(last_committed_round);
        assert!(sequence.is_empty());
    }

    #[test]
    #[tracing_test::traced_test]
    fn commit_10() {
        let committee = committee(4);
        let wave_length = 3;

        let n = 10;
        let enough_blocks = wave_length * (n + 1) - 1;
        let mut block_manager = BlockManager::default();
        build_dag(&committee, &mut block_manager, enough_blocks);

        let committer = Committer::new(committee.clone(), &block_manager, wave_length);

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        assert_eq!(sequence.len(), n as usize);
        for (i, leader_block) in sequence.iter().enumerate() {
            let leader_round = (i as u64 + 1) * wave_length;
            assert_eq!(leader_block.author(), committee.elect_leader(leader_round));
        }
    }

    #[test]
    #[tracing_test::traced_test]
    fn commit_none() {
        let committee = committee(4);
        let wave_length = 3;

        let first_commit_round = 2 * wave_length - 1;
        for r in 0..first_commit_round - 1 {
            let mut block_manager = BlockManager::default();
            build_dag(&committee, &mut block_manager, r);

            let committer = Committer::new(committee.clone(), &block_manager, wave_length);

            let last_committed_round = 0;
            let sequence = committer.try_commit(last_committed_round);
            assert!(sequence.is_empty());
        }
    }

    #[test]
    #[tracing_test::traced_test]
    fn commit_incremental() {
        let committee = committee(4);
        let wave_length = 3;

        let mut last_committed_round = 0;
        for n in 1..=10 {
            let enough_blocks = wave_length * (n + 1) - 1;
            let mut block_manager = BlockManager::default();
            build_dag(&committee, &mut block_manager, enough_blocks);

            let committer = Committer::new(committee.clone(), &block_manager, wave_length);

            let sequence = committer.try_commit(last_committed_round);
            assert_eq!(sequence.len(), 1);
            for leader_block in sequence {
                let leader_round = n as u64 * wave_length;
                assert_eq!(leader_block.author(), committee.elect_leader(leader_round));
                last_committed_round = leader_round;
            }
        }
    }
}
