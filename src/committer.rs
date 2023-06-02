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
pub struct Committer {
    /// The committee information
    committee: Arc<Committee>,
    /// The length of a wave (minimum 3)
    wave_length: u64,
}

impl Committer {
    /// Create a new [`Committer`] interpreting the dag using the provided committee and wave length.
    pub fn new(committee: Arc<Committee>, wave_length: u64) -> Self {
        assert!(wave_length >= 3);

        Self {
            committee,
            wave_length,
        }
    }

    /// Return the wave in which the specified round belongs.
    fn wave_number(&self, round: RoundNumber) -> WaveNumber {
        round / self.wave_length
    }

    /// Return the leader round of the specified wave number. The leader round is always the first
    /// round of the wave.
    fn leader_round(&self, wave: WaveNumber) -> RoundNumber {
        wave
    }

    /// Return the decision round of the specified wave. The decision round is always the last
    /// round of the wave.
    fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        wave + self.wave_length - 1
    }

    /// Check whether `earlier_block` is an ancestor of `later_block`.
    fn linked(
        &self,
        later_block: &Data<StatementBlock>,
        earlier_block: &Data<StatementBlock>,
        block_manager: &BlockManager,
    ) -> bool {
        let mut parents = vec![later_block];
        for r in (earlier_block.round()..later_block.round()).rev() {
            parents = block_manager
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
        block_manager: &BlockManager,
    ) -> bool {
        let mut votes_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for reference in potential_certificate.includes() {
            let potential_vote = block_manager
                .get_processed_block(reference)
                .expect("We should have the whole sub-dag by now");

            if self.linked(potential_vote, leader_block, block_manager) {
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
        block_manager: &BlockManager,
    ) -> bool {
        let decision_blocks = block_manager.get_blocks_by_round(decision_round);

        let mut certificate_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for decision_block in &decision_blocks {
            let authority = decision_block.reference().authority;
            if self.is_certificate(leader_block, &decision_block, block_manager) {
                if certificate_stake_aggregator.add(authority, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Output an ordered list of leader blocks (that we should commit in order).
    fn order_leaders<'a>(
        &self,
        last_committed_wave: WaveNumber,
        latest_leader_block: &'a Data<StatementBlock>,
        block_manager: &'a BlockManager,
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
            let leader_blocks = block_manager.get_blocks_at_authority_round(leader, leader_round);

            // Get all blocks that could be potential certificates for these leader blocks, if
            // they are ancestors of the current leader.
            let decision_round = self.decision_round(w);
            let decision_blocks = block_manager.get_blocks_by_round(decision_round);
            let potential_certificates: Vec<_> = decision_blocks
                .iter()
                .filter(|block| self.linked(block, current_leader_block, block_manager))
                .collect();

            // Use those potential certificates to determine which (if any) of the previous leader
            // blocks can be committed.
            let mut certified_leader_blocks: Vec<_> = leader_blocks
                .iter()
                .filter(|leader_block| {
                    potential_certificates.iter().any(|potential_certificate| {
                        self.is_certificate(leader_block, potential_certificate, block_manager)
                    })
                })
                .collect();

            // There can be at most one certified leader.
            match certified_leader_blocks.len().cmp(&1) {
                // We skip the previous leader if it has no certificates that are ancestors of current
                // leader.
                std::cmp::Ordering::Less => continue,
                // Proceed to the next iteration using the previous leader as anchor.
                std::cmp::Ordering::Equal => {
                    let certified_leader_block = certified_leader_blocks.pop().unwrap();
                    to_commit.push(certified_leader_block);
                    current_leader_block = certified_leader_block;
                }
                // Something very wrong happened: we have more than f Byzantine nodes.
                std::cmp::Ordering::Greater => {
                    panic!("More than one certified block at wave {w} from leader {leader}")
                }
            }
        }
        to_commit
    }

    /// Commit the specified leader block as well as any eligible past leader (recursively)
    /// that we did not already commit.
    fn commit(
        &mut self,
        last_committed_wave: WaveNumber,
        leader_block: &Data<StatementBlock>,
        block_manager: &BlockManager,
    ) -> Vec<Data<StatementBlock>> {
        self.order_leaders(last_committed_wave, leader_block, block_manager)
            .into_iter()
            .cloned()
            .rev()
            .collect()
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered committed sub-dags.
    pub fn try_commit(
        &mut self,
        last_committer_round: RoundNumber,
        block_manager: &BlockManager,
    ) -> Vec<Data<StatementBlock>> {
        let last_committed_wave = self.wave_number(last_committer_round);

        let highest_round = block_manager.highest_round;
        let wave = self.wave_number(highest_round);
        let leader_round = self.leader_round(wave);
        let decision_round = self.decision_round(wave);

        // Ensure we commit each leader at most once (and skip genesis).
        if wave <= last_committed_wave {
            return vec![];
        }

        // We only act during decision rounds (except the first).
        if highest_round != decision_round {
            return vec![];
        }

        // Check whether the leader(s) has enough support. That is, whether there are 2f+1
        // certificates over the leader. Note that there could be more than one leader block
        // (created by Byzantine leaders).
        let leader = self.committee.elect_leader(leader_round);
        let leader_blocks = block_manager.get_blocks_at_authority_round(leader, leader_round);
        let mut leaders_with_enough_support: Vec<_> = leader_blocks
            .iter()
            .filter(|l| self.enough_leader_support(decision_round, l, block_manager))
            .collect();

        // Commit the leader. There can be at most one leader with enough support for each round.
        match leaders_with_enough_support.len().cmp(&1) {
            // There is no leader to commit.
            std::cmp::Ordering::Less => return vec![],
            // We can now commit the leader as well as all its linked predecessors (recursively).
            std::cmp::Ordering::Equal => {
                let leader_block = leaders_with_enough_support.pop().unwrap();
                self.commit(last_committed_wave, leader_block, block_manager)
            }
            // Something very wrong happened: we have more than f Byzantine nodes.
            std::cmp::Ordering::Greater => {
                panic!("More than one certified block at wave {wave} from leader {leader}")
            }
        }
    }
}
