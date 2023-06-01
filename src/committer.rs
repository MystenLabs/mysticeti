// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use crate::{
    block_manager::BlockManager,
    committee::{Committee, QuorumThreshold, StakeAggregator},
    data::Data,
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
};

pub struct CommittedSubDag {
    /// A reference to the leader of the sub-dag
    leader: BlockReference,
    /// All the committed blocks that are part of this sub-dag
    blocks: Vec<Data<StatementBlock>>,
}

pub struct Committer {
    authority: AuthorityIndex,
    committee: Arc<Committee>,
    period: u64,
    last_committed_round: RoundNumber,
}

impl Committer {
    /// Create a new [`Committer`] interpreting the dag using the provided period. The period
    /// must be at least 3: we need one leader round, at least one round to vote for the leader,
    /// and one round to collect 2f+1 certificates for the leader. A longer period increases the
    /// chance of committing the leader under asynchrony at the cost of latency in the common case.
    pub fn new(authority: AuthorityIndex, committee: Arc<Committee>, period: u64) -> Self {
        assert!(period >= 3);

        Self {
            authority,
            committee,
            period,
            last_committed_round: 0,
        }
    }

    /// Check whether the specified round is a 'decision round'.
    fn is_decision_round(&self, round: RoundNumber) -> bool {
        round % self.period == self.period - 1
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

    fn commit(
        &self,
        leader_block: &Data<StatementBlock>,
        block_manager: &BlockManager,
    ) -> Vec<CommittedSubDag> {
        todo!()
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered committed sub-dags.
    pub fn try_commit(&self, block_manager: &BlockManager) -> Vec<CommittedSubDag> {
        // We only act upon decision rounds (except the first).
        let highest_round = block_manager.highest_round;
        if !self.is_decision_round(highest_round) || highest_round < self.period {
            return vec![];
        }

        // Ensure we commit each leader at most once.
        let leader_round = highest_round - self.period + 1;
        if leader_round <= self.last_committed_round {
            return vec![];
        }

        // Check whether the leader(s) has enough support. That is, whether there are 2f+1
        // certificates over the leader. Note that there could be more than one leader block
        // (created by Byzantine leaders).
        let leader = self.leader_at_round(leader_round);
        let leader_blocks = block_manager.get_block_at_authority_round(leader, leader_round);
        let mut leaders_with_enough_support: Vec<_> = leader_blocks
            .iter()
            .filter(|l| self.enough_leader_support(highest_round, l, block_manager))
            .collect();

        // Commit the leader. There can be at most one leader with enough for each round.
        match leaders_with_enough_support.len().cmp(&1) {
            // There is no leader to commit.
            std::cmp::Ordering::Less => return vec![],
            // We can now commit the leader as well as all its linked predecessors (recursively).
            std::cmp::Ordering::Equal => {
                let leader_block = leaders_with_enough_support.pop().unwrap();
                self.commit(leader_block, block_manager)
            }
            // Something very wrong happened: we have more than f Byzantine nodes.
            std::cmp::Ordering::Greater => panic!("More than one certified leader"),
        }
    }

    // TODO: Move to committee.rs
    fn leader_at_round(&self, round: RoundNumber) -> AuthorityIndex {
        assert!(round % self.period == 0);
        round % self.committee.len() as u64
    }
}
