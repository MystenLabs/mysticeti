// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    block_manager::BlockManager,
    committee::{Committee, QuorumThreshold, StakeAggregator},
    data::Data,
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
};

pub struct CommittedSubDag {
    /// A reference to the leader of the sub-dag
    pub leader: BlockReference,
    /// All the committed blocks that are part of this sub-dag
    pub blocks: Vec<Data<StatementBlock>>,
}

impl CommittedSubDag {
    pub fn new(leader: BlockReference) -> Self {
        Self {
            leader,
            blocks: Vec::new(),
        }
    }

    pub fn add(&mut self, block: Data<StatementBlock>) {
        self.blocks.push(block)
    }

    pub fn sort(&mut self) {
        self.blocks.sort_by_key(|x| x.round());
    }
}

type WaveNumber = u64;

pub struct Committer {
    committee: Arc<Committee>,
    wave_length: u64,
    last_committed_rounds: HashMap<AuthorityIndex, RoundNumber>,
}

impl Committer {
    /// Create a new [`Committer`] interpreting the dag using the provided 'wave length'. The wave
    /// length must be at least 3: we need one leader round, at least one round to vote for the leader,
    /// and one round to collect 2f+1 certificates for the leader. A longer wave_length increases the
    /// chance of committing the leader under asynchrony at the cost of latency in the common case.
    pub fn new(committee: Arc<Committee>, wave_length: u64) -> Self {
        assert!(wave_length >= 3);

        Self {
            committee,
            wave_length,
            last_committed_rounds: HashMap::new(),
        }
    }

    fn last_committed_wave(&self) -> WaveNumber {
        let round = self.last_committed_rounds.values().max().map_or(0, |r| *r);
        self.wave_number(round)
    }

    fn wave_number(&self, round: RoundNumber) -> WaveNumber {
        round / self.wave_length
    }

    fn leader_round(&self, wave: WaveNumber) -> RoundNumber {
        wave
    }

    fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        wave + self.wave_length - 1
    }

    fn update_last_committed_rounds(&mut self, sub_dag: &CommittedSubDag) {
        for block in &sub_dag.blocks {
            let authority = block.author();
            let r = self.last_committed_rounds.entry(authority).or_insert(0);
            *r = max(*r, block.round());
        }
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

    fn order_leaders<'a>(
        &self,
        latest_wave: WaveNumber,
        latest_leader_block: &'a Data<StatementBlock>,
        block_manager: &'a BlockManager,
    ) -> Vec<&'a Data<StatementBlock>> {
        let mut to_commit = vec![latest_leader_block];
        let mut current_leader_block = latest_leader_block;

        let earliest_wave = self.last_committed_wave() + 1;
        for w in (earliest_wave..latest_wave).rev() {
            // Get the block proposed by the previous leader(s). There could be more than one
            // leader block (produced by a Byzantine leader).
            let leader_round = self.leader_round(w);
            let leader = self.committee.elect_leader(leader_round);
            let leader_blocks = block_manager.get_blocks_at_authority_round(leader, leader_round);

            //
            let decision_round = self.decision_round(w);
            let decision_blocks = block_manager.get_blocks_by_round(decision_round);
            let potential_certificates: Vec<_> = decision_blocks
                .iter()
                .filter(|block| self.linked(block, current_leader_block, block_manager))
                .collect();

            //
            let mut certified_leader_blocks: Vec<_> = leader_blocks
                .iter()
                .filter(|leader_block| {
                    potential_certificates.iter().any(|potential_certificate| {
                        self.is_certificate(leader_block, potential_certificate, block_manager)
                    })
                })
                .collect();

            match certified_leader_blocks.len().cmp(&1) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => {
                    let certified_leader_block = certified_leader_blocks.pop().unwrap();
                    to_commit.push(certified_leader_block);
                    current_leader_block = certified_leader_block;
                }
                // Something very wrong happened: we have more than f Byzantine nodes.
                std::cmp::Ordering::Greater => panic!("More than one certified leader"),
            }
        }
        to_commit
    }

    fn collect_sub_dag(
        &self,
        leader_block: &Data<StatementBlock>,
        block_manager: &BlockManager,
    ) -> CommittedSubDag {
        let mut sub_dag = CommittedSubDag::new(*leader_block.reference());

        let mut already_processed = HashSet::new();
        let mut buffer = vec![leader_block];
        while let Some(x) = buffer.pop() {
            sub_dag.add(x.clone());
            for reference in x.includes() {
                //
                let block = block_manager
                    .get_processed_block(reference)
                    .expect("We should have the complete sub-dag");

                //
                let mut skip = already_processed.contains(&reference);
                skip |= self
                    .last_committed_rounds
                    .get(&reference.authority)
                    .map_or_else(|| false, |r| r >= &block.round());
                if !skip {
                    buffer.push(block);
                    already_processed.insert(reference);
                }
            }
        }
        sub_dag
    }

    fn commit(
        &mut self,
        wave: WaveNumber,
        leader_block: &Data<StatementBlock>,
        block_manager: &BlockManager,
    ) -> Vec<CommittedSubDag> {
        let mut commit_sequence = Vec::new();
        for leader_block in self
            .order_leaders(wave, leader_block, block_manager)
            .iter()
            .rev()
        {
            //
            let mut sub_dag = self.collect_sub_dag(leader_block, block_manager);

            //
            self.update_last_committed_rounds(&sub_dag);

            //
            sub_dag.sort();
            commit_sequence.push(sub_dag);
        }
        commit_sequence
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered committed sub-dags.
    pub fn try_commit(&mut self, block_manager: &BlockManager) -> Vec<CommittedSubDag> {
        let highest_round = block_manager.highest_round;
        let wave = self.wave_number(highest_round);
        let leader_round = self.leader_round(wave);
        let decision_round = self.decision_round(wave);

        // Ensure we commit each leader at most once (and skip genesis).
        if wave <= self.last_committed_wave() {
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

        // Commit the leader. There can be at most one leader with enough for each round.
        match leaders_with_enough_support.len().cmp(&1) {
            // There is no leader to commit.
            std::cmp::Ordering::Less => return vec![],
            // We can now commit the leader as well as all its linked predecessors (recursively).
            std::cmp::Ordering::Equal => {
                let leader_block = leaders_with_enough_support.pop().unwrap();
                self.commit(wave, leader_block, block_manager)
            }
            // Something very wrong happened: we have more than f Byzantine nodes.
            std::cmp::Ordering::Greater => panic!("More than one certified leader"),
        }
    }
}
