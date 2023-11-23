// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_store::{BlockStore, CommitData};
use crate::commit_observer::CommitObserverRecoveredState;
use crate::committee::Committee;
use crate::consensus::reputation_scores::ReputationScores;
use crate::{
    data::Data,
    types::{BlockReference, StatementBlock},
};
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// The output of consensus is an ordered list of [`CommittedSubDag`]. The application can arbitrarily
/// sort the blocks within each sub-dag (but using a deterministic algorithm).
#[derive(Clone)]
pub struct CommittedSubDag {
    /// A reference to the anchor of the sub-dag
    pub anchor: BlockReference,
    /// All the committed blocks that are part of this sub-dag
    pub blocks: Vec<Data<StatementBlock>>,
    /// The timestamp of the commit, obtained from the timestamp of the anchor block.
    pub timestamp_ms: u64,
    /// Height of the commit.
    /// First commit after genesis has a height of 1, then every next commit has a height incremented by 1.
    pub height: u64,
    /// The reputation scores
    pub reputation_scores: ReputationScores,
}

impl CommittedSubDag {
    /// Create new (empty) sub-dag.
    pub fn new(
        anchor: BlockReference,
        blocks: Vec<Data<StatementBlock>>,
        timestamp_ms: u64,
        height: u64,
        reputation_scores: ReputationScores,
    ) -> Self {
        Self {
            anchor,
            blocks,
            timestamp_ms,
            height,
            reputation_scores,
        }
    }

    pub fn new_from_commit_data(commit_data: CommitData, block_store: &BlockStore) -> Self {
        let mut leader_block_idx = None;
        let blocks = commit_data
            .sub_dag
            .into_iter()
            .enumerate()
            .map(|(idx, block_ref)| {
                let block = block_store
                    .get_block(block_ref)
                    .expect("We should have the block referenced in the commit data");
                if block_ref == commit_data.leader {
                    leader_block_idx = Some(idx);
                }
                block
            })
            .collect::<Vec<_>>();
        let leader_block_idx = leader_block_idx.expect("Leader block must be in the sub-dag");
        let leader_block_ref = blocks[leader_block_idx].reference();
        let timestamp_ms = blocks[leader_block_idx].meta_creation_time_ms();
        CommittedSubDag::new(
            *leader_block_ref,
            blocks,
            timestamp_ms,
            commit_data.height,
            commit_data.reputation_scores,
        )
    }

    /// Sort the blocks of the sub-dag by round number. Any deterministic algorithm works.
    pub fn sort(&mut self) {
        self.blocks.sort_by_key(|x| x.round());
    }
}

impl Display for CommittedSubDag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CommittedSubDag(anchor={}, height={}, blocks=[",
            self.anchor.digest, self.height
        )?;
        for (idx, block) in self.blocks.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", block.digest())?;
        }
        write!(f, "])")
    }
}

/// Expand a committed sequence of leader into a sequence of sub-dags.
pub struct Linearizer {
    block_store: BlockStore,
    /// Keep track of all committed blocks to avoid committing the same block twice.
    committed: HashSet<BlockReference>,
    /// Keep track of the height of last linearized commit
    last_height: u64,
    /// Keeps the last committed leader
    last_committed_leader: BlockReference,
    /// the latest reputation scores
    reputation_scores: ReputationScores,
    committee: Arc<Committee>,
}

impl Linearizer {
    pub fn new(block_store: BlockStore, committee: Arc<Committee>) -> Self {
        Self {
            block_store,
            committed: Default::default(),
            last_height: Default::default(),
            last_committed_leader: BlockReference::default(),
            reputation_scores: ReputationScores::new(&committee),
            committee,
        }
    }

    pub fn recover_state(&mut self, recovered_state: &CommitObserverRecoveredState) {
        assert!(self.committed.is_empty());
        assert_eq!(self.last_height, 0);
        for commit in recovered_state.sub_dags.iter() {
            assert!(commit.height > self.last_height);
            self.last_height = commit.height;

            for block in commit.sub_dag.iter() {
                self.committed.insert(*block);
            }
            // Leader must be part of the subdag and hence should have been inserted in the loop above.
            assert!(self.committed.contains(&commit.leader));
        }
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or blocks that
    /// have already been committed (within previous sub-dags).
    fn collect_sub_dag(&mut self, leader_block: Data<StatementBlock>) -> CommittedSubDag {
        let mut to_commit = Vec::new();

        let timestamp_ms = leader_block.meta_creation_time_ms();
        let leader_block_ref = *leader_block.reference();
        let mut buffer = vec![leader_block];
        assert!(self.committed.insert(leader_block_ref));
        while let Some(x) = buffer.pop() {
            to_commit.push(x.clone());
            for reference in x.includes() {
                // The block manager may have cleaned up blocks passed the latest committed rounds.
                let block = self
                    .block_store
                    .get_block(*reference)
                    .expect("We should have the whole sub-dag by now");

                // Skip the block if we already committed it (either as part of this sub-dag or
                // a previous one).
                if self.committed.insert(*reference) {
                    buffer.push(block);
                }
            }
        }
        self.last_height += 1;

        // update the reputation scores
        self.update_reputation_scores(self.last_height, &to_commit);

        CommittedSubDag::new(
            leader_block_ref,
            to_commit,
            timestamp_ms,
            self.last_height,
            self.reputation_scores.clone(),
        )
    }

    pub fn handle_commit(
        &mut self,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag> {
        let mut committed = vec![];
        for leader_block in committed_leaders {
            let leader_ref = *leader_block.reference();
            // Collect the sub-dag generated using each of these leaders as anchor.
            let mut sub_dag = self.collect_sub_dag(leader_block);

            // [Optional] sort the sub-dag using a deterministic algorithm.
            sub_dag.sort();
            committed.push(sub_dag);

            self.last_committed_leader = leader_ref;
        }
        committed
    }

    fn update_reputation_scores(&mut self, height: u64, to_commit: &Vec<Data<StatementBlock>>) {
        static NUM_SUB_DAGS_PER_SCHEDULE: u64 = 100;

        // always reset when it is the first for the new window
        if height % NUM_SUB_DAGS_PER_SCHEDULE == 1 {
            self.reputation_scores = ReputationScores::new(&self.committee);
        }

        for block in to_commit {
            for include in block.includes() {
                if self.last_committed_leader == *include {
                    self.reputation_scores.add_score(block.author(), 1);
                    break;
                }
            }
        }

        // mark the reputation scores as final
        self.reputation_scores.final_of_schedule = height % NUM_SUB_DAGS_PER_SCHEDULE == 0;
    }
}

impl fmt::Debug for CommittedSubDag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}(", self.anchor, self.height)?;
        for block in &self.blocks {
            write!(f, "{}, ", block.reference())?;
        }
        write!(f, ")")
    }
}
