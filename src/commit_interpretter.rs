// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};

use crate::{
    block_manager::BlockManager,
    data::Data,
    syncer::CommitObserver,
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
};

/// The output of consensus is an ordered list of [`CommittedSubDag`]. The application can arbitrarily
/// sort the blocks within each sub-dag (but using a deterministic algorithm).
pub struct CommittedSubDag {
    /// A reference to the anchor of the sub-dag
    pub anchor: BlockReference,
    /// All the committed blocks that are part of this sub-dag
    pub blocks: Vec<Data<StatementBlock>>,
}

impl CommittedSubDag {
    /// Create new (empty) sub-dag.
    pub fn new(anchor: BlockReference, blocks: Vec<Data<StatementBlock>>) -> Self {
        Self { anchor, blocks }
    }

    /// Sort the blocks of the sub-dag by round number. Any deterministic algorithm works.
    pub fn sort(&mut self) {
        self.blocks.sort_by_key(|x| x.round());
    }
}

pub struct CommitInterpreter<'a> {
    /// Reference to the block manager holding all block data.
    block_manager: &'a BlockManager,
    /// Keep track of the latest committed round for each authority; avoid committing a block more
    /// than once
    last_committed_rounds: HashMap<AuthorityIndex, RoundNumber>,
    /// Hold the commit sequence.
    commit_sequence: Vec<CommittedSubDag>,
}

impl<'a> CommitObserver for CommitInterpreter<'a> {
    fn handle_commit<I>(&mut self, committed_leaders: I)
    where
        I: Iterator<Item = Data<StatementBlock>>,
    {
        for leader_block in committed_leaders {
            // Collect the sub-dag generated using each of these leaders as anchor.
            let mut sub_dag = self.collect_sub_dag(&leader_block);

            // Update the last committed round for each authority. This avoid committing
            // twice the same blocks (within different sub-dags).
            self.update_last_committed_rounds(&sub_dag);

            // [Optional] sort the sub-dag using a deterministic algorithm.
            sub_dag.sort();
            self.commit_sequence.push(sub_dag);
        }
    }
}

impl<'a> CommitInterpreter<'a> {
    pub fn new(block_manager: &'a BlockManager) -> Self {
        Self {
            block_manager,
            last_committed_rounds: HashMap::new(),
            commit_sequence: Vec::new(),
        }
    }

    /// Update the last committed rounds. We must call this method after committing each sub-dag.
    fn update_last_committed_rounds(&mut self, sub_dag: &CommittedSubDag) {
        for block in &sub_dag.blocks {
            let authority = block.author();
            let r = self.last_committed_rounds.entry(authority).or_insert(0);
            *r = max(*r, block.round());
        }
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or blocks that
    /// have already been committed (within previous sub-dags).
    fn collect_sub_dag(&self, leader_block: &'a Data<StatementBlock>) -> CommittedSubDag {
        let mut to_commit = Vec::new();

        let mut already_processed = HashSet::new();
        let mut buffer = vec![leader_block];
        while let Some(x) = buffer.pop() {
            to_commit.push(x.clone());
            for reference in x.includes() {
                // The block manager may have cleaned up blocks passed the latest committed rounds.
                let block = self
                    .block_manager
                    .get_processed_block(reference)
                    .expect("We should have the whole sub-dag by now");

                // Skip the block if we already committed it (either as part of this sub-dag or
                // a previous one).
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
        CommittedSubDag::new(*leader_block.reference(), to_commit)
    }
}
