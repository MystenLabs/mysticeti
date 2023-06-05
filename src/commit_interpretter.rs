// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use crate::{
    block_manager::BlockManager,
    data::Data,
    syncer::CommitObserver,
    types::{BlockReference, StatementBlock},
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

/// Expand a committed sequence of leader into a sequence of sub-dags.
pub struct CommitInterpreter {
    /// Keep track of all committed blocks to avoid committing the same block twice.
    committed: HashSet<BlockReference>,
    /// Hold the commit sequence.
    commit_sequence: Vec<CommittedSubDag>,
}

impl CommitObserver for CommitInterpreter {
    fn handle_commit(
        &mut self,
        block_manager: &BlockManager,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) {
        for leader_block in committed_leaders {
            // Collect the sub-dag generated using each of these leaders as anchor.
            let mut sub_dag = self.collect_sub_dag(block_manager, leader_block);

            // [Optional] sort the sub-dag using a deterministic algorithm.
            sub_dag.sort();
            self.commit_sequence.push(sub_dag);
        }
    }
}

impl CommitInterpreter {
    pub fn new() -> Self {
        Self {
            committed: HashSet::new(),
            commit_sequence: Vec::new(),
        }
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or blocks that
    /// have already been committed (within previous sub-dags).
    fn collect_sub_dag(
        &mut self,
        block_manager: &BlockManager,
        leader_block: Data<StatementBlock>,
    ) -> CommittedSubDag {
        let mut to_commit = Vec::new();

        let mut buffer = vec![&leader_block];
        while let Some(x) = buffer.pop() {
            to_commit.push(x.clone());
            for reference in x.includes() {
                // The block manager may have cleaned up blocks passed the latest committed rounds.
                let block = block_manager
                    .get_processed_block(reference)
                    .expect("We should have the whole sub-dag by now");

                // Skip the block if we already committed it (either as part of this sub-dag or
                // a previous one).
                if !self.committed.contains(&reference) {
                    buffer.push(block);
                    self.committed.insert(*reference);
                }
            }
        }
        CommittedSubDag::new(*leader_block.reference(), to_commit)
    }
}
