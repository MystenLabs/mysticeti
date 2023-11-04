// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_store::BlockStore;
use crate::{
    data::Data,
    types::{BlockReference, StatementBlock},
};
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter};

/// The output of consensus is an ordered list of [`CommittedSubDag`]. The application can arbitrarily
/// sort the blocks within each sub-dag (but using a deterministic algorithm).
pub struct CommittedSubDag {
    /// A reference to the anchor of the sub-dag
    pub anchor: BlockReference,
    /// All the committed blocks that are part of this sub-dag
    pub blocks: Vec<Data<StatementBlock>>,
    /// The timestamp of the commit, obtained from the timestamp of the anchor block.
    pub timestamp_ms: u64,
}

impl CommittedSubDag {
    /// Create new (empty) sub-dag.
    pub fn new(
        anchor: BlockReference,
        blocks: Vec<Data<StatementBlock>>,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            anchor,
            blocks,
            timestamp_ms,
        }
    }

    /// Sort the blocks of the sub-dag by round number. Any deterministic algorithm works.
    pub fn sort(&mut self) {
        self.blocks.sort_by_key(|x| x.round());
    }
}

impl Display for CommittedSubDag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CommittedSubDag(anchor={}, blocks=[", self.anchor.digest)?;
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
#[derive(Default)]
pub struct Linearizer {
    /// Keep track of all committed blocks to avoid committing the same block twice.
    pub committed: HashSet<BlockReference>,
}

impl Linearizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or blocks that
    /// have already been committed (within previous sub-dags).
    fn collect_sub_dag(
        &mut self,
        block_store: &BlockStore,
        leader_block: Data<StatementBlock>,
    ) -> CommittedSubDag {
        let mut to_commit = Vec::new();

        // TODO: Verify that this can never overflow.
        let timestamp_ms = (leader_block.meta_creation_time_ns() / 1000) as u64;
        let leader_block_ref = *leader_block.reference();
        let mut buffer = vec![leader_block];
        assert!(self.committed.insert(leader_block_ref));
        while let Some(x) = buffer.pop() {
            to_commit.push(x.clone());
            for reference in x.includes() {
                // The block manager may have cleaned up blocks passed the latest committed rounds.
                let block = block_store
                    .get_block(*reference)
                    .expect("We should have the whole sub-dag by now");

                // Skip the block if we already committed it (either as part of this sub-dag or
                // a previous one).
                if self.committed.insert(*reference) {
                    buffer.push(block);
                }
            }
        }
        CommittedSubDag::new(leader_block_ref, to_commit, timestamp_ms)
    }

    pub fn handle_commit(
        &mut self,
        block_store: &BlockStore,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag> {
        let mut committed = vec![];
        for leader_block in committed_leaders {
            // Collect the sub-dag generated using each of these leaders as anchor.
            let mut sub_dag = self.collect_sub_dag(block_store, leader_block);

            // [Optional] sort the sub-dag using a deterministic algorithm.
            sub_dag.sort();
            committed.push(sub_dag);
        }
        committed
    }
}

impl fmt::Debug for CommittedSubDag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.anchor)?;
        for block in &self.blocks {
            write!(f, "{}, ", block.reference())?;
        }
        write!(f, ")")
    }
}
