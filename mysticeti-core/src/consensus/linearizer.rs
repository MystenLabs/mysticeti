// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashSet, HashMap, VecDeque};
use std::fmt;

use crate::block_store::BlockStore;
use crate::types::AuthorityIndex;
use crate::{
    data::Data,
    types::{BlockReference, StatementBlock},
};

use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;

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
#[derive(Default)]
pub struct Linearizer {
    /// Keep track of all committed blocks to avoid committing the same block twice.
    pub committed: HashSet<BlockReference>,
    commit_messages: Mutex<HashMap<u64, HashMap<AuthorityIndex, Data<StatementBlock>>>>,
    num_validators: usize,
}

impl Linearizer {
    pub fn new(num_validators: usize) -> Self {
        Self {
            committed: HashSet::new(),
            commit_messages: Mutex::new(HashMap::new()),
            num_validators,
        }
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or blocks that
    /// have already been committed (within previous sub-dags).
    fn _collect_sub_dag(
        &mut self,
        block_store: &BlockStore,
        leader_block: Data<StatementBlock>,
    ) -> CommittedSubDag {
        let mut to_commit = Vec::new();

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
        CommittedSubDag::new(leader_block_ref, to_commit)
    }

    // Handle commit messages by grouping them by round number and sorting them once all validators have sent their messages for the round.
    // Collect blocks from all validators before creating a CommittedSubDag
    // TODO: Consider removing block_ref
    pub async fn handle_commit(
        &mut self,
        round: u64,
        validator_id: AuthorityIndex,
        _block_ref: BlockReference,
        block: Data<StatementBlock>,
    ) -> Vec<CommittedSubDag>{

            let mut commit_messages = self.commit_messages.lock().await;
            let round_messages = commit_messages.entry(round).or_insert(HashMap::new());
            round_messages.insert(validator_id, block);

            // Check if we have received commit messages from all validators for this round
            if round_messages.len() == self.num_validators {
                let mut sub_dag_blocks = Vec::new();
                let mut to_process = VecDeque::new();

                // Start with the leader block
                let leader_block = round_messages.values().next().unwrap().clone();
                to_process.push_back(leader_block.clone());
                self.committed.insert(*leader_block.reference());

                // Collect all blocks in the sub-DAG
                while let Some(current_block) = to_process.pop_front() {
                    sub_dag_blocks.push(current_block.clone());

                    for included_ref in current_block.includes() {
                        if self.committed.insert(*included_ref) {
                            // Find the included block from the commit messages
                            if let Some(included_block) = round_messages.values().find(|b| b.reference() == included_ref) {
                                to_process.push_back(included_block.clone());
                            }
                        }
                    }
                }

                commit_messages.remove(&round);

                // Create and sort the CommittedSubDag
                let mut sub_dag = CommittedSubDag::new(*leader_block.reference(), sub_dag_blocks);
                sub_dag.sort();

                vec![sub_dag]
            } else {
                Vec::new()
            }
    }
}

pub struct LinearizerTask {
    global_linearizer: Arc<Mutex<Linearizer>>,
    receiver: mpsc::Receiver<(BlockReference, Data<StatementBlock>)>,
}

impl LinearizerTask {
    pub fn new(receiver: mpsc::Receiver<(BlockReference, Data<StatementBlock>)>, global_linearizer: Arc<Mutex<Linearizer>>) -> Self {
        Self { receiver, global_linearizer }
    }

    pub async fn run(mut self) {
        while let Some(block_reference) = self.receiver.recv().await {
            // Process the block reference
            let (block_reference, block) = block_reference;
            let (validator_id, round) = block_reference.author_round();

            // Handle the commit
            self.global_linearizer.lock().await.handle_commit(round, validator_id, block_reference, block).await;
        }
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
