// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_manager::BlockManager;
use crate::commit_interpreter::{CommitInterpreter, CommittedSubDag};
use crate::committee::{Committee, QuorumThreshold, TransactionAggregator};
use crate::data::Data;
use crate::syncer::CommitObserver;
use crate::types::{AuthorityIndex, BaseStatement, BlockReference, StatementBlock, TransactionId};
use std::sync::Arc;

pub trait BlockHandler: Send + Sync {
    fn handle_blocks(&mut self, blocks: &Vec<Data<StatementBlock>>) -> Vec<BaseStatement>;
}

// Immediately votes and generates new transactions
pub struct TestBlockHandler {
    pub last_transaction: u64,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,
}

#[allow(dead_code)]
pub struct TestCommitHandler {
    commit_interpreter: CommitInterpreter,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    committee: Arc<Committee>,
    committed_leaders: Vec<BlockReference>,
    committed_dags: Vec<CommittedSubDag>,
}

impl TestBlockHandler {
    pub fn new(
        last_transaction: u64,
        committee: Arc<Committee>,
        authority: AuthorityIndex,
    ) -> Self {
        Self {
            last_transaction,
            transaction_votes: Default::default(),
            committee,
            authority,
        }
    }

    pub fn is_certified(&self, txid: TransactionId) -> bool {
        self.transaction_votes.is_processed(&txid)
    }
}

impl BlockHandler for TestBlockHandler {
    fn handle_blocks(&mut self, blocks: &Vec<Data<StatementBlock>>) -> Vec<BaseStatement> {
        let mut response = vec![];
        self.last_transaction += 1;
        response.push(BaseStatement::Share(
            self.last_transaction,
            self.last_transaction,
        ));
        self.transaction_votes
            .register(self.last_transaction, self.authority, &self.committee)
            .ok();
        for block in blocks {
            self.transaction_votes
                .process_block(block, Some(&mut response), &self.committee);
        }
        response
    }
}

impl TestCommitHandler {
    pub fn new(committee: Arc<Committee>) -> Self {
        Self {
            commit_interpreter: CommitInterpreter::new(),
            transaction_votes: Default::default(),
            committee,
            committed_leaders: vec![],
            committed_dags: vec![],
        }
    }

    pub fn committed_leaders(&self) -> &Vec<BlockReference> {
        &self.committed_leaders
    }
}

impl CommitObserver for TestCommitHandler {
    fn handle_commit(
        &mut self,
        block_manager: &BlockManager,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) {
        let committed = self
            .commit_interpreter
            .handle_commit(block_manager, committed_leaders);
        for commit in committed {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                self.transaction_votes
                    .process_block(block, None, &self.committee);
            }
            self.committed_dags.push(commit);
        }
    }
}
