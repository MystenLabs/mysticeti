// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_manager::BlockManager;
use crate::commit_interpreter::{CommitInterpreter, CommittedSubDag};
use crate::committee::{Committee, QuorumThreshold, TransactionAggregator};
use crate::data::Data;
use crate::runtime::TimeInstant;
use crate::stat::PreciseHistogram;
use crate::syncer::CommitObserver;
use crate::types::{AuthorityIndex, BaseStatement, BlockReference, StatementBlock, TransactionId};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub trait BlockHandler: Send + Sync {
    fn handle_blocks(&mut self, blocks: &Vec<Data<StatementBlock>>) -> Vec<BaseStatement>;
}

// Immediately votes and generates new transactions
pub struct TestBlockHandler {
    pub last_transaction: u64,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,

    pub transaction_certified_latency: PreciseHistogram<Duration>,
}

#[allow(dead_code)]
pub struct TestCommitHandler {
    commit_interpreter: CommitInterpreter,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    committee: Arc<Committee>,
    committed_leaders: Vec<BlockReference>,
    committed_dags: Vec<CommittedSubDag>,

    transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    pub certificate_committed_latency: PreciseHistogram<Duration>,
    pub transaction_committed_latency: PreciseHistogram<Duration>,
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
            transaction_time: Default::default(),
            committee,
            authority,

            transaction_certified_latency: Default::default(),
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
        let mut transaction_time = self.transaction_time.lock();
        transaction_time.insert(self.last_transaction, TimeInstant::now());
        self.transaction_votes
            .register(self.last_transaction, self.authority, &self.committee)
            .ok();
        for block in blocks {
            let processed =
                self.transaction_votes
                    .process_block(block, Some(&mut response), &self.committee);
            for processed_id in processed {
                if let Some(instant) = transaction_time.get(&processed_id) {
                    self.transaction_certified_latency
                        .observe(instant.elapsed());
                }
            }
        }
        response
    }
}

impl TestCommitHandler {
    pub fn new(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    ) -> Self {
        Self {
            commit_interpreter: CommitInterpreter::new(),
            transaction_votes: Default::default(),
            committee,
            committed_leaders: vec![],
            committed_dags: vec![],

            transaction_time,
            certificate_committed_latency: Default::default(),
            transaction_committed_latency: Default::default(),
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
        let transaction_time = self.transaction_time.lock();
        for commit in committed {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                let processed = self
                    .transaction_votes
                    .process_block(block, None, &self.committee);
                for processed_id in processed {
                    if let Some(instant) = transaction_time.get(&processed_id) {
                        self.certificate_committed_latency
                            .observe(instant.elapsed());
                    }
                }
                for statement in block.statements() {
                    if let BaseStatement::Share(id, _) = statement {
                        if let Some(instant) = transaction_time.get(id) {
                            self.transaction_committed_latency
                                .observe(instant.elapsed());
                        }
                    }
                }
            }
            self.committed_dags.push(commit);
        }
    }
}
