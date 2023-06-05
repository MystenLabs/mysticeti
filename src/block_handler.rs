// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::committee::{Committee, QuorumThreshold, TransactionAggregator};
use crate::data::Data;
use crate::types::{AuthorityIndex, BaseStatement, StatementBlock, TransactionId, Vote};
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

pub struct TestCommitHandler {}

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
            for statement in block.statements() {
                match statement {
                    BaseStatement::Share(id, _transaction) => {
                        if self
                            .transaction_votes
                            .register(*id, block.author(), &self.committee)
                            .is_err()
                        {
                            panic!("Duplicate transaction: {id} from {}", block.author());
                        }
                        response.push(BaseStatement::Vote(*id, Vote::Accept));
                    }
                    BaseStatement::Vote(id, vote) => match vote {
                        Vote::Accept => {
                            if self
                                .transaction_votes
                                .vote(*id, block.author(), &self.committee)
                                .is_err()
                            {
                                panic!("Unexpected - got vote for unknown transaction {}", id);
                            }
                        }
                        Vote::Reject(_) => unimplemented!(),
                    },
                }
            }
        }
        response
    }
}
