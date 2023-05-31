// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::committee::{Committee, QuorumThreshold, StakeAggregator};
use crate::data::Data;
use crate::types::{AuthorityIndex, BaseStatement, StatementBlock, TransactionId, Vote};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub trait BlockHandler: Send + Sync {
    fn handle_blocks(&mut self, blocks: &Vec<Data<StatementBlock>>) -> Vec<BaseStatement>;
}

// Immediately votes and generates new transactions
pub struct TestBlockHandler {
    pub last_transaction: u64,
    transaction_votes: HashMap<TransactionId, StakeAggregator<QuorumThreshold>>,
    certified_transactions: HashSet<TransactionId>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,
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
            certified_transactions: Default::default(),
            committee,
            authority,
        }
    }

    pub fn is_certified(&self, txid: TransactionId) -> bool {
        self.certified_transactions.contains(&txid)
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
        let mut aggregator = StakeAggregator::new();
        aggregator.add(self.authority, &self.committee);
        self.transaction_votes
            .insert(self.last_transaction, aggregator);
        for block in blocks {
            for statement in block.statements() {
                match statement {
                    BaseStatement::Share(id, _transaction) => {
                        // vote immediately
                        match self.transaction_votes.entry(*id) {
                            Entry::Occupied(_oc) => {
                                eprintln!("Duplicate transaction: {id}");
                            }
                            Entry::Vacant(va) => {
                                if self.certified_transactions.contains(id) {
                                    eprintln!("Duplicate transaction: {id}");
                                } else {
                                    // We count proposal of transaction as a vote
                                    let mut aggregator = StakeAggregator::new();
                                    aggregator.add(block.author(), &self.committee);
                                    va.insert(aggregator);
                                }
                            }
                        }
                        response.push(BaseStatement::Vote(*id, Vote::Accept));
                    }
                    BaseStatement::Vote(id, vote) => match vote {
                        Vote::Accept => {
                            if self.certified_transactions.contains(id) {
                                continue;
                            }
                            let Some(aggregator) = self
                                .transaction_votes
                                .get_mut(id) else {
                                panic!("Unexpected - got vote for unknown transaction {}", id);
                            };
                            if aggregator.add(block.author(), &self.committee) {
                                self.transaction_votes.remove(id);
                                self.certified_transactions.insert(*id);
                                // eprintln!("Transaction certified: {id}");
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
