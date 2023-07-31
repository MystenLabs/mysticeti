use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::block_store::BlockStore;
use crate::types::{AuthorityIndex, BaseStatement, EpochStatus, TransactionLocator, Vote};
use crate::{
    committee::{Committee, QuorumThreshold, StakeAggregator},
    data::Data,
    types::{BlockReference, StatementBlock},
};

#[allow(dead_code)]
pub struct FinalizationInterpreter<'a> {
    transaction_aggregator:
        HashMap<BlockReference, HashMap<TransactionLocator, StakeAggregator<QuorumThreshold>>>,
    certificate_aggregator: HashMap<TransactionLocator, StakeAggregator<QuorumThreshold>>,
    transaction_certificates: HashMap<TransactionLocator, HashSet<BlockReference>>,
    committee: Arc<Committee>,
    block_store: &'a BlockStore,
    finalized_transactions: HashSet<TransactionLocator>,
}

#[allow(dead_code)]
impl<'a> FinalizationInterpreter<'a> {
    pub fn new(block_store: &'a BlockStore, committee: Arc<Committee>) -> Self {
        Self {
            transaction_aggregator: Default::default(),
            certificate_aggregator: Default::default(),
            transaction_certificates: Default::default(),
            committee,
            block_store,
            finalized_transactions: Default::default(),
        }
    }

    pub fn finalized_tx_certifying_blocks(
        &mut self,
    ) -> Vec<(TransactionLocator, HashSet<BlockReference>)> {
        for round in 0..=self.block_store.highest_round() {
            println!("ROUND: {}", round);
            for block in self.block_store.get_blocks_by_round(round) {
                self._finalized_tx_certifying_blocks(&block);
            }
        }
        let mut result = vec![];
        for (k, v) in &self.transaction_certificates {
            if self.finalized_transactions.contains(k) {
                result.push((*k, v.clone()));
            }
        }
        println!("Total finalized transactions: {}", result.len());
        result
    }

    fn _finalized_tx_certifying_blocks(&mut self, block: &Data<StatementBlock>) {
        if self.transaction_aggregator.contains_key(block.reference()) {
            // already processed
            return;
        }
        self.transaction_aggregator
            .insert(*block.reference(), Default::default());

        for (offset, statement) in block.statements().iter().enumerate() {
            match statement {
                BaseStatement::Vote(locator, vote) => {
                    if let Vote::Accept = vote {
                        self.vote(block, locator, block.author());
                    }
                }
                BaseStatement::VoteRange(tx_locator_range) => {
                    for locator in tx_locator_range.locators() {
                        self.vote(block, &locator, block.author());
                    }
                }
                BaseStatement::Share(_) => {
                    let locator = TransactionLocator::new(*block.reference(), offset as u64);
                    self.vote(block, &locator, block.author());
                }
            }
        }
        for parent in block.includes() {
            self._finalized_tx_certifying_blocks(&self.block_store.get_block(*parent).unwrap());
            let parent_aggregator =
                std::mem::take(self.transaction_aggregator.get_mut(parent).unwrap());
            for (tx, parent_aggregator) in &parent_aggregator {
                for voter in parent_aggregator.voters() {
                    self.vote(block, tx, voter);
                }
            }
            // let _ = std::mem::replace(self.transaction_aggregator_for(parent), parent_aggregator);
            self.transaction_aggregator
                .insert(*parent, parent_aggregator);
        }
    }

    fn vote(
        &mut self,
        block: &Data<StatementBlock>,
        transaction: &TransactionLocator,
        tx_voter: AuthorityIndex,
    ) {
        let block_transaction_aggregator = self
            .transaction_aggregator
            .get_mut(block.reference())
            .unwrap();
        if !block_transaction_aggregator.contains_key(transaction) {
            block_transaction_aggregator
                .insert(*transaction, StakeAggregator::<QuorumThreshold>::new());
        }
        if block_transaction_aggregator
            .get_mut(transaction)
            .unwrap()
            .add(tx_voter, &self.committee)
        {
            if let EpochStatus::Open = block.epoch_marker() {
                // this is a certifying block
                if !self.transaction_certificates.contains_key(transaction) {
                    self.transaction_certificates
                        .insert(*transaction, Default::default());
                }
                self.transaction_certificates
                    .get_mut(transaction)
                    .unwrap()
                    .insert(*block.reference());

                if !self.certificate_aggregator.contains_key(transaction) {
                    self.certificate_aggregator
                        .insert(*transaction, StakeAggregator::new());
                }

                if self
                    .certificate_aggregator
                    .get_mut(transaction)
                    .unwrap()
                    .add(block.author(), &self.committee)
                {
                    self.finalized_transactions.insert(*transaction);
                }
            }
        }
    }

    fn transaction_aggregator_for(
        &mut self,
        block: &BlockReference,
    ) -> &mut HashMap<TransactionLocator, StakeAggregator<QuorumThreshold>> {
        self.transaction_aggregator.get_mut(block).unwrap()
    }

    // fn is_tx_finalized(
    //     &mut self,
    //     block_reference: BlockReference,
    //     transaction: &TransactionLocator,
    // ) -> bool {
    //     let block = self
    //         .block_store
    //         .get_block(block_reference)
    //         .expect("We should have the whole sub-dag by now");
    //     for statement in block.statements() {
    //         if let BaseStatement::Vote(locator, vote) = statement {
    //             if locator == transaction {
    //                 if let Vote::Accept = vote {
    //                     if self
    //                         .transaction_aggregator
    //                         .add(block.author(), &self.committee)
    //                     {
    //                         if let EpochStatus::Open = block.epoch_marker() {
    //                             if self
    //                                 .certificate_aggregator
    //                                 .add(block.author(), &self.committee)
    //                             {
    //                                 return true;
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     for parent in block.includes() {
    //         if self.is_tx_finalized(*parent, transaction) {
    //             return true;
    //         }
    //     }
    //     return false;
    // }
}
