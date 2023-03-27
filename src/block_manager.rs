use crate::types::{
    Authority, BaseStatement, BlockReference, Committee, MetaStatement, MetaStatementBlock,
    SequenceNumber, Transaction, TransactionId, Vote,
};

use std::collections::{HashMap, HashSet, VecDeque};

// Only used for testing
#[cfg(test)]
fn make_test_transaction(transaction: Transaction) -> MetaStatement {
    MetaStatement::Base(BaseStatement::Share(transaction, transaction))
}

#[cfg(test)]
fn make_test_vote_accept(txid: TransactionId) -> MetaStatement {
    MetaStatement::Base(BaseStatement::Vote(txid, Vote::Accept))
}

// Algorithm that takes ONE MetaStatement::Block(Authority, SequenceNumber, SequenceDigest, Vec<MetaStatement>)
// and turns it into a sequence Vec<BaseStatement> where all base statements are emitted by the Authority that
// made the block.

#[derive(Default)]
pub struct BlockManager {
    /// Structures to keep track of blocks incl our own.
    blocks_pending: HashMap<BlockReference, MetaStatementBlock>,
    block_references_waiting: HashMap<BlockReference, HashSet<BlockReference>>,

    /// Our own strucutres for the next block
    own_next_sequence_number: SequenceNumber,
    own_next_block: Vec<MetaStatement>,

    /// The transactions and how many votes they got so far, incl potential conlicts.
    transaction_entries: HashMap<TransactionId, TransactionEntry>,
    blocks_processed: HashMap<BlockReference, MetaStatementBlock>,

    // Maintain an index of blocks by round as well.
    blocks_processed_by_round: HashMap<SequenceNumber, Vec<BlockReference>>,
}

/// A TransactionEntry structure stores the transactionId, the Transaction, and two maps. One that
/// stores all votes seen by authorities that are accept, and one that stores all votes that are reject.

pub struct TransactionEntry {
    #[allow(dead_code)]
    transaction_id: TransactionId,
    transaction: Transaction,
    accept_votes: HashSet<Authority>,
    reject_votes: HashMap<Authority, Vote>,
}

impl TransactionEntry {
    /// A constructor for a new TransactionEntry, that takes a transactionId and a transaction.
    pub fn new(transaction_id: TransactionId, transaction: Transaction) -> Self {
        TransactionEntry {
            transaction_id,
            transaction,
            accept_votes: HashSet::new(),
            reject_votes: HashMap::new(),
        }
    }

    /// Adds a vote to the TransactionEntry.
    pub fn add_vote(&mut self, authority: Authority, vote: Vote) -> bool {
        match vote {
            Vote::Accept => {
                return self.accept_votes.insert(authority);
            }
            Vote::Reject(_) => {
                self.reject_votes.insert(authority, vote);
                return true; // Always return since it might be a new conflict.
            }
        }
    }

    /// Returns true if the given authority has voted
    pub fn has_voted(&self, authority: &Authority) -> bool {
        self.accept_votes.contains(authority) || self.reject_votes.contains_key(authority)
    }

    /// Get the transaction by reference
    pub fn get_transaction(&self) -> &Transaction {
        &self.transaction
    }
}

/// A structure that holds the result of adding blocks, namely the list of the newly added transactions
/// as well as the list of transactions that have received fresh votes.
pub struct AddBlocksResult {
    pub newly_added_transactions: Vec<TransactionId>,
    pub transactions_with_fresh_votes: HashSet<TransactionId>,
}

impl AddBlocksResult {
    pub fn new() -> Self {
        AddBlocksResult {
            newly_added_transactions: vec![],
            transactions_with_fresh_votes: HashSet::default(),
        }
    }

    // Get the newlly added transactions by reference
    pub fn get_newly_added_transactions(&self) -> &Vec<TransactionId> {
        &self.newly_added_transactions
    }

    // Get the transactions with fresh votes by reference
    pub fn get_transactions_with_fresh_votes(&self) -> &HashSet<TransactionId> {
        &self.transactions_with_fresh_votes
    }
}

impl BlockManager {
    /// Processes a bunch of blocks, and returns the transactions that must be decided.
    pub fn add_blocks(&mut self, mut blocks: VecDeque<MetaStatementBlock>) -> AddBlocksResult {
        let mut local_blocks_processed: Vec<BlockReference> = vec![];
        while let Some(block) = blocks.pop_front() {
            let block_reference = block.get_reference();

            // check whether we have already processed this block and skip it if so.
            if self.blocks_processed.contains_key(block_reference)
                || self.blocks_pending.contains_key(block_reference)
            {
                continue;
            }

            let mut processed = true;
            for included_reference in block.get_includes() {
                // If we are missing a reference then we insert into pending and update the waiting index
                if !self.blocks_processed.contains_key(&included_reference) {
                    processed = false;
                    self.block_references_waiting
                        .entry(included_reference.clone())
                        .or_default()
                        .insert(block_reference.clone());
                }
            }
            if !processed {
                self.blocks_pending.insert(block_reference.clone(), block);
            } else {
                let block_reference = block_reference.clone();

                // Block can be processed. So need to update indexes etc
                self.blocks_processed.insert(block_reference.clone(), block);
                local_blocks_processed.push(block_reference.clone());

                // Update the index of blocks by round
                self.blocks_processed_by_round
                    .entry(block_reference.1)
                    .or_default()
                    .push(block_reference.clone());

                // Now unlock any pending blocks, and process them if ready.
                if let Some(waiting_references) =
                    self.block_references_waiting.remove(&block_reference)
                {
                    // For each reference see if its unblocked.
                    for waiting_block_reference in waiting_references {
                        let block_pointer = self.blocks_pending.get(&waiting_block_reference).expect("Safe since we ensure the block waiting reference has a valid primary key.");

                        if block_pointer
                            .get_includes()
                            .into_iter()
                            .all(|item_ref| !self.block_references_waiting.contains_key(item_ref))
                        {
                            // No dependencies are left unprocessed, so remove from unprocessed list, and add to the
                            // blocks we are processing now.
                            let block = self.blocks_pending.remove(&waiting_block_reference).expect("Safe since we ensure the block waiting reference has a valid primary key.");
                            blocks.push_front(block);
                        }
                    }
                }
            }
        }

        // Now we are going to "walk" the blocks processed in order and extract all included transactions and votes.
        let mut add_result = AddBlocksResult::new();

        for new_block_reference in local_blocks_processed {
            let block = self
                .blocks_processed
                .get(&new_block_reference)
                .expect("We added this to the blocks processed above.");

            // Update our own next block
            self.own_next_block.push(block.into_include());

            for base_item in block.get_base_statements() {
                match base_item {
                    BaseStatement::Share(txid, tx) => {
                        // This is a new transactions, so insert it, and ask for a vote.
                        if !self.transaction_entries.contains_key(&txid) {
                            self.transaction_entries
                                .insert(*txid, TransactionEntry::new(*txid, tx.clone()));
                            add_result.newly_added_transactions.push(*txid);
                        }
                    }
                    BaseStatement::Vote(txid, vote) => {
                        // Record the vote, if it is fresh
                        if let Some(entry) = self.transaction_entries.get_mut(txid) {
                            // If we have not seen this vote before, then add it.
                            if entry.add_vote(block.get_authority().clone(), vote.clone()) {
                                add_result
                                    .transactions_with_fresh_votes
                                    .insert(txid.clone());
                            }
                        }
                    }
                }
            }
        }

        add_result
    }

    /// List authorities waiting on a particular block reference. These are the authorities
    /// in the HashSet contained in block_reference_waiting.
    pub fn get_authorities_waiting_on_block(
        &self,
        block_ref: &BlockReference,
    ) -> Option<HashSet<Authority>> {
        let mut result = HashSet::new();
        if let Some(waiting_references) = self.block_references_waiting.get(block_ref) {
            for waiting_reference in waiting_references {
                result.insert(waiting_reference.0.clone());
            }
            return Some(result);
        }
        None
    }

    /// We guarantee that these base statements will be broadcast until included by others
    /// or epoch change, after they have been signaled as included.
    pub fn add_base_statements(
        &mut self,
        our_name: &Authority,
        statements: Vec<BaseStatement>,
    ) -> SequenceNumber {
        for base_statement in statements {
            match base_statement {
                BaseStatement::Share(txid, tx) => {
                    // Our own transactions must not have been seen before.
                    // This is a soft constraint, if we send a tx twice we just skip.
                    if self.transaction_entries.contains_key(&txid) {
                        continue;
                    }
                    self.transaction_entries
                        .insert(txid.clone(), TransactionEntry::new(txid, tx.clone()));
                    self.own_next_block
                        .push(MetaStatement::Base(BaseStatement::Share(txid, tx)));
                }
                BaseStatement::Vote(txid, vote) => {
                    // We should vote on existing transactions and only once.
                    // This is a hard constraint, and violation indicates serious issues, and we panic!
                    if !self.transaction_entries.contains_key(&txid)
                        || self
                            .transaction_entries
                            .get(&txid)
                            .unwrap()
                            .has_voted(our_name)
                    {
                        panic!("Vote should be on a transaction that exists and has not been voted on before.")
                    }

                    self.transaction_entries
                        .get_mut(&txid)
                        .unwrap()
                        .add_vote(our_name.clone(), vote.clone());
                    self.own_next_block
                        .push(MetaStatement::Base(BaseStatement::Vote(txid, vote)));
                }
            }
        }

        self.next_sequence_number()
    }

    /// A link into the structure of missing blocks
    pub fn missing_blocks(&self) -> &HashMap<BlockReference, HashSet<BlockReference>> {
        &self.block_references_waiting
    }

    /// The sequence number of the next block to be created. Items in previous sequence numbers
    /// will be broadcast until they are included by all or end of epoch.
    pub fn next_sequence_number(&self) -> SequenceNumber {
        self.own_next_sequence_number
    }

    /// Get a transaction from an entry by reference
    pub fn get_transaction(&self, txid: &TransactionId) -> Option<&Transaction> {
        self.transaction_entries
            .get(txid)
            .map(|entry| entry.get_transaction())
    }

    pub fn get_transaction_entry(&self, txid: &TransactionId) -> Option<&TransactionEntry> {
        self.transaction_entries.get(txid)
    }

    pub fn seal_next_block(
        &mut self,
        our_name: Authority,
        sequence_number: SequenceNumber,
    ) -> BlockReference {
        // Assert sequence number is higher than next sequence number
        assert!(sequence_number >= self.own_next_sequence_number);

        // Find the index of the first include in own_next_block that has a reference
        // to a block with sequence number equal or larger to sequence_number.
        let first_include_index = self
            .own_next_block
            .iter()
            .position(|statement| match statement {
                MetaStatement::Include(block_ref) => block_ref.1 >= sequence_number,
                _ => false,
            })
            .unwrap_or(self.own_next_block.len());

        let take_entries: Vec<_> = self.own_next_block.drain(..first_include_index).collect();

        // Make a new block
        let block = MetaStatementBlock::new(&our_name, sequence_number, take_entries);

        let block_ref = block.get_reference().clone();

        // Update our own next block
        self.own_next_sequence_number = sequence_number + 1;

        // Add the block to the block manager
        let results = self.add_blocks(vec![block].into_iter().collect());

        // Assert that the results are empty, since we just added this block.
        assert!(results.newly_added_transactions.is_empty());
        assert!(results.transactions_with_fresh_votes.is_empty());

        block_ref
    }
}

trait PersistBlockManager {}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::*;

    fn make_test_committee() -> Arc<Committee> {
        Committee::new(0, vec![1, 1, 1, 1])
    }

    #[test]
    fn add_one_block_no_dependencies() {
        let mut bm = BlockManager::default();
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);
        let block = MetaStatementBlock::new_for_testing(&auth0, 0);
        bm.add_blocks([block].into_iter().collect());
    }

    #[test]
    fn add_one_block_one_met_dependency() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);

        let block0 = MetaStatementBlock::new_for_testing(&auth0, 0);
        let block1 =
            MetaStatementBlock::new_for_testing(&auth0, 1).extend_with(block0.into_include());

        // Add one then the other block. Dependencies are met in order, no pending.
        let mut bm = BlockManager::default();
        bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(bm.blocks_pending.len() == 0);
        bm.add_blocks([block1.clone()].into_iter().collect());
        assert!(bm.blocks_pending.len() == 0);

        // Add out of order, one by one. First dependecy not me, then met.
        let mut bm = BlockManager::default();
        bm.add_blocks([block1.clone()].into_iter().collect());
        assert!(bm.blocks_pending.len() == 1);
        assert!(bm.block_references_waiting.len() == 1);
        bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(bm.blocks_pending.len() == 0);
        assert!(bm.block_references_waiting.len() == 0);

        // In order but added in one go, in correct order.
        let mut bm = BlockManager::default();
        bm.add_blocks([block0.clone(), block1.clone()].into_iter().collect());
        assert!(bm.blocks_pending.len() == 0);
        assert!(bm.block_references_waiting.len() == 0);

        // In order but added in one go, in reverse order.
        let mut bm = BlockManager::default();
        bm.add_blocks([block1.clone(), block0.clone()].into_iter().collect());
        assert!(bm.blocks_pending.len() == 0);
        assert!(bm.block_references_waiting.len() == 0);

        assert!(bm.own_next_block.len() == 2);
    }

    #[test]
    fn test_two_transactions_ordering() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);

        let block0 =
            MetaStatementBlock::new_for_testing(&auth0, 0).extend_with(make_test_transaction(0));
        let block1 = MetaStatementBlock::new_for_testing(&auth0, 1)
            .extend_with(block0.into_include())
            .extend_with(make_test_transaction(1));

        // Add one then the other block. Dependencies are met in order, no pending.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 1);
        let t1 = bm.add_blocks([block1.clone()].into_iter().collect());
        assert!(t1.get_newly_added_transactions().len() == 1);

        // Wrong order
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks([block1.clone()].into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 0);
        let t1 = bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(t1.get_newly_added_transactions().len() == 2);
        assert!(t1.get_newly_added_transactions()[0] == 0);
        assert!(t1.get_newly_added_transactions()[1] == 1);

        // Repetition
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 1);
        let t1 = bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(t1.get_newly_added_transactions().len() == 0);
    }

    // Submitting twice the same transaction only returns it the first time.
    #[test]
    pub fn two_same_blocks_with_same_transaction() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);

        let block0 =
            MetaStatementBlock::new_for_testing(&auth0, 0).extend_with(make_test_transaction(0));
        let block1 = MetaStatementBlock::new_for_testing(&auth0, 1)
            .extend_with(block0.into_include())
            .extend_with(make_test_transaction(0));

        // Add one then the other block. Dependencies are met in order, no pending.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 1);
        let t1 = bm.add_blocks([block1.clone()].into_iter().collect());
        assert!(t1.get_newly_added_transactions().len() == 0);
    }

    #[test]
    fn test_two_transactions_ordering_embeded() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);

        let block0 =
            MetaStatementBlock::new_for_testing(&auth0, 0).extend_with(make_test_transaction(0));
        let block1 = MetaStatementBlock::new_for_testing(&auth0, 1)
            .extend_with(block0.into_include())
            .extend_with(make_test_transaction(1))
            .extend_with(make_test_transaction(0));

        // Check that when I add the blocks to a block manager only two transactions are returned.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 1);
        let t1 = bm.add_blocks([block1.clone()].into_iter().collect());
        assert!(t1.get_newly_added_transactions().len() == 1);

        // Insert the blocks in the bm in one call of add_blocks.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks([block0.clone(), block1.clone()].into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 2);
        // check the transaction in block0 is returned first.
        assert!(t0.get_newly_added_transactions()[0] == 0);
        assert!(t0.get_newly_added_transactions()[1] == 1);

        // Now inster the blocks in the opposite order, and test that the same transactions are returned.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks([block1.clone(), block0.clone()].into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 2);
        // check the transaction in block0 is returned first.
        assert!(t0.get_newly_added_transactions()[0] == 0);
        assert!(t0.get_newly_added_transactions()[1] == 1);
    }

    // A test that creates many blocks and transaction, inlcudes them in the block manager in different orders
    // and checks that the transactions are returned in the correct order.
    #[test]
    pub fn test_many_blocks() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);

        let mut blocks = vec![];
        for i in 0..100 {
            blocks.push(
                MetaStatementBlock::new_for_testing(&auth0, i)
                    .extend_with(make_test_transaction(i)),
            );
        }

        // Add blocks in order.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks(blocks.clone().into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 100);
        for i in 0..100 {
            assert!(t0.get_newly_added_transactions()[i] == i as u64);
        }

        // Add blocks in reverse order.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks(blocks.clone().into_iter().rev().collect());
        assert_eq!(t0.get_newly_added_transactions().len(), 100);
        for i in 0..100 {
            assert!(t0.get_newly_added_transactions()[i] == 99 - i as u64);
        }
    }

    // Make 100 blocks each including the previous one. Each block needs to contain one transaction.
    // Insert them in two different block managers in different orders, and check all transactions are
    // returned in the same order.
    #[test]
    pub fn test_many_blocks_embeded() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);

        let mut blocks = vec![];
        for i in 0..100 {
            if i == 0 {
                blocks.push(
                    MetaStatementBlock::new_for_testing(&auth0, i)
                        .extend_with(make_test_transaction(i)),
                );
            } else {
                blocks.push(
                    MetaStatementBlock::new_for_testing(&auth0, i)
                        .extend_with(blocks[i as usize - 1].clone().into_include())
                        .extend_with(make_test_transaction(i)),
                );
            }
        }

        // Add blocks in order.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks(blocks.clone().into_iter().collect());
        assert!(t0.get_newly_added_transactions().len() == 100);
        for i in 0..100 {
            assert!(t0.get_newly_added_transactions()[i] == i as u64);
        }

        // Add blocks in reverse order.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks(blocks.clone().into_iter().rev().collect());
        assert_eq!(t0.get_newly_added_transactions().len(), 100);
        for i in 0..100 {
            assert!(t0.get_newly_added_transactions()[i] == i as u64);
        }
    }

    #[test]
    fn add_one_block_and_check_authority_reference() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);

        let block0 = MetaStatementBlock::new_for_testing(&auth0, 0);
        let block1 =
            MetaStatementBlock::new_for_testing(&auth0, 1).extend_with(block0.into_include());

        // Add out of order, one by one. First dependecy not me, then met.
        let mut bm = BlockManager::default();
        bm.add_blocks([block1.clone()].into_iter().collect());
        // check that the authority reference is set and equal to authority in block0
        assert!(
            bm.get_authorities_waiting_on_block(block0.get_reference())
                == Some(HashSet::from([block1.get_reference().0.clone()]))
        );
        // When the block is added, the authority reference should be removed.
        bm.add_blocks([block0.clone()].into_iter().collect());
        assert!(bm.get_authorities_waiting_on_block(block0.get_reference()) == None);
    }

    /// Make 4 blocks from 4 difference authorities. All blocks besides the fist include the first block.
    /// The first block contains a transaction and all blocks contain a vote for the transaction. We add
    /// all blocks into a block manager, and check that the votes are all present in the TransactionEntry
    /// for the transaction.
    #[test]
    fn add_blocks_with_votes() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);
        let auth1 = cmt.get_rich_authority(1);
        let auth2 = cmt.get_rich_authority(2);
        let auth3 = cmt.get_rich_authority(3);

        let tx = make_test_transaction(0);
        let vote = make_test_vote_accept(0);
        let block0 = MetaStatementBlock::new_for_testing(&auth0, 0)
            .extend_with(tx)
            .extend_with(vote.clone());
        let block1 = MetaStatementBlock::new_for_testing(&auth1, 1)
            .extend_with(block0.clone().into_include())
            .extend_with(vote.clone());
        let block2 = MetaStatementBlock::new_for_testing(&auth2, 2)
            .extend_with(block0.clone().into_include())
            .extend_with(vote.clone());
        let block3 = MetaStatementBlock::new_for_testing(&auth3, 3)
            .extend_with(block0.clone().into_include())
            .extend_with(vote.clone());

        // Add all blocks to a block manager and check that the transaction is present in the return value.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks(
            [
                block0.clone(),
                block1.clone(),
                block2.clone(),
                block3.clone(),
            ]
            .into_iter()
            .collect(),
        );
        assert!(t0.get_newly_added_transactions().len() == 1);

        // Check that the vote is also included in the return value.
        assert!(t0.get_transactions_with_fresh_votes().len() == 1);

        // The authorities keys in the entry for the transaction should equal the authorities that voted.
        let tx_entry = bm.get_transaction_entry(&0).unwrap();
        assert!(tx_entry.accept_votes.len() == 4);
        assert!(tx_entry.reject_votes.is_empty());
        assert!(tx_entry.reject_votes.is_empty());

        // Make another block from authority 1 with the same vote
        let block4 = MetaStatementBlock::new_for_testing(&auth1, 4)
            .extend_with(block0.clone().into_include())
            .extend_with(vote.clone());
        // Add it to the bm and check that the vote is not included in the return value.
        let t1 = bm.add_blocks([block4.clone()].into_iter().collect());
        assert!(t1.get_newly_added_transactions().is_empty());
        assert!(t1.get_transactions_with_fresh_votes().is_empty());
    }

    // Make 4 blocks from 4 difference authorities. All blocks besides the fist include the first block.
    // Each block contains a different transaction, and a vote for the first transaction.
    // At the end we seal a new block, amd check that the reference is as expected
    #[test]
    fn seal_block() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);
        let auth1 = cmt.get_rich_authority(1);
        let auth2 = cmt.get_rich_authority(2);
        let auth3 = cmt.get_rich_authority(3);

        let tx0 = make_test_transaction(0);
        let tx1 = make_test_transaction(1);
        let tx2 = make_test_transaction(2);
        let tx3 = make_test_transaction(3);
        let vote = make_test_vote_accept(0);
        let block0 = MetaStatementBlock::new_for_testing(&auth0, 0)
            .extend_with(tx0.clone())
            .extend_with(vote.clone());
        let block1 = MetaStatementBlock::new_for_testing(&auth1, 1)
            .extend_with(block0.clone().into_include())
            .extend_with(tx1.clone())
            .extend_with(vote.clone());
        let block2 = MetaStatementBlock::new_for_testing(&auth2, 2)
            .extend_with(block0.clone().into_include())
            .extend_with(tx2.clone())
            .extend_with(vote.clone());
        let block3 = MetaStatementBlock::new_for_testing(&auth3, 3)
            .extend_with(block0.clone().into_include())
            .extend_with(tx3.clone())
            .extend_with(vote.clone());

        // Add all blocks to a block manager and check that the transaction is present in the return value.
        let mut bm = BlockManager::default();
        let t0 = bm.add_blocks(
            [
                block0.clone(),
                block1.clone(),
                block2.clone(),
                block3.clone(),
            ]
            .into_iter()
            .collect(),
        );
        assert!(t0.get_newly_added_transactions().len() == 4);

        // Seal a block with all transactions. The reference should be to block0.
        let sealed_block_reference = bm.seal_next_block(auth0.clone(), 1);
        assert!(sealed_block_reference == (auth0, 1, 0));

        // Check own next block includes this reference as the first entry
        assert!(
            *bm.own_next_block.last().unwrap() == MetaStatement::Include(sealed_block_reference)
        );

        // Check that the first entry in the bm.own_next_block is an include statement for a block with
        // sequence number 1 and authority 1.
        let include = bm.own_next_block.first().unwrap();
        if let MetaStatement::Include((auth, seq, _)) = include {
            assert!(*auth == auth1);
            assert!(*seq == 1);
        } else {
            panic!("First entry in own_next_block is not an include statement");
        }
    }
}
