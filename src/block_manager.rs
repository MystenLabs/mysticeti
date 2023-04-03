use crate::types::{
    Authority, BaseStatement, BlockReference, MetaStatement, MetaStatementBlock, RoundNumber,
    Transaction, TransactionId, Vote,
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

// Algorithm that takes ONE MetaStatement::Block(Authority, RoundNumber, BlockDigest, Vec<MetaStatement>)
// and turns it into a sequence Vec<BaseStatement> where all base statements are emitted by the Authority that
// made the block.

#[derive(Default)]
pub struct BlockManager {
    /// Structures to keep track of blocks incl our own.
    blocks_pending: HashMap<BlockReference, MetaStatementBlock>,
    block_references_waiting: HashMap<BlockReference, HashSet<BlockReference>>,

    /// Our own strucutres for the next block
    own_next_round_number: RoundNumber,
    own_next_block: VecDeque<MetaStatement>,

    /// The transactions and how many votes they got so far, incl potential conlicts.
    transaction_entries: HashMap<TransactionId, TransactionEntry>,
    blocks_processed: HashMap<BlockReference, MetaStatementBlock>,

    // Maintain an index of blocks by round as well.
    blocks_processed_by_round: HashMap<RoundNumber, Vec<BlockReference>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionStatus {
    New,
    PendingVotes,
    Certified,
    Executed,
    Final,
}

/// A TransactionEntry structure stores the transactionId, the Transaction, and two maps. One that
/// stores all votes seen by authorities that are accept, and one that stores all votes that are reject.

pub struct TransactionEntry {
    #[allow(dead_code)]
    transaction_id: TransactionId,
    transaction: Transaction,
    status: TransactionStatus,
    accept_votes: HashSet<Authority>,
    reject_votes: HashMap<Authority, Vote>,
}

impl TransactionEntry {
    /// A constructor for a new TransactionEntry, that takes a transactionId and a transaction.
    pub fn new(transaction_id: TransactionId, transaction: Transaction) -> Self {
        TransactionEntry {
            transaction_id,
            transaction,
            status: TransactionStatus::New,
            accept_votes: HashSet::new(),
            reject_votes: HashMap::new(),
        }
    }

    pub fn set_status(&mut self, status: TransactionStatus) {
        self.status = status;
    }

    /// Adds a vote to the TransactionEntry.
    pub fn add_vote(&mut self, authority: Authority, vote: Vote) -> bool {
        // if the status is new, then we need to change it to pending votes.
        // (Note: if higher than New we do not change the status as we do not want
        // to downgrade the status.)
        if self.status == TransactionStatus::New {
            self.status = TransactionStatus::PendingVotes;
        }

        // Then we update the vote
        match vote {
            Vote::Accept => self.accept_votes.insert(authority),
            Vote::Reject(_) => {
                self.reject_votes.insert(authority, vote);
                true // Always return since it might be a new conflict.
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

    /// A reference to the accept votes
    pub fn get_accept_votes(&self) -> &HashSet<Authority> {
        &self.accept_votes
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
    // Set the status of an array of transactions / status tuples, and return their previous status. Panic if the
    // transaction id is not found.
    pub fn set_status<'a>(
        &mut self,
        transaction_id: &'a [(TransactionId, TransactionStatus)],
    ) -> Vec<(&'a TransactionId, TransactionStatus)> {
        let mut result = Vec::with_capacity(transaction_id.len());
        for (txid, status) in transaction_id {
            if let Some(entry) = self.transaction_entries.get_mut(txid) {
                result.push((txid, entry.status));
                entry.status = *status;
            } else {
                panic!("Transaction id not found");
            }
        }
        result
    }

    // Get the status of an array of transaction IDs, return some status or None if not found.
    pub fn get_status<'a>(
        &mut self,
        transaction_id: &'a [TransactionId],
    ) -> Vec<(&'a TransactionId, Option<TransactionStatus>)> {
        let mut result = Vec::with_capacity(transaction_id.len());
        for txid in transaction_id {
            if let Some(entry) = self.transaction_entries.get(txid) {
                result.push((txid, Some(entry.status)));
            } else {
                result.push((txid, None));
            }
        }
        result
    }

    /// Returns a reference to blocks for this round or None
    pub fn get_blocks_for_round(&self, round: RoundNumber) -> Option<&Vec<BlockReference>> {
        self.blocks_processed_by_round.get(&round)
    }

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
                if !self.blocks_processed.contains_key(included_reference) {
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
                            .iter()
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
            self.own_next_block.push_back(block.into_include());

            for base_item in block.get_base_statements() {
                match base_item {
                    BaseStatement::Share(txid, tx) => {
                        // This is a new transactions, so insert it, and ask for a vote.
                        if !self.transaction_entries.contains_key(txid) {
                            self.transaction_entries
                                .insert(*txid, TransactionEntry::new(*txid, *tx));
                            add_result.newly_added_transactions.push(*txid);
                        }
                    }
                    BaseStatement::Vote(txid, vote) => {
                        // Record the vote, if it is fresh
                        if let Some(entry) = self.transaction_entries.get_mut(txid) {
                            // If we have not seen this vote before, then add it.
                            if entry.add_vote(block.get_authority().clone(), vote.clone()) {
                                add_result.transactions_with_fresh_votes.insert(*txid);
                            }
                        }
                    }
                }
            }
        }

        add_result
    }

    /// We look back at the includes from latest_round - depth. And we list which of the authorities with blocks in
    /// the round latest_round include the blocks indirectly though the references they include.
    pub fn get_blocks_consensus_committed(
        &self,
        latest_round: RoundNumber,
        target_round: RoundNumber,
    ) -> HashMap<BlockReference, HashSet<Authority>> {
        assert!(target_round < latest_round);
        let mut result: HashMap<BlockReference, HashSet<Authority>> = HashMap::new();
        let mut round_to_included_history: HashMap<BlockReference, HashSet<BlockReference>> =
            HashMap::new();

        // Seed the result with the block references at round target_round to start with
        for block_reference in self
            .blocks_processed_by_round
            .get(&target_round)
            .unwrap_or(&vec![])
        {
            round_to_included_history.insert(block_reference.clone(), HashSet::new());
            result.insert(block_reference.clone(), HashSet::new());
        }

        // Now we walk the rounds from target_round to latest_round, and add the blocks that are included
        // by in the round, according to the includes in the blocks.
        for round in target_round..=latest_round {
            let blocks_in_later_round = self.blocks_processed_by_round.get(&round).unwrap();
            for block_reference_in_later_round in blocks_in_later_round {
                let later_block = self
                    .blocks_processed
                    .get(block_reference_in_later_round)
                    .unwrap();

                // Add the direct includes to the result
                round_to_included_history
                    .entry(block_reference_in_later_round.clone())
                    .or_default()
                    .extend(later_block.get_includes().clone());

                // Add the indirect includes to the result
                for block_reference_in_earlier_round in later_block.get_includes() {
                    let included_blocks = round_to_included_history
                        .get(block_reference_in_earlier_round)
                        .unwrap()
                        .clone();
                    round_to_included_history
                        .get_mut(block_reference_in_later_round)
                        .unwrap()
                        .extend(included_blocks)
                }
            }
        }

        // Remove from result all keys not in latest_round
        round_to_included_history.retain(|key, _| key.1 == latest_round);

        // Take each reference in the values of the map round_to_included_history and make an entry in result. Add the authority
        // in the key of the map round_to_included_history coresponding to the value, to the value in result.
        for (block_reference, included_blocks) in round_to_included_history {
            for included_block in included_blocks {
                // Only care about the blocks in the target round
                if included_block.1 != target_round {
                    continue;
                }

                // List the authority that links to this block (directly or indirectly)
                result
                    .entry(included_block)
                    .or_default()
                    .insert(block_reference.0.clone());
            }
        }

        result
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
    ) -> RoundNumber {
        for base_statement in statements {
            match base_statement {
                BaseStatement::Share(txid, tx) => {
                    // Our own transactions must not have been seen before.
                    // This is a soft constraint, if we send a tx twice we just skip.
                    if self.transaction_entries.contains_key(&txid) {
                        continue;
                    }
                    self.transaction_entries
                        .insert(txid, TransactionEntry::new(txid, tx));
                    self.own_next_block
                        .push_back(MetaStatement::Base(BaseStatement::Share(txid, tx)));
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

                    // Get a mutable reference to the transaction entry and add the vote
                    let entry = self.transaction_entries.get_mut(&txid).unwrap();

                    // If the status is new we change the status to pending
                    if entry.status == TransactionStatus::New {
                        entry.status = TransactionStatus::PendingVotes;
                    }

                    // If the status is pending we add it to our block -- no need to add certified
                    // or higher status transactions to our block, since including our history will
                    // make others reach the same conclusion.
                    if entry.status == TransactionStatus::PendingVotes {
                        entry.add_vote(our_name.clone(), vote.clone());
                        self.own_next_block
                            .push_back(MetaStatement::Base(BaseStatement::Vote(txid, vote)));
                    }
                }
            }
        }

        self.next_round_number()
    }

    /// A link into the structure of missing blocks
    pub fn missing_blocks(&self) -> &HashMap<BlockReference, HashSet<BlockReference>> {
        &self.block_references_waiting
    }

    /// The round number of the next block to be created. Items in previous round numbers
    /// will be broadcast until they are included by all or end of epoch.
    pub fn next_round_number(&self) -> RoundNumber {
        self.own_next_round_number
    }

    /// Set the next own round number
    pub fn set_next_round_number(&mut self, round_number: RoundNumber) {
        self.own_next_round_number = round_number;
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

    pub fn get_transaction_entry_mut(
        &mut self,
        txid: &TransactionId,
    ) -> Option<&mut TransactionEntry> {
        self.transaction_entries.get_mut(txid)
    }

    pub fn seal_next_block(
        &mut self,
        our_name: Authority,
        round_number: RoundNumber,
    ) -> BlockReference {
        // Assert round number is higher than next round number
        assert!(round_number >= self.own_next_round_number);

        // Find the index of the first include in own_next_block that has a reference
        // to a block with round number equal or larger to round_number.
        let first_include_index = self
            .own_next_block
            .iter()
            .position(|statement| match statement {
                MetaStatement::Include(block_ref) => block_ref.1 >= round_number,
                _ => false,
            })
            .unwrap_or(self.own_next_block.len());

        let take_entries: Vec<_> = self.own_next_block.drain(..first_include_index).collect();

        // Compress the references in the block
        // Iterate through all the include statements in the block, and make a set of all the references in their includes.
        let mut references_in_block = HashSet::new();
        for statement in &take_entries {
            if let MetaStatement::Include(block_ref) = statement {
                // for all the includes in the block, add the references in the block to the set
                if let Some(block) = self.blocks_processed.get(block_ref) {
                    references_in_block.extend(block.get_includes());
                }
            }
        }

        // Only keep the includes in the take_entries vector that are not in the references_in_block set.
        let take_entries: Vec<_> = take_entries
            .into_iter()
            .filter(|statement| match statement {
                MetaStatement::Include(block_ref) => !references_in_block.contains(block_ref),
                _ => true,
            })
            .collect();

        // Make a new block
        let block = MetaStatementBlock::new(&our_name, round_number, take_entries);
        let block_ref = block.get_reference().clone();

        // Update our own next block
        self.own_next_round_number = round_number + 1;

        self.blocks_processed.insert(block_ref.clone(), block);
        self.blocks_processed_by_round
            .entry(block_ref.1)
            .or_default()
            .push(block_ref.clone());
        self.own_next_block
            .push_front(MetaStatement::Include(block_ref.clone()));

        block_ref
    }

    // Get a reference to the processed blocks
    pub fn get_blocks_processed(&self) -> &HashMap<BlockReference, MetaStatementBlock> {
        &self.blocks_processed
    }

    // Get a reference to blocks processed by round
    pub fn get_blocks_processed_by_round(&self) -> &HashMap<RoundNumber, Vec<BlockReference>> {
        &self.blocks_processed_by_round
    }
}

trait PersistBlockManager {}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::types::Committee;

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

        // Check own next block includes this reference as the first entry as the first item
        assert!(
            *bm.own_next_block.iter().next().unwrap()
                == MetaStatement::Include(sealed_block_reference)
        );

        // Check that the seconds entry is the next block reference
        let mut iter = bm.own_next_block.iter();
        iter.next();

        let include = iter.next().unwrap();
        if let MetaStatement::Include((auth, seq, _)) = include {
            assert!(*auth == auth1);
            assert!(*seq == 1);
        } else {
            panic!("First entry in own_next_block is not an include statement");
        }
    }

    #[test]
    fn seal_block_compress() {
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
            .extend_with(block1.clone().into_include())
            .extend_with(tx2.clone())
            .extend_with(vote.clone());
        let block3 = MetaStatementBlock::new_for_testing(&auth3, 3)
            .extend_with(block2.clone().into_include())
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
        let sealed_block_reference = bm.seal_next_block(auth0.clone(), 4);
        assert!(sealed_block_reference == (auth0.clone(), 4, 0));

        // Check that the includes of the block do not contain block2, block1 or block0 -- they are compressed away
        assert!(bm
            .blocks_processed
            .get(&sealed_block_reference)
            .unwrap()
            .get_includes()
            .iter()
            .all(|x| {
                x != block0.get_reference()
                    && x != block1.get_reference()
                    && x != block2.get_reference()
            }));

        // But it includes block3
        assert!(bm
            .blocks_processed
            .get(&sealed_block_reference)
            .unwrap()
            .get_includes()
            .iter()
            .all(|x| { x == block3.get_reference() }));
    }

    // Make 4 authorities. For rounds 0, 1 and 2 they all make a block. And the block contains 3 blocks from the previous round.
    // Then we call get_blocks_consensus_committed for target round 0 and latest round 2 and chekck that the result is correct.
    #[test]
    fn get_blocks_consensus_committed_irregular() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);
        let auth1 = cmt.get_rich_authority(1);
        let auth2 = cmt.get_rich_authority(2);
        let auth3 = cmt.get_rich_authority(3);

        let block01 = MetaStatementBlock::new_for_testing(&auth0, 0);
        let block02 = MetaStatementBlock::new_for_testing(&auth1, 0);
        let block11 = MetaStatementBlock::new_for_testing(&auth2, 1)
            .extend_with(block01.clone().into_include());
        let block12 = MetaStatementBlock::new_for_testing(&auth3, 1)
            .extend_with(block02.clone().into_include());
        let block21 = MetaStatementBlock::new_for_testing(&auth0, 2)
            .extend_with(block11.clone().into_include())
            .extend_with(block12.clone().into_include());
        let block22 = MetaStatementBlock::new_for_testing(&auth1, 2);

        // Add all blocks to a block manager and check that the transaction is present in the return value.
        let mut bm = BlockManager::default();
        let _t0 = bm.add_blocks(
            [
                block01.clone(),
                block02.clone(),
                block11.clone(),
                block12.clone(),
                block21.clone(),
                block22.clone(),
            ]
            .into_iter()
            .collect(),
        );

        // Check there are 6 blocks processed
        assert!(bm.blocks_processed.len() == 6);

        let result = bm.get_blocks_consensus_committed(2, 0);
        // Ensure we have two blocks are a result
        assert!(result.len() == 2);

        // Assert all keys in result are block reference for round 0
        assert!(result.keys().all(|x| x.1 == 0));

        // Assert only auth0 is in the values of the result
        assert!(result.values().all(|x| x.contains(&auth0)));
    }

    #[test]
    fn get_blocks_consensus_committed_unconnected() {
        let cmt = make_test_committee();
        let auth0 = cmt.get_rich_authority(0);
        let auth1 = cmt.get_rich_authority(1);
        let auth2 = cmt.get_rich_authority(2);
        let auth3 = cmt.get_rich_authority(3);

        let block01 = MetaStatementBlock::new_for_testing(&auth0, 0);
        let block02 = MetaStatementBlock::new_for_testing(&auth1, 0);
        let block11 = MetaStatementBlock::new_for_testing(&auth2, 1);
        let block12 = MetaStatementBlock::new_for_testing(&auth3, 1);
        let block21 = MetaStatementBlock::new_for_testing(&auth0, 2);
        let block22 = MetaStatementBlock::new_for_testing(&auth1, 2);

        // Add all blocks to a block manager and check that the transaction is present in the return value.
        let mut bm = BlockManager::default();
        let _t0 = bm.add_blocks(
            [
                block01.clone(),
                block02.clone(),
                block11.clone(),
                block12.clone(),
                block21.clone(),
                block22.clone(),
            ]
            .into_iter()
            .collect(),
        );

        // Check there are 6 blocks processed
        assert!(bm.blocks_processed.len() == 6);

        let result = bm.get_blocks_consensus_committed(2, 0);
        // Ensure we have two blocks are a result
        assert!(result.len() == 2);

        // Assert all keys are for round 0
        assert!(result.keys().all(|x| x.1 == 0));
        // Assert all value HashSets are empty
        assert!(result.values().all(|x| x.is_empty()));
    }
}
