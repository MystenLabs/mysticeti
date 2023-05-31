use std::collections::{HashMap, HashSet};

use crate::block_manager::{AddBlocksResult, BlockManager, TransactionStatus};
use crate::threshold_clock::*;
use crate::types::{
    Authority, BaseStatement, BlockReference, MetaStatementBlock, RichAuthority, RoundNumber,
    TransactionId,
};

// In this file we define a node that combines a block manager with the constraints
// of the threshold clock, in order to make a node that can fully run the mysticeti
// DAG. In the tests we create an example network to ensure the APIs make sense.

pub struct Node {
    pub auth: Authority,
    pub block_manager: BlockManager,
    pub last_commit_round: RoundNumber,
}

impl Node {
    pub fn new(auth: Authority) -> Self {
        // Create a default block manager
        let mut block_manager = BlockManager::default();
        // Get the genesis blocks for this committee
        let genesis_blocks = MetaStatementBlock::genesis(auth.get_committee());
        // Add the genesis blocks to the block manager
        block_manager.add_blocks(genesis_blocks.into());
        block_manager.set_next_round_number(1);

        Node {
            auth,
            block_manager,
            last_commit_round: 0,
        }
    }

    // Upgrade a list of transaction Ids to certified if they have a quorum of votes
    pub fn upgrade_to_certified(
        &mut self,
        transaction_ids: &Vec<TransactionId>,
    ) -> Vec<TransactionId> {
        let transaction_status_list = self.block_manager.get_status(transaction_ids);
        let mut newly_certified_transactions = Vec::new();

        for (transaction_id, transaction_status) in transaction_status_list {
            if transaction_status.as_ref().unwrap() == &TransactionStatus::PendingVotes {
                let transaction = self
                    .block_manager
                    .get_transaction_entry_mut(transaction_id)
                    .unwrap();
                let total_stake = self
                    .auth
                    .get_committee()
                    .get_total_stake(&transaction.get_accept_votes());
                if self.auth.get_committee().is_quorum(total_stake) {
                    transaction.set_status(TransactionStatus::Certified);
                    newly_certified_transactions.push(*transaction_id);
                }
            }
        }

        newly_certified_transactions
    }

    // Add blocks to the block manager
    pub fn add_blocks(
        &mut self,
        blocks: Vec<MetaStatementBlock>,
    ) -> (Vec<TransactionId>, Vec<TransactionId>) {
        let AddBlocksResult {
            newly_added_transactions,
            transactions_with_fresh_votes,
        } = self.block_manager.add_blocks(blocks.into());

        // For all transactions with fresh votes, we get their status from the
        // block manager and if they are pending votes we check if the votes form a quorum
        // If they do, we change the status to certified, and add the transaction id to a list
        // of transactions that are now certified.
        let transactions_with_fresh_votes: Vec<_> =
            transactions_with_fresh_votes.into_iter().collect();
        let newly_certified_transactions =
            self.upgrade_to_certified(&transactions_with_fresh_votes);

        (newly_added_transactions, newly_certified_transactions)
    }

    pub fn add_base_statements(
        &mut self,
        base_statements: Vec<BaseStatement>,
    ) -> Vec<TransactionId> {
        // Collect all votes into a vector
        let mut transactions_with_fresh_votes = Vec::with_capacity(base_statements.len());
        for base_statement in &base_statements {
            match base_statement {
                BaseStatement::Vote(txid, _) => transactions_with_fresh_votes.push(*txid),
                _ => (),
            }
        }

        self.block_manager
            .add_base_statements(&self.auth, base_statements);
        let newly_certified_transactions =
            self.upgrade_to_certified(&transactions_with_fresh_votes);

        newly_certified_transactions
    }

    pub fn try_commit(&mut self, period: u64) -> Option<BlockReference> {
        let committee = self.auth.get_committee();

        // Assert that for this node we are ready to emit the next block
        let quorum_round = get_highest_threshold_clock_round_number(
            committee.as_ref(),
            self.block_manager.get_blocks_processed_by_round(),
            self.last_commit_round,
        );

        let quorum_round = quorum_round.unwrap();
        if quorum_round > self.last_commit_round.max(period - 1) && quorum_round % period == 0 {
            // Get the authority that is the leader at this round
            let target_authority = self.leader_at_round(quorum_round, period).unwrap();

            println!("Committing round {}", quorum_round);
            let support = self.block_manager.get_decision_round_certificates(
                quorum_round - period,
                &target_authority,
                quorum_round - 1,
            );

            // iterate over all references in support
            let mut certificate_votes: HashMap<&BlockReference, HashSet<&RichAuthority>> =
                HashMap::new();
            for (decision_reference, leader_votes) in support {
                for (potential_cert, votes) in leader_votes {
                    if committee.is_quorum(committee.get_total_stake(&votes)) {
                        let votes = certificate_votes
                            .entry(potential_cert)
                            .or_insert(HashSet::new());
                        votes.insert(&decision_reference.0);
                    }
                }
            }

            // Assert there is only one entry in certificate_votes, since only one certificate can ever
            // exist with a quorum of votes.
            assert!(certificate_votes.len() <= 1);

            // If there is a certificate, then we can commit the block
            for (block_ref, votes) in certificate_votes {
                if committee.is_quorum(committee.get_total_stake(&votes)) {
                    // Set the last commit round
                    self.last_commit_round = quorum_round - period;

                    return Some(block_ref.clone());
                }
            }
        }

        return None;
    }

    pub fn try_new_block(&mut self) -> Option<&MetaStatementBlock> {
        let committee = self.auth.get_committee();

        // Assert that for this node we are ready to emit the next block
        let quorum_round = get_highest_threshold_clock_round_number(
            committee.as_ref(),
            self.block_manager.get_blocks_processed_by_round(),
            self.block_manager.next_round_number() - 1,
        );

        if quorum_round.is_some()
            && quorum_round.clone().unwrap() >= self.block_manager.next_round_number() - 1
        {
            // We are ready to emit the next block
            let next_block_ref = self
                .block_manager
                .seal_next_block(self.auth.clone(), quorum_round.unwrap() + 1);

            self.try_commit(3);
            // TODO: Here we should go back and commit all previous leaders with a certificate in
            // the causal graph.

            let next_block = self
                .block_manager
                .get_blocks_processed()
                .get(&next_block_ref)
                .unwrap();

            return Some(next_block);
        }

        None
    }

    pub fn leader_at_round(&self, round: RoundNumber, period: u64) -> Option<RichAuthority> {
        // We only have leaders for round numbers that are multiples of the period
        if round == 0 || round % period != 0 {
            return None;
        }

        // TODO: fix to select by stake
        let committee = self.auth.get_committee();
        let target_authority = committee
            .get_rich_authority((round / period) as usize % committee.get_authorities().len());
        Some(target_authority)
    }
}

#[cfg(test)]
mod tests {

    use crate::types::{Committee, MetaStatement, Vote};

    use super::*;
    use std::sync::Arc;

    // Make a new node with a committee of 4 authorities each with Stake 1
    // And check that its block manager contains 4 processed blocks
    #[test]
    fn test_new_node_genesis() {
        let committee = Arc::new(Committee::new(0, vec![1, 1, 1, 1]));
        let auth0 = committee.get_rich_authority(0);
        let node = Node::new(auth0);
        assert_eq!(node.block_manager.get_blocks_processed().len(), 4);

        // Check that the blocks indexed by round number allow for a block at round 1 to be emmitted.
        let round = get_highest_threshold_clock_round_number(
            committee.as_ref(),
            node.block_manager.get_blocks_processed_by_round(),
            0,
        );
        assert_eq!(round, Some(0));
    }

    // Make a committee of 4 authorities each with Stake 1, and the corresponding 4 nodes
    // Simulate round 1..1000, where each node emits a block at round number n
    // and at the end of the round all nodes include all blocks from the previous round
    // Check that the threshold clock is valid at each round
    #[test]
    fn test_threshold_clock_valid() {
        let committee = Arc::new(Committee::new(0, vec![1, 1, 1, 1]));
        let auth0 = committee.get_rich_authority(0);
        let auth1 = committee.get_rich_authority(1);
        let auth2 = committee.get_rich_authority(2);
        let auth3 = committee.get_rich_authority(3);

        let mut nodes = vec![
            Node::new(auth0),
            Node::new(auth1),
            Node::new(auth2),
            Node::new(auth3),
        ];

        for round in 1..10 {
            // Each node emits a block at round number n
            let mut blocks = Vec::new();
            for node in &mut nodes {
                // Assert that for this node we are ready to emit the next block
                let quorum_round = get_highest_threshold_clock_round_number(
                    committee.as_ref(),
                    node.block_manager.get_blocks_processed_by_round(),
                    0,
                );
                assert_eq!(quorum_round, Some(round - 1));

                // Seal the block for round
                let next_block_ref = node.block_manager.seal_next_block(node.auth.clone(), round);
                let next_block = node
                    .block_manager
                    .get_blocks_processed()
                    .get(&next_block_ref)
                    .unwrap();
                blocks.push(next_block.clone());
            }

            // Add the blocks to the block manager
            for node in &mut nodes {
                let result = node
                    .block_manager
                    .add_blocks(blocks.clone().into_iter().collect());
                // Assert this result is empty
                assert!(result.get_newly_added_transactions().is_empty());
                assert!(result.get_transactions_with_fresh_votes().is_empty());
            }
        }
    }

    #[test]
    fn test_block_manager_certify() {
        let committee = Arc::new(Committee::new(0, vec![1, 1, 1, 1]));
        let auth0 = committee.get_rich_authority(0);
        let auth1 = committee.get_rich_authority(1);
        let auth2 = committee.get_rich_authority(2);
        let auth3 = committee.get_rich_authority(3);

        let mut node = Node::new(auth0.clone());

        // Make a Share base statement with a new transaction
        let txid = 0;
        let share = BaseStatement::Share(txid, 0);

        // Insert the share into the block manager
        node.add_base_statements(vec![share]);

        // Try to upgrade to certified this transaction
        let newly_certified_transactions = node.upgrade_to_certified(&vec![txid]);
        // check the transaction is not certified
        assert!(newly_certified_transactions.is_empty());

        // get the entry for this transaction
        let entry = node.block_manager.get_transaction_entry_mut(&txid).unwrap();
        entry.add_vote(auth0.clone(), Vote::Accept);
        entry.add_vote(auth1.clone(), Vote::Accept);

        // Try to upgrade to certified this transaction (2 votes are not enough)
        let newly_certified_transactions = node.upgrade_to_certified(&vec![txid]);
        // check the transaction is not certified
        assert!(newly_certified_transactions.is_empty());

        let entry = node.block_manager.get_transaction_entry_mut(&txid).unwrap();
        entry.add_vote(auth2.clone(), Vote::Accept);

        // Try to upgrade to certified this transaction (2 votes are not enough)
        let newly_certified_transactions = node.upgrade_to_certified(&vec![txid]);
        // check txid in the certified list
        assert_eq!(newly_certified_transactions, vec![txid]);

        // But when we add the final vote, we do not get back the txid (since its already certified)
        let entry = node.block_manager.get_transaction_entry_mut(&txid).unwrap();
        entry.add_vote(auth3.clone(), Vote::Accept);
        let newly_certified_transactions = node.upgrade_to_certified(&vec![txid]);
        assert!(newly_certified_transactions.is_empty());
    }

    // Make a committee of 4 authorities each with Stake 1, and the corresponding 4 nodes
    // Make 4 blocks one from each authority
    // The first block contains a new transaction, and a vote for the transaction
    // the other 3 include the first block and a vote for the transaction
    // The blocks are added to the node
    // We check that the transaction is certified
    #[test]
    fn test_block_manager_certify_2() {
        let committee = Arc::new(Committee::new(0, vec![1, 1, 1, 1]));
        let auth0 = committee.get_rich_authority(0);
        let auth1 = committee.get_rich_authority(1);
        let auth2 = committee.get_rich_authority(2);
        let auth3 = committee.get_rich_authority(3);

        let mut node = Node::new(auth0.clone());

        // Make a Share base statement with a new transaction
        let txid = 0;
        let share = BaseStatement::Share(txid, 0);

        // Make a Meta statement Block from auth0 including the share and a vote
        let block0 = MetaStatementBlock::new_for_testing(&auth0, 1)
            .extend_with(MetaStatement::Base(share))
            .extend_with(MetaStatement::Base(BaseStatement::Vote(txid, Vote::Accept)));

        // Make a block from auth1 including the block0 and a vote
        let block1 = MetaStatementBlock::new_for_testing(&auth1, 2)
            .extend_with(MetaStatement::Include(block0.get_reference().clone()))
            .extend_with(MetaStatement::Base(BaseStatement::Vote(txid, Vote::Accept)));

        // Add all the blocks to the node
        node.add_blocks(vec![block0.clone(), block1]);

        // Check that the transaction is pending votes
        let entry = node.block_manager.get_transaction_entry_mut(&txid).unwrap();
        assert_eq!(entry.get_status(), &TransactionStatus::PendingVotes);

        let block2 = MetaStatementBlock::new_for_testing(&auth2, 2)
            .extend_with(MetaStatement::Include(block0.get_reference().clone()))
            .extend_with(MetaStatement::Base(BaseStatement::Vote(txid, Vote::Accept)));

        let block3 = MetaStatementBlock::new_for_testing(&auth3, 2)
            .extend_with(MetaStatement::Include(block0.get_reference().clone()))
            .extend_with(MetaStatement::Base(BaseStatement::Vote(txid, Vote::Accept)));

        // Add all the blocks to the node
        node.add_blocks(vec![block2, block3]);

        // Check that the transaction is certified
        let entry = node.block_manager.get_transaction_entry_mut(&txid).unwrap();
        assert_eq!(entry.get_status(), &TransactionStatus::Certified);
    }
}
