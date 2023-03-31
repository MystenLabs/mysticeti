use std::sync::Arc;

use crate::block_manager::BlockManager;
use crate::threshold_clock::*;
use crate::types::{Authority, Committee, MetaStatementBlock};

// In this file we define a node that combines a block manager with the constraints
// of the threshold clock, in order to make a node that can fully run the mysticeti
// DAG. In the tests we create an example network to ensure the APIs make sense.

pub struct Node {
    pub auth: Authority,
    pub block_manager: BlockManager,
}

pub fn genesis(committee: &Arc<Committee>) -> Vec<MetaStatementBlock> {
    // For each authority in the committee, create a block at round 0 from this authority
    // with no includes.
    committee
        .get_authorities()
        .iter()
        .enumerate()
        .map(|(i, _authority)| {
            MetaStatementBlock::new(&committee.get_rich_authority(i), 0, Vec::new())
        })
        .collect()
}

impl Node {
    pub fn new(auth: Authority) -> Self {
        // Create a default block manager
        let mut block_manager = BlockManager::default();
        // Get the genesis blocks for this committee
        let genesis_blocks = genesis(auth.get_committee());
        // Add the genesis blocks to the block manager
        block_manager.add_blocks(genesis_blocks.into());
        block_manager.set_next_round_number(1);

        Node {
            auth,
            block_manager,
        }
    }
}

#[cfg(test)]
mod tests {

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
}
