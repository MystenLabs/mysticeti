// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_store::{BlockStore, BlockWriter};
use crate::data::Data;
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use crate::{
    committee::Committee,
    types::{BlockReference, StatementBlock},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

/// Block manager suspends incoming blocks until they are connected to the existing graph,
/// returning newly connected blocks
pub struct BlockManager {
    /// Keeps all pending blocks.
    blocks_pending: HashMap<BlockReference, Data<StatementBlock>>,
    /// Keeps all the blocks (`HashSet<BlockReference>`) waiting for `BlockReference` to be processed.
    block_references_waiting: HashMap<BlockReference, HashSet<BlockReference>>,
    /// Keeps all blocks that need to be synced in order to unblock the processing of other pending
    /// blocks. The indices of the vector correspond the authority indices.
    missing: Vec<HashSet<BlockReference>>,
    block_store: BlockStore,
    metrics: Arc<Metrics>,
}

impl BlockManager {
    pub fn new(block_store: BlockStore, committee: &Arc<Committee>, metrics: Arc<Metrics>) -> Self {
        Self {
            blocks_pending: Default::default(),
            block_references_waiting: Default::default(),
            missing: (0..committee.len()).map(|_| HashSet::new()).collect(),
            block_store,
            metrics,
        }
    }

    /// Attempts to process (accept) the provided blocks and stores them only when the causal history
    /// is already present. If a block can't be processed, then it is parked in the `blocks_pending` map
    /// and any missing references are recorded in the `block_references_waiting` and in the `missing` vector.
    /// The method returns a tuple where are returned (1) the newly accepted/processed blocks (2) the missing references
    /// of the provided blocks. Keep in mind that the missing references are returned only the first one for a specific
    /// provided block. If we attempt to add the same block again, then its missing references won't be returned again.
    pub fn add_blocks(
        &mut self,
        mut blocks: Vec<Data<StatementBlock>>,
        block_writer: &mut impl BlockWriter,
    ) -> (
        Vec<(WalPosition, Data<StatementBlock>)>,
        HashSet<BlockReference>,
    ) {
        // process the blocks in round order ascending to ensure that we do not create unnecessary missing references
        blocks.sort_by_key(|b1| b1.round());
        let mut blocks: VecDeque<Data<StatementBlock>> = blocks.into();
        let mut newly_blocks_processed: Vec<(WalPosition, Data<StatementBlock>)> = vec![];
        // missing references that we see them for first time
        let mut missing_references = HashSet::new();
        while let Some(block) = blocks.pop_front() {
            // Update the highest known round number.

            // check whether we have already processed this block and skip it if so.
            let block_reference = block.reference();

            if self.block_store.block_exists(*block_reference)
                || self.blocks_pending.contains_key(block_reference)
            {
                continue;
            }

            let mut processed = true;
            for included_reference in block.includes() {
                // If we are missing a reference then we insert into pending and update the waiting index
                if !self.block_store.block_exists(*included_reference) {
                    processed = false;

                    // we inserted the missing reference for the first time and the block has not been
                    // fetched already.
                    if !self
                        .block_references_waiting
                        .contains_key(included_reference)
                        && !self.blocks_pending.contains_key(included_reference)
                    {
                        missing_references.insert(*included_reference);
                    }

                    self.block_references_waiting
                        .entry(*included_reference)
                        .or_default()
                        .insert(*block_reference);
                    if !self.blocks_pending.contains_key(included_reference) {
                        self.missing[included_reference.authority as usize]
                            .insert(*included_reference);
                    }
                }
            }
            self.missing[block_reference.authority as usize].remove(block_reference);

            if !processed {
                self.blocks_pending.insert(*block_reference, block);
                self.metrics.blocks_suspended.inc();
            } else {
                let block_reference = *block_reference;

                // Block can be processed. So need to update indexes etc
                let position = block_writer.insert_block(block.clone());
                newly_blocks_processed.push((position, block.clone()));

                // Now unlock any pending blocks, and process them if ready.
                if let Some(waiting_references) =
                    self.block_references_waiting.remove(&block_reference)
                {
                    // For each reference see if its unblocked.
                    for waiting_block_reference in waiting_references {
                        let block_pointer = self.blocks_pending.get(&waiting_block_reference).expect("Safe since we ensure the block waiting reference has a valid primary key.");

                        if block_pointer
                            .includes()
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

        (newly_blocks_processed, missing_references)
    }

    pub fn missing_blocks(&self) -> &[HashSet<BlockReference>] {
        &self.missing
    }

    pub fn exists_or_pending(&self, id: BlockReference) -> bool {
        self.block_store.block_exists(id) || self.blocks_pending.contains_key(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TestBlockWriter;
    use crate::types::Dag;
    use prometheus::Registry;
    use rand::prelude::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_block_manager_add_block() {
        let (metrics, _reporter) = Metrics::new(&Registry::new(), None);
        let dag =
            Dag::draw("A1:[A0, B0]; B1:[A0, B0]; B2:[A0, B1]; A2:[A1, B2]").add_genesis_blocks();
        assert_eq!(dag.len(), 6); // 4 blocks in dag + 2 genesis
        for seed in 0..100u8 {
            let mut block_writer = TestBlockWriter::new(&dag.committee());
            println!("Seed {seed}");
            let iter = dag.random_iter(&mut rng(seed));
            let mut bm = BlockManager::new(
                block_writer.block_store(),
                &dag.committee(),
                metrics.clone(),
            );
            let mut processed_blocks = HashSet::new();
            for block in iter {
                let (processed, _missing) = bm.add_blocks(vec![block.clone()], &mut block_writer);
                print!("Adding {:?}:", block.reference());
                for (_, p) in processed {
                    print!("{:?},", p.reference());
                    if !processed_blocks.insert(p.reference().clone()) {
                        panic!("Block {:?} processed twice", p.reference());
                    }
                }
                println!();
            }
            assert_eq!(bm.block_references_waiting.len(), 0);
            assert_eq!(bm.blocks_pending.len(), 0);
            assert_eq!(processed_blocks.len(), dag.len());
            assert_eq!(bm.block_store.len_expensive(), dag.len());
            println!("======");
        }
    }

    #[test]
    fn test_block_manager_add_block_missing_references() {
        let (metrics, _reporter) = Metrics::new(&Registry::new(), None);
        let dag =
            Dag::draw("A1:[A0, B0]; B1:[A0, B0]; B2:[A0, B1]; A2:[A1, B1]").add_genesis_blocks();
        assert_eq!(dag.len(), 6); // 4 blocks in dag + 2 genesis

        let mut block_writer = TestBlockWriter::new(&dag.committee());
        let mut iter = dag.iter_rev();
        let mut bm = BlockManager::new(
            block_writer.block_store(),
            &dag.committee(),
            metrics.clone(),
        );

        let a2 = iter.next().unwrap();

        // WHEN processing block A2 for first time we should get back 2 missing references
        let (_processed, missing) = bm.add_blocks(vec![a2.clone()], &mut block_writer);
        assert_eq!(missing.len(), 2);

        // WHEN processing again block A2, then now missing should be empty
        let (_processed, missing) = bm.add_blocks(vec![a2.clone()], &mut block_writer);
        assert!(missing.is_empty());

        // WHEN processing block B2, should now yield one missing blocks
        let b2 = iter.next().unwrap();
        let (_processed, missing) = bm.add_blocks(vec![b2.clone()], &mut block_writer);
        assert_eq!(missing.len(), 1);

        // Now processing all the rest of the blocks should yield as missing zero
        let (_processed, missing) = bm.add_blocks(iter.cloned().collect(), &mut block_writer);

        assert!(missing.is_empty());
        assert_eq!(bm.block_references_waiting.len(), 0);
        assert_eq!(bm.blocks_pending.len(), 0);
        assert_eq!(bm.block_store.len_expensive(), dag.len());
    }

    fn rng(s: u8) -> StdRng {
        let mut seed = [0; 32];
        seed[0] = s;
        StdRng::from_seed(seed)
    }
}
