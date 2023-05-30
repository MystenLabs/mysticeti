// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::v2::data::Data;
use crate::v2::types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock};
use std::collections::{HashMap, HashSet, VecDeque};

/// Block manager suspends incoming blocks until they are connected to the existing graph,
/// returning newly connected blocks
#[derive(Default)]
pub struct BlockManager {
    blocks_pending: HashMap<BlockReference, Data<StatementBlock>>,
    block_references_waiting: HashMap<BlockReference, HashSet<BlockReference>>,
    blocks_processed: HashMap<BlockReference, Data<StatementBlock>>,
    // Maintain an index of blocks by round as well.
    blocks_processed_by_round: HashMap<RoundNumber, Vec<BlockReference>>,
}

impl BlockManager {
    #[allow(dead_code)]
    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) -> Vec<Data<StatementBlock>> {
        let mut blocks: VecDeque<Data<StatementBlock>> = blocks.into();
        let mut newly_blocks_processed: Vec<Data<StatementBlock>> = vec![];
        while let Some(block) = blocks.pop_front() {
            let block_reference = block.reference();

            // check whether we have already processed this block and skip it if so.
            if self.blocks_processed.contains_key(block_reference)
                || self.blocks_pending.contains_key(block_reference)
            {
                continue;
            }

            let mut processed = true;
            for included_reference in block.includes() {
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
                newly_blocks_processed.push(block.clone());
                self.blocks_processed.insert(block_reference, block);

                // Update the index of blocks by round
                self.blocks_processed_by_round
                    .entry(block_reference.round)
                    .or_default()
                    .push(block_reference);

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

        newly_blocks_processed
    }

    pub fn add_own_block(&mut self, block: StatementBlock) -> Data<StatementBlock> {
        let block_ref = *block.reference();
        let block = Data::new(block);
        self.blocks_processed.insert(block_ref, block.clone());
        self.blocks_processed_by_round
            .entry(block_ref.round)
            .or_default()
            .push(block_ref);
        block
    }

    pub fn get_processed_block(&self, reference: &BlockReference) -> Option<&Data<StatementBlock>> {
        self.blocks_processed.get(reference)
    }

    pub fn processed_block_exists(&self, authority: AuthorityIndex, round: RoundNumber) -> bool {
        if let Some(r) = self.blocks_processed_by_round.get(&round) {
            r.iter().any(|r| r.authority == authority)
        } else {
            false
        }
    }

    /// Apply the consensus decision rule for a given leader round and decision round.
    #[allow(dead_code)]
    pub fn get_decision_round_certificates(
        &self,
        leader_round: RoundNumber,
        leader_name: &AuthorityIndex,
        decision_round: RoundNumber,
    ) -> HashMap<&BlockReference, HashMap<&BlockReference, HashSet<&AuthorityIndex>>> {
        assert!(leader_round < decision_round);

        // The candidates blocks on which all subsequent blocks will vote.
        // TODO: if more than one, tag the leader as byzantine.
        let candidates: Option<Vec<_>> =
            self.blocks_processed_by_round.get(&leader_round).map(|b| {
                b.iter()
                    .filter(|block_reference| block_reference.authority == *leader_name)
                    .collect()
            });

        // if the list is empty exit early.
        let Some(candidates) = candidates else {
            return HashMap::new();
        };

        // Initialize the blocks to votes map
        let mut blocks_to_votes: HashMap<
            &BlockReference,
            HashMap<&BlockReference, HashSet<&AuthorityIndex>>,
        > = HashMap::new();

        // Stores the first seen candidate block for each block, this is the candidate block for
        // which a correct block will vote if it does not already contain a vote for a candidate
        // block. Reminder: a correct authority only emits one block, it links to its previous blocks
        // and as a result will only vote for a single candidate block.
        let mut first_seen_block: HashMap<&BlockReference, &BlockReference> = HashMap::new();

        // A correct authority will emit only one block, but here we have to take into account the
        // posibility that a byzantine authority proposed two different blocks for the same round it
        // is a leader. So we initialize the blocks_to_votes map with all the candidate blocks.
        for cand in &candidates {
            // Each candidate block votes for itself
            blocks_to_votes.insert(cand, HashMap::new());
            blocks_to_votes
                .get_mut(cand)
                .unwrap()
                .insert(&cand, [&cand.authority].into_iter().collect());

            // Set the first seen to itself
            first_seen_block.insert(&cand, &cand);
        }

        // Now we walk the rounds from leader_round to decision_round, and collect votes associated with each block
        for round in leader_round..=decision_round {
            let blocks_in_later_round = self.blocks_processed_by_round.get(&round).unwrap();
            for block_reference_in_later_round in blocks_in_later_round {
                let later_block = self
                    .blocks_processed
                    .get(block_reference_in_later_round)
                    .unwrap();

                // We will gather all the votes linked from this later block in this map
                let mut votes: HashMap<&BlockReference, HashSet<&AuthorityIndex>> = HashMap::new();
                let current_authority = &block_reference_in_later_round.authority;
                let mut has_voted = false;

                for block_reference_in_earlier_round in later_block.includes() {
                    // If the reference is not in blocks_to_vote continue
                    if !blocks_to_votes.contains_key(block_reference_in_earlier_round) {
                        continue;
                    }

                    // If the first_seen for this block does not exit set it to the first seen of this block
                    if !first_seen_block.contains_key(block_reference_in_later_round) {
                        // note: this is safe since we initialize all entires in blocks_to_votes to also have a first_seen_block
                        first_seen_block.insert(
                            block_reference_in_later_round,
                            &first_seen_block[&block_reference_in_earlier_round],
                        );
                    }

                    if let Some(past_votes) = blocks_to_votes.get(block_reference_in_earlier_round)
                    {
                        for (block, new_votes) in past_votes {
                            // If the block is not in the votes map, add it
                            if !votes.contains_key(block) {
                                votes.insert(block, HashSet::new());
                            }

                            // merge the past votes into the votes map
                            votes.get_mut(block).unwrap().extend(new_votes);

                            // if the current authority is in the votes we set the has_voted flag to true
                            has_voted |= new_votes.contains(current_authority);
                        }
                    }
                }

                // If the votes map is empty continue, this block links to no candidates
                if votes.is_empty() {
                    continue;
                }

                // if has_voted flag is false, add the current authority to the votes map
                if !has_voted {
                    let my_first_seen_block = first_seen_block[block_reference_in_later_round];
                    // assert this is a candidate block
                    assert!(candidates.contains(&my_first_seen_block));

                    if !votes.contains_key(&my_first_seen_block) {
                        votes.insert(&my_first_seen_block, HashSet::new());
                    }
                    votes
                        .get_mut(&my_first_seen_block)
                        .unwrap()
                        .insert(current_authority);
                }

                // Update the blocks_to_votes map
                blocks_to_votes.insert(&block_reference_in_later_round, votes);
            }
        }

        // filter out only the decision round blocks
        let decision_round_blocks: HashMap<_, _> = blocks_to_votes
            .into_iter()
            .filter(|(block, _)| block.round == decision_round)
            .collect();
        decision_round_blocks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::types::Dag;
    use rand::prelude::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_block_manager_add_block() {
        let dag =
            Dag::draw("A1:[A0, B0]; B1:[A0, B0]; B2:[A0, B1]; A2:[A1, B2]").add_genesis_blocks();
        assert_eq!(dag.len(), 6); // 4 blocks in dag + 2 genesis
        for seed in 0..100u8 {
            println!("Seed {seed}");
            let iter = dag.random_iter(&mut rng(seed));
            let mut bm = BlockManager::default();
            let mut processed_blocks = HashSet::new();
            for block in iter {
                let processed = bm.add_blocks(vec![block.clone()]);
                print!("Adding {:?}:", block.reference());
                for p in processed {
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
            assert_eq!(bm.blocks_processed.len(), dag.len());
            println!("======");
        }
    }

    fn rng(s: u8) -> StdRng {
        let mut seed = [0; 32];
        seed[0] = s;
        StdRng::from_seed(seed)
    }
}
