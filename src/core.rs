use crate::block_handler::BlockHandler;
use crate::block_manager::BlockManager;
use crate::committee::Committee;
use crate::data::Data;
use crate::threshold_clock::ThresholdClockAggregator;
use crate::types::{AuthorityIndex, BaseStatement, BlockReference, RoundNumber, StatementBlock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;
use std::sync::Arc;

pub struct Core<H: BlockHandler> {
    block_manager: BlockManager,
    pending: VecDeque<MetaStatement>,
    block_handler: H,
    authority: AuthorityIndex,
    threshold_clock: ThresholdClockAggregator,
    committee: Arc<Committee>,
    last_proposed: RoundNumber,
    last_commit_round: RoundNumber,
}

#[derive(Debug)]
enum MetaStatement {
    Include(BlockReference),
    Payload(Vec<BaseStatement>),
}

impl<H: BlockHandler> Core<H> {
    pub fn new(block_handler: H, authority: AuthorityIndex, committee: Arc<Committee>) -> Self {
        let block_manager = BlockManager::default();
        let pending = Default::default();
        let last_proposed = 0;
        let threshold_clock = ThresholdClockAggregator::new(last_proposed);
        let last_commit_round = 0;

        Self {
            block_manager,
            pending,
            block_handler,
            authority,
            threshold_clock,
            committee,
            last_proposed,
            last_commit_round,
        }
    }

    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) -> Vec<Data<StatementBlock>> {
        let processed = self.block_manager.add_blocks(blocks);
        let statements = self.block_handler.handle_blocks(&processed);
        for processed in &processed {
            self.threshold_clock
                .add_block(*processed.reference(), &self.committee);
            self.pending
                .push_back(MetaStatement::Include(*processed.reference()));
        }
        self.pending.push_back(MetaStatement::Payload(statements));
        processed
    }

    pub fn try_new_block(&mut self) -> Option<Data<StatementBlock>> {
        let clock_round = self.threshold_clock.get_round();
        if clock_round <= self.last_proposed {
            return None;
        }

        let mut includes = vec![];
        let mut statements = vec![];

        let first_include_index = self
            .pending
            .iter()
            .position(|statement| match statement {
                MetaStatement::Include(block_ref) => block_ref.round >= clock_round,
                _ => false,
            })
            .unwrap_or(self.pending.len());

        let mut taken = self.pending.split_off(first_include_index);
        // Split off returns the "tail", what we want is keep the tail in "pending" and get the head
        mem::swap(&mut taken, &mut self.pending);
        // At least one include statement should always be present - when creating new block
        // we immediately insert an include reference to it to self.pending
        assert!(taken.len() > 0);
        let our_authority = self.authority;
        // The first statement should always be Include(our_previous_block)
        assert!(
            matches!(taken.get(0).unwrap(), MetaStatement::Include(BlockReference{authority, ..}) if authority == &our_authority)
        );
        // Compress the references in the block
        // Iterate through all the include statements in the block, and make a set of all the references in their includes.
        let mut references_in_block: HashSet<BlockReference> = HashSet::new();
        for statement in &taken {
            if let MetaStatement::Include(block_ref) = statement {
                // for all the includes in the block, add the references in the block to the set
                if let Some(block) = self.block_manager.get_processed_block(&block_ref) {
                    references_in_block.extend(block.includes());
                }
            }
        }
        for (i, statement) in taken.into_iter().enumerate() {
            match statement {
                MetaStatement::Include(include) => {
                    // First block in includes should always be our block
                    // We should not exclude it even if some other block already references it
                    if i == 0 || !references_in_block.contains(&include) {
                        includes.push(include);
                    }
                }
                MetaStatement::Payload(payload) => {
                    statements.extend(payload);
                }
            }
        }

        let block_ref = BlockReference {
            authority: self.authority,
            round: clock_round,
            digest: 0,
        };
        assert!(includes.len() > 0);
        let block = StatementBlock::new(block_ref, includes, statements);
        assert_eq!(
            block.includes().get(0).unwrap().authority,
            self.authority,
            "Invalid block {}",
            block
        );

        let block = self.block_manager.add_own_block(block);
        self.pending.push_front(MetaStatement::Include(block_ref));

        self.last_proposed = clock_round;

        Some(block)
    }

    #[allow(dead_code)]
    pub fn try_commit(&mut self, period: u64) -> Vec<Data<StatementBlock>> {
        // todo - this needs to go to previous leaders
        let quorum_round = self.threshold_clock.get_round();
        if quorum_round > self.last_commit_round.max(period - 1) && quorum_round % period == 0 {
            // Get the authority that is the leader at this round
            let target_authority = self.leader_at_round(quorum_round, period);

            println!("Committing round {}", quorum_round);
            let support = self.block_manager.get_decision_round_certificates(
                quorum_round - period,
                &target_authority,
                quorum_round - 1,
            );

            // iterate over all references in support
            let mut certificate_votes: HashMap<&BlockReference, HashSet<&AuthorityIndex>> =
                HashMap::new();
            for (decision_reference, leader_votes) in support {
                for (potential_cert, votes) in leader_votes {
                    if self
                        .committee
                        .is_quorum(self.committee.get_total_stake(&votes))
                    {
                        let votes = certificate_votes
                            .entry(potential_cert)
                            .or_insert(HashSet::new());
                        votes.insert(&decision_reference.authority);
                    }
                }
            }

            // Assert there is only one entry in certificate_votes, since only one certificate can ever
            // exist with a quorum of votes.
            assert!(certificate_votes.len() <= 1);

            // If there is a certificate, then we can commit the block
            for (block_ref, votes) in certificate_votes {
                if self
                    .committee
                    .is_quorum(self.committee.get_total_stake(&votes))
                {
                    // Set the last commit round
                    self.last_commit_round = quorum_round - period;

                    let block = self
                        .block_manager
                        .get_processed_block(block_ref)
                        .expect("We should have committed block");
                    return vec![block.clone()];
                }
            }
        }

        return vec![];
    }

    /// This only checks readiness in terms of helping liveness for commit rule,
    /// try_new_block might still return None if threshold clock is not ready
    ///
    /// The algorithm to calling is roughly: if timeout || commit_ready_new_block then try_new_block(..)
    pub fn ready_new_block(&self, period: u64) -> bool {
        let quorum_round = self.threshold_clock.get_round();
        if quorum_round % period != 0 {
            // Non leader round we are ready to emit block asap
            true
        } else {
            // Leader round we check if we have a leader block
            if quorum_round > self.last_commit_round.max(period - 1) {
                let leader_round = quorum_round - period;
                let leader = self.leader_at_round(leader_round, period);
                self.block_manager
                    .processed_block_exists(leader, leader_round)
            } else {
                false
            }
        }
    }

    pub fn last_proposed(&self) -> RoundNumber {
        self.last_proposed
    }

    pub fn authority(&self) -> AuthorityIndex {
        self.authority
    }

    pub fn block_handler(&self) -> &H {
        &self.block_handler
    }

    pub fn committee(&self) -> &Arc<Committee> {
        &self.committee
    }

    fn leader_at_round(&self, round: RoundNumber, period: u64) -> AuthorityIndex {
        assert!(round == 0 || round % period == 0);

        self.committee.elect_leader(round / period)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_util::committee_and_cores;
    use crate::threshold_clock;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use std::fmt::Write;

    #[test]
    fn test_core_simple_exchange() {
        let (committee, mut cores) = committee_and_cores(4);

        let mut proposed_transactions = vec![];
        let mut blocks = vec![];
        for core in &mut cores {
            core.add_blocks(committee.genesis_blocks(core.authority));
            let block = core
                .try_new_block()
                .expect("Must be able to create block after genesis");
            assert_eq!(block.reference().round, 1);
            proposed_transactions.push(core.block_handler.last_transaction);
            eprintln!("{}: {}", core.authority, block);
            blocks.push(block.clone());
        }
        assert_eq!(proposed_transactions.len(), 4);
        let more_blocks = blocks.split_off(2);

        eprintln!("===");

        let mut blocks_r2 = vec![];
        for core in &mut cores {
            core.add_blocks(blocks.clone());
            assert!(core.try_new_block().is_none());
            core.add_blocks(more_blocks.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority, block);
            assert_eq!(block.reference().round, 2);
            blocks_r2.push(block.clone());
        }

        for core in &mut cores {
            core.add_blocks(blocks_r2.clone());
            let block = core
                .try_new_block()
                .expect("Must be able to create block after full round");
            eprintln!("{}: {}", core.authority, block);
            assert_eq!(block.reference().round, 3);
            for txid in &proposed_transactions {
                assert!(
                    core.block_handler.is_certified(*txid),
                    "Transaction {} is not certified by {}",
                    txid,
                    core.authority
                );
            }
        }
    }

    #[test]
    fn test_randomized_simple_exchange() {
        'l: for seed in 0..100 {
            let mut rng = StdRng::from_seed([seed; 32]);
            let (committee, mut cores) = committee_and_cores(4);

            let mut proposed_transactions = vec![];
            let mut pending: Vec<_> = committee.authorities().map(|_| vec![]).collect();
            for core in &mut cores {
                let block = core
                    .try_new_block()
                    .expect("Must be able to create block after genesis");
                assert_eq!(block.reference().round, 1);
                proposed_transactions.push(core.block_handler.last_transaction);
                eprintln!("{}: {}", core.authority, block);
                assert!(
                    threshold_clock::threshold_clock_valid(&block, &committee),
                    "Invalid clock {}",
                    block
                );
                push_all(&mut pending, core.authority, &block);
            }
            // Each iteration we pick one authority and deliver to it 1..3 random blocks
            // First 20 iterations we record all transactions created by authorities
            // After this we wait for those recorded transactions to be eventually certified
            'a: for i in 0..1000 {
                let authority = committee.random_authority(&mut rng);
                eprintln!("Iteration {i}, authority {authority}");
                let core = &mut cores[authority as usize];
                let this_pending = &mut pending[authority as usize];
                let c = rng.gen_range(1..4usize);
                let mut blocks = vec![];
                let mut deliver = String::new();
                for _ in 0..c {
                    if this_pending.is_empty() {
                        break;
                    }
                    let block = this_pending.remove(rng.gen_range(0..this_pending.len()));
                    write!(deliver, "{}, ", block).ok();
                    blocks.push(block);
                }
                if blocks.is_empty() {
                    eprintln!("No pending blocks for {authority}");
                    continue;
                }
                eprint!("Deliver {deliver} to {authority} => ");
                core.add_blocks(blocks);
                let Some(block) = core.try_new_block() else {
                    eprintln!("No new block");
                    continue;
                };
                assert!(
                    threshold_clock::threshold_clock_valid(&block, &committee),
                    "Invalid clock {}",
                    block
                );
                eprintln!("Created {block}");
                push_all(&mut pending, core.authority, &block);
                if i < 20 {
                    // First 20 iterations we record proposed transactions
                    proposed_transactions.push(core.block_handler.last_transaction);
                } else {
                    assert!(!proposed_transactions.is_empty());
                    // After 20 iterations we just wait for all transactions to be committed everywhere
                    for proposed in &proposed_transactions {
                        for core in &cores {
                            if !core.block_handler.is_certified(*proposed) {
                                continue 'a;
                            }
                        }
                    }
                    println!(
                        "Seed {seed} succeed, {} transactions certified in {i} exchanges",
                        proposed_transactions.len()
                    );
                    continue 'l;
                }
            }
            panic!("Seed {seed} failed - not all transactions are committed");
        }
    }

    fn push_all(
        p: &mut Vec<Vec<Data<StatementBlock>>>,
        except: AuthorityIndex,
        block: &Data<StatementBlock>,
    ) {
        for (i, q) in p.iter_mut().enumerate() {
            if i as AuthorityIndex != except {
                q.push(block.clone());
            }
        }
    }
}
