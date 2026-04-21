// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod block_handler;
mod block_manager;
pub mod core_thread;
pub mod syncer;
pub mod threshold_clock;

use std::{
    collections::{HashSet, VecDeque},
    mem,
    sync::Arc,
};

use self::{
    block_handler::RealBlockHandler, block_manager::BlockManager,
    threshold_clock::ThresholdClockAggregator,
};
use crate::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber, transaction::Transaction},
    block_store::{CommitData, OwnBlockData},
    committee::Committee,
    committee::Stake,
    consensus::{CommittedSubDag, DagConsensus},
    context::Ctx,
    crypto::{CryptoEngine, CryptoVerifier},
    data::Data,
    metrics::Metrics,
    state::RecoveredState,
    storage::BlockReader,
    storage::Storage,
    wal::{WalPosition, WalSyncer},
};

pub struct Core<C: Ctx, D: DagConsensus> {
    block_manager: BlockManager,
    pending: VecDeque<(WalPosition, MetaStatement)>,
    last_own_block: OwnBlockData,
    block_handler: RealBlockHandler<C>,
    authority: Authority,
    threshold_clock: ThresholdClockAggregator,
    pub(crate) committee: Arc<Committee>,
    last_commit_leader: BlockReference,
    storage: Storage,
    pub metrics: Arc<Metrics>,
    fsync: bool,
    crypto: CryptoEngine,
    // todo - ugly, probably need to merge syncer and core
    recovered_committed_blocks: Option<HashSet<BlockReference>>,
    committer: D,
}

#[derive(Debug)]
pub enum MetaStatement {
    Include(BlockReference),
    Payload(Vec<Transaction>),
}

impl<C: Ctx, D: DagConsensus> Core<C, D> {
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        block_handler: RealBlockHandler<C>,
        authority: Authority,
        committee: Arc<Committee>,
        metrics: Arc<Metrics>,
        mut storage: Storage,
        recovered: RecoveredState,
        fsync: bool,
        committer: D,
        crypto: CryptoEngine,
    ) -> Self {
        let RecoveredState {
            last_own_block,
            mut pending,
            unprocessed_blocks,
            last_committed_leader,
            committed_blocks,
        } = recovered;
        let quorum_threshold = committer.quorum_threshold();
        let mut threshold_clock = ThresholdClockAggregator::new(0, quorum_threshold);
        let last_own_block = if let Some(own_block) = last_own_block {
            for (_, pending_block) in pending.iter() {
                if let MetaStatement::Include(include) = pending_block {
                    threshold_clock.add_block(*include, &committee);
                }
            }
            own_block
        } else {
            // todo(fix) - this technically has a race condition if node crashes after genesis
            assert!(pending.is_empty());
            // Initialize empty block store
            // A lot of this code is shared with Self::add_blocks,
            // this is not great and some code reuse would be great
            let own_genesis_block = Block::genesis(authority);
            let other_genesis_blocks: Vec<_> = committee
                .authorities()
                .filter(|&a| a != authority)
                .map(Block::genesis)
                .collect();
            assert_eq!(own_genesis_block.author(), authority);
            for block in other_genesis_blocks {
                let reference = *block.reference();
                threshold_clock.add_block(reference, &committee);
                let position = storage.insert_block(block);
                pending.push_back((position, MetaStatement::Include(reference)));
            }
            threshold_clock.add_block(*own_genesis_block.reference(), &committee);
            let own_block_data = OwnBlockData {
                next_entry: WalPosition::MAX,
                block: own_genesis_block,
            };
            storage.insert_own_block(&own_block_data);
            own_block_data
        };
        let block_manager = BlockManager::new(storage.block_reader().clone(), &committee);

        let mut this = Self {
            block_manager,
            pending,
            last_own_block,
            block_handler,
            authority,
            threshold_clock,
            committee,
            last_commit_leader: last_committed_leader.unwrap_or_default(),
            storage,
            metrics,
            fsync,
            crypto,
            recovered_committed_blocks: Some(committed_blocks),
            committer,
        };

        if !unprocessed_blocks.is_empty() {
            tracing::info!(
                "Replaying {} blocks for transaction aggregator",
                unprocessed_blocks.len()
            );
            this.run_block_handler();
        }

        this
    }

    pub fn quorum_threshold(&self) -> Stake {
        self.committer.quorum_threshold()
    }

    pub fn verifier(&self) -> CryptoVerifier {
        self.crypto.verifier()
    }

    // Note that generally when you update this function you
    // also want to change genesis initialization above
    pub fn add_blocks(&mut self, blocks: Vec<Data<Block>>) -> Vec<Data<Block>> {
        let _timer = self.metrics.utilization_timer("Core::add_blocks");
        let processed = self.block_manager.add_blocks(blocks, &mut self.storage);
        let mut result = Vec::with_capacity(processed.len());
        for (position, processed) in processed.into_iter() {
            self.threshold_clock
                .add_block(*processed.reference(), &self.committee);
            self.pending
                .push_back((position, MetaStatement::Include(*processed.reference())));
            result.push(processed);
        }
        self.run_block_handler();
        result
    }

    fn run_block_handler(&mut self) {
        let _timer = self.metrics.utilization_timer("Core::run_block_handler");
        // Always accept transactions for now; pass `false` during shutdown
        // to stop including new transactions in proposed blocks.
        let statements = self.block_handler.handle_blocks(true);
        let serialized_statements =
            bincode::serialize(&statements).expect("Payload serialization failed");
        let position = self.storage.write_payload(&serialized_statements);
        self.pending
            .push_back((position, MetaStatement::Payload(statements)));
    }

    pub fn try_new_block(&mut self) -> Option<Data<Block>> {
        let _timer = self.metrics.utilization_timer("Core::try_new_block");
        let clock_round = self.threshold_clock.get_round();
        if clock_round <= self.last_proposed() {
            return None;
        }

        let mut includes = vec![];
        let mut statements = vec![];

        let first_include_index = self
            .pending
            .iter()
            .position(|(_, statement)| match statement {
                MetaStatement::Include(block_ref) => block_ref.round >= clock_round,
                _ => false,
            })
            .unwrap_or(self.pending.len());

        let mut taken = self.pending.split_off(first_include_index);
        // Split off returns the "tail", what we want is keep the tail in "pending" and get the head
        mem::swap(&mut taken, &mut self.pending);
        // Compress the references in the block
        // Iterate through all the include statements in the block,
        // and make a set of all the references in their includes.
        let mut references_in_block: HashSet<BlockReference> = HashSet::new();
        references_in_block.extend(self.last_own_block.block.includes());
        for (_, statement) in &taken {
            if let MetaStatement::Include(block_ref) = statement {
                // for all the includes in the block, add the references in the block to the set
                if let Some(block) = self.storage.block_reader().get_block(*block_ref) {
                    references_in_block.extend(block.includes());
                }
            }
        }
        includes.push(*self.last_own_block.block.reference());
        for (_, statement) in taken.into_iter() {
            match statement {
                MetaStatement::Include(include) => {
                    if !references_in_block.contains(&include) {
                        includes.push(include);
                    }
                }
                MetaStatement::Payload(payload) => {
                    statements.extend(payload);
                }
            }
        }

        assert!(!includes.is_empty());
        let time_ns = C::timestamp_utc().as_nanos() as u64;
        let block = Block::new(
            self.authority,
            clock_round,
            includes,
            statements,
            time_ns,
            &self.crypto,
        );
        assert_eq!(
            block.includes().first().unwrap().authority,
            self.authority,
            "Invalid block {}",
            block
        );

        let block = Data::new(block);
        if block.serialized_bytes().len() > crate::storage::wal::MAX_ENTRY_SIZE / 2 {
            panic!(
                "Created an oversized block \
                (check all limits set properly: {} > {}): {:#?}",
                block.serialized_bytes().len(),
                crate::storage::wal::MAX_ENTRY_SIZE / 2,
                block
            );
        }
        self.threshold_clock
            .add_block(*block.reference(), &self.committee);
        self.block_handler.handle_proposal(&block);
        self.proposed_block_stats(&block);
        let next_entry = if let Some((pos, _)) = self.pending.front() {
            *pos
        } else {
            WalPosition::MAX
        };
        self.last_own_block = OwnBlockData {
            next_entry,
            block: block.clone(),
        };
        self.storage.insert_own_block(&self.last_own_block);

        if self.fsync {
            self.storage.sync();
        }

        tracing::debug!("Created block {block:?}");
        Some(block)
    }

    pub fn wal_syncer(&self) -> WalSyncer {
        self.storage.syncer()
    }

    fn proposed_block_stats(&self, block: &Data<Block>) {
        self.metrics
            .observe_proposed_block_size_bytes(block.serialized_bytes().len());
        let transactions = block.transactions().len();
        self.metrics
            .observe_proposed_block_transaction_count(transactions);
        self.metrics.observe_proposed_block_vote_count(0);
    }

    pub fn try_commit(&mut self) -> Vec<Data<Block>> {
        let metrics = &self.metrics;
        let sequence: Vec<_> = self
            .committer
            .try_commit(self.last_commit_leader)
            .inspect(|leader| metrics.inc_decided_leaders(leader))
            .filter_map(|leader| leader.into_decided_block())
            .collect();

        if let Some(last) = sequence.last() {
            self.last_commit_leader = *last.reference();
        }

        sequence
    }

    pub fn cleanup(&self) {
        const RETAIN_BELOW_COMMIT_ROUNDS: RoundNumber = 100;

        self.storage.block_reader().cleanup(
            self.last_commit_leader
                .round()
                .saturating_sub(RETAIN_BELOW_COMMIT_ROUNDS),
        );

        self.block_handler.cleanup();
    }

    /// This only checks readiness in terms of helping liveness for commit rule,
    /// try_new_block might still return None if threshold clock is not ready
    ///
    /// The algorithm to calling is roughly:
    /// if timeout || commit_ready_new_block then try_new_block(..)
    pub fn ready_new_block(&self, connected_authorities: &HashSet<Authority>) -> bool {
        let quorum_round = self.threshold_clock.get_round();

        // Leader round check. The floor of 1 keeps `leader_round = quorum_round - 1`
        // positive at startup; the committer itself filters genesis (round 0) out of
        // its output sequence. In steady state `last_commit_leader.round()` dominates
        // the max.
        if quorum_round > self.last_commit_leader.round().max(1) {
            let leader_round = quorum_round - 1;
            let filter = |a: &Authority| connected_authorities.contains(a);
            match self.committer.get_leaders(leader_round) {
                Some(leaders) => self
                    .storage
                    .block_reader()
                    .all_blocks_exists_at_authority_round(leaders.filter(filter), leader_round),
                None => self
                    .storage
                    .block_reader()
                    .all_blocks_exists_at_authority_round(
                        self.committee.authorities().filter(filter),
                        leader_round,
                    ),
            }
        } else {
            false
        }
    }

    pub fn handle_committed_subdag(&mut self, committed: Vec<CommittedSubDag>) -> Vec<CommitData> {
        let commit_data: Vec<_> = committed.iter().map(CommitData::from).collect();
        self.storage.write_commits(&commit_data);
        commit_data
    }

    pub fn take_recovered_committed_blocks(&mut self) -> HashSet<BlockReference> {
        self.recovered_committed_blocks
            .take()
            .expect("take_recovered_committed_blocks called twice")
    }

    pub fn block_reader(&self) -> &BlockReader {
        self.storage.block_reader()
    }

    pub fn last_own_block(&self) -> &Data<Block> {
        &self.last_own_block.block
    }

    pub fn last_proposed(&self) -> RoundNumber {
        self.last_own_block.block.round()
    }

    pub fn authority(&self) -> Authority {
        self.authority
    }

    pub fn block_handler(&self) -> &RealBlockHandler<C> {
        &self.block_handler
    }

    pub fn block_manager(&self) -> &BlockManager {
        &self.block_manager
    }

    pub fn block_handler_mut(&mut self) -> &mut RealBlockHandler<C> {
        &mut self.block_handler
    }

    pub fn committee(&self) -> &Arc<Committee> {
        &self.committee
    }
}
