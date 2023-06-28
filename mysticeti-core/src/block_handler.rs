// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::commit_interpreter::{CommitInterpreter, CommittedSubDag};
use crate::committee::{Committee, QuorumThreshold, TransactionAggregator};
use crate::config::StorageDir;
use crate::data::Data;
use crate::log::CertifiedTransactionLog;
use crate::runtime::TimeInstant;
use crate::stat::PreciseHistogram;
use crate::syncer::CommitObserver;
use crate::types::{AuthorityIndex, BaseStatement, BlockReference, StatementBlock, TransactionId};
use crate::{
    block_store::{BlockStore, CommitData},
    types::Transaction,
};
use minibytes::Bytes;
use parking_lot::Mutex;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::{cmp::max, collections::VecDeque};

pub trait BlockHandler: Send + Sync {
    /// Handle transactions that were received from the clients. We do not promise clients that
    /// their transactions will be included in any block, so it is acceptable to drop transactions
    /// (e.g., upon crash-recovery or to apply back pressure).
    fn handle_transactions(&mut self, transactions: Vec<Data<Transaction>>);

    fn handle_blocks(&mut self, blocks: &[Data<StatementBlock>]) -> Vec<BaseStatement>;

    fn state(&self) -> Bytes;

    fn recover_state(&mut self, _state: &Bytes);
}

pub struct RealBlockHandler {
    transaction_votes:
        TransactionAggregator<TransactionId, QuorumThreshold, CertifiedTransactionLog>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,
    pub transaction_certified_latency: PreciseHistogram<Duration>,
    rng: StdRng,
    /// Transactions that are pending to be included in a block.
    pending_transactions: VecDeque<Data<Transaction>>,
}

impl RealBlockHandler {
    pub const MAX_PENDING_TRANSACTIONS: usize = 300_000;

    pub fn new(committee: Arc<Committee>, authority: AuthorityIndex, config: &StorageDir) -> Self {
        let rng = StdRng::seed_from_u64(authority);
        let transaction_log = CertifiedTransactionLog::start(config.certified_transactions_log())
            .expect("Failed to open certified transaction log for write");
        Self {
            transaction_votes: TransactionAggregator::with_handler(transaction_log),
            transaction_time: Default::default(),
            committee,
            authority,
            transaction_certified_latency: Default::default(),
            rng,
            pending_transactions: VecDeque::with_capacity(Self::MAX_PENDING_TRANSACTIONS * 2),
        }
    }
}

impl BlockHandler for RealBlockHandler {
    fn handle_transactions(&mut self, transactions: Vec<Data<Transaction>>) {
        for transaction in transactions {
            if self.pending_transactions.len() >= Self::MAX_PENDING_TRANSACTIONS {
                tracing::warn!("Too many pending transactions, dropping transaction");
            } else {
                self.pending_transactions.push_back(transaction);
            }
        }
    }

    fn handle_blocks(&mut self, blocks: &[Data<StatementBlock>]) -> Vec<BaseStatement> {
        let mut response = vec![];
        let next_transaction = self.rng.next_u64();
        response.push(BaseStatement::Share(next_transaction, next_transaction));
        let mut transaction_time = self.transaction_time.lock();
        transaction_time.insert(next_transaction, TimeInstant::now());
        self.transaction_votes
            .register(next_transaction, self.authority, &self.committee);
        for block in blocks {
            let processed =
                self.transaction_votes
                    .process_block(block, Some(&mut response), &self.committee);
            for processed_id in processed {
                if let Some(instant) = transaction_time.get(&processed_id) {
                    self.transaction_certified_latency
                        .observe(instant.elapsed());
                }
            }
        }
        response
    }

    fn state(&self) -> Bytes {
        self.transaction_votes.state()
    }

    fn recover_state(&mut self, state: &Bytes) {
        self.transaction_votes.with_state(state);
    }
}

// Immediately votes and generates new transactions
pub struct TestBlockHandler {
    pub last_transaction: u64,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,

    pub transaction_certified_latency: PreciseHistogram<Duration>,
}

#[allow(dead_code)]
pub struct TestCommitHandler {
    commit_interpreter: CommitInterpreter,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    committee: Arc<Committee>,
    committed_leaders: Vec<BlockReference>,
    committed_dags: Vec<CommittedSubDag>,

    transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    pub certificate_committed_latency: PreciseHistogram<Duration>,
    pub transaction_committed_latency: PreciseHistogram<Duration>,
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
            transaction_time: Default::default(),
            committee,
            authority,
            transaction_certified_latency: Default::default(),
        }
    }

    pub fn is_certified(&self, txid: TransactionId) -> bool {
        self.transaction_votes.is_processed(&txid)
    }
}

impl BlockHandler for TestBlockHandler {
    fn handle_transactions(&mut self, _transactions: Vec<Data<Transaction>>) {
        unimplemented!("Transactions are self-generated when creating blocks")
    }

    fn handle_blocks(&mut self, blocks: &[Data<StatementBlock>]) -> Vec<BaseStatement> {
        // todo - this is ugly, but right now we need a way to recover self.last_transaction
        for block in blocks {
            if block.author() == self.authority {
                // We can see our own block in handle_blocks - this can happen during core recovery
                // Todo - we might also need to process pending Payload statements as well
                for statement in block.statements() {
                    if let BaseStatement::Share(id, _) = statement {
                        self.last_transaction = max(self.last_transaction, *id);
                    }
                }
            }
        }
        let mut response = vec![];
        self.last_transaction += 1;
        response.push(BaseStatement::Share(
            self.last_transaction,
            self.last_transaction,
        ));
        let mut transaction_time = self.transaction_time.lock();
        transaction_time.insert(self.last_transaction, TimeInstant::now());
        self.transaction_votes
            .register(self.last_transaction, self.authority, &self.committee);
        for block in blocks {
            println!("Processing {block:?}");
            let processed =
                self.transaction_votes
                    .process_block(block, Some(&mut response), &self.committee);
            for processed_id in processed {
                if let Some(instant) = transaction_time.get(&processed_id) {
                    self.transaction_certified_latency
                        .observe(instant.elapsed());
                }
            }
        }
        response
    }

    fn state(&self) -> Bytes {
        let state = (&self.transaction_votes.state(), &self.last_transaction);
        let bytes =
            bincode::serialize(&state).expect("Failed to serialize transaction aggregator state");
        bytes.into()
    }

    fn recover_state(&mut self, state: &Bytes) {
        let (transaction_votes, last_transaction) = bincode::deserialize(state)
            .expect("Failed to deserialize transaction aggregator state");
        self.transaction_votes.with_state(&transaction_votes);
        self.last_transaction = last_transaction;
    }
}

impl TestCommitHandler {
    pub fn new(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    ) -> Self {
        Self {
            commit_interpreter: CommitInterpreter::new(),
            transaction_votes: Default::default(),
            committee,
            committed_leaders: vec![],
            committed_dags: vec![],

            transaction_time,
            certificate_committed_latency: Default::default(),
            transaction_committed_latency: Default::default(),
        }
    }

    pub fn committed_leaders(&self) -> &Vec<BlockReference> {
        &self.committed_leaders
    }
}

impl CommitObserver for TestCommitHandler {
    fn handle_commit(
        &mut self,
        block_store: &BlockStore,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommitData> {
        let committed = self
            .commit_interpreter
            .handle_commit(block_store, committed_leaders);
        let transaction_time = self.transaction_time.lock();
        let mut commit_data = vec![];
        for commit in committed {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                let processed = self
                    .transaction_votes
                    .process_block(block, None, &self.committee);
                for processed_id in processed {
                    if let Some(instant) = transaction_time.get(&processed_id) {
                        self.certificate_committed_latency
                            .observe(instant.elapsed());
                    }
                }
                for statement in block.statements() {
                    if let BaseStatement::Share(id, _) = statement {
                        if let Some(instant) = transaction_time.get(id) {
                            self.transaction_committed_latency
                                .observe(instant.elapsed());
                        }
                    }
                }
            }
            commit_data.push(CommitData::from(&commit));
            self.committed_dags.push(commit);
        }
        commit_data
    }

    fn aggregator_state(&self) -> Bytes {
        self.transaction_votes.state()
    }

    fn recover_committed(&mut self, committed: HashSet<BlockReference>, state: Option<Bytes>) {
        assert!(self.commit_interpreter.committed.is_empty());
        if let Some(state) = state {
            self.transaction_votes.with_state(&state);
        } else {
            assert!(committed.is_empty());
        }
        self.commit_interpreter.committed = committed;
    }
}
