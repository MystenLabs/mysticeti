// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::committee::{
    Committee, ProcessedTransactionHandler, QuorumThreshold, TransactionAggregator,
};
use crate::config::StorageDir;
use crate::consensus::linearizer::{CommittedSubDag, Linearizer};
use crate::data::Data;
use crate::log::TransactionLog;
use crate::metrics::UtilizationTimerExt;
use crate::metrics::UtilizationTimerVecExt;
use crate::runtime;
use crate::runtime::TimeInstant;
use crate::syncer::CommitObserver;
use crate::types::{
    AuthorityIndex, BaseStatement, BlockReference, StatementBlock, Transaction, TransactionLocator,
};
use crate::{block_store::BlockStore, metrics::Metrics};
use minibytes::Bytes;
use parking_lot::Mutex;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub trait BlockHandler: Send + Sync {
    fn handle_blocks(
        &mut self,
        blocks: &[Data<StatementBlock>],
        require_response: bool,
    ) -> Vec<BaseStatement>;

    fn handle_proposal(&mut self, block: &Data<StatementBlock>);

    fn state(&self) -> Bytes;

    fn recover_state(&mut self, _state: &Bytes);

    fn cleanup(&self) {}
}

const REAL_BLOCK_HANDLER_TXN_SIZE: usize = 512;
const REAL_BLOCK_HANDLER_TXN_GEN_STEP: usize = 32;
const _: () = assert_constants();

#[allow(dead_code)]
const fn assert_constants() {
    if REAL_BLOCK_HANDLER_TXN_SIZE % REAL_BLOCK_HANDLER_TXN_GEN_STEP != 0 {
        panic!("REAL_BLOCK_HANDLER_TXN_SIZE % REAL_BLOCK_HANDLER_TXN_GEN_STEP != 0")
    }
}

pub struct RealBlockHandler {
    transaction_votes: TransactionAggregator<TransactionLocator, QuorumThreshold, TransactionLog>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,
    metrics: Arc<Metrics>,
    receiver: mpsc::Receiver<Vec<Transaction>>,
    pending_transactions: usize,
}

const SOFT_MAX_PROPOSED_PER_BLOCK: usize = 20 * 1000;

impl RealBlockHandler {
    pub fn new(
        committee: Arc<Committee>,
        authority: AuthorityIndex,
        config: &StorageDir,
        metrics: Arc<Metrics>,
    ) -> (Self, mpsc::Sender<Vec<Transaction>>) {
        let (sender, receiver) = mpsc::channel(1024);
        let transaction_log = TransactionLog::start(config.certified_transactions_log())
            .expect("Failed to open certified transaction log for write");
        let this = Self {
            transaction_votes: TransactionAggregator::with_handler(transaction_log),
            transaction_time: Default::default(),
            committee,
            authority,
            metrics,
            receiver,
            pending_transactions: 0, // todo - need to initialize correctly when loaded from disk
        };
        (this, sender)
    }
}

impl RealBlockHandler {
    fn receive_with_limit(&mut self) -> Option<Vec<Transaction>> {
        if self.pending_transactions >= SOFT_MAX_PROPOSED_PER_BLOCK {
            return None;
        }
        let received = self.receiver.try_recv().ok()?;
        self.pending_transactions += received.len();
        Some(received)
    }
}

impl BlockHandler for RealBlockHandler {
    fn handle_blocks(
        &mut self,
        blocks: &[Data<StatementBlock>],
        require_response: bool,
    ) -> Vec<BaseStatement> {
        let _timer = self
            .metrics
            .utilization_timer
            .utilization_timer("BlockHandler::handle_blocks");
        let mut response = vec![];
        if require_response {
            while let Some(data) = self.receive_with_limit() {
                for tx in data {
                    response.push(BaseStatement::Share(tx));
                }
            }
        }
        let transaction_time = self.transaction_time.lock();
        for block in blocks {
            let response_option: Option<&mut Vec<BaseStatement>> = if require_response {
                Some(&mut response)
            } else {
                None
            };
            let processed =
                self.transaction_votes
                    .process_block(block, response_option, &self.committee);
            for processed_locator in processed {
                if let Some(instant) = transaction_time.get(&processed_locator) {
                    self.metrics
                        .transaction_certified_latency
                        .observe(instant.elapsed());
                }
            }
        }
        self.metrics
            .block_handler_pending_certificates
            .set(self.transaction_votes.len() as i64);
        response
    }

    fn handle_proposal(&mut self, block: &Data<StatementBlock>) {
        // todo - this is not super efficient
        self.pending_transactions -= block.shared_transactions().count();
        let mut transaction_time = self.transaction_time.lock();
        for (locator, _) in block.shared_transactions() {
            transaction_time.insert(locator, TimeInstant::now());
            self.transaction_votes
                .register(locator, self.authority, &self.committee);
        }
    }

    fn state(&self) -> Bytes {
        self.transaction_votes.state()
    }

    fn recover_state(&mut self, state: &Bytes) {
        self.transaction_votes.with_state(state);
    }

    fn cleanup(&self) {
        let _timer = self.metrics.block_handler_cleanup_util.utilization_timer();
        // todo - all of this should go away and we should measure tx latency differently
        let mut l = self.transaction_time.lock();
        l.retain(|_k, v| v.elapsed() < Duration::from_secs(10));
    }
}

// Immediately votes and generates new transactions
pub struct TestBlockHandler {
    last_transaction: u64,
    transaction_votes: TransactionAggregator<TransactionLocator, QuorumThreshold>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,
    pub proposed: Vec<TransactionLocator>,

    metrics: Arc<Metrics>,
}

impl TestBlockHandler {
    pub fn new(
        last_transaction: u64,
        committee: Arc<Committee>,
        authority: AuthorityIndex,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            last_transaction,
            transaction_votes: Default::default(),
            transaction_time: Default::default(),
            committee,
            authority,
            proposed: Default::default(),
            metrics,
        }
    }

    pub fn is_certified(&self, locator: &TransactionLocator) -> bool {
        self.transaction_votes.is_processed(locator)
    }

    pub fn make_transaction(i: u64) -> Transaction {
        Transaction::new(i.to_le_bytes().to_vec())
    }
}

impl BlockHandler for TestBlockHandler {
    fn handle_blocks(
        &mut self,
        blocks: &[Data<StatementBlock>],
        require_response: bool,
    ) -> Vec<BaseStatement> {
        // todo - this is ugly, but right now we need a way to recover self.last_transaction
        let mut response = vec![];
        if require_response {
            for block in blocks {
                if block.author() == self.authority {
                    // We can see our own block in handle_blocks - this can happen during core recovery
                    // Todo - we might also need to process pending Payload statements as well
                    for statement in block.statements() {
                        if let BaseStatement::Share(_) = statement {
                            self.last_transaction += 1;
                        }
                    }
                }
            }
            self.last_transaction += 1;
            let next_transaction = Self::make_transaction(self.last_transaction);
            response.push(BaseStatement::Share(next_transaction));
        }
        let transaction_time = self.transaction_time.lock();
        for block in blocks {
            println!("Processing {block:?}");
            let response_option: Option<&mut Vec<BaseStatement>> = if require_response {
                Some(&mut response)
            } else {
                None
            };
            let processed =
                self.transaction_votes
                    .process_block(block, response_option, &self.committee);
            for processed_locator in processed {
                if let Some(instant) = transaction_time.get(&processed_locator) {
                    self.metrics
                        .transaction_certified_latency
                        .observe(instant.elapsed());
                }
            }
        }
        response
    }

    fn handle_proposal(&mut self, block: &Data<StatementBlock>) {
        let mut transaction_time = self.transaction_time.lock();
        for (locator, _) in block.shared_transactions() {
            transaction_time.insert(locator, TimeInstant::now());
            self.proposed.push(locator);
            self.transaction_votes
                .register(locator, self.authority, &self.committee);
        }
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

pub struct BatchGenerator {
    transactions_receiver: mpsc::Receiver<Vec<Transaction>>,
    batch_sender: mpsc::Sender<Vec<Transaction>>,
    max_block_size: usize,
    min_block_delay: Duration,
}

impl BatchGenerator {
    pub async fn start(
        transactions_receiver: mpsc::Receiver<Vec<Transaction>>,
        batch_sender: mpsc::Sender<Vec<Transaction>>,
        max_block_size: usize,
        min_block_delay: Duration,
    ) {
        let this = Self {
            transactions_receiver,
            batch_sender,
            max_block_size,
            min_block_delay,
        };
        runtime::Handle::current().spawn(this.run());
    }

    async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.max_block_size);
        loop {
            let mut ready = false;
            tokio::select! {
                _ = runtime::sleep(self.min_block_delay) => {
                    ready = true;
                }
                Some(transactions) = self.transactions_receiver.recv() => {
                    batch.extend(transactions);
                    if batch.len() >= self.max_block_size {
                        ready = true;
                    }
                },
                else => break
            }

            if ready {
                if self.batch_sender.send(batch.clone()).await.is_err() {
                    break;
                }
                batch.clear();
            }
        }
    }
}

pub struct TransactionGenerator {
    sender: mpsc::Sender<Vec<Transaction>>,
    rng: StdRng,
    transactions_per_100ms: usize,
    initial_delay: Duration,
}

impl TransactionGenerator {
    pub fn start(
        sender: mpsc::Sender<Vec<Transaction>>,
        seed: AuthorityIndex,
        transactions_per_100ms: usize,
        initial_delay: Duration,
    ) {
        let rng = StdRng::seed_from_u64(seed);
        let this = TransactionGenerator {
            sender,
            rng,
            transactions_per_100ms,
            initial_delay,
        };
        runtime::Handle::current().spawn(this.run());
    }

    pub async fn run(mut self) {
        runtime::sleep(self.initial_delay).await;
        loop {
            runtime::sleep(Duration::from_millis(100)).await;
            let mut block = Vec::with_capacity(self.transactions_per_100ms);

            for _ in 0..self.transactions_per_100ms {
                let mut transaction: Vec<u8> = Vec::with_capacity(REAL_BLOCK_HANDLER_TXN_SIZE);
                while transaction.len() < REAL_BLOCK_HANDLER_TXN_SIZE {
                    transaction.extend(&self.rng.gen::<[u8; REAL_BLOCK_HANDLER_TXN_GEN_STEP]>());
                }
                let transaction = Transaction::new(transaction);
                block.push(transaction);
            }

            if self.sender.send(block).await.is_err() {
                break;
            }
        }
    }
}

pub struct TestCommitHandler<H = HashSet<TransactionLocator>> {
    commit_interpreter: Linearizer,
    transaction_votes: TransactionAggregator<TransactionLocator, QuorumThreshold, H>,
    committee: Arc<Committee>,
    committed_leaders: Vec<BlockReference>,
    // committed_dags: Vec<CommittedSubDag>,
    start_time: TimeInstant,
    transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,

    metrics: Arc<Metrics>,
}

impl<H: ProcessedTransactionHandler<TransactionLocator> + Default> TestCommitHandler<H> {
    pub fn new(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::new_with_handler(committee, transaction_time, metrics, Default::default())
    }
}

impl<H: ProcessedTransactionHandler<TransactionLocator>> TestCommitHandler<H> {
    pub fn new_with_handler(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
        handler: H,
    ) -> Self {
        Self {
            commit_interpreter: Linearizer::new(),
            transaction_votes: TransactionAggregator::with_handler(handler),
            committee,
            committed_leaders: vec![],
            // committed_dags: vec![],
            start_time: TimeInstant::now(),
            transaction_time,

            metrics,
        }
    }

    pub fn committed_leaders(&self) -> &Vec<BlockReference> {
        &self.committed_leaders
    }

    /// Note: these metrics are used to compute performance during benchmarks.
    fn update_metrics(&self, timestamp: &Duration) {
        let time_from_start = self.start_time.elapsed();
        let benchmark_duration = self.metrics.benchmark_duration.get();
        if let Some(delta) = time_from_start.as_secs().checked_sub(benchmark_duration) {
            self.metrics.benchmark_duration.inc_by(delta);
        }

        self.metrics
            .latency_s
            .with_label_values(&["default"])
            .observe(timestamp.as_secs_f64());

        let square_latency = timestamp.as_secs_f64().powf(2.0);
        self.metrics
            .latency_squared_s
            .with_label_values(&["default"])
            .inc_by(square_latency);
    }
}

impl<H: ProcessedTransactionHandler<TransactionLocator> + Send + Sync> CommitObserver
    for TestCommitHandler<H>
{
    fn handle_commit(
        &mut self,
        block_store: &BlockStore,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag> {
        let committed = self
            .commit_interpreter
            .handle_commit(block_store, committed_leaders);
        let transaction_time = self.transaction_time.lock();
        for commit in &committed {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                let processed = self
                    .transaction_votes
                    .process_block(block, None, &self.committee);
                for processed_locator in processed {
                    if let Some(instant) = transaction_time.get(&processed_locator) {
                        // todo - batch send data points
                        self.metrics
                            .certificate_committed_latency
                            .observe(instant.elapsed());
                    }
                }
                for (locator, _) in block.shared_transactions() {
                    if let Some(instant) = transaction_time.get(&locator) {
                        let timestamp = instant.elapsed();
                        self.update_metrics(&timestamp);
                        // todo - batch send data points
                        self.metrics
                            .transaction_committed_latency
                            .observe(timestamp);
                    }
                }
            }
            // self.committed_dags.push(commit);
        }
        self.metrics
            .commit_handler_pending_certificates
            .set(self.transaction_votes.len() as i64);
        committed
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
