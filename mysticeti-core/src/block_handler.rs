// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    env,
    path::Path,
    sync::Arc,
    time::Duration,
};

use minibytes::Bytes;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;
use async_trait::async_trait;

use crate::{
    block_store::BlockStore,
    committee::{Committee, ProcessedTransactionHandler, QuorumThreshold, TransactionAggregator},
    consensus::linearizer::{CommittedSubDag, Linearizer},
    data::Data,
    log::TransactionLog,
    metrics::{Metrics, UtilizationTimerExt, UtilizationTimerVecExt},
    runtime,
    runtime::TimeInstant,
    syncer::CommitObserver,
    transactions_generator::TransactionGenerator,
    types::{
        AuthorityIndex,
        BaseStatement,
        BlockReference,
        StatementBlock,
        Transaction,
        TransactionLocator,
    },
};

#[async_trait]
pub trait BlockHandler: Send + Sync {
    async fn handle_blocks(
        &mut self,
        blocks: &[Data<StatementBlock>],
        require_response: bool,
    ) -> Vec<BaseStatement>;

    async fn handle_proposal(&mut self, block: &Data<StatementBlock>);

    fn state(&self) -> Bytes;

    fn recover_state(&mut self, _state: &Bytes);

    async fn cleanup(&self) {}
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
    transaction_votes: TransactionAggregator<QuorumThreshold, TransactionLog>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,
    block_store: BlockStore,
    metrics: Arc<Metrics>,
    receiver: mpsc::Receiver<Vec<Transaction>>,
    pending_transactions: usize,
    consensus_only: bool,
}

/// The max number of transactions per block.
// todo - This value should be in bytes because it is capped by the wal entry size.
pub const SOFT_MAX_PROPOSED_PER_BLOCK: usize = 20 * 1000;

impl RealBlockHandler {
    pub fn new(
        committee: Arc<Committee>,
        authority: AuthorityIndex,
        certified_transactions_log_path: &Path,
        block_store: BlockStore,
        metrics: Arc<Metrics>,
    ) -> (Self, mpsc::Sender<Vec<Transaction>>) {
        let (sender, receiver) = mpsc::channel(1024);
        let transaction_log = TransactionLog::start(certified_transactions_log_path)
            .expect("Failed to open certified transaction log for write");

        let consensus_only = env::var("CONSENSUS_ONLY").is_ok();
        let this = Self {
            transaction_votes: TransactionAggregator::with_handler(transaction_log),
            transaction_time: Default::default(),
            committee,
            authority,
            block_store,
            metrics,
            receiver,
            pending_transactions: 0, // todo - need to initialize correctly when loaded from disk
            consensus_only,
        };
        (this, sender)
    }
}

impl RealBlockHandler {
    async fn receive_with_limit(&mut self) -> Option<Vec<Transaction>> {
        let pending = self.pending_transactions;
        tracing::info!("Receiving tx, currently {pending}");
        if self.pending_transactions >= SOFT_MAX_PROPOSED_PER_BLOCK {
            return None;
        }
        let received = self.receiver.recv().await?;
        self.pending_transactions += received.len();
        Some(received)
    }

    /// Expose a metric for certified transactions.
    fn update_metrics(
        &self,
        block_creation: Option<&TimeInstant>,
        transaction: &Transaction,
        current_timestamp: &Duration,
    ) {
        // Record inter-block latency.
        if let Some(instant) = block_creation {
            let latency = instant.elapsed();
            self.metrics.transaction_certified_latency.observe(latency);
            self.metrics
                .inter_block_latency_s
                .with_label_values(&["owned"])
                .observe(latency.as_secs_f64());
        }

        // Record end-to-end latency.
        let tx_submission_timestamp = TransactionGenerator::extract_timestamp(transaction);
        let latency = current_timestamp.saturating_sub(tx_submission_timestamp);
        let square_latency = latency.as_secs_f64().powf(2.0);
        self.metrics
            .latency_s
            .with_label_values(&["owned"])
            .observe(latency.as_secs_f64());
        self.metrics
            .latency_squared_s
            .with_label_values(&["owned"])
            .inc_by(square_latency);
    }
}

#[async_trait]
impl BlockHandler for RealBlockHandler {
    async fn handle_blocks(
        &mut self,
        blocks: &[Data<StatementBlock>],
        require_response: bool,
    ) -> Vec<BaseStatement> {
        let current_timestamp = runtime::timestamp_utc();
        let _timer = self
            .metrics
            .utilization_timer
            .utilization_timer("BlockHandler::handle_blocks");
        let mut response = vec![];
        if require_response {
            while let Some(data) = self.receive_with_limit().await {
                tracing::info!("Received {} transactions", data.len());
                let tx_count = data.len();
                for tx in data {
                    response.push(BaseStatement::Share(tx));
                }
            }
        }
        // Use try_lock instead of block_on
        let transaction_time = self.transaction_time.lock().await;
        for block in blocks {
            let response_option: Option<&mut Vec<BaseStatement>> = if require_response {
                Some(&mut response)
            } else {
                None
            };
            if !self.consensus_only {
                let processed =
                    self.transaction_votes
                        .process_block(block, response_option, &self.committee);
                for processed_locator in processed {
                    let block_creation = transaction_time.get(&processed_locator);
                    let transaction = self
                        .block_store
                        .get_transaction(&processed_locator)
                        .expect("Failed to get certified transaction");
                    self.update_metrics(block_creation, &transaction, &current_timestamp);
                }
            }
        }
    
    self.metrics
        .block_handler_pending_certificates
        .set(self.transaction_votes.len() as i64);
    response
    }

    async fn handle_proposal(&mut self, block: &Data<StatementBlock>) {
        // todo - this is not super efficient
        self.pending_transactions -= block.shared_transactions().count();
        let mut transaction_time = self.transaction_time.lock().await;
        for (locator, _) in block.shared_transactions() {
            transaction_time.insert(locator, TimeInstant::now());
        }
        if !self.consensus_only {
            for range in block.shared_ranges() {
                self.transaction_votes
                    .register(range, self.authority, &self.committee);
            }
        }
    }

    fn state(&self) -> Bytes {
        self.transaction_votes.state()
    }

    fn recover_state(&mut self, state: &Bytes) {
        self.transaction_votes.with_state(state);
    }

    async fn cleanup(&self) {
        let _timer = self.metrics.block_handler_cleanup_util.utilization_timer();
        let mut l = self.transaction_time.lock().await;
        l.retain(|_k, v| v.elapsed() < Duration::from_secs(10));
    }
}

// Immediately votes and generates new transactions
pub struct TestBlockHandler {
    last_transaction: u64,
    transaction_votes: TransactionAggregator<QuorumThreshold>,
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

#[async_trait]
impl BlockHandler for TestBlockHandler {
    async fn handle_blocks(
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
        let transaction_time = self.transaction_time.lock().await;
        for block in blocks {
            tracing::debug!("Processing {block:?}");
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

    async fn handle_proposal(&mut self, block: &Data<StatementBlock>) {
        let mut transaction_time = self.transaction_time.lock().await;
        for (locator, _) in block.shared_transactions() {
            transaction_time.insert(locator, TimeInstant::now());
            self.proposed.push(locator);
        }
        for range in block.shared_ranges() {
            self.transaction_votes
                .register(range, self.authority, &self.committee);
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

pub struct TestCommitHandler<H = HashSet<TransactionLocator>> {
    global_linearizer: Arc<Mutex<Linearizer>>,
    transaction_votes: TransactionAggregator<QuorumThreshold, H>,
    committee: Arc<Committee>,
    committed_leaders: Vec<BlockReference>,
    // committed_dags: Vec<CommittedSubDag>,
    start_time: TimeInstant,
    transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,

    metrics: Arc<Metrics>,
    consensus_only: bool,
}

impl<H: ProcessedTransactionHandler<TransactionLocator> + Default> TestCommitHandler<H> {
    pub fn new(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
        global_linearizer: Arc<Mutex<Linearizer>>,
    ) -> Self {
        Self::new_with_handler(committee, transaction_time, metrics, Default::default(), global_linearizer)
    }
}

impl<H: ProcessedTransactionHandler<TransactionLocator>> TestCommitHandler<H> {
    pub fn new_with_handler(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
        handler: H,
        global_linearizer: Arc<Mutex<Linearizer>>,
    ) -> Self {
        let consensus_only = env::var("CONSENSUS_ONLY").is_ok();
        Self {
            transaction_votes: TransactionAggregator::with_handler(handler),
            committee,
            committed_leaders: vec![],
            // committed_dags: vec![],
            start_time: TimeInstant::now(),
            transaction_time,
            metrics,
            consensus_only,
            global_linearizer,
        }
    }

    pub fn committed_leaders(&self) -> &Vec<BlockReference> {
        &self.committed_leaders
    }

    /// Note: these metrics are used to compute performance during benchmarks.
    fn update_metrics(
        &self,
        block_creation: Option<&TimeInstant>,
        current_timestamp: Duration,
        transaction: &Transaction,
    ) {
        // Record inter-block latency.
        if let Some(instant) = block_creation {
            let latency = instant.elapsed();
            self.metrics.transaction_committed_latency.observe(latency);
            self.metrics
                .inter_block_latency_s
                .with_label_values(&["shared"])
                .observe(latency.as_secs_f64());
        }

        // Record benchmark start time.
        let time_from_start = self.start_time.elapsed();
        let benchmark_duration = self.metrics.benchmark_duration.get();
        if let Some(delta) = time_from_start.as_secs().checked_sub(benchmark_duration) {
            self.metrics.benchmark_duration.inc_by(delta);
        }

        // Record end-to-end latency. The first 8 bytes of the transaction are the timestamp of the
        // transaction submission.
        let tx_submission_timestamp = TransactionGenerator::extract_timestamp(transaction);
        let latency = current_timestamp.saturating_sub(tx_submission_timestamp);
        let square_latency = latency.as_secs_f64().powf(2.0);
        self.metrics
            .latency_s
            .with_label_values(&["shared"])
            .observe(latency.as_secs_f64());
        self.metrics
            .latency_squared_s
            .with_label_values(&["shared"])
            .inc_by(square_latency);
    }
}

#[async_trait]
impl<H: ProcessedTransactionHandler<TransactionLocator> + Send + Sync> CommitObserver
    for TestCommitHandler<H>
{
    async fn observe_commit(
        &mut self,
        committed_subdags: Vec<CommittedSubDag>,
    ) -> Vec<CommittedSubDag> {
        let current_timestamp = runtime::timestamp_utc();
        let transaction_time = self.transaction_time.lock().await;

        for commit in &committed_subdags {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                if !self.consensus_only {
                    let processed =
                        self.transaction_votes
                            .process_block(block, None, &self.committee);
                    for processed_locator in processed {
                        if let Some(instant) = transaction_time.get(&processed_locator) {
                            self.metrics
                                .certificate_committed_latency
                                .observe(instant.elapsed());
                        }
                    }
                }
                for (locator, transaction) in block.shared_transactions() {
                    self.update_metrics(
                        transaction_time.get(&locator),
                        current_timestamp,
                        transaction,
                    );
                }
            }
        }
        self.metrics
            .commit_handler_pending_certificates
            .set(self.transaction_votes.len() as i64);
        
        // Return the input committed_subdags
        committed_subdags
    }
    
    fn aggregator_state(&self) -> Bytes {
        self.transaction_votes.state()
    }

    async fn recover_committed(&mut self, committed: HashSet<BlockReference>, state: Option<Bytes>) {
        let mut linearizer = self.global_linearizer.lock().await;
        assert!(linearizer.committed.is_empty());
        if let Some(state) = state {
            self.transaction_votes.with_state(&state);
        } else {
            assert!(committed.is_empty());
        }
        linearizer.committed = committed;
    }
}
