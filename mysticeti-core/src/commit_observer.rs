// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_store::BlockStore;
use crate::committee::{
    Committee, ProcessedTransactionHandler, QuorumThreshold, TransactionAggregator,
};
use crate::consensus::linearizer::{CommittedSubDag, Linearizer};
use crate::data::Data;
use crate::metrics::Metrics;
use crate::runtime;
use crate::runtime::TimeInstant;
use crate::transactions_generator::TransactionGenerator;
use crate::types::{BlockReference, StatementBlock, Transaction, TransactionLocator};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use std::time::Duration;

/// CommitObserver is called by core when it detects new commit.
pub trait CommitObserver: Send + Sync {
    /// handle_commit is called by the core every time commit(s) are observed.
    fn handle_commit(
        &mut self,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag>;

    /// If CommitObserver has an aggregator (such as TransactionAggregator for fast path), this method return serialized state of such aggregator to be persisted in wal.
    fn aggregator_state(&self) -> Bytes;

    /// When core is restored from wal, it calls CommitObserver::recover_committed and passes the relevant information recovered from wal(see CommitObserverRecoveredState).
    /// recover_committed is guaranteed to be called on CommitObserver before any other call(e.g. handle_commit or aggregator_state).
    /// recover_committed is only called if core is recovered from wal, for newly created instance of the protocol recover_committed won't be called.
    fn recover_committed(&mut self, state: CommitObserverRecoveredState);
}

pub struct CommitObserverRecoveredState {
    /// set of references of all previously committed blocks
    pub committed: HashSet<BlockReference>,
    /// height of the last committed sub dag. This is 0 if no subdags were committed
    pub last_committed_height: u64,
    /// Last observed state of the commit observer returned by CommitObserver::aggregator_state
    pub state: Option<Bytes>,
}

pub struct TestCommitObserver<H = HashSet<TransactionLocator>> {
    commit_interpreter: Linearizer,
    transaction_votes: TransactionAggregator<QuorumThreshold, H>,
    committee: Arc<Committee>,
    committed_leaders: Vec<BlockReference>,
    // committed_dags: Vec<CommittedSubDag>,
    start_time: TimeInstant,
    transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,

    metrics: Arc<Metrics>,
    consensus_only: bool,
}

impl<H: ProcessedTransactionHandler<TransactionLocator> + Default> TestCommitObserver<H> {
    pub fn new(
        block_store: BlockStore,
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::new_with_handler(
            block_store,
            committee,
            transaction_time,
            metrics,
            Default::default(),
        )
    }
}

impl<H: ProcessedTransactionHandler<TransactionLocator>> TestCommitObserver<H> {
    pub fn new_with_handler(
        block_store: BlockStore,
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
        handler: H,
    ) -> Self {
        let consensus_only = env::var("CONSENSUS_ONLY").is_ok();
        Self {
            commit_interpreter: Linearizer::new(block_store),
            transaction_votes: TransactionAggregator::with_handler(handler),
            committee,
            committed_leaders: vec![],
            // committed_dags: vec![],
            start_time: TimeInstant::now(),
            transaction_time,

            metrics,
            consensus_only,
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

impl<H: ProcessedTransactionHandler<TransactionLocator> + Send + Sync> CommitObserver
    for TestCommitObserver<H>
{
    fn handle_commit(
        &mut self,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag> {
        let current_timestamp = runtime::timestamp_utc();

        let committed = self.commit_interpreter.handle_commit(committed_leaders);
        let transaction_time = self.transaction_time.lock();
        for commit in &committed {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                if !self.consensus_only {
                    let processed =
                        self.transaction_votes
                            .process_block(block, None, &self.committee);
                    for processed_locator in processed {
                        if let Some(instant) = transaction_time.get(&processed_locator) {
                            // todo - batch send data points
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

    fn recover_committed(&mut self, recovered_state: CommitObserverRecoveredState) {
        if let Some(state) = recovered_state.state {
            self.transaction_votes.with_state(&state);
        } else {
            assert!(recovered_state.committed.is_empty());
        }
        self.commit_interpreter.recover_state(
            recovered_state.committed,
            recovered_state.last_committed_height,
        );
    }
}

pub struct SimpleCommitObserver {
    #[allow(dead_code)] // will be used later during replay
    block_store: BlockStore,
    commit_interpreter: Linearizer,
    /// A channel to send committed sub-dags to the consumer of consensus output.
    /// TODO: We will need to figure out a solution to handle back pressure.
    sender: tokio::sync::mpsc::UnboundedSender<CommittedSubDag>,
}

impl SimpleCommitObserver {
    pub fn new(
        block_store: BlockStore,
        // Channel where core will send commits, application can read commits form the other end
        sender: tokio::sync::mpsc::UnboundedSender<CommittedSubDag>,
        // Last CommittedSubDag::height(excluded) that we need to replay commits from.
        // First commit in the replayed sequence will have height replay_from_height+1.
        // Set to 0 to replay from the start.
        _replay_from_height: u64,
    ) -> Self {
        Self {
            block_store: block_store.clone(),
            commit_interpreter: Linearizer::new(block_store),
            sender,
        }
    }
}

impl CommitObserver for SimpleCommitObserver {
    fn handle_commit(
        &mut self,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag> {
        let committed = self.commit_interpreter.handle_commit(committed_leaders);
        for commit in &committed {
            // TODO: Could we get rid of this clone latter?
            if let Err(err) = self.sender.send(commit.clone()) {
                tracing::error!("Failed to send committed sub-dag: {:?}", err);
            }
        }
        committed
    }

    fn aggregator_state(&self) -> Bytes {
        Bytes::new()
    }

    fn recover_committed(&mut self, recovered_state: CommitObserverRecoveredState) {
        // The committed state is set only when we care about fast path aggregation. It must be
        // empty for the simple observer right now.
        assert!(recovered_state.state.map(|s| s.is_empty()).unwrap_or(true));
        self.commit_interpreter.recover_state(
            recovered_state.committed,
            recovered_state.last_committed_height,
        );
    }
}
