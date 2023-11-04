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

pub trait CommitObserver: Send + Sync {
    fn handle_commit(
        &mut self,
        block_store: &BlockStore,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag>;

    fn aggregator_state(&self) -> Bytes;

    fn recover_committed(&mut self, committed: HashSet<BlockReference>, state: Option<Bytes>);
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
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::new_with_handler(committee, transaction_time, metrics, Default::default())
    }
}

impl<H: ProcessedTransactionHandler<TransactionLocator>> TestCommitObserver<H> {
    pub fn new_with_handler(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionLocator, TimeInstant>>>,
        metrics: Arc<Metrics>,
        handler: H,
    ) -> Self {
        let consensus_only = env::var("CONSENSUS_ONLY").is_ok();
        Self {
            commit_interpreter: Linearizer::new(),
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
        block_store: &BlockStore,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommittedSubDag> {
        let current_timestamp = runtime::timestamp_utc();

        let committed = self
            .commit_interpreter
            .handle_commit(block_store, committed_leaders);
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
