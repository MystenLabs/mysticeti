// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_store::{BlockStore, CommitData};
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
use crate::validator::TransactionTimeMap;
use minibytes::Bytes;
use std::collections::HashSet;
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
}

#[derive(Default)]
pub struct CommitObserverRecoveredState {
    /// All previously committed subdags.
    pub sub_dags: Vec<CommitData>,
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
    transaction_time: TransactionTimeMap,

    metrics: Arc<Metrics>,
    consensus_only: bool,
}

impl<H: ProcessedTransactionHandler<TransactionLocator> + Default> TestCommitObserver<H> {
    pub fn new_for_testing(
        block_store: BlockStore,
        committee: Arc<Committee>,
        transaction_time: TransactionTimeMap,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::new(
            block_store,
            committee,
            transaction_time,
            metrics,
            Default::default(),
            Default::default(),
        )
    }
}

impl<H: ProcessedTransactionHandler<TransactionLocator>> TestCommitObserver<H> {
    pub fn new(
        block_store: BlockStore,
        committee: Arc<Committee>,
        transaction_time: TransactionTimeMap,
        metrics: Arc<Metrics>,
        handler: H,
        recovered_state: CommitObserverRecoveredState,
    ) -> Self {
        let consensus_only = env::var("CONSENSUS_ONLY").is_ok();
        let mut observer = Self {
            commit_interpreter: Linearizer::new(block_store),
            transaction_votes: TransactionAggregator::with_handler(handler),
            committee,
            committed_leaders: vec![],
            // committed_dags: vec![],
            start_time: TimeInstant::now(),
            transaction_time,

            metrics,
            consensus_only,
        };
        observer.recover_committed(recovered_state);
        observer
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

    fn recover_committed(&mut self, recovered_state: CommitObserverRecoveredState) {
        if let Some(state) = &recovered_state.state {
            self.transaction_votes.with_state(state);
        } else {
            assert!(recovered_state.sub_dags.is_empty());
        }
        self.commit_interpreter.recover_state(&recovered_state);
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
        // Last CommittedSubDag::height that has been successfully sent to the output channel.
        // First commit in the replayed sequence will have height last_sent_height + 1.
        // Set to 0 to replay from the start (as normal sequence starts at height = 1).
        last_sent_height: u64,
        recover_state: CommitObserverRecoveredState,
    ) -> Self {
        let mut observer = Self {
            block_store: block_store.clone(),
            commit_interpreter: Linearizer::new(block_store),
            sender,
        };
        observer.recover_committed(last_sent_height, recover_state);
        observer
    }

    fn recover_committed(
        &mut self,
        last_sent_height: u64,
        recovered_state: CommitObserverRecoveredState,
    ) {
        self.commit_interpreter.recover_state(&recovered_state);
        for commit_data in recovered_state.sub_dags {
            // Resend all the committed subdags to the consensus output channel for all the commits
            // above the last sent height.
            // TODO: Since commit data is ordered by height, we can optimize this by first doing
            // a binary search and skip all the commits below the last sent height.
            if commit_data.height > last_sent_height {
                let committed_subdag =
                    CommittedSubDag::new_from_commit_data(commit_data, &self.block_store);
                if let Err(err) = self.sender.send(committed_subdag) {
                    tracing::error!("Failed to send committed sub-dag: {:?}", err);
                }
            }
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
}
