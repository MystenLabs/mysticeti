// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::committee::Committee;
use crate::data::{IN_MEMORY_BLOCKS, IN_MEMORY_BLOCKS_BYTES};
use crate::runtime;
use crate::runtime::TimeInstant;
use crate::stat::{histogram, HistogramSender, PreciseHistogram};
use crate::types::{format_authority_index, AuthorityIndex};
use prometheus::{
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry, CounterVec, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, Registry,
};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tabled::{Table, Tabled};
use tokio::time::Instant;

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 0.75, 1., 1.25, 1.5, 1.75, 2., 2.5, 5., 10., 20., 30., 60., 90.,
];

/// Metrics collected by the benchmark.
pub const BENCHMARK_DURATION: &str = "benchmark_duration";
pub const LATENCY_S: &str = "latency_s";
pub const LATENCY_SQUARED_S: &str = "latency_squared_s";

#[derive(Clone)]
pub struct Metrics {
    pub benchmark_duration: IntCounter,
    pub latency_s: HistogramVec,
    pub latency_squared_s: CounterVec,
    pub committed_leaders_total: IntCounterVec,
    pub leader_timeout_total: IntCounter,

    pub block_store_unloaded_blocks: IntCounter,
    pub block_store_loaded_blocks: IntCounter,
    pub block_store_entries: IntCounter,

    pub wal_mappings: IntGauge,

    pub core_lock_util: IntCounter,
    pub core_lock_wait: IntCounter,

    pub block_handler_pending_certificates: IntGauge,

    pub commit_handler_pending_certificates: IntGauge,

    pub transaction_certified_latency: HistogramSender<Duration>,
    pub certificate_committed_latency: HistogramSender<Duration>,
    pub transaction_committed_latency: HistogramSender<Duration>,

    pub proposed_block_size_bytes: HistogramSender<usize>,
    pub proposed_block_transaction_count: HistogramSender<usize>,
    pub proposed_block_vote_count: HistogramSender<usize>,

    pub connection_latency_sender: Vec<HistogramSender<Duration>>,
}

pub struct MetricReporter {
    // When adding field here make sure to update
    // MetricsReporter::receive_all and MetricsReporter::run_report.
    pub transaction_certified_latency: PreciseHistogram<Duration>,
    pub certificate_committed_latency: PreciseHistogram<Duration>,
    pub transaction_committed_latency: PreciseHistogram<Duration>,

    pub proposed_block_size_bytes: PreciseHistogram<usize>,
    pub proposed_block_transaction_count: PreciseHistogram<usize>,
    pub proposed_block_vote_count: PreciseHistogram<usize>,

    pub connection_latency: Vec<PreciseHistogram<Duration>>,

    pub global_in_memory_blocks: IntGauge,
    pub global_in_memory_blocks_bytes: IntGauge,

    started: TimeInstant,
}

impl Metrics {
    pub fn new(registry: &Registry, committee: Option<&Committee>) -> (Arc<Self>, MetricReporter) {
        let (transaction_certified_latency_hist, transaction_certified_latency) = histogram();
        let (certificate_committed_latency_hist, certificate_committed_latency) = histogram();
        let (transaction_committed_latency_hist, transaction_committed_latency) = histogram();

        let (proposed_block_size_bytes_hist, proposed_block_size_bytes) = histogram();
        let (proposed_block_transaction_count_hist, proposed_block_transaction_count) = histogram();
        let (proposed_block_vote_count_hist, proposed_block_vote_count) = histogram();

        let commitee_size = committee.map(Committee::len).unwrap_or_default();
        let (connection_latency_hist, connection_latency_sender) =
            (0..commitee_size).map(|_| histogram()).unzip();
        let reporter = MetricReporter {
            transaction_certified_latency: transaction_certified_latency_hist,
            certificate_committed_latency: certificate_committed_latency_hist,
            transaction_committed_latency: transaction_committed_latency_hist,

            proposed_block_size_bytes: proposed_block_size_bytes_hist,
            proposed_block_transaction_count: proposed_block_transaction_count_hist,
            proposed_block_vote_count: proposed_block_vote_count_hist,

            connection_latency: connection_latency_hist,

            global_in_memory_blocks: register_int_gauge_with_registry!(
                "global_in_memory_blocks",
                "Number of blocks loaded in memory",
                registry,
            )
            .unwrap(),
            global_in_memory_blocks_bytes: register_int_gauge_with_registry!(
                "global_in_memory_blocks_bytes",
                "Total size of blocks loaded in memory",
                registry,
            )
            .unwrap(),

            started: TimeInstant::now(),
        };
        let metrics = Self {
            benchmark_duration: register_int_counter_with_registry!(
                BENCHMARK_DURATION,
                "Duration of the benchmark",
                registry,
            )
            .unwrap(),
            latency_s: register_histogram_vec_with_registry!(
                LATENCY_S,
                "Buckets measuring the end-to-end latency of a workload in seconds",
                &["workload"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            latency_squared_s: register_counter_vec_with_registry!(
                LATENCY_SQUARED_S,
                "Square of total end-to-end latency of a workload in seconds",
                &["workload"],
                registry,
            )
            .unwrap(),
            committed_leaders_total: register_int_counter_vec_with_registry!(
                "committed_leaders_total",
                "Total number of (direct or indirect) committed leaders per authority",
                &["authority", "commit_type"],
                registry,
            )
            .unwrap(),
            leader_timeout_total: register_int_counter_with_registry!(
                "leader_timeout_total",
                "Total number of leader timeouts",
                registry,
            )
            .unwrap(),

            block_store_loaded_blocks: register_int_counter_with_registry!(
                "block_store_loaded_blocks",
                "Blocks loaded from wal position in the block store",
                registry,
            )
            .unwrap(),
            block_store_unloaded_blocks: register_int_counter_with_registry!(
                "block_store_unloaded_blocks",
                "Blocks unloaded from wal position during cleanup",
                registry,
            )
            .unwrap(),
            block_store_entries: register_int_counter_with_registry!(
                "block_store_entries",
                "Number of entries in block store",
                registry,
            )
            .unwrap(),

            wal_mappings: register_int_gauge_with_registry!(
                "wal_mappings",
                "Number of mappings retained by the wal",
                registry,
            )
            .unwrap(),

            core_lock_util: register_int_counter_with_registry!(
                "core_lock_util",
                "Utilization of core write lock",
                registry,
            )
            .unwrap(),
            core_lock_wait: register_int_counter_with_registry!(
                "core_lock_wait",
                "Time to wait for core lock",
                registry,
            )
            .unwrap(),

            block_handler_pending_certificates: register_int_gauge_with_registry!(
                "block_handler_pending_certificates",
                "Number of pending certificates in block handler",
                registry,
            )
            .unwrap(),

            commit_handler_pending_certificates: register_int_gauge_with_registry!(
                "commit_handler_pending_certificates",
                "Number of pending certificates in commit handler",
                registry,
            )
            .unwrap(),

            transaction_certified_latency,
            certificate_committed_latency,
            transaction_committed_latency,

            proposed_block_size_bytes,
            proposed_block_transaction_count,
            proposed_block_vote_count,

            connection_latency_sender,
        };

        (Arc::new(metrics), reporter)
    }
}

impl MetricReporter {
    pub fn start(self) {
        runtime::Handle::current().spawn(self.run());
    }

    pub fn receive_all(&mut self) {
        self.transaction_certified_latency.receive_all();
        self.certificate_committed_latency.receive_all();
        self.transaction_committed_latency.receive_all();

        self.proposed_block_size_bytes.receive_all();
        self.proposed_block_transaction_count.receive_all();
        self.proposed_block_vote_count.receive_all();

        self.connection_latency
            .iter_mut()
            .for_each(PreciseHistogram::receive_all);
    }

    // todo - this task never stops
    async fn run(mut self) {
        const REPORT_INTERVAL: Duration = Duration::from_secs(10);
        let mut deadline = Instant::now();
        loop {
            deadline += REPORT_INTERVAL;
            tokio::time::sleep_until(deadline).await;
            self.run_report().await;
        }
    }

    async fn run_report(&mut self) {
        self.global_in_memory_blocks
            .set(IN_MEMORY_BLOCKS.load(Ordering::Relaxed) as i64);
        self.global_in_memory_blocks_bytes
            .set(IN_MEMORY_BLOCKS_BYTES.load(Ordering::Relaxed) as i64);

        self.receive_all();
        let elapsed = self.started.elapsed();
        Self::report_hist(
            "transaction_certified_latency",
            &mut self.transaction_certified_latency,
            elapsed,
        );
        Self::report_hist(
            "certificate_committed_latency",
            &mut self.certificate_committed_latency,
            elapsed,
        );
        Self::report_hist(
            "transaction_committed_latency",
            &mut self.transaction_committed_latency,
            elapsed,
        );
        Self::report_size_hist(
            "proposed_block_size_bytes",
            &mut self.proposed_block_size_bytes,
        );
        Self::report_size_hist(
            "proposed_block_transaction_count",
            &mut self.proposed_block_transaction_count,
        );
        Self::report_size_hist(
            "proposed_block_vote_count",
            &mut self.proposed_block_vote_count,
        );

        let mut latencies = vec![];
        for (peer, hist) in self.connection_latency.iter_mut().enumerate() {
            if let Some(report) = Self::latency_report(peer, hist) {
                latencies.push(report);
            }
        }
        tracing::info!("Network latency report:\n{}", Table::new(latencies));
    }

    fn latency_report(peer: usize, hist: &mut PreciseHistogram<Duration>) -> Option<LatencyReport> {
        let [p50, p90, p99] = hist.pcts([500, 900, 999])?;
        let avg = hist.avg()?;
        Some(LatencyReport {
            peer: format_authority_index(peer as AuthorityIndex),
            p50: p50.as_millis() as u64,
            p90: p90.as_millis() as u64,
            p99: p99.as_millis() as u64,
            avg: avg.as_millis() as u64,
        })
    }

    fn report_size_hist(name: &str, h: &mut PreciseHistogram<usize>) -> Option<()> {
        let [p50, p90, p99] = h.pcts([500, 900, 999])?;
        let avg = h.avg()?;
        tracing::info!(
            "{}: avg={:?}, p50={:?}, p90={:?}, p99={:?}",
            name,
            avg,
            p50,
            p90,
            p99
        );
        None
    }

    fn report_hist(
        name: &str,
        h: &mut PreciseHistogram<Duration>,
        elapsed: Duration,
    ) -> Option<()> {
        let [p50, p90, p99] = h.pcts([500, 900, 999])?;
        let avg = h.avg()?;
        let count = h.count();
        let tps = if elapsed.as_secs() > 0 {
            count / elapsed.as_secs() as usize
        } else {
            0
        };
        tracing::info!(
            "{}: tps={}, avg={:?}, p50={:?}, p90={:?}, p99={:?}",
            name,
            tps,
            avg,
            p50,
            p90,
            p99
        );
        None
    }
}

pub fn print_network_address_table(addresses: &[SocketAddr]) {
    let table: Vec<_> = addresses
        .iter()
        .enumerate()
        .map(|(peer, address)| NetworkAddressTable {
            peer: format_authority_index(peer as AuthorityIndex),
            address: address.to_string(),
        })
        .collect();
    tracing::info!("Network address table:\n{}", Table::new(table));
}

pub trait UtilizationTimerExt {
    fn utilization_timer(&self) -> UtilizationTimer;
}

impl UtilizationTimerExt for IntCounter {
    fn utilization_timer(&self) -> UtilizationTimer {
        UtilizationTimer {
            metric: self,
            start: Instant::now(),
        }
    }
}

pub struct UtilizationTimer<'a> {
    metric: &'a IntCounter,
    start: Instant,
}

impl<'a> Drop for UtilizationTimer<'a> {
    fn drop(&mut self) {
        self.metric.inc_by(self.start.elapsed().as_micros() as u64);
    }
}

#[derive(Tabled)]
struct NetworkAddressTable {
    peer: char,
    address: String,
}

#[derive(Tabled)]
struct LatencyReport {
    peer: char,
    p50: u64,
    p90: u64,
    p99: u64,
    avg: u64,
}
