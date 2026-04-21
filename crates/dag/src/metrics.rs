// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod aggregate;
mod coarse;
mod histogram;
mod names;
mod precise;
mod snapshot;
mod timers;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use ::prometheus::Registry;
use tabled::{Table, Tabled};
use tokio::time::Instant;

pub use self::aggregate::AggregateMetrics;
pub(crate) use self::names::WORKLOAD_SHARED;
pub use self::names::{
    BENCHMARK_DURATION, BLOCK_SYNC_REQUESTS_SENT, LABEL_AUTHORITY, LABEL_WORKLOAD, LATENCY_S,
    LATENCY_SQUARED_S, LEADER_TIMEOUT_TOTAL, SyncRequestFulfilled,
};
pub use self::snapshot::MetricsSnapshot;
pub use self::timers::{OwnedUtilizationTimer, UtilizationTimer};
use self::{
    coarse::CoarseMetrics,
    names::{
        COMMIT_TYPE_DIRECT_COMMIT, COMMIT_TYPE_DIRECT_SKIP, COMMIT_TYPE_INDIRECT_COMMIT,
        COMMIT_TYPE_INDIRECT_SKIP,
    },
    precise::PreciseMetrics,
};
use crate::{authority::Authority, consensus::LeaderStatus};

pub struct Metrics {
    coarse: CoarseMetrics,
    precise: PreciseMetrics,
    registry: Option<Registry>,
}

impl Metrics {
    /// Create metrics and start the background reporter.
    pub fn new(
        registry: &Registry,
        committee_size: usize,
        report_interval: Option<Duration>,
    ) -> Arc<Self> {
        let coarse = CoarseMetrics::new(registry);
        let precise = PreciseMetrics::spawn(registry, committee_size, report_interval);
        Arc::new(Self {
            coarse,
            precise,
            registry: None, // Not needed in production
        })
    }

    /// Create metrics for tests. The registry is stored internally
    /// and drained on-demand via [`Metrics::collect`].
    pub fn new_for_test(committee_size: usize) -> Arc<Self> {
        let registry = Registry::new();
        let coarse = CoarseMetrics::new(&registry);
        let precise = PreciseMetrics::new_for_test(&registry, committee_size);
        Arc::new(Self {
            coarse,
            precise,
            registry: Some(registry),
        })
    }
}

impl Metrics {
    pub fn inc_leader_timeout(&self) {
        self.coarse.leader_timeout_total.inc();
    }

    pub fn inc_core_lock_enqueued(&self) {
        self.coarse.core_lock_enqueued.inc();
    }

    pub fn inc_core_lock_dequeued(&self) {
        self.coarse.core_lock_dequeued.inc();
    }

    pub fn inc_block_store_entries(&self) {
        self.coarse.block_store_entries.inc();
    }

    pub fn inc_block_store_entries_by(&self, n: u64) {
        self.coarse.block_store_entries.inc_by(n);
    }

    pub fn inc_block_store_loaded_blocks(&self) {
        self.coarse.block_store_loaded_blocks.inc();
    }

    pub fn inc_block_store_unloaded_blocks_by(&self, n: u64) {
        self.coarse.block_store_unloaded_blocks.inc_by(n);
    }

    pub fn inc_submitted_transactions(&self, n: u64) {
        self.coarse.submitted_transactions.inc_by(n);
    }

    pub fn inc_benchmark_duration_by(&self, delta: u64) {
        self.coarse.benchmark_duration.inc_by(delta);
    }

    pub fn set_wal_mappings(&self, value: i64) {
        self.coarse.wal_mappings.set(value);
    }

    pub fn benchmark_duration_secs(&self) -> u64 {
        self.coarse.benchmark_duration.get()
    }

    pub fn observe_transaction_committed_latency(&self, d: Duration) {
        self.precise.observe_transaction_committed_latency(d);
    }

    pub fn observe_proposed_block_size_bytes(&self, size: usize) {
        self.precise.observe_proposed_block_size_bytes(size);
    }

    pub fn observe_proposed_block_transaction_count(&self, count: usize) {
        self.precise.observe_proposed_block_transaction_count(count);
    }

    pub fn observe_proposed_block_vote_count(&self, count: usize) {
        self.precise.observe_proposed_block_vote_count(count);
    }

    pub fn observe_latency_s(&self, workload: &str, value: f64) {
        self.coarse
            .latency_s
            .with_label_values(&[workload])
            .observe(value);
    }

    pub fn observe_latency_squared_s(&self, workload: &str, value: f64) {
        self.coarse
            .latency_squared_s
            .with_label_values(&[workload])
            .inc_by(value);
    }

    pub fn observe_inter_block_latency_s(&self, workload: &str, value: f64) {
        self.coarse
            .inter_block_latency_s
            .with_label_values(&[workload])
            .observe(value);
    }

    /// Record a decided leader on `committed_leaders_total`. Silent no-op on
    /// `LeaderStatus::Undecided` — only decided statuses (commit or skip, direct or indirect)
    /// produce a metric increment.
    pub fn inc_decided_leaders(&self, status: &LeaderStatus) {
        let label = match status {
            LeaderStatus::DirectCommit(_) => COMMIT_TYPE_DIRECT_COMMIT,
            LeaderStatus::IndirectCommit(_) => COMMIT_TYPE_INDIRECT_COMMIT,
            LeaderStatus::DirectSkip(..) => COMMIT_TYPE_DIRECT_SKIP,
            LeaderStatus::IndirectSkip(..) => COMMIT_TYPE_INDIRECT_SKIP,
            LeaderStatus::Undecided(..) => return,
        };
        let authority = status.authority().to_string();
        self.coarse
            .committed_leaders_total
            .with_label_values(&[&authority, label])
            .inc();
    }

    pub fn set_missing_blocks(&self, authority: Authority, value: i64) {
        let label = authority.to_string();
        self.coarse
            .missing_blocks
            .with_label_values(&[&label])
            .set(value);
    }

    pub fn inc_block_sync_requests_sent(&self, authority: Authority) {
        let label = authority.to_string();
        self.coarse
            .block_sync_requests_sent
            .with_label_values(&[&label])
            .inc();
    }

    pub fn inc_block_sync_requests_received(
        &self,
        authority: Authority,
        fulfilled: SyncRequestFulfilled,
    ) {
        let label = authority.to_string();
        self.coarse
            .block_sync_requests_received
            .with_label_values(&[&label, fulfilled.as_label()])
            .inc();
    }

    pub fn observe_connection_latency(&self, peer: usize, d: Duration) {
        self.precise.observe_connection_latency(peer, d);
    }

    pub fn core_lock_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.coarse.core_lock_util,
            start: Instant::now(),
        }
    }

    pub fn block_store_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.coarse.block_store_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn block_handler_cleanup_utilization_timer(&self) -> UtilizationTimer<'_> {
        UtilizationTimer {
            metric: &self.coarse.block_handler_cleanup_util,
            start: Instant::now(),
        }
    }

    pub fn utilization_timer(&self, label: &str) -> OwnedUtilizationTimer {
        OwnedUtilizationTimer {
            metric: self.coarse.utilization_timer.with_label_values(&[label]),
            start: Instant::now(),
        }
    }

    /// Flush precise metrics to Prometheus gauges and return
    /// a snapshot of all metrics. Only works in test mode —
    /// panics if called on production metrics.
    pub fn collect(&self) -> MetricsSnapshot {
        let registry = self
            .registry
            .as_ref()
            .expect("collect() is only available on test metrics");
        self.precise.flush();
        MetricsSnapshot::from_families(registry.gather())
    }

    /// Abort the background reporter and drop all observers.
    /// Consumes self to prevent use after shutdown.
    pub fn shutdown(self) {
        self.precise.shutdown();
    }
}

pub fn print_network_address_table(addresses: &[SocketAddr]) {
    let table: Vec<_> = addresses
        .iter()
        .enumerate()
        .map(|(peer, address)| NetworkAddressTable {
            peer: Authority::from(peer).to_string(),
            address: address.to_string(),
        })
        .collect();
    tracing::info!("Network address table:\n{}", Table::new(table));
}

#[derive(Tabled)]
struct NetworkAddressTable {
    peer: String,
    address: String,
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::{Authority, Metrics};
    use crate::consensus::LeaderStatus;

    #[test]
    fn new_for_test_collect_roundtrip() {
        let metrics = Metrics::new_for_test(4);
        metrics.inc_block_store_entries();
        metrics.inc_block_store_entries();
        metrics.inc_submitted_transactions(100);
        let snapshot = metrics.collect();
        assert_eq!(snapshot.metric("block_store_entries", &[]), 2.0);
        assert_eq!(snapshot.metric("submitted_transactions", &[]), 100.0);
    }

    #[test]
    fn benchmark_duration_secs() {
        let metrics = Metrics::new_for_test(4);
        metrics.inc_benchmark_duration_by(10);
        assert_eq!(metrics.benchmark_duration_secs(), 10);
        metrics.inc_benchmark_duration_by(5);
        assert_eq!(metrics.benchmark_duration_secs(), 15);
    }

    #[test]
    fn labeled_metrics_roundtrip() {
        let a = Authority::from(0_usize);
        let b = Authority::from(1_usize);
        let metrics = Metrics::new_for_test(4);
        metrics.inc_decided_leaders(&LeaderStatus::DirectSkip(a, 1));
        metrics.inc_decided_leaders(&LeaderStatus::DirectSkip(a, 2));
        metrics.inc_decided_leaders(&LeaderStatus::IndirectSkip(b, 1));
        metrics.set_missing_blocks(a, 3);
        metrics.inc_block_sync_requests_sent(a);
        let snapshot = metrics.collect();
        assert_eq!(
            snapshot.metric(
                "committed_leaders_total",
                &[("authority", "A"), ("commit_type", "direct-skip")]
            ),
            2.0
        );
        assert_eq!(
            snapshot.metric(
                "committed_leaders_total",
                &[("authority", "B"), ("commit_type", "indirect-skip")]
            ),
            1.0
        );
        assert_eq!(snapshot.missing_blocks(a), 3);
        assert_eq!(
            snapshot.metric("block_sync_requests_sent", &[("authority", "A")]),
            1.0
        );
    }

    #[test]
    fn observe_precise_metrics() {
        let metrics = Metrics::new_for_test(4);
        for i in 1..=100 {
            metrics.observe_transaction_committed_latency(Duration::from_micros(i));
        }
        let snapshot = metrics.collect();
        let p50 = snapshot.metric("transaction_committed_latency", &[("v", "p50")]);
        assert!(p50 > 0.0, "p50 should be populated after flush");
    }

    #[test]
    fn set_gauges_roundtrip() {
        let metrics = Metrics::new_for_test(4);
        metrics.set_wal_mappings(42);
        let snapshot = metrics.collect();
        assert_eq!(snapshot.metric("wal_mappings", &[]), 42.0);
    }

    #[test]
    #[should_panic(expected = "collect() is only available on test metrics")]
    fn collect_panics_without_registry() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let registry = prometheus::Registry::new();
        let metrics = Metrics::new(&registry, 4, None);
        metrics.collect();
    }
}
