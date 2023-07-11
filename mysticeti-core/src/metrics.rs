// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::committee::Committee;
use crate::runtime;
use crate::runtime::TimeInstant;
use crate::stat::{histogram, HistogramSender, PreciseHistogram};
use crate::types::{format_authority_index, AuthorityIndex};
use prometheus::{
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry, CounterVec,
    HistogramVec, IntCounter, IntCounterVec, Registry,
};
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

    pub transaction_certified_latency: HistogramSender<Duration>,
    pub certificate_committed_latency: HistogramSender<Duration>,
    pub transaction_committed_latency: HistogramSender<Duration>,
    pub connection_latency_sender: Vec<HistogramSender<Duration>>,
}

pub struct MetricReporter {
    // When adding field here make sure to update
    // MetricsReporter::receive_all and MetricsReporter::run_report.
    pub transaction_certified_latency: PreciseHistogram<Duration>,
    pub certificate_committed_latency: PreciseHistogram<Duration>,
    pub transaction_committed_latency: PreciseHistogram<Duration>,
    pub connection_latency: Vec<PreciseHistogram<Duration>>,
    started: TimeInstant,
}

impl Metrics {
    pub fn new(registry: &Registry, committee: Option<&Committee>) -> (Arc<Self>, MetricReporter) {
        let (transaction_certified_latency_hist, transaction_certified_latency) = histogram();
        let (certificate_committed_latency_hist, certificate_committed_latency) = histogram();
        let (transaction_committed_latency_hist, transaction_committed_latency) = histogram();
        let commitee_size = committee.map(Committee::len).unwrap_or_default();
        let (connection_latency_hist, connection_latency_sender) =
            (0..commitee_size).map(|_| histogram()).unzip();
        let reporter = MetricReporter {
            transaction_certified_latency: transaction_certified_latency_hist,
            certificate_committed_latency: certificate_committed_latency_hist,
            transaction_committed_latency: transaction_committed_latency_hist,
            connection_latency: connection_latency_hist,
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

            transaction_certified_latency,
            certificate_committed_latency,
            transaction_committed_latency,
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

        let mut latencies = vec![];
        for (peer, hist) in self.connection_latency.iter_mut().enumerate() {
            if let Some(report) = Self::latency_report(peer, hist) {
                latencies.push(report);
            }
        }
        tracing::info!("{}", Table::new(latencies));
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

#[derive(Tabled)]
struct LatencyReport {
    peer: char,
    p50: u64,
    p90: u64,
    p99: u64,
    avg: u64,
}
