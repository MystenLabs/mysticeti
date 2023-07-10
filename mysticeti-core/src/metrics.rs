// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::runtime;
use crate::stat::{histogram, HistogramSender, PreciseHistogram};
use prometheus::{
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry, CounterVec,
    HistogramVec, IntCounter, IntCounterVec, Registry,
};
use std::sync::Arc;
use std::time::Duration;
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
}

pub struct MetricReporter {
    pub transaction_certified_latency: PreciseHistogram<Duration>,
    pub certificate_committed_latency: PreciseHistogram<Duration>,
    pub transaction_committed_latency: PreciseHistogram<Duration>,
}

impl Metrics {
    pub fn new(registry: &Registry) -> (Arc<Self>, MetricReporter) {
        let (transaction_certified_latency_hist, transaction_certified_latency) = histogram();
        let (certificate_committed_latency_hist, certificate_committed_latency) = histogram();
        let (transaction_committed_latency_hist, transaction_committed_latency) = histogram();
        let reporter = MetricReporter {
            transaction_certified_latency: transaction_certified_latency_hist,
            certificate_committed_latency: certificate_committed_latency_hist,
            transaction_committed_latency: transaction_committed_latency_hist,
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
        };

        (Arc::new(metrics), reporter)
    }
}

impl MetricReporter {
    pub fn start(self) {
        runtime::Handle::current().spawn(self.run());
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
        Self::report_hist(
            "transaction_certified_latency",
            &mut self.transaction_certified_latency,
        );
        Self::report_hist(
            "certificate_committed_latency",
            &mut self.certificate_committed_latency,
        );
        Self::report_hist(
            "transaction_committed_latency",
            &mut self.transaction_committed_latency,
        );
    }

    fn report_hist(name: &str, h: &mut PreciseHistogram<Duration>) -> Option<()> {
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
}
