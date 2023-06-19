// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry, CounterVec,
    HistogramVec, IntCounter, IntCounterVec, Registry,
};

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
}

impl Metrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
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
        }
    }
}
