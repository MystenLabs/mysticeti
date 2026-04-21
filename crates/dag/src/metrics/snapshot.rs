// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use prometheus::proto::MetricFamily;

use super::names::{
    COMMIT_TYPE_DIRECT_COMMIT, COMMIT_TYPE_INDIRECT_COMMIT, COMMITTED_LEADERS_TOTAL,
    LABEL_AUTHORITY, LABEL_COMMIT_TYPE, LABEL_WORKLOAD, LATENCY_S, MISSING_BLOCKS, WORKLOAD_SHARED,
};
use crate::authority::Authority;

/// A point-in-time snapshot of all metrics from a Prometheus
/// registry. Test-only — no production cost.
#[derive(Debug)]
pub struct MetricsSnapshot {
    families: Vec<MetricFamily>,
}

impl MetricsSnapshot {
    pub(super) fn from_families(families: Vec<MetricFamily>) -> Self {
        Self { families }
    }

    /// Read a metric by name and optional label key-value pairs.
    pub fn metric(&self, name: &str, label_values: &[(&str, &str)]) -> f64 {
        for family in &self.families {
            if family.get_name() != name {
                continue;
            }
            for metric in family.get_metric() {
                if !Self::labels_match(metric, label_values) {
                    continue;
                }
                if metric.has_counter() {
                    return metric.get_counter().get_value();
                } else if metric.has_gauge() {
                    return metric.get_gauge().get_value();
                } else if metric.has_untyped() {
                    return metric.get_untyped().get_value();
                }
            }
        }
        0.0
    }

    /// Mean of all observations in a histogram, in the histogram's
    /// native unit. Returns `None` when the histogram is absent or
    /// has zero observations.
    pub fn histogram_mean(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let (sum, count) = self.histogram_stats(name, labels)?;
        (count > 0).then(|| sum / count as f64)
    }

    /// Mean end-to-end transaction latency (ms) observed by this
    /// replica. Returns `None` when no committed transaction reached
    /// `update_metrics`.
    pub fn mean_latency_ms(&self) -> Option<f64> {
        self.histogram_mean(LATENCY_S, &[(LABEL_WORKLOAD, WORKLOAD_SHARED)])
            .map(|seconds| seconds * 1000.0)
    }

    /// Number of blocks this replica knows it's still missing from the given peer authority.
    pub fn missing_blocks(&self, authority: Authority) -> i64 {
        let label = authority.to_string();
        self.metric(MISSING_BLOCKS, &[(LABEL_AUTHORITY, &label)]) as i64
    }

    /// Total leaders *committed* by `authority` — the commit rows of `committed_leaders_total`
    /// (the `direct-commit` and `indirect-commit` `commit_type` labels). Skipped leaders are
    /// excluded.
    pub fn committed_leaders(&self, authority: Authority) -> u64 {
        let label = authority.to_string();
        let direct = self.metric(
            COMMITTED_LEADERS_TOTAL,
            &[
                (LABEL_AUTHORITY, &label),
                (LABEL_COMMIT_TYPE, COMMIT_TYPE_DIRECT_COMMIT),
            ],
        );
        let indirect = self.metric(
            COMMITTED_LEADERS_TOTAL,
            &[
                (LABEL_AUTHORITY, &label),
                (LABEL_COMMIT_TYPE, COMMIT_TYPE_INDIRECT_COMMIT),
            ],
        );
        (direct + indirect) as u64
    }

    /// Committed leaders per second observed by this replica. Returns `None` when `duration` is
    /// zero or nothing committed.
    pub fn leader_commits_per_second(
        &self,
        authority: Authority,
        duration: Duration,
    ) -> Option<f64> {
        if duration.is_zero() {
            return None;
        }
        let count = self.committed_leaders(authority);
        (count > 0).then(|| count as f64 / duration.as_secs_f64())
    }

    /// Total committed leaders observed by this replica. Skipped leaders are excluded, same
    /// rule as [`committed_leaders`].
    pub fn total_committed_leaders(&self) -> u64 {
        let mut total = 0.0;
        for family in &self.families {
            if family.get_name() != COMMITTED_LEADERS_TOTAL {
                continue;
            }
            for metric in family.get_metric() {
                let is_commit = metric.get_label().iter().any(|l| {
                    l.get_name() == LABEL_COMMIT_TYPE
                        && matches!(
                            l.get_value(),
                            COMMIT_TYPE_DIRECT_COMMIT | COMMIT_TYPE_INDIRECT_COMMIT,
                        )
                });
                if is_commit && metric.has_counter() {
                    total += metric.get_counter().get_value();
                }
            }
        }
        total as u64
    }

    /// Percentile `p` (clamped to `[0, 1]`) of a histogram's observations, in the histogram's
    /// native unit. Uses the Prometheus `histogram_quantile` idiom: linear interpolation between
    /// the upper bounds of adjacent buckets. Returns `None` when the histogram is absent or has
    /// zero observations. When the selected bucket is the `+Inf` terminal, falls back to the
    /// previous finite upper bound so the result stays plottable.
    pub fn histogram_percentile(&self, name: &str, labels: &[(&str, &str)], p: f64) -> Option<f64> {
        let p = p.clamp(0.0, 1.0);
        for family in &self.families {
            if family.get_name() != name {
                continue;
            }
            for metric in family.get_metric() {
                if !metric.has_histogram() || !Self::labels_match(metric, labels) {
                    continue;
                }
                let histogram = metric.get_histogram();
                let total = histogram.get_sample_count();
                if total == 0 {
                    return None;
                }
                let buckets = histogram.get_bucket();
                if buckets.is_empty() {
                    return None;
                }
                let target = p * total as f64;
                let mut prev_bound = 0.0_f64;
                let mut prev_count = 0_u64;
                let mut last_finite_bound = 0.0_f64;
                for bucket in buckets {
                    let upper = bucket.get_upper_bound();
                    let count = bucket.get_cumulative_count();
                    if count as f64 >= target {
                        let high = if upper.is_finite() {
                            upper
                        } else {
                            last_finite_bound
                        };
                        if count == prev_count {
                            return Some(prev_bound);
                        }
                        let fraction = (target - prev_count as f64) / (count - prev_count) as f64;
                        return Some(prev_bound + fraction * (high - prev_bound));
                    }
                    if upper.is_finite() {
                        last_finite_bound = upper;
                    }
                    prev_bound = if upper.is_finite() { upper } else { prev_bound };
                    prev_count = count;
                }
                // Unreachable: the `+Inf` bucket's cumulative_count always equals total, so the
                // `count >= target` branch must fire before falling out of the loop.
                unreachable!(
                    "malformed histogram {name:?}: cumulative_count never reaches sample_count"
                );
            }
        }
        None
    }

    /// Read a histogram's sample sum and count. Returns `None` when no
    /// matching histogram is found (distinct from a present histogram
    /// with zero observations, which returns `Some((0.0, 0))`).
    pub fn histogram_stats(&self, name: &str, labels: &[(&str, &str)]) -> Option<(f64, u64)> {
        for family in &self.families {
            if family.get_name() != name {
                continue;
            }
            for metric in family.get_metric() {
                if !metric.has_histogram() || !Self::labels_match(metric, labels) {
                    continue;
                }
                let histogram = metric.get_histogram();
                return Some((histogram.get_sample_sum(), histogram.get_sample_count()));
            }
        }
        None
    }

    fn labels_match(metric: &prometheus::proto::Metric, expected: &[(&str, &str)]) -> bool {
        let actual = metric.get_label();
        actual.len() == expected.len()
            && expected.iter().all(|(key, value)| {
                actual
                    .iter()
                    .any(|l| l.get_name() == *key && l.get_value() == *value)
            })
    }
}

#[cfg(test)]
mod test {
    use super::MetricsSnapshot;
    use crate::authority::Authority;
    use prometheus::{
        Registry, register_histogram_with_registry, register_int_counter_vec_with_registry,
        register_int_counter_with_registry, register_int_gauge_with_registry,
    };

    fn collect_snapshot(registry: &Registry) -> MetricsSnapshot {
        MetricsSnapshot::from_families(registry.gather())
    }

    #[test]
    fn counter_lookup() {
        let registry = Registry::new();
        let counter =
            register_int_counter_with_registry!("test_counter", "help", registry).unwrap();
        counter.inc_by(5);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("test_counter", &[]), 5.0);
    }

    #[test]
    fn gauge_lookup() {
        let registry = Registry::new();
        let gauge = register_int_gauge_with_registry!("test_gauge", "help", registry).unwrap();
        gauge.set(42);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("test_gauge", &[]), 42.0);
    }

    #[test]
    fn labeled_metric_lookup() {
        let registry = Registry::new();
        let counter_vec =
            register_int_counter_vec_with_registry!("request_total", "help", &["method"], registry)
                .unwrap();
        counter_vec.with_label_values(&["GET"]).inc_by(3);
        counter_vec.with_label_values(&["POST"]).inc_by(7);
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("request_total", &[("method", "GET")]), 3.0);
        assert_eq!(snapshot.metric("request_total", &[("method", "POST")]), 7.0);
        assert_eq!(snapshot.metric("request_total", &[("method", "PUT")]), 0.0);
    }

    #[test]
    fn not_found_returns_zero() {
        let registry = Registry::new();
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.metric("nonexistent", &[]), 0.0);
    }

    #[test]
    fn committed_leaders_excludes_skips() {
        // Drives `committed_leaders_total` directly so the test doesn't need a `Data<Block>` to
        // construct `LeaderStatus::DirectCommit`. Label values here must match the wire strings
        // that `Metrics::inc_decided_leaders` writes.
        let registry = Registry::new();
        let counter = register_int_counter_vec_with_registry!(
            "committed_leaders_total",
            "help",
            &["authority", "commit_type"],
            registry
        )
        .unwrap();
        counter.with_label_values(&["A", "direct-commit"]).inc();
        counter.with_label_values(&["A", "indirect-commit"]).inc();
        counter.with_label_values(&["A", "direct-skip"]).inc();
        counter.with_label_values(&["A", "indirect-skip"]).inc();
        let snapshot = collect_snapshot(&registry);
        assert_eq!(snapshot.committed_leaders(Authority::from(0_usize)), 2);
    }

    #[test]
    fn histogram_percentile_interpolates_within_bucket() {
        // Buckets ≤0.25, ≤0.5, ≤0.75, ≤1.0, ≤+Inf. 100 obs uniformly spread across each of the
        // first four buckets gives cumulative counts [100, 200, 300, 400, 400]; p50 sits at
        // target=200, exactly the upper edge of the second bucket, so it should return 0.5.
        let registry = Registry::new();
        let histogram = register_histogram_with_registry!(
            "demo_latency_s",
            "help",
            vec![0.25, 0.5, 0.75, 1.0],
            registry
        )
        .unwrap();
        for value in [0.1, 0.3, 0.6, 0.8] {
            for _ in 0..100 {
                histogram.observe(value);
            }
        }
        let snapshot = collect_snapshot(&registry);
        assert_eq!(
            snapshot.histogram_percentile("demo_latency_s", &[], 0.0),
            Some(0.0)
        );
        assert_eq!(
            snapshot.histogram_percentile("demo_latency_s", &[], 0.5),
            Some(0.5)
        );
        // p90 target = 360, lies in the fourth bucket between cumulative 300 and 400 → 0.75 +
        // 0.6 * 0.25 = 0.9.
        let p90 = snapshot
            .histogram_percentile("demo_latency_s", &[], 0.9)
            .unwrap();
        assert!((p90 - 0.9).abs() < 1e-9, "p90 = {p90}");
        // p100: prometheus crate adds an implicit +Inf bucket; fall back to the previous finite
        // edge.
        assert_eq!(
            snapshot.histogram_percentile("demo_latency_s", &[], 1.0),
            Some(1.0)
        );
    }

    #[test]
    fn histogram_percentile_empty_returns_none() {
        let registry = Registry::new();
        let _histogram =
            register_histogram_with_registry!("demo_empty_s", "help", vec![0.25, 0.5], registry)
                .unwrap();
        let snapshot = collect_snapshot(&registry);
        assert_eq!(
            snapshot.histogram_percentile("demo_empty_s", &[], 0.5),
            None
        );
    }

    #[test]
    fn partial_label_mismatch() {
        let registry = Registry::new();
        let counter_vec =
            register_int_counter_vec_with_registry!("label_test", "help", &["a", "b"], registry)
                .unwrap();
        counter_vec.with_label_values(&["x", "y"]).inc();
        let snapshot = collect_snapshot(&registry);
        // Only one label when metric has two → mismatch
        assert_eq!(snapshot.metric("label_test", &[("a", "x")]), 0.0);
        // No labels → mismatch
        assert_eq!(snapshot.metric("label_test", &[]), 0.0);
        // Correct labels → match
        assert_eq!(
            snapshot.metric("label_test", &[("a", "x"), ("b", "y")]),
            1.0
        );
    }
}
