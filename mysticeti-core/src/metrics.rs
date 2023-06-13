use prometheus::{
    register_int_counter_vec_with_registry, register_int_counter_with_registry, IntCounter,
    IntCounterVec, Registry,
};

#[derive(Clone)]
pub struct Metrics {
    pub committed_leaders_total: IntCounter,
    pub leaders_with_enough_support_total: IntCounterVec,
}

impl Metrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            committed_leaders_total: register_int_counter_with_registry!(
                "committed_leaders_total",
                "Total number of committed leaders",
                registry,
            )
            .unwrap(),
            leaders_with_enough_support_total: register_int_counter_vec_with_registry!(
                "leaders_with_enough_support_total",
                "Total number of leaders with enough and not enough support",
                &["enough_support", "not_enough_support"],
                registry,
            )
            .unwrap(),
        }
    }
}
