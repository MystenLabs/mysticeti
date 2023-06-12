use prometheus::{register_int_counter_with_registry, IntCounter, Registry};

#[derive(Clone)]
pub struct Metrics {
    pub committed_leaders_total: IntCounter,
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
        }
    }
}
