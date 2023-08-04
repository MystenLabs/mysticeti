use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::committee::{Committee, QuorumThreshold, StakeAggregator};
use crate::data::Data;
use crate::runtime::timestamp_utc;
use crate::types::{InternalEpochStatus, StatementBlock};

pub struct EpochManager {
    epoch_status: InternalEpochStatus,
    change_aggregator: StakeAggregator<QuorumThreshold>,
    epoch_close_time: Arc<AtomicU64>,
}

impl EpochManager {
    pub fn new() -> Self {
        Self {
            epoch_status: Default::default(),
            change_aggregator: StakeAggregator::new(),
            epoch_close_time: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn epoch_change_begun(&mut self) {
        if let InternalEpochStatus::Open = self.epoch_status {
            self.epoch_status = InternalEpochStatus::BeginChange;
            tracing::info!("Epoch change has begun");
        }
    }

    pub fn observe_committed_block(&mut self, block: &Data<StatementBlock>, committee: &Committee) {
        if block.epoch_changed() {
            let is_quorum = self.change_aggregator.add(block.author(), committee);
            if is_quorum && (self.epoch_status != InternalEpochStatus::SafeToClose) {
                assert!(self.epoch_status == InternalEpochStatus::BeginChange); // Agreement and total ordering property of BA
                self.epoch_status = InternalEpochStatus::SafeToClose;
                self.epoch_close_time
                    .store(timestamp_utc().as_millis() as u64, Ordering::Relaxed);
                tracing::info!("Epoch is now safe to close");
            }
        }
    }

    pub fn changing(&self) -> bool {
        self.epoch_status != InternalEpochStatus::Open
    }

    pub fn closed(&self) -> bool {
        self.epoch_status == InternalEpochStatus::SafeToClose
    }

    pub fn closing_time(&self) -> Arc<AtomicU64> {
        self.epoch_close_time.clone()
    }
}
