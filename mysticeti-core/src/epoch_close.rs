use crate::committee::{Committee, QuorumThreshold, StakeAggregator};
use crate::data::Data;
use crate::runtime::TimeInstant;
use crate::types::{EpochStatus, InternalEpochStatus, StatementBlock};

pub struct EpochManager {
    epoch_status: InternalEpochStatus,
    change_aggregator: StakeAggregator<QuorumThreshold>,
    epoch_close_time: TimeInstant,
}

impl EpochManager {
    pub fn new() -> Self {
        Self {
            epoch_status: Default::default(),
            change_aggregator: StakeAggregator::new(),
            epoch_close_time: TimeInstant::now(),
        }
    }

    pub fn epoch_change_begun(&mut self) {
        if let InternalEpochStatus::Open = self.epoch_status {
            self.epoch_status = InternalEpochStatus::BeginChange;
            tracing::info!("Epoch change has begun");
        }
    }

    pub fn epoch_status(&self) -> EpochStatus {
        match self.epoch_status {
            InternalEpochStatus::Open => EpochStatus::Open,
            InternalEpochStatus::BeginChange => EpochStatus::BeginChange,
            InternalEpochStatus::SafeToClose => EpochStatus::SafeToClose,
        }
    }

    pub fn observe_committed_block(&mut self, block: &Data<StatementBlock>, committee: &Committee) {
        match block.epoch_marker() {
            EpochStatus::Open => (),
            EpochStatus::BeginChange => {
                let is_quorum = self.change_aggregator.add(block.author(), committee);
                if is_quorum && (self.epoch_status != InternalEpochStatus::SafeToClose) {
                    assert!(self.epoch_status == InternalEpochStatus::BeginChange); // Agreement and total ordering property of BA
                    self.epoch_status = InternalEpochStatus::SafeToClose;
                    self.epoch_close_time = TimeInstant::now();
                    tracing::info!("Epoch is now safe to close");
                }
            }
            EpochStatus::SafeToClose => (), // other nodes may be shutting down sync soon
        }
    }

    pub fn closed(&self) -> bool {
        self.epoch_status == InternalEpochStatus::SafeToClose
    }

    pub fn closing_time(&self) -> TimeInstant {
        assert!(self.closed());
        self.epoch_close_time.clone()
    }
}
