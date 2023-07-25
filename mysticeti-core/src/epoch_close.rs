use crate::committee::{Committee, QuorumThreshold, StakeAggregator, ValidityThreshold};
use crate::data::Data;
use crate::types::{EpochStatus, InternalEpochStatus, StatementBlock};
use tokio::sync::mpsc;

pub struct EpochManager {
    epoch_status: InternalEpochStatus,
    change_aggregator: StakeAggregator<QuorumThreshold>,
    close_aggregator: StakeAggregator<ValidityThreshold>,
    close_signal: mpsc::Receiver<()>,
}

impl EpochManager {
    pub fn new(close_signal: mpsc::Receiver<()>) -> Self {
        Self {
            epoch_status: Default::default(),
            change_aggregator: StakeAggregator::new(),
            close_aggregator: StakeAggregator::new(),
            close_signal,
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
            InternalEpochStatus::Closed => EpochStatus::Closed,
        }
    }

    pub fn observe_committed_block(&mut self, block: &Data<StatementBlock>, committee: &Committee) {
        match block.epoch_marker() {
            EpochStatus::Open => (),
            EpochStatus::BeginChange => {
                if self.change_aggregator.add(block.author(), committee) {
                    self.epoch_status = InternalEpochStatus::SafeToClose;
                    tracing::info!("Epoch is now safe to close");
                }
            }
            EpochStatus::SafeToClose => {
                if self.close_aggregator.add(block.author(), committee) {
                    self.epoch_status = InternalEpochStatus::Closed;
                    tracing::info!("Epoch should be closed now");
                }
            }
            EpochStatus::Closed => {
                self.close_signal.close(); // corresponding senders unblock and allow all tasks to join
            }
        }
    }
}
