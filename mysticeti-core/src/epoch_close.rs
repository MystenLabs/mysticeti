use crate::committee::{Committee, QuorumThreshold, StakeAggregator, ValidityThreshold};
use crate::data::Data;
use crate::types::{EpochStatus, StatementBlock};
use tokio::sync::mpsc;

pub struct EpochManager {
    epoch_status: EpochStatus,
    change_receiver: mpsc::Receiver<()>,
    change_aggregator: StakeAggregator<QuorumThreshold>,
    close_aggregator: StakeAggregator<ValidityThreshold>,
    close_signal: mpsc::Receiver<()>,
}

impl EpochManager {
    pub fn new(change_receiver: mpsc::Receiver<()>, close_signal: mpsc::Receiver<()>) -> Self {
        Self {
            epoch_status: Default::default(),
            change_receiver,
            change_aggregator: StakeAggregator::new(),
            close_aggregator: StakeAggregator::new(),
            close_signal,
        }
    }

    pub fn epoch_status(&mut self) -> EpochStatus {
        if let EpochStatus::Open = self.epoch_status {
            if self.change_receiver.try_recv().is_ok() {
                self.epoch_status = EpochStatus::BeginChange;
                tracing::info!("Epoch change has begun");
            }
        }
        self.epoch_status.clone()
    }

    pub fn observe_block(&mut self, block: &Data<StatementBlock>, committee: &Committee) {
        match block.epoch_marker() {
            EpochStatus::Open => (),
            EpochStatus::BeginChange => {
                if self.change_aggregator.add(block.author(), committee) {
                    self.epoch_status = EpochStatus::SafeToClose;
                    tracing::info!("Epoch is now safe to close");
                }
            }
            EpochStatus::SafeToClose => {
                if self.close_aggregator.add(block.author(), committee) {
                    self.epoch_status = EpochStatus::Closed;
                    tracing::info!("Epoch should be closed now");
                }
            }
            EpochStatus::Closed => {
                self.close_signal.close(); // corresponding senders unblock and allow all tasks to join
            }
        }
    }
}
