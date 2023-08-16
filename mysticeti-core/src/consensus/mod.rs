use crate::{
    data::Data,
    types::{RoundNumber, StatementBlock},
};

use self::base_committer::BaseCommitter;

pub mod base_committer;
pub mod linearizer;
pub mod multi_committer;
pub mod pipelined_committer;

/// Default wave length for all committers.
pub const DEFAULT_WAVE_LENGTH: RoundNumber = BaseCommitter::MINIMUM_WAVE_LENGTH;

/// The status of every leader output by the committers. While the core only cares about committed
/// leaders, providing a richer status allows for easier debugging, testing, and composition with
/// advanced commit strategies.
#[derive(Debug)]
pub enum LeaderStatus {
    Commit(Data<StatementBlock>),
    Skip(RoundNumber),
}

impl LeaderStatus {
    pub fn round(&self) -> RoundNumber {
        match self {
            LeaderStatus::Commit(block) => block.round(),
            LeaderStatus::Skip(round) => *round,
        }
    }
}

pub trait Committer {
    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered committed leaders.
    fn try_commit(&self, last_committed_round: RoundNumber) -> Vec<LeaderStatus>;
}
