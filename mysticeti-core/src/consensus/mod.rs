use crate::{
    data::Data,
    types::{AuthorityIndex, RoundNumber, StatementBlock},
};

use self::base_committer::BaseCommitter;

pub mod base_committer;
pub mod linearizer;
pub mod multi_committer;
pub mod pipelined_committer;

/// Default wave length for all committers. A longer wave_length increases the chance of committing the leader
/// under asynchrony at the cost of latency in the common case.
pub const DEFAULT_WAVE_LENGTH: RoundNumber = BaseCommitter::MINIMUM_WAVE_LENGTH;

/// The status of every leader output by the committers. While the core only cares about committed
/// leaders, providing a richer status allows for easier debugging, testing, and composition with
/// advanced commit strategies.
#[derive(Debug, Clone)]
pub enum LeaderStatus {
    Commit(Data<StatementBlock>),
    Skip(AuthorityIndex, RoundNumber),
    Undecided(AuthorityIndex, RoundNumber),
}

impl LeaderStatus {
    pub fn round(&self) -> RoundNumber {
        match self {
            LeaderStatus::Commit(block) => block.round(),
            LeaderStatus::Skip(_, round) => *round,
            LeaderStatus::Undecided(_, round) => *round,
        }
    }

    pub fn authority(&self) -> AuthorityIndex {
        match self {
            LeaderStatus::Commit(block) => block.author(),
            LeaderStatus::Skip(authority, _) => *authority,
            LeaderStatus::Undecided(authority, _) => *authority,
        }
    }

    pub fn decided(&self) -> bool {
        match self {
            LeaderStatus::Commit(_) => true,
            LeaderStatus::Skip(_, _) => true,
            LeaderStatus::Undecided(_, _) => false,
        }
    }
}

pub trait Committer {
    type LastCommitted;

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered committed leaders.
    fn try_commit(
        &self,
        last_committed: Self::LastCommitted,
    ) -> (Vec<LeaderStatus>, Self::LastCommitted);

    /// Return list of leaders for the round. Syncer will give those leaders some extra time.
    /// Can return empty vec if round does not have a designated leader.
    fn leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex>;
}
