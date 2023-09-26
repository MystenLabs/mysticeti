// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data::Data,
    types::{AuthorityIndex, RoundNumber, StatementBlock},
};

pub mod base_committer;
pub mod linearizer;
pub mod universal_committer;

/// Default wave length for all committers. A longer wave_length increases the chance of committing the leader
/// under asynchrony at the cost of latency in the common case.
pub const DEFAULT_WAVE_LENGTH: RoundNumber = MINIMUM_WAVE_LENGTH;

/// We need at least one leader round, one voting round, and one decision round.
pub const MINIMUM_WAVE_LENGTH: RoundNumber = 3;

/// The status of every leader output by the committers. While the core only cares about committed
/// leaders, providing a richer status allows for easier debugging, testing, and composition with
/// advanced commit strategies.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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

    pub fn is_decided(&self) -> bool {
        match self {
            LeaderStatus::Commit(_) => true,
            LeaderStatus::Skip(_, _) => true,
            LeaderStatus::Undecided(_, _) => false,
        }
    }
}

impl PartialOrd for LeaderStatus {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LeaderStatus {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.round(), self.authority()).cmp(&(other.round(), other.authority()))
    }
}
