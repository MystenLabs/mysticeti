// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;

use crate::types::AuthorityRound;
use crate::{
    data::Data,
    types::{format_authority_round, AuthorityIndex, RoundNumber, StatementBlock},
};

pub mod base_committer;
pub mod linearizer;
pub mod universal_committer;

#[cfg(test)]
mod tests;

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
    Skip(AuthorityRound),
    Undecided(AuthorityRound),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Decision {
    Direct,
    Indirect,
}

impl LeaderStatus {
    pub fn round(&self) -> RoundNumber {
        match self {
            Self::Commit(block) => block.round(),
            Self::Skip(leader) => leader.round,
            Self::Undecided(leader) => leader.round,
        }
    }

    pub fn authority(&self) -> AuthorityIndex {
        match self {
            Self::Commit(block) => block.author(),
            Self::Skip(leader) => leader.authority,
            Self::Undecided(leader) => leader.authority,
        }
    }

    pub fn is_decided(&self) -> bool {
        match self {
            Self::Commit(_) => true,
            Self::Skip(_) => true,
            Self::Undecided(_) => false,
        }
    }

    pub fn into_decided_author_round(self) -> AuthorityRound {
        match self {
            Self::Commit(block) => AuthorityRound::new(block.author(), block.round()),
            Self::Skip(leader) => leader,
            Self::Undecided(..) => panic!("Decided block is either Commit or Skip"),
        }
    }

    pub fn into_committed_block(self) -> Option<Data<StatementBlock>> {
        match self {
            Self::Commit(block) => Some(block),
            Self::Skip(..) => None,
            Self::Undecided(..) => panic!("Decided block is either Commit or Skip"),
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

impl Display for LeaderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Commit(block) => write!(f, "Commit({})", block.reference()),
            Self::Skip(leader) => write!(
                f,
                "Skip({})",
                format_authority_round(leader.authority, leader.round)
            ),
            Self::Undecided(leader) => write!(
                f,
                "Undecided({})",
                format_authority_round(leader.authority, leader.round)
            ),
        }
    }
}
