// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::v2::types::{AuthorityIndex, Stake, StatementBlock};
use rand::Rng;
use std::borrow::Borrow;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

pub struct Committee {
    stake: Vec<Stake>,
    validity_threshold: Stake, // The minimum stake required for validity
    quorum_threshold: Stake,   // The minimum stake required for quorum
}

impl Committee {
    pub fn new(stake: Vec<Stake>) -> Arc<Self> {
        // Ensure the list is not empty
        assert!(!stake.is_empty());

        // Ensure all stakes are positive
        assert!(stake.iter().all(|stake| *stake > 0));

        let total_stake: Stake = stake.iter().sum();
        let validity_threshold = total_stake / 3;
        let quorum_threshold = 2 * total_stake / 3;
        Arc::new(Committee {
            stake,
            validity_threshold,
            quorum_threshold,
        })
    }

    pub fn get_stake(&self, authority: AuthorityIndex) -> Option<Stake> {
        self.stake.get(authority as usize).copied()
    }

    pub fn authorities(&self) -> Range<u64> {
        0u64..(self.stake.len() as u64)
    }

    /// Block for for_authority will go first
    pub fn genesis_blocks(&self, for_authority: AuthorityIndex) -> Vec<StatementBlock> {
        let mut blocks: Vec<_> = self
            .authorities()
            .map(StatementBlock::new_genesis)
            .collect();
        blocks.swap(0, for_authority as usize);
        blocks
    }

    pub fn is_valid(&self, amount: Stake) -> bool {
        amount > self.validity_threshold
    }

    pub fn is_quorum(&self, amount: Stake) -> bool {
        amount > self.quorum_threshold
    }

    pub fn get_total_stake<A: Borrow<AuthorityIndex>>(&self, authorities: &HashSet<A>) -> Stake {
        let mut total_stake = 0;
        for authority in authorities {
            total_stake += self.stake[*authority.borrow() as usize];
        }
        total_stake
    }

    // TODO: fix to select by stake
    pub fn pick_authority(&self, r: u64) -> AuthorityIndex {
        (r % self.stake.len() as u64) as AuthorityIndex
    }

    pub fn random_authority(&self, rng: &mut impl Rng) -> AuthorityIndex {
        rng.gen_range(self.authorities())
    }
}

pub trait CommitteeThreshold {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool;
}

pub struct QuorumThreshold;
pub struct ValidityThreshold;

impl CommitteeThreshold for QuorumThreshold {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool {
        committee.is_quorum(amount)
    }
}

impl CommitteeThreshold for ValidityThreshold {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool {
        committee.is_valid(amount)
    }
}

pub struct StakeAggregator<TH> {
    votes: HashSet<AuthorityIndex>,
    stake: Stake,
    _phantom: PhantomData<TH>,
}

impl<TH: CommitteeThreshold> StakeAggregator<TH> {
    pub fn new() -> Self {
        Self {
            votes: Default::default(),
            stake: 0,
            _phantom: Default::default(),
        }
    }

    pub fn add(&mut self, vote: AuthorityIndex, committee: &Committee) -> bool {
        let stake = committee.get_stake(vote).expect("Authority not found");
        if self.votes.insert(vote) {
            self.stake += stake;
        }
        TH::is_threshold(committee, self.stake)
    }

    pub fn clear(&mut self) {
        self.votes.clear();
        self.stake = 0;
    }
}
