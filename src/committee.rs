// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::data::Data;
use crate::types::{AuthorityIndex, BaseStatement, Stake, StatementBlock, TransactionId, Vote};
use rand::Rng;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
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

    pub fn authorities(&self) -> Range<AuthorityIndex> {
        0u64..(self.stake.len() as AuthorityIndex)
    }

    /// Block for for_authority will go first
    pub fn genesis_blocks(&self, for_authority: AuthorityIndex) -> Vec<Data<StatementBlock>> {
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
    pub fn elect_leader(&self, r: u64) -> AuthorityIndex {
        (r % self.stake.len() as u64) as AuthorityIndex
    }

    pub fn random_authority(&self, rng: &mut impl Rng) -> AuthorityIndex {
        rng.gen_range(self.authorities())
    }

    pub fn len(&self) -> usize {
        self.stake.len()
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

/// Tracks votes for pending transactions and set of certified transactions
pub struct TransactionAggregator<K, TH> {
    pending: HashMap<K, StakeAggregator<TH>>,
    processed: HashSet<K>,
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

impl<K: Hash + Eq + Copy, TH: CommitteeThreshold> TransactionAggregator<K, TH> {
    pub fn new() -> Self {
        Self {
            pending: Default::default(),
            processed: Default::default(),
        }
    }

    /// Returns Ok(()) if this is first time we see transaction and Err otherwise
    /// When Err is returned transaction is ignored (todo - is this what we want?)
    pub fn register(
        &mut self,
        k: K,
        vote: AuthorityIndex,
        committee: &Committee,
    ) -> Result<(), ()> {
        if self.processed.contains(&k) {
            return Err(());
        }
        match self.pending.entry(k) {
            Entry::Occupied(_) => Err(()),
            Entry::Vacant(va) => {
                let mut aggregator = StakeAggregator::<TH>::new();
                aggregator.add(vote, committee);
                va.insert(aggregator);
                Ok(())
            }
        }
    }

    pub fn vote(&mut self, k: K, vote: AuthorityIndex, committee: &Committee) -> Result<(), ()> {
        if self.processed.contains(&k) {
            return Ok(());
        }
        if let Some(aggregator) = self.pending.get_mut(&k) {
            if aggregator.add(vote, committee) {
                // todo - reuse entry and remove Copy constraint when "entry_insert" is stable
                self.pending.remove(&k).unwrap();
                self.processed.insert(k);
            }
            Ok(())
        } else {
            // Unknown transaction
            Err(())
        }
    }

    pub fn is_processed(&self, k: &K) -> bool {
        self.processed.contains(k)
    }
}

impl<TH: CommitteeThreshold> TransactionAggregator<TransactionId, TH> {
    pub fn process_block(
        &mut self,
        block: &Data<StatementBlock>,
        mut response: Option<&mut Vec<BaseStatement>>,
        committee: &Committee,
    ) {
        for statement in block.statements() {
            match statement {
                BaseStatement::Share(id, _transaction) => {
                    if self.register(*id, block.author(), &committee).is_err() {
                        panic!("Duplicate transaction: {id} from {}", block.author());
                    }
                    if let Some(ref mut response) = response {
                        response.push(BaseStatement::Vote(*id, Vote::Accept));
                    }
                }
                BaseStatement::Vote(id, vote) => match vote {
                    Vote::Accept => {
                        if self.vote(*id, block.author(), &committee).is_err() {
                            panic!("Unexpected - got vote for unknown transaction {}", id);
                        }
                    }
                    Vote::Reject(_) => unimplemented!(),
                },
            }
        }
    }
}

impl<TH: CommitteeThreshold> Default for StakeAggregator<TH> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Hash + Eq + Copy, TH: CommitteeThreshold> Default for TransactionAggregator<K, TH> {
    fn default() -> Self {
        Self::new()
    }
}
