// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::types::{
    AuthorityIndex, AuthoritySet, BaseStatement, Stake, StatementBlock, TransactionId, Vote,
};
use crate::{config::Print, data::Data};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct Committee {
    stake: Vec<Stake>,
    validity_threshold: Stake, // The minimum stake required for validity
    quorum_threshold: Stake,   // The minimum stake required for quorum
}

impl Committee {
    pub const DEFAULT_FILENAME: &'static str = "committee.yaml";

    pub fn new(stake: Vec<Stake>) -> Arc<Self> {
        // Ensure the list is not empty
        assert!(!stake.is_empty());

        // Ensure all stakes are positive
        assert!(stake.iter().all(|stake| *stake > 0));
        assert!(stake.len() <= 128); // For now AuthoritySet only supports up to 128 authorities

        let mut total_stake: Stake = 0;
        for stake in stake.iter() {
            total_stake = total_stake
                .checked_add(*stake)
                .expect("Total stake overflow");
        }
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

    /// Return own genesis block and other genesis blocks
    pub fn genesis_blocks(
        &self,
        for_authority: AuthorityIndex,
    ) -> (Data<StatementBlock>, Vec<Data<StatementBlock>>) {
        let other_blocks: Vec<_> = self
            .authorities()
            .filter_map(|a| {
                if a == for_authority {
                    None
                } else {
                    Some(StatementBlock::new_genesis(a))
                }
            })
            .collect();
        let own_genesis_block = StatementBlock::new_genesis(for_authority);
        (own_genesis_block, other_blocks)
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

    pub fn new_for_benchmarks(committee_size: usize) -> Arc<Self> {
        let stake = vec![1; committee_size];
        Self::new(stake)
    }
}

impl Print for Committee {}

pub trait CommitteeThreshold {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool;
}

#[derive(Serialize, Deserialize)]
pub struct QuorumThreshold;
#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct StakeAggregator<TH> {
    votes: AuthoritySet,
    stake: Stake,
    _phantom: PhantomData<TH>,
}

/// Tracks votes for pending transactions and outputs certified transactions to a handler
#[derive(Serialize, Deserialize)]
pub struct TransactionAggregator<K: TransactionAggregatorKey, TH, H = HashSet<K>> {
    pending: HashMap<K, StakeAggregator<TH>>,
    // todo - need to figure out something with this
    // Currently we skip serialization for test handler,
    // but it also means some invariants wrt unknown_transaction might be potentially broken in some tests
    #[serde(skip)]
    handler: H,
}

pub trait TransactionAggregatorKey: Hash + Eq + Copy + Display + Serialize {}
impl<T> TransactionAggregatorKey for T where T: Hash + Eq + Copy + Display + Serialize {}

pub trait ProcessedTransactionHandler<K> {
    fn transaction_processed(&mut self, k: K);
    fn duplicate_transaction(&mut self, _k: K, _from: AuthorityIndex) {}
    fn unknown_transaction(&mut self, _k: K, _from: AuthorityIndex) {}
}

impl<K: TransactionAggregatorKey> ProcessedTransactionHandler<K> for HashSet<K> {
    fn transaction_processed(&mut self, k: K) {
        self.insert(k);
    }

    fn duplicate_transaction(&mut self, k: K, from: AuthorityIndex) {
        if !self.contains(&k) {
            panic!("Duplicate transaction {k}: from {from}");
        }
    }

    fn unknown_transaction(&mut self, k: K, from: AuthorityIndex) {
        if !self.contains(&k) {
            panic!("Unexpected - got vote for unknown transaction {k} from {from}");
        }
    }
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

impl<
        K: TransactionAggregatorKey,
        TH: CommitteeThreshold,
        H: ProcessedTransactionHandler<K> + Default,
    > TransactionAggregator<K, TH, H>
{
    pub fn new() -> Self {
        Self {
            pending: Default::default(),
            handler: Default::default(),
        }
    }

    /// Returns Ok(()) if this is first time we see transaction and Err otherwise
    /// When Err is returned transaction is ignored
    pub fn register(&mut self, k: K, vote: AuthorityIndex, committee: &Committee) {
        match self.pending.entry(k) {
            Entry::Occupied(_) => {
                self.handler.duplicate_transaction(k, vote);
            }
            Entry::Vacant(va) => {
                let mut aggregator = StakeAggregator::<TH>::new();
                aggregator.add(vote, committee);
                va.insert(aggregator);
            }
        }
    }

    pub fn vote(
        &mut self,
        k: K,
        vote: AuthorityIndex,
        committee: &Committee,
    ) -> Result<TransactionVoteResult, ()> {
        if let Some(aggregator) = self.pending.get_mut(&k) {
            if aggregator.add(vote, committee) {
                // todo - reuse entry and remove Copy constraint when "entry_insert" is stable
                self.pending.remove(&k).unwrap();
                self.handler.transaction_processed(k);
                Ok(TransactionVoteResult::Processed)
            } else {
                Ok(TransactionVoteResult::VoteAccepted)
            }
        } else {
            self.handler.unknown_transaction(k, vote);
            Err(())
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

impl<K: TransactionAggregatorKey, TH: CommitteeThreshold> TransactionAggregator<K, TH> {
    pub fn is_processed(&self, k: &K) -> bool {
        self.handler.contains(k)
    }
}

pub enum TransactionVoteResult {
    Processed,
    VoteAccepted,
}

impl<TH: CommitteeThreshold> TransactionAggregator<TransactionId, TH> {
    pub fn process_block(
        &mut self,
        block: &Data<StatementBlock>,
        mut response: Option<&mut Vec<BaseStatement>>,
        committee: &Committee,
    ) -> Vec<TransactionId> {
        let mut processed = vec![];
        for statement in block.statements() {
            match statement {
                BaseStatement::Share(id, _transaction) => {
                    self.register(*id, block.author(), committee);
                    if let Some(ref mut response) = response {
                        response.push(BaseStatement::Vote(*id, Vote::Accept));
                    }
                }
                BaseStatement::Vote(id, vote) => match vote {
                    Vote::Accept => {
                        if matches!(
                            self.vote(*id, block.author(), committee),
                            Ok(TransactionVoteResult::Processed)
                        ) {
                            processed.push(*id);
                        }
                    }
                    Vote::Reject(_) => unimplemented!(),
                },
            }
        }
        processed
    }
}

impl<TH: CommitteeThreshold> Default for StakeAggregator<TH> {
    fn default() -> Self {
        Self::new()
    }
}

impl<
        K: TransactionAggregatorKey,
        TH: CommitteeThreshold,
        H: ProcessedTransactionHandler<K> + Default,
    > Default for TransactionAggregator<K, TH, H>
{
    fn default() -> Self {
        Self::new()
    }
}
