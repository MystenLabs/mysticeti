// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::crypto::{dummy_public_key, PublicKey};
use crate::types::{
    AuthorityIndex, AuthoritySet, BaseStatement, Stake, StatementBlock, TransactionLocator,
    TransactionLocatorRange, Vote,
};
use crate::{config::Print, data::Data};
use minibytes::Bytes;
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
    authorities: Vec<Authority>,
    validity_threshold: Stake, // The minimum stake required for validity
    quorum_threshold: Stake,   // The minimum stake required for quorum
}

impl Committee {
    pub const DEFAULT_FILENAME: &'static str = "committee.yaml";

    pub fn new_test(stake: Vec<Stake>) -> Arc<Self> {
        let authorities = stake.into_iter().map(Authority::test_from_stake).collect();
        Self::new(authorities)
    }

    pub fn new(authorities: Vec<Authority>) -> Arc<Self> {
        // todo - check duplicate public keys
        // Ensure the list is not empty
        assert!(!authorities.is_empty());

        // Ensure all stakes are positive
        assert!(authorities.iter().all(|a| a.stake() > 0));
        assert!(authorities.len() <= 128); // For now AuthoritySet only supports up to 128 authorities

        let mut total_stake: Stake = 0;
        for a in authorities.iter() {
            total_stake = total_stake
                .checked_add(a.stake())
                .expect("Total stake overflow");
        }
        let validity_threshold = total_stake / 3;
        let quorum_threshold = 2 * total_stake / 3;
        Arc::new(Committee {
            authorities,
            validity_threshold,
            quorum_threshold,
        })
    }

    pub fn get_stake(&self, authority: AuthorityIndex) -> Option<Stake> {
        self.authorities
            .get(authority as usize)
            .map(Authority::stake)
    }

    pub fn quorum_threshold(&self) -> Stake {
        self.quorum_threshold
    }

    pub fn get_public_key(&self, authority: AuthorityIndex) -> Option<&PublicKey> {
        self.authorities
            .get(authority as usize)
            .map(Authority::public_key)
    }

    pub fn known_authority(&self, authority: AuthorityIndex) -> bool {
        authority < self.len() as AuthorityIndex
    }

    pub fn authorities(&self) -> Range<AuthorityIndex> {
        0u64..(self.authorities.len() as AuthorityIndex)
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
            total_stake += self.authorities[*authority.borrow() as usize].stake();
        }
        total_stake
    }

    // TODO: fix to select by stake
    pub fn elect_leader(&self, r: u64) -> AuthorityIndex {
        (r % self.authorities.len() as u64) as AuthorityIndex
    }

    pub fn random_authority(&self, rng: &mut impl Rng) -> AuthorityIndex {
        rng.gen_range(self.authorities())
    }

    pub fn len(&self) -> usize {
        self.authorities.len()
    }

    pub fn new_for_benchmarks(committee_size: usize) -> Arc<Self> {
        let stake = vec![1; committee_size];
        Self::new_test(stake)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Authority {
    stake: Stake,
    public_key: PublicKey,
}

impl Authority {
    pub fn test_from_stake(stake: Stake) -> Self {
        Self {
            stake,
            public_key: dummy_public_key(),
        }
    }

    pub fn stake(&self) -> Stake {
        self.stake
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
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
pub struct TransactionAggregator<K: TransactionAggregatorKey, TH, H = HashSet<K>> {
    pending: HashMap<K, StakeAggregator<TH>>,
    // todo - need to figure out serialization story with this
    // Currently we skip serialization for test handler,
    // but it also means some invariants wrt unknown_transaction might be potentially broken in some tests
    handler: H,
}

pub trait TransactionAggregatorKey:
    Hash + Eq + Copy + Display + Serialize + for<'a> Deserialize<'a>
{
}
impl<T> TransactionAggregatorKey for T where
    T: Hash + Eq + Copy + Display + Serialize + for<'a> Deserialize<'a>
{
}

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

    pub fn voters(&self) -> impl Iterator<Item = AuthorityIndex> + '_ {
        self.votes.present()
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
}

impl<K: TransactionAggregatorKey, TH: CommitteeThreshold, H: ProcessedTransactionHandler<K>>
    TransactionAggregator<K, TH, H>
{
    pub fn with_handler(handler: H) -> Self {
        Self {
            pending: Default::default(),
            handler,
        }
    }

    pub fn state(&self) -> Bytes {
        bincode::serialize(&self.pending)
            .expect("Serialization failed")
            .into()
    }

    pub fn with_state(&mut self, state: &Bytes) {
        assert!(self.pending.is_empty());
        self.pending = bincode::deserialize(state).expect("Deserialization failed");
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

    fn vote(&mut self, k: K, vote: AuthorityIndex, committee: &Committee, processed: &mut Vec<K>) {
        if let Some(aggregator) = self.pending.get_mut(&k) {
            if aggregator.add(vote, committee) {
                // todo - reuse entry and remove Copy constraint when "entry_insert" is stable
                self.pending.remove(&k).unwrap();
                self.handler.transaction_processed(k);
                processed.push(k);
            }
        } else {
            self.handler.unknown_transaction(k, vote);
        }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
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

impl<TH: CommitteeThreshold, H: ProcessedTransactionHandler<TransactionLocator>>
    TransactionAggregator<TransactionLocator, TH, H>
{
    pub fn process_block(
        &mut self,
        block: &Data<StatementBlock>,
        mut response: Option<&mut Vec<BaseStatement>>,
        committee: &Committee,
    ) -> Vec<TransactionLocator> {
        let mut processed = vec![];
        let mut vote_range_builder = VoteRangeBuilder::default();
        for (offset, statement) in block.statements().iter().enumerate() {
            match statement {
                BaseStatement::Share(_transaction) => {
                    let locator = TransactionLocator::new(*block.reference(), offset as u64);
                    self.register(locator, block.author(), committee);
                    if let Some(ref mut response) = response {
                        if let Some(range) = vote_range_builder.add(locator.offset()) {
                            response.push(BaseStatement::VoteRange(TransactionLocatorRange::new(
                                *block.reference(),
                                range,
                            )));
                        }
                    }
                }
                BaseStatement::Vote(locator, vote) => match vote {
                    Vote::Accept => self.vote(*locator, block.author(), committee, &mut processed),
                    Vote::Reject(_) => unimplemented!(),
                },
                BaseStatement::VoteRange(range) => {
                    for locator in range.locators() {
                        self.vote(locator, block.author(), committee, &mut processed);
                    }
                }
            }
        }
        if let Some(ref mut response) = response {
            if let Some(range) = vote_range_builder.finish() {
                response.push(BaseStatement::VoteRange(TransactionLocatorRange::new(
                    *block.reference(),
                    range,
                )));
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

#[derive(Default)]
struct VoteRangeBuilder {
    range: Option<Range<u64>>,
}

impl VoteRangeBuilder {
    #[must_use]
    pub fn add(&mut self, offset: u64) -> Option<Range<u64>> {
        if let Some(range) = &mut self.range {
            if range.end == offset {
                range.end = offset + 1;
                None
            } else {
                let result = self.range.take();
                self.range = Some(offset..offset + 1);
                result
            }
        } else {
            self.range = Some(offset..offset + 1);
            None
        }
    }

    pub fn finish(self) -> Option<Range<u64>> {
        self.range
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn vote_range_builder_test() {
        let mut b = VoteRangeBuilder::default();
        assert_eq!(None, b.add(1));
        assert_eq!(None, b.add(2));
        assert_eq!(Some(1..3), b.add(4));
        assert_eq!(Some(4..5), b.add(6));
        assert_eq!(Some(6..7), b.finish());
    }
}
