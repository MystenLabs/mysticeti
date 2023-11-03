// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::crypto::{dummy_public_key, PublicKey};
use crate::range_map::RangeMap;
use crate::types::{
    AuthorityIndex, AuthoritySet, BaseStatement, BlockReference, Stake, StatementBlock,
    TransactionLocator, TransactionLocatorRange, Vote,
};
use crate::{config::Print, data::Data};
use minibytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
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

#[allow(clippy::len_without_is_empty)]
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
        assert!(authorities.len() <= AuthoritySet::MAX_SIZE);

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

    pub fn validity_threshold(&self) -> Stake {
        self.validity_threshold + 1
    }

    pub fn quorum_threshold(&self) -> Stake {
        self.quorum_threshold + 1
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
        0..(self.authorities.len() as AuthorityIndex)
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
    pub fn new(stake: Stake, public_key: PublicKey) -> Self {
        Self { stake, public_key }
    }

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

pub trait CommitteeThreshold: Clone {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct QuorumThreshold;
#[derive(Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Clone)]
pub struct StakeAggregator<TH> {
    votes: AuthoritySet,
    stake: Stake,
    _phantom: PhantomData<TH>,
}

/// Tracks votes for pending transactions and outputs certified transactions to a handler
pub struct TransactionAggregator<TH, H = HashSet<TransactionLocator>> {
    pending: HashMap<BlockReference, RangeMap<u64, StakeAggregator<TH>>>,
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

impl<TH: CommitteeThreshold, H: ProcessedTransactionHandler<TransactionLocator> + Default>
    TransactionAggregator<TH, H>
{
    pub fn new() -> Self {
        Self {
            pending: Default::default(),
            handler: Default::default(),
        }
    }
}

impl<TH: CommitteeThreshold, H: ProcessedTransactionHandler<TransactionLocator>>
    TransactionAggregator<TH, H>
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

    pub fn register(
        &mut self,
        locator_range: TransactionLocatorRange,
        vote: AuthorityIndex,
        committee: &Committee,
    ) {
        let range_map = self.pending.entry(*locator_range.block()).or_default();
        range_map.mutate_range(locator_range.range(), |range, aggregator_opt| {
            if aggregator_opt.is_some() {
                for l in range {
                    let k = TransactionLocator::new(*locator_range.block(), l);
                    // todo - make duplicate_transaction take TransactionLocatorRange instead
                    self.handler.duplicate_transaction(k, vote);
                }
            } else {
                let mut aggregator = StakeAggregator::<TH>::new();
                aggregator.add(vote, committee);
                *aggregator_opt = Some(aggregator);
            }
        });
    }

    fn vote(
        &mut self,
        locator_range: TransactionLocatorRange,
        vote: AuthorityIndex,
        committee: &Committee,
        processed: &mut Vec<TransactionLocator>,
    ) {
        if let Some(range_map) = self.pending.get_mut(locator_range.block()) {
            range_map.mutate_range(locator_range.range(), |range, aggregator_opt| {
                match aggregator_opt {
                    None => {
                        for l in range {
                            let k = TransactionLocator::new(*locator_range.block(), l);
                            // todo - make unknown_transaction take TransactionLocatorRange instead
                            self.handler.unknown_transaction(k, vote);
                        }
                    }
                    Some(aggregator) => {
                        if aggregator.add(vote, committee) {
                            for l in range {
                                let k = TransactionLocator::new(*locator_range.block(), l);
                                // todo - make transaction_processed take TransactionLocatorRange instead
                                self.handler.transaction_processed(k);
                                processed.push(k);
                            }
                            *aggregator_opt = None;
                        }
                    }
                }
            });
            if range_map.is_empty() {
                self.pending.remove(locator_range.block());
            }
        } else {
            for l in locator_range.locators() {
                // todo - make unknown_transaction take TransactionLocatorRange instead
                self.handler.unknown_transaction(l, vote);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

impl<TH: CommitteeThreshold> TransactionAggregator<TH> {
    pub fn is_processed(&self, k: &TransactionLocator) -> bool {
        self.handler.contains(k)
    }
}

pub enum TransactionVoteResult {
    Processed,
    VoteAccepted,
}

impl<TH: CommitteeThreshold, H: ProcessedTransactionHandler<TransactionLocator>>
    TransactionAggregator<TH, H>
{
    pub fn process_block(
        &mut self,
        block: &Data<StatementBlock>,
        mut response: Option<&mut Vec<BaseStatement>>,
        committee: &Committee,
    ) -> Vec<TransactionLocator> {
        let mut processed = vec![];
        for range in block.shared_ranges() {
            self.register(range, block.author(), committee);
            if let Some(ref mut response) = response {
                response.push(BaseStatement::VoteRange(range));
            }
        }
        for statement in block.statements() {
            match statement {
                BaseStatement::Share(_transaction) => {}
                BaseStatement::Vote(locator, vote) => match vote {
                    Vote::Accept => self.vote(
                        TransactionLocatorRange::one(*locator),
                        block.author(),
                        committee,
                        &mut processed,
                    ),
                    Vote::Reject(_) => unimplemented!(),
                },
                BaseStatement::VoteRange(range) => {
                    self.vote(*range, block.author(), committee, &mut processed);
                }
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

impl<TH: CommitteeThreshold, H: ProcessedTransactionHandler<TransactionLocator> + Default> Default
    for TransactionAggregator<TH, H>
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
pub struct VoteRangeBuilder {
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
