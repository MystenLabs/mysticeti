// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::core::Core;
use crate::data::Data;
use crate::runtime::timestamp_utc;
use crate::types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock};
use crate::{block_handler::BlockHandler, metrics::Metrics};
use crate::{
    block_store::{BlockStore, CommitData},
    types::Transaction,
};
use minibytes::Bytes;
use std::collections::HashSet;
use std::{collections::BTreeMap, sync::Arc};

pub struct Syncer<H: BlockHandler, S: SyncerSignals, C: CommitObserver> {
    core: Core<H>,
    own_blocks: BTreeMap<RoundNumber, Data<StatementBlock>>,
    last_seen_by_authority: Vec<RoundNumber>,
    force_new_block: bool,
    commit_period: u64,
    signals: S,
    commit_observer: C,
    metrics: Arc<Metrics>,
}

pub trait SyncerSignals: Send + Sync {
    fn new_block_ready(&mut self);
}

pub trait CommitObserver: Send + Sync {
    fn handle_commit(
        &mut self,
        block_store: &BlockStore,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommitData>;

    fn aggregator_state(&self) -> Bytes;

    fn recover_committed(&mut self, committed: HashSet<BlockReference>, state: Option<Bytes>);
}

#[allow(dead_code)]
impl<H: BlockHandler, S: SyncerSignals, C: CommitObserver> Syncer<H, S, C> {
    pub fn new(
        core: Core<H>,
        commit_period: u64,
        signals: S,
        commit_observer: C,
        metrics: Arc<Metrics>,
    ) -> Self {
        let last_seen_by_authority = core.committee().authorities().map(|_| 0).collect();
        Self {
            core,
            own_blocks: Default::default(),
            force_new_block: false,
            commit_period,
            signals,
            commit_observer,
            last_seen_by_authority,
            metrics,
        }
    }

    pub fn add_transactions(&mut self, transactions: Vec<Data<Transaction>>) {
        self.core.add_transactions(transactions);
    }

    pub fn add_blocks(&mut self, blocks: Vec<Data<StatementBlock>>) {
        let processed = self.core.add_blocks(blocks);
        for block in processed {
            let last_seen = self
                .last_seen_by_authority
                .get_mut(block.author() as usize)
                .expect("last_seen_by_authority not found");
            if block.round() > *last_seen {
                *last_seen = block.round();
            }
        }
        self.try_new_block();
    }

    pub fn force_new_block(&mut self, round: RoundNumber) -> bool {
        if self.core.last_proposed() == round {
            self.metrics.leader_timeout_total.inc();
            self.force_new_block = true;
            self.try_new_block();
            true
        } else {
            false
        }
    }

    fn try_new_block(&mut self) {
        if self.force_new_block || self.core.ready_new_block(self.commit_period) {
            let Some(block) = self.core.try_new_block() else { return; };
            assert!(self.own_blocks.insert(block.round(), block).is_none());
            self.signals.new_block_ready();
            self.force_new_block = false;

            let newly_committed = self.core.try_commit(3);
            let utc_now = timestamp_utc();
            if !newly_committed.is_empty() {
                let committed_refs: Vec<_> = newly_committed
                    .iter()
                    .map(|block| {
                        let age = utc_now
                            .checked_sub(block.meta_creation_time())
                            .unwrap_or_default();
                        format!("{}({}ms)", block.reference(), age.as_millis())
                    })
                    .collect();
                tracing::debug!("Committed {:?}", committed_refs);
            }
            let commit_data = self
                .commit_observer
                .handle_commit(self.core.block_store(), newly_committed);
            self.core
                .write_commits(&commit_data, &self.commit_observer.aggregator_state());
        }
    }

    pub fn get_own_blocks(
        &self,
        from_excluded: RoundNumber,
        limit: usize,
    ) -> Vec<Data<StatementBlock>> {
        self.own_blocks
            .range((from_excluded + 1)..)
            .take(limit)
            .map(|(_k, v)| v.clone())
            .collect()
    }

    pub fn last_own_block(&self) -> Option<Data<StatementBlock>> {
        self.own_blocks.values().last().cloned()
    }

    pub fn last_seen_by_authority(&self, authority: AuthorityIndex) -> RoundNumber {
        *self
            .last_seen_by_authority
            .get(authority as usize)
            .expect("last_seen_by_authority not found")
    }

    pub fn commit_observer(&self) -> &C {
        &self.commit_observer
    }

    pub fn core(&self) -> &Core<H> {
        &self.core
    }

    #[cfg(test)]
    pub fn scheduler_state_id(&self) -> usize {
        self.core.authority() as usize
    }
}

impl SyncerSignals for bool {
    fn new_block_ready(&mut self) {
        *self = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_handler::{TestBlockHandler, TestCommitHandler};
    use crate::data::Data;
    use crate::simulator::{Scheduler, Simulator, SimulatorState};
    use crate::test_util::{
        check_commits, committee_and_syncers, first_n_transactions, rng_at_seed,
    };
    use rand::Rng;
    use std::ops::Range;
    use std::time::Duration;

    const ROUND_TIMEOUT: Duration = Duration::from_millis(1000);
    const LATENCY_RANGE: Range<Duration> = Duration::from_millis(100)..Duration::from_millis(1800);

    pub enum SyncerEvent {
        ForceNewBlock(RoundNumber),
        DeliverBlock(Data<StatementBlock>),
    }

    impl SimulatorState for Syncer<TestBlockHandler, bool, TestCommitHandler> {
        type Event = SyncerEvent;

        fn handle_event(&mut self, event: Self::Event) {
            match event {
                SyncerEvent::ForceNewBlock(round) => {
                    if self.force_new_block(round) {
                        // eprintln!("[{:06} {}] Proposal timeout for {round}", scheduler.time_ms(), self.core.authority());
                    }
                }
                SyncerEvent::DeliverBlock(block) => {
                    // eprintln!("[{:06} {}] Deliver {block}", scheduler.time_ms(), self.core.authority());
                    self.add_blocks(vec![block]);
                }
            }

            // New block was created
            if self.signals {
                self.signals = false;
                let last_block = self.last_own_block().unwrap();
                Scheduler::schedule_event(
                    ROUND_TIMEOUT,
                    self.scheduler_state_id(),
                    SyncerEvent::ForceNewBlock(last_block.round()),
                );
                for authority in self.core.committee().authorities() {
                    if authority == self.core.authority() {
                        continue;
                    }
                    let latency =
                        Scheduler::<SyncerEvent>::with_rng(|rng| rng.gen_range(LATENCY_RANGE));
                    Scheduler::schedule_event(
                        latency,
                        authority as usize,
                        SyncerEvent::DeliverBlock(last_block.clone()),
                    );
                }
            }
        }
    }

    #[test]
    pub fn test_syncer() {
        for seed in 0..10 {
            test_syncer_at(seed);
        }
    }

    pub fn test_syncer_at(seed: u64) {
        eprintln!("Seed {seed}");
        let rng = rng_at_seed(seed);
        let (committee, syncers) = committee_and_syncers(4);
        let mut simulator = Simulator::new(syncers, rng);

        // Kick off process by asking validators create a block after genesis
        for authority in committee.authorities() {
            simulator.schedule_event(
                Duration::ZERO,
                authority as usize,
                SyncerEvent::ForceNewBlock(0),
            );
        }
        // Simulation awaits for first num_txn transactions proposed by each authority to certify
        let num_txn = 40;
        let await_transactions = first_n_transactions(&committee, num_txn);
        assert_eq!(await_transactions.len(), num_txn as usize * committee.len());

        let mut iteration = 0u64;
        loop {
            iteration += 1;
            assert!(!simulator.run_one());
            let not_certified: Vec<_> = simulator
                .states()
                .iter()
                .map(|syncer| {
                    await_transactions
                        .iter()
                        .map(|txid| {
                            if syncer.core.block_handler().is_certified(*txid) {
                                0usize
                            } else {
                                1usize
                            }
                        })
                        .sum::<usize>()
                })
                .collect();

            if not_certified.iter().sum::<usize>() == 0 {
                let time = simulator.time();
                let rounds = simulator
                    .states()
                    .iter()
                    .map(|syncer| syncer.core.last_proposed())
                    .max()
                    .unwrap();
                eprintln!("Certified {} transactions in {time:.2?}, {rounds} rounds, {iteration} iterations", await_transactions.len());
                check_commits(simulator.states());
                break;
            } /*else if iteration % 100 == 0 {
                  eprintln!("Not certified: {not_certified:?}");
              }*/
        }
    }
}
