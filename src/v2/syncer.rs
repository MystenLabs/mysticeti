// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::v2::block_handler::BlockHandler;
use crate::v2::core::Core;
use crate::v2::types::{BlockReference, RoundNumber, StatementBlock};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct Syncer<H: BlockHandler, S: SyncerSignals, C: CommitObserver> {
    core: Core<H>,
    own_blocks: BTreeMap<RoundNumber, Arc<StatementBlock>>,
    force_new_block: bool,
    commit_period: u64,
    signals: S,
    commit_observer: C,
}

pub trait SyncerSignals: Send + Sync {
    fn new_block_ready(&mut self);
}

pub trait CommitObserver: Send + Sync {
    fn handle_commit(&mut self, committed_leader: BlockReference);
}

#[allow(dead_code)]
impl<H: BlockHandler, S: SyncerSignals, C: CommitObserver> Syncer<H, S, C> {
    pub fn new(core: Core<H>, commit_period: u64, signals: S, commit_observer: C) -> Self {
        Self {
            core,
            own_blocks: Default::default(),
            force_new_block: false,
            commit_period,
            signals,
            commit_observer,
        }
    }

    pub fn add_blocks(&mut self, blocks: Vec<Arc<StatementBlock>>) {
        self.core.add_blocks(blocks);
        self.try_new_block();
    }

    pub fn force_new_block(&mut self, round: RoundNumber) -> bool {
        if self.core.last_proposed() == round {
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
            if let Some(commit) = self.core.try_commit(3) {
                // todo - we need to inspect previous leaders too
                self.commit_observer.handle_commit(commit);
            }
        }
    }

    pub fn get_own_blocks(
        &self,
        from_excluded: RoundNumber,
        limit: usize,
    ) -> Vec<Arc<StatementBlock>> {
        self.own_blocks
            .range((from_excluded + 1)..)
            .take(limit)
            .map(|(_k, v)| v.clone())
            .collect()
    }

    fn last_own_block(&self) -> Option<Arc<StatementBlock>> {
        self.own_blocks.values().last().cloned()
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

impl CommitObserver for Vec<BlockReference> {
    fn handle_commit(&mut self, committed_leader: BlockReference) {
        self.push(committed_leader);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::block_handler::TestBlockHandler;
    use crate::v2::simulator::{Scheduler, Simulator, SimulatorState};
    use crate::v2::test_util::{committee_and_syncers, first_n_transactions, rng_at_seed};
    use rand::Rng;
    use std::collections::HashSet;
    use std::ops::Range;
    use std::time::Duration;

    const ROUND_TIMEOUT: Duration = Duration::from_millis(1000);
    const LATENCY_RANGE: Range<Duration> = Duration::from_millis(100)..Duration::from_millis(1200);

    pub enum SyncerEvent {
        ForceNewBlock(RoundNumber),
        DeliverBlock(Arc<StatementBlock>),
    }

    impl SimulatorState for Syncer<TestBlockHandler, bool, Vec<BlockReference>> {
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
        for seed in 0..100 {
            test_syncer_at(seed);
        }
    }

    pub fn test_syncer_at(seed: u64) {
        eprintln!("Seed {seed}");
        let rng = rng_at_seed(seed);
        let (committee, syncers) = committee_and_syncers();
        let mut simulator = Simulator::new(syncers, rng);

        // Kick off process by asking validators create a block after genesis
        for authority in committee.authorities() {
            simulator.schedule_event(
                Duration::ZERO,
                authority as usize,
                SyncerEvent::ForceNewBlock(0),
            );
        }
        // Simulation awaits for first 5 transactions proposed by each authority to certify
        let num_txn = 20;
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
                let mut commits: HashSet<_> = simulator
                    .states()
                    .iter()
                    .map(|state| &state.commit_observer)
                    .collect();
                let zero_commit = vec![];
                let mut max_commit = &zero_commit;
                for commit in commits {
                    if commit.len() >= max_commit.len() {
                        if is_prefix(&max_commit, &commit) {
                            max_commit = commit;
                        } else {
                            panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
                        }
                    } else {
                        if !is_prefix(&commit, &max_commit) {
                            panic!("[!] Commits diverged: {max_commit:?}, {commit:?}");
                        }
                    }
                }
                eprintln!("Max commit sequence: {max_commit:?}");
                break;
            } /*else if iteration % 100 == 0 {
                  eprintln!("Not certified: {not_certified:?}");
              }*/
        }
    }

    fn is_prefix(short: &[BlockReference], long: &[BlockReference]) -> bool {
        assert!(short.len() <= long.len());
        for (a, b) in short.iter().zip(long.iter().take(short.len())) {
            if a != b {
                return false;
            }
        }
        return true;
    }
}
