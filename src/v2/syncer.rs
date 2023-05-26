// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::v2::block_handler::BlockHandler;
use crate::v2::core::Core;
use crate::v2::types::{RoundNumber, StatementBlock};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct Syncer<H: BlockHandler, S: SyncerSignals> {
    core: Core<H>,
    own_blocks: BTreeMap<RoundNumber, Arc<StatementBlock>>,
    force_new_block: bool,
    commit_period: u64,
    signals: S,
}

pub trait SyncerSignals: Send + Sync {
    fn new_block_ready(&mut self);
}

#[allow(dead_code)]
impl<H: BlockHandler, S: SyncerSignals> Syncer<H, S> {
    pub fn new(core: Core<H>, commit_period: u64, signals: S) -> Self {
        Self {
            core,
            own_blocks: Default::default(),
            force_new_block: false,
            commit_period,
            signals,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::block_handler::TestBlockHandler;
    use crate::v2::simulator::{Scheduler, Simulator, SimulatorState};
    use crate::v2::test_util::{committee_and_syncers, first_n_transactions, rng_at_seed};
    use rand::Rng;
    use std::ops::Range;
    use std::time::Duration;

    const ROUND_TIMEOUT: Duration = Duration::from_millis(1000);
    const LATENCY_RANGE: Range<Duration> = Duration::from_millis(100)..Duration::from_millis(1200);

    pub enum SyncerEvent {
        ForceNewBlock(RoundNumber),
        DeliverBlock(Arc<StatementBlock>),
    }

    impl SimulatorState for Syncer<TestBlockHandler, bool> {
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
        let await_transactions = first_n_transactions(&committee, 5);
        assert_eq!(await_transactions.len(), 20); // 4 authorities 5 transactions each

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
                break;
            } /*else if iteration % 100 == 0 {
                  eprintln!("Not certified: {not_certified:?}");
              }*/
        }
    }
}
