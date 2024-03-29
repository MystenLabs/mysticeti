// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::commit_observer::CommitObserver;
use crate::core::Core;
use crate::data::Data;
use crate::metrics::UtilizationTimerVecExt;
use crate::runtime::timestamp_utc;
use crate::types::{AuthoritySet, BlockReference, RoundNumber, StatementBlock};
use crate::{block_handler::BlockHandler, metrics::Metrics};
use std::sync::Arc;
use tokio::sync::watch::Sender;
use tokio::sync::Notify;

pub struct Syncer<H: BlockHandler, S: SyncerSignals, C: CommitObserver> {
    core: Core<H>,
    force_new_block: bool,
    commit_period: u64,
    signals: S,
    commit_observer: C,
    metrics: Arc<Metrics>,
}

pub struct Signals {
    pub round_advanced_sender: Sender<RoundNumber>,
    pub block_ready: Arc<Notify>,
}

impl Signals {
    pub fn new(block_ready: Arc<Notify>, round_advanced_sender: Sender<RoundNumber>) -> Self {
        Self {
            round_advanced_sender,
            block_ready,
        }
    }
}

pub trait SyncerSignals: Send + Sync {
    fn new_block_ready(&mut self);

    fn new_round(&mut self, round_number: RoundNumber);
}

impl SyncerSignals for Signals {
    fn new_block_ready(&mut self) {
        self.block_ready.notify_waiters();
    }

    fn new_round(&mut self, round_number: RoundNumber) {
        self.round_advanced_sender.send(round_number).ok();
    }
}

impl<H: BlockHandler, S: SyncerSignals, C: CommitObserver> Syncer<H, S, C> {
    pub fn new(
        core: Core<H>,
        commit_period: u64,
        signals: S,
        commit_observer: C,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            core,
            force_new_block: false,
            commit_period,
            signals,
            commit_observer,
            metrics,
        }
    }

    pub fn add_blocks(
        &mut self,
        blocks: Vec<Data<StatementBlock>>,
        connected_authorities: AuthoritySet,
    ) -> Vec<BlockReference> {
        let _timer = self
            .metrics
            .utilization_timer
            .utilization_timer("Syncer::add_blocks");
        let previous_round = self.core().current_round();
        let missing_references = self.core.add_blocks(blocks);
        let new_round = self.core().current_round();

        // we got a new quorum of blocks. Let leader timeout task know about it.
        if new_round > previous_round {
            self.signals.new_round(new_round);
        }

        self.try_new_block(connected_authorities);

        missing_references
    }

    pub fn force_new_block(
        &mut self,
        round: RoundNumber,
        connected_authorities: AuthoritySet,
    ) -> bool {
        if self.core.last_proposed() < round {
            self.metrics.leader_timeout_total.inc();
            self.force_new_block = true;
            self.try_new_block(connected_authorities);
            true
        } else {
            false
        }
    }

    fn try_new_block(&mut self, connected_authorities: AuthoritySet) {
        let _timer = self
            .metrics
            .utilization_timer
            .utilization_timer("Syncer::try_new_block");
        if self.force_new_block
            || self
                .core
                .ready_new_block(self.commit_period, connected_authorities)
        {
            if self.core.try_new_block().is_none() {
                return;
            }

            if self.force_new_block {
                let leaders = self
                    .core
                    .leaders(self.core.last_proposed().saturating_sub(1));
                tracing::debug!(
                    "Proposed with timeout for round {} missing {:?} ",
                    self.core.last_proposed(),
                    leaders
                );
                self.metrics
                    .ready_new_block
                    .with_label_values(&["leader_timeout"])
                    .inc();
            }

            self.signals.new_block_ready();
            self.force_new_block = false;

            if self.core.epoch_closed() {
                return;
            }; // No need to commit after epoch is safe to close

            let newly_committed = self.core.try_commit();
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

            let committed_subdag = self.commit_observer.handle_commit(newly_committed);
            self.core.handle_committed_subdag(
                committed_subdag,
                &self.commit_observer.aggregator_state(),
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_handler::TestBlockHandler;
    use crate::commit_observer::TestCommitObserver;
    use crate::data::Data;
    use crate::simulator::{Scheduler, Simulator, SimulatorState};
    use crate::test_util::{check_commits, committee_and_syncers, rng_at_seed, SyncerSignalsMock};
    use rand::Rng;
    use std::ops::Range;
    use std::time::Duration;

    const ROUND_TIMEOUT: Duration = Duration::from_millis(1000);
    const LATENCY_RANGE: Range<Duration> = Duration::from_millis(100)..Duration::from_millis(1800);

    pub enum SyncerEvent {
        ForceNewBlock(RoundNumber),
        DeliverBlock(Data<StatementBlock>),
    }

    impl SimulatorState for Syncer<TestBlockHandler, SyncerSignalsMock, TestCommitObserver> {
        type Event = SyncerEvent;

        fn handle_event(&mut self, event: Self::Event) {
            match event {
                SyncerEvent::ForceNewBlock(round) => {
                    if self.force_new_block(round, AuthoritySet::all_set()) {
                        // eprintln!("[{:06} {}] Proposal timeout for {round}", scheduler.time_ms(), self.core.authority());
                    }
                }
                SyncerEvent::DeliverBlock(block) => {
                    // eprintln!("[{:06} {}] Deliver {block}", scheduler.time_ms(), self.core.authority());
                    self.add_blocks(vec![block], AuthoritySet::all_set());
                }
            }

            // New quorum has been received and round has advanced
            if let Some(new_round) = self.signals.new_round {
                self.signals.new_round = None;
                Scheduler::schedule_event(
                    ROUND_TIMEOUT,
                    self.scheduler_state_id(),
                    SyncerEvent::ForceNewBlock(new_round),
                );
            }

            // New block was created
            if self.signals.new_block_ready {
                self.signals.new_block_ready = false;
                let last_block = self.core.last_own_block().clone();
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
                SyncerEvent::ForceNewBlock(1),
            );
        }
        // Simulation awaits for first num_txn transactions proposed by each authority to certify
        let num_txn = 40;
        let mut await_transactions = vec![];
        let await_num_txn = num_txn as usize * committee.len();

        let mut iteration = 0u64;
        loop {
            iteration += 1;
            assert!(!simulator.run_one());
            // todo - we might want to wait for exactly num_txn from each authority, rather then num_txn as usize * committee.len() total
            if await_transactions.len() < await_num_txn {
                for state in simulator.states_mut() {
                    await_transactions.extend(state.core.block_handler_mut().proposed.drain(..))
                }
                continue;
            }
            let not_certified: Vec<_> = simulator
                .states()
                .iter()
                .map(|syncer| {
                    await_transactions
                        .iter()
                        .map(|txid| {
                            if syncer.core.block_handler().is_certified(txid) {
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
