// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, sync::Arc};

use crate::consensus::leader_schedule::{LeaderSchedule, LeaderSwapTable};
use crate::consensus::linearizer::Linearizer;
use crate::types::AuthorityRound;
use crate::{
    block_store::BlockStore,
    committee::Committee,
    consensus::base_committer::BaseCommitterOptions,
    metrics::Metrics,
    types::{format_authority_round, AuthorityIndex, RoundNumber},
};

use super::{base_committer::BaseCommitter, LeaderStatus, DEFAULT_WAVE_LENGTH};

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct UniversalCommitter {
    block_store: BlockStore,
    committers: Vec<BaseCommitter>,
    metrics: Arc<Metrics>,
    /// the latest reputation scores
    leader_schedule: LeaderSchedule,
    committee: Arc<Committee>,
    linearizer: Linearizer,
}

impl UniversalCommitter {
    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders.
    #[tracing::instrument(skip_all, fields(last_decided = %last_decided))]
    pub fn try_commit(&mut self, last_decided: AuthorityRound) -> Vec<LeaderStatus> {
        let highest_known_round = self.block_store.highest_round();
        // let last_decided_round = max(last_decided.round(), 1); // Skip genesis.
        let last_decided_round = last_decided.round();
        let last_decided_round_authority = (last_decided.round(), last_decided.authority);

        // Try to decide as many leaders as possible, starting with the highest round.
        let mut leaders = VecDeque::new();
        'outer: for round in (last_decided_round..=highest_known_round.saturating_sub(2)).rev() {
            for committer in self.committers.iter().rev() {
                // Skip committers that don't have a leader for this round.
                let Some(leader) = committer.elect_leader(round) else {
                    continue;
                };

                // now that we reached the last committed leader we can stop the commit rule
                if (round, leader) == last_decided_round_authority {
                    tracing::debug!(
                        "Leader of round {round} -> {leader} - reached last committed, now exit"
                    );
                    break 'outer;
                }

                tracing::debug!(
                    "Trying to decide {} with {committer}",
                    format_authority_round(leader, round)
                );

                // Try to directly decide the leader.
                let mut status = committer.try_direct_decide(leader, round);
                self.update_metrics(&status, true);
                tracing::debug!("Outcome of direct rule: {status}");

                // If we can't directly decide the leader, try to indirectly decide it.
                if status.is_decided() {
                    leaders.push_front((status.clone(), true));
                } else {
                    status = committer.try_indirect_decide(
                        leader,
                        round,
                        leaders.iter().map(|(x, _)| x),
                    );
                    leaders.push_front((status.clone(), false));
                    tracing::debug!("Outcome of indirect rule: {status}");
                }
            }
        }

        let mut committed_leaders = Vec::new();
        for (leader, direct_decided) in leaders {
            if leader.round() == 0 {
                continue;
            }
            if leader.is_decided() {
                tracing::debug!("Decided {leader}");
                committed_leaders.push(leader.clone());
                self.update_metrics(&leader, direct_decided);

                // we only care here about the actually committed blocks not skipped ones
                if let Some(block) = leader.into_committed_block() {
                    // try to linearise and update scores
                    let sub_dags = self.linearizer.handle_commit(vec![block]);
                    let sub_dag = sub_dags
                        .first()
                        .expect("Should have produced at least one committed sub dag");

                    // update now the leader schedule
                    if sub_dag.reputation_scores.final_of_schedule {
                        let table = LeaderSwapTable::new(
                            self.committee.clone(),
                            sub_dag.anchor.round(),
                            &sub_dag.reputation_scores,
                            20,
                        );
                        self.leader_schedule.update_leader_swap_table(table);

                        // for now just break when the schedule table is updated so the next leaders
                        // will need to get re-calculated on the next commit.
                        break;
                    }
                }
            } else {
                // break when we come across an undecided leader
                break;
            }
        }

        committed_leaders

        /*
        // The decided sequence is the longest prefix of decided leaders.
        leaders
            .into_iter()
            // Filter out all the genesis.
            .filter(|(x, _)| x.round() > 0)
            // Stop the sequence upon encountering an undecided leader.
            .take_while(|(x, _)| x.is_decided())
            // We want to report metrics at this point to ensure that the decisions are reported only once
            // hence we increase our accuracy
            .inspect(|(x, direct_decided)| {
                self.update_metrics(x, *direct_decided);
                tracing::debug!("Decided {x}");
            })
            .map(|(x, _)| x)
            .collect()*/
    }

    /// Return list of leaders for the round. Syncer may give those leaders some extra time.
    /// To preserve (theoretical) liveness, we should wait `Delta` time for at least the first leader.
    /// Can return empty vec if round does not have a designated leader.
    pub fn get_leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        self.committers
            .iter()
            .filter_map(|committer| committer.elect_leader(round))
            .collect()
    }

    /// Update metrics.
    fn update_metrics(&self, leader: &LeaderStatus, direct_decide: bool) {
        let authority = leader.authority().to_string();
        let direct_or_indirect = if direct_decide { "direct" } else { "indirect" };
        let status = match leader {
            LeaderStatus::Commit(..) => format!("{direct_or_indirect}-commit"),
            LeaderStatus::Skip(..) => format!("{direct_or_indirect}-skip"),
            LeaderStatus::Undecided(..) => return,
        };
        self.metrics
            .committed_leaders_total
            .with_label_values(&[&authority, &status])
            .inc();
    }
}

/// A builder for a universal committer. By default, the builder creates a single base committer,
/// that is, a single leader and no pipeline.
pub struct UniversalCommitterBuilder {
    committee: Arc<Committee>,
    block_store: BlockStore,
    metrics: Arc<Metrics>,
    wave_length: RoundNumber,
    number_of_leaders: usize,
    pipeline: bool,
    leader_schedule: LeaderSchedule,
}

impl UniversalCommitterBuilder {
    pub fn new(committee: Arc<Committee>, block_store: BlockStore, metrics: Arc<Metrics>) -> Self {
        Self {
            committee: committee.clone(),
            block_store,
            metrics,
            wave_length: DEFAULT_WAVE_LENGTH,
            number_of_leaders: 1,
            pipeline: false,
            leader_schedule: LeaderSchedule::new(committee, LeaderSwapTable::default()),
        }
    }

    pub fn new_with_schedule(
        committee: Arc<Committee>,
        block_store: BlockStore,
        metrics: Arc<Metrics>,
        leader_schedule: LeaderSchedule,
    ) -> Self {
        Self {
            committee,
            block_store,
            metrics,
            wave_length: DEFAULT_WAVE_LENGTH,
            number_of_leaders: 1,
            pipeline: false,
            leader_schedule,
        }
    }

    pub fn with_wave_length(mut self, wave_length: RoundNumber) -> Self {
        self.wave_length = wave_length;
        self
    }

    pub fn with_number_of_leaders(mut self, number_of_leaders: usize) -> Self {
        self.number_of_leaders = number_of_leaders;
        self
    }

    pub fn with_pipeline(mut self, pipeline: bool) -> Self {
        self.pipeline = pipeline;
        self
    }

    pub fn build(self) -> UniversalCommitter {
        let mut committers = Vec::new();
        let pipeline_stages = if self.pipeline { self.wave_length } else { 1 };
        for round_offset in 0..pipeline_stages {
            for leader_offset in 0..self.number_of_leaders {
                let options = BaseCommitterOptions {
                    wave_length: self.wave_length,
                    round_offset,
                    leader_offset: leader_offset as RoundNumber,
                };
                let committer = BaseCommitter::new_with_schedule(
                    self.committee.clone(),
                    self.block_store.clone(),
                    self.leader_schedule.clone(),
                )
                .with_options(options);
                committers.push(committer);
            }
        }

        let linearizer = Linearizer::new(self.block_store.clone(), self.committee.clone());

        UniversalCommitter {
            block_store: self.block_store,
            committers,
            metrics: self.metrics,
            linearizer,
            leader_schedule: self.leader_schedule.clone(),
            committee: self.committee.clone(),
        }
    }
}
