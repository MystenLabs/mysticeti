// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, sync::Arc};

use crate::runtime::timestamp_utc;
use crate::{
    block_store::BlockStore,
    committee::Committee,
    consensus::base_committer::BaseCommitterOptions,
    metrics::Metrics,
    types::{AuthorityIndex, BlockReference, RoundNumber},
};

use super::{base_committer::BaseCommitter, LeaderStatus, DEFAULT_WAVE_LENGTH};

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct UniversalCommitter {
    block_store: BlockStore,
    pub committers: Vec<BaseCommitter>,
    metrics: Arc<Metrics>,
}

impl UniversalCommitter {
    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders.
    #[tracing::instrument(skip_all, fields(last_decided = %last_decided))]
    pub fn try_commit(&self, last_decided: BlockReference) -> Vec<LeaderStatus> {
        let start = timestamp_utc();
        let highest_known_round = self.block_store.highest_round();
        // let last_decided_round = max(last_decided.round(), 1); // Skip genesis.
        let last_decided_round = last_decided.round();
        let last_decided_round_authority = (last_decided.round(), last_decided.authority);

        // Try to decide as many leaders as possible, starting with the highest round.
        let mut leaders = VecDeque::new();
        tracing::debug!(
            "Try commit with range: {} -> {}",
            last_decided_round,
            highest_known_round
        );
        for round in (last_decided_round..=highest_known_round).rev() {
            for committer in self.committers.iter().rev() {
                // Skip committers that don't have a leader for this round.
                let Some(leader) = committer.elect_leader(round) else {
                    continue;
                };

                // Try to directly decide the leader.
                let mut status = committer.try_direct_decide(leader, round);
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

                tracing::debug!(
                    "Leader commit outcome: {}, {}, with committer {}",
                    status.round(),
                    status.to_string(),
                    committer
                );
            }
        }

        tracing::debug!(
            "Try commit with range: {} -> {} - total leaders: {}",
            last_decided_round,
            highest_known_round,
            leaders.len()
        );

        // The decided sequence is the longest prefix of decided leaders.
        let end = timestamp_utc();
        self.metrics
            .code_scope_latency
            .with_label_values(&["UniversalCommitter::try_commit"])
            .observe(end.checked_sub(start).unwrap_or_default().as_secs_f64());
        leaders
            .into_iter()
            // Skip all leaders before the last decided round.
            .skip_while(|(x, _)| (x.round(), x.authority()) != last_decided_round_authority)
            // Skip the last decided leader.
            .skip(1)
            // Filter out all the genesis.
            .filter(|(x, _)| x.round() > 0)
            // Stop the sequence upon encountering an undecided leader.
            .take_while(|(x, _)| x.is_decided())
            .inspect(|(x, direct_decided)| {
                self.update_metrics(x, *direct_decided);
                tracing::debug!("Decided {x}");
            })
            .map(|(x, _)| x)
            .collect()
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
}

impl UniversalCommitterBuilder {
    pub fn new(committee: Arc<Committee>, block_store: BlockStore, metrics: Arc<Metrics>) -> Self {
        Self {
            committee,
            block_store,
            metrics,
            wave_length: DEFAULT_WAVE_LENGTH,
            number_of_leaders: 1,
            pipeline: false,
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
                let committer = BaseCommitter::new(
                    self.committee.clone(),
                    self.block_store.clone(),
                    self.metrics.clone(),
                )
                .with_options(options);
                committers.push(committer);
            }
        }

        UniversalCommitter {
            block_store: self.block_store,
            committers,
            metrics: self.metrics,
        }
    }
}
