// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, sync::Arc};

use crate::types::AuthorityRound;
use crate::{
    block_store::BlockStore,
    committee::Committee,
    consensus::base_committer::BaseCommitterOptions,
    metrics::Metrics,
    types::{format_authority_round, AuthorityIndex, RoundNumber},
};

use super::{base_committer::BaseCommitter, Decision, LeaderStatus, DEFAULT_WAVE_LENGTH};

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct UniversalCommitter {
    block_store: BlockStore,
    committers: Vec<BaseCommitter>,
    metrics: Arc<Metrics>,
}

impl UniversalCommitter {
    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders.
    #[tracing::instrument(skip_all, fields(last_decided = %last_decided))]
    pub fn try_commit(&self, last_decided: AuthorityRound) -> Vec<LeaderStatus> {
        let highest_known_round = self.block_store.highest_round();

        // Try to decide as many leaders as possible, starting with the highest round.
        let mut leaders = VecDeque::new();
        // try to commit a leader up to the highest_known_round - 2. There is no reason to try and
        // iterate on higher rounds as in order to make a direct decision for a leader at round R we
        // need blocks from round R+2 to figure out that enough certificates and support exist to commit a leader.
        'outer: for round in (last_decided.round()..=highest_known_round.saturating_sub(2)).rev() {
            for committer in self.committers.iter().rev() {
                // Skip committers that don't have a leader for this round.
                let Some(leader) = committer.elect_leader(round) else {
                    continue;
                };

                // now that we reached the last committed leader we can stop the commit rule
                if leader == last_decided {
                    tracing::debug!(
                        "Leader of round {} -> {} - reached last committed, now exit",
                        leader.round,
                        leader.authority
                    );
                    break 'outer;
                }

                tracing::debug!(
                    "Trying to decide {} with {committer}",
                    format_authority_round(leader.authority, leader.round)
                );

                // Try to directly decide the leader.
                let mut status = committer.try_direct_decide(leader);
                tracing::debug!("Outcome of direct rule: {status}");

                // If we can't directly decide the leader, try to indirectly decide it.
                if status.is_decided() {
                    leaders.push_front((status.clone(), Decision::Direct));
                } else {
                    status = committer.try_indirect_decide(leader, leaders.iter().map(|(x, _)| x));
                    leaders.push_front((status.clone(), Decision::Indirect));
                    tracing::debug!("Outcome of indirect rule: {status}");
                }
            }
        }

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
            .collect()
    }

    /// Return list of leaders for the round. Syncer may give those leaders some extra time.
    /// To preserve (theoretical) liveness, we should wait `Delta` time for at least the first leader.
    /// Can return empty vec if round does not have a designated leader.
    pub fn get_leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        self.committers
            .iter()
            .filter_map(|committer| committer.elect_leader(round))
            .map(|l| l.authority)
            .collect()
    }

    /// Update metrics.
    fn update_metrics(&self, leader: &LeaderStatus, decision: Decision) {
        let authority = leader.authority().to_string();
        let direct_or_indirect = if decision == Decision::Direct {
            "direct"
        } else {
            "indirect"
        };
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
                let committer =
                    BaseCommitter::new(self.committee.clone(), self.block_store.clone())
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
