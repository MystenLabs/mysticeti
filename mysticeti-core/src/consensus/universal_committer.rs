// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    block_store::BlockStore,
    types::{AuthorityIndex, BlockReference, RoundNumber},
};

use super::{base_committer::BaseCommitter, LeaderStatus};

pub struct UniversalCommitter {
    committers: Vec<BaseCommitter>,
    block_store: BlockStore,
}

impl UniversalCommitter {
    pub fn new(committers: Vec<BaseCommitter>, block_store: BlockStore) -> Self {
        assert!(!committers.is_empty());
        Self {
            committers,
            block_store,
        }
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders.
    pub fn try_commit(&self, last_decided: BlockReference) -> Vec<LeaderStatus> {
        let highest_known_round = self.block_store.highest_round();
        let last_decided_round = last_decided.round();
        let last_decided_round_authority = (last_decided.authority, last_decided_round);

        // Try to decide as many leaders as possible, starting with the highest round.
        let mut leaders = Vec::new();
        for round in (last_decided_round..=highest_known_round).rev() {
            for committer in &self.committers {
                // Skip committers that don't have a leader for this round.
                let Some(leader) = committer.elect_leader(round) else {
                    continue;
                };

                // Try to directly decide the leader.
                let mut status = committer.try_direct_decide(leader, round);

                // If we can't directly decide the leader, try to indirectly decide it.
                if !status.is_decided() {
                    status = committer.try_indirect_decide(leader, round, &mut leaders);
                }

                // Only one committer can try to decide each leader.
                leaders.push(status);
                break;
            }
        }

        // Sort the leaders by round and authority.
        leaders.sort();

        // The decided sequence is the longest prefix of decided leaders.
        leaders
            .into_iter()
            .filter(|x| (x.round(), x.authority()) > last_decided_round_authority)
            .take_while(|x| x.is_decided())
            .collect()
    }

    /// Return list of leaders for the round. Syncer may give those leaders some extra time.
    /// To preserve (theoretical) liveness, we should wait `Delta` time for at least the first leader.
    /// Can return empty vec if round does not have a designated leader.
    pub fn get_leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        let mut leaders: Vec<_> = self
            .committers
            .iter()
            .filter_map(|committer| committer.elect_leader(round))
            .collect();

        leaders.sort();
        leaders
    }
}
