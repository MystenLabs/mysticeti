// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::committee::{Authority, Committee};
use crate::consensus::reputation_scores::ReputationScores;
use crate::types::{AuthorityIndex, RoundNumber, Stake};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Arc,
};
use tracing::{debug, trace};
#[derive(Default, Clone)]
pub struct LeaderSwapTable {
    /// The round on which the leader swap table get into effect.
    round: RoundNumber,
    /// The list of `f` (by stake) authorities with best scores as those defined by the provided `ReputationScores`.
    /// Those authorities will be used in the position of the `bad_nodes` on the final leader schedule.
    good_nodes: Vec<(AuthorityIndex, Authority)>,
    /// The set of `f` (by stake) authorities with the worst scores as those defined by the provided `ReputationScores`.
    /// Every time where such authority is elected as leader on the schedule, it will swapped by one
    /// of the authorities of the `good_nodes`.
    bad_nodes: HashMap<AuthorityIndex, Authority>,
}
impl Debug for LeaderSwapTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "LeaderSwapTable round:{}, good_nodes:{:?} with stake:{}, bad_nodes:{:?} with stake:{}",
            self.round,
            self.good_nodes
                .iter()
                .map(|a| a.0)
                .collect::<Vec<AuthorityIndex>>(),
            self.good_nodes.iter().map(|a| a.1.stake()).sum::<Stake>(),
            self.bad_nodes
                .iter()
                .map(|a| *a.0)
                .collect::<Vec<AuthorityIndex>>(),
            self.bad_nodes.iter().map(|a| a.1.stake()).sum::<Stake>(),
        ))
    }
}
impl LeaderSwapTable {
    // constructs a new table based on the provided reputation scores. The `bad_nodes_stake_threshold` designates the
    // total (by stake) nodes that will be considered as "bad" based on their scores and will be replaced by good nodes.
    // The `bad_nodes_stake_threshold` should be in the range of [0 - 33].
    pub fn new(
        committee: Arc<Committee>,
        round: RoundNumber,
        reputation_scores: &ReputationScores,
        bad_nodes_stake_threshold: u64,
    ) -> Self {
        assert!((0..=33).contains(&bad_nodes_stake_threshold), "The bad_nodes_stake_threshold should be in range [0 - 33], out of bounds parameter detected");
        assert!(reputation_scores.final_of_schedule, "Only reputation scores that have been calculated on the end of a schedule are accepted");
        // calculating the good nodes
        let good_nodes = Self::retrieve_first_nodes(
            committee.clone(),
            reputation_scores.authorities_by_score_desc().into_iter(),
            bad_nodes_stake_threshold,
        );
        // calculating the bad nodes
        // we revert the sorted authorities to score ascending so we get the first low scorers
        // up to the dictated stake threshold.
        let bad_nodes = Self::retrieve_first_nodes(
            committee,
            reputation_scores
                .authorities_by_score_desc()
                .into_iter()
                .rev(),
            bad_nodes_stake_threshold,
        )
        .into_iter()
        .map(|(id, authority)| (id, authority))
        .collect::<HashMap<AuthorityIndex, Authority>>();
        good_nodes.iter().for_each(|good_node| {
            debug!(
                "Good node on round {}: {} -> {}",
                round,
                good_node.1.hostname(),
                reputation_scores
                    .scores_per_authority
                    .get(&good_node.0)
                    .unwrap()
            );
        });
        bad_nodes.iter().for_each(|(id, bad_node)| {
            debug!(
                "Bad node on round {}: {} -> {}",
                round,
                bad_node.hostname(),
                reputation_scores.scores_per_authority.get(id).unwrap()
            );
        });
        debug!("Reputation scores on round {round}: {reputation_scores:?}");
        Self {
            round,
            good_nodes,
            bad_nodes,
        }
    }
    /// Checks whether the provided leader is a bad performer and needs to be swapped in the schedule
    /// with a good performer. If not, then the method returns None. Otherwise the leader to swap with
    /// is returned instead. The `leader_round` represents the DAG round on which the provided AuthorityIdentifier
    /// is a leader on and is used as a seed to random function in order to calculate the good node that
    /// will swap in that round with the bad node. We are intentionally not doing weighted randomness as
    /// we want to give to all the good nodes equal opportunity to get swapped with bad nodes and not
    /// have one node with enough stake end up swapping bad nodes more frequently than the others on
    /// the final schedule.
    pub fn swap(
        &self,
        leader: &AuthorityIndex,
        leader_round: RoundNumber,
        offset: u64,
    ) -> Option<AuthorityIndex> {
        if self.bad_nodes.contains_key(leader) {
            let mut seed_bytes = [0u8; 32];
            seed_bytes[32 - 8..].copy_from_slice(&leader_round.to_le_bytes());
            let mut rng = StdRng::from_seed(seed_bytes);
            let good_node = self.good_nodes[offset as usize..]
                .choose(&mut rng)
                .expect("There should be at least one good node available");
            trace!(
                "Swapping bad leader {} -> {} for round {}",
                leader,
                good_node.0,
                leader_round
            );
            return Some(good_node.0);
        }
        None
    }
    // Retrieves the first nodes provided by the iterator `authorities` until the `stake_threshold` has been
    // reached. The `stake_threshold` should be between [0, 100] and expresses the percentage of stake that is
    // considered the cutoff. Basically we keep adding to the response authorities until the sum of the stake
    // reaches the `stake_threshold`. It's the caller's responsibility to ensure that the elements of the `authorities`
    // input is already sorted.
    fn retrieve_first_nodes(
        committee: Arc<Committee>,
        authorities: impl Iterator<Item = (AuthorityIndex, u64)>,
        stake_threshold: u64,
    ) -> Vec<(AuthorityIndex, Authority)> {
        let mut filtered_authorities = Vec::new();
        let mut stake = 0;
        for (authority_id, _score) in authorities {
            stake += committee
                .get_stake(authority_id)
                .expect("authority should be present");
            // if the total accumulated stake has surpassed the stake threshold then we omit this
            // last authority and we exit the loop.
            if stake > (stake_threshold * committee.total_stake()) / 100 as Stake {
                break;
            }
            filtered_authorities.push((
                authority_id,
                committee.authority_safe(authority_id).to_owned(),
            ));
        }
        filtered_authorities
    }
}
/// The LeaderSchedule is responsible for producing the leader schedule across an epoch. It provides
/// methods to derive the leader of a round based on the provided leader swap table. This struct can
/// be cloned and shared freely as the internal parts are atomically updated.
#[derive(Clone)]
pub struct LeaderSchedule {
    pub committee: Arc<Committee>,
    pub leader_swap_table: Arc<RwLock<LeaderSwapTable>>,
}
impl LeaderSchedule {
    pub fn new(committee: Arc<Committee>, table: LeaderSwapTable) -> Self {
        Self {
            committee,
            leader_swap_table: Arc::new(RwLock::new(table)),
        }
    }
    /// Atomically updates the leader swap table with the new provided one. Any leader queried from
    /// now on will get calculated according to this swap table until a new one is provided again.
    pub fn update_leader_swap_table(&self, table: LeaderSwapTable) {
        trace!("Updating swap table {:?}", table);
        let mut write = self.leader_swap_table.write();
        *write = table;
    }
    /// Returns the leader for the provided round. Keep in mind that this method will return a leader
    /// according to the provided LeaderSwapTable. Providing a different table can potentially produce
    /// a different leader for the same round.
    pub fn elect_leader(&self, round: RoundNumber, offset: u64) -> AuthorityIndex {
        cfg_if::cfg_if! {
            if #[cfg(test)] {
                // We apply round robin in leader election. Since we expect round to be an even number,
                // 2, 4, 6, 8... it can't work well for leader election as we'll omit leaders. Thus
                // we can always divide by 2 to get a monotonically incremented sequence,
                // 2/2 = 1, 4/2 = 2, 6/2 = 3, 8/2 = 4  etc, and then do minus 1 so we can always
                // start with base zero 0.
                let next_leader = ((round + offset) % self.committee.len() as u64) as AuthorityIndex;
                let table = self.leader_swap_table.read();
                table.swap(&next_leader, round, offset).unwrap_or(next_leader)
            } else {
                // Elect the leader in a stake-weighted choice seeded by the round
                let leader = self.committee.elect_leader(round, offset);
                let table = self.leader_swap_table.read();
                table.swap(&leader, round, offset).unwrap_or(leader)
            }
        }
    }
    pub fn num_of_bad_nodes(&self) -> usize {
        let read = self.leader_swap_table.read();
        read.bad_nodes.len()
    }
}