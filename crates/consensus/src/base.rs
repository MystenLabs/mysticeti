// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{self, Display},
    sync::Arc,
};

use crate::{leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::Stake,
    committee::{Committee, StakeAggregator},
    consensus::LeaderStatus,
    data::Data,
    storage::BlockReader,
};

/// The consensus protocol operates in 'waves'. Each wave is composed of a leader round, at least
///  one voting round, and one decision round.
type WaveNumber = u64;

/// The [`BaseCommitter`] contains the bare bone commit
/// logic. Once instantiated, the methods
/// `try_direct_decide` and `try_indirect_decide` can be
/// called at any time and any number of times
/// (idempotent) to determine whether a leader can be
/// committed or skipped.
pub(crate) struct BaseCommitter {
    committee: Arc<Committee>,
    block_reader: BlockReader,
    leader_elector: LeaderElector,
    strong_quorum: Stake,
    weak_quorum: Stake,
    wave_length: RoundNumber,
    leader_offset: RoundNumber,
    round_offset: RoundNumber,
}

impl BaseCommitter {
    pub(crate) fn new(
        committee: Arc<Committee>,
        block_reader: BlockReader,
        leader_elector: LeaderElector,
        protocol: &Protocol,
        leader_offset: RoundNumber,
        round_offset: RoundNumber,
    ) -> Self {
        Self {
            committee,
            block_reader,
            leader_elector,
            strong_quorum: protocol.strong_quorum,
            weak_quorum: protocol.weak_quorum,
            wave_length: protocol.wave_length,
            leader_offset,
            round_offset,
        }
    }

    /// Return the wave in which the specified round belongs.
    #[inline]
    fn wave_number(&self, round: RoundNumber) -> WaveNumber {
        round.saturating_sub(self.round_offset) / self.wave_length
    }

    /// Return the leader round of the specified wave number.
    #[inline]
    fn leader_round(&self, wave: WaveNumber) -> RoundNumber {
        wave * self.wave_length + self.round_offset
    }

    /// Return the voting round of the specified wave.
    #[inline]
    fn voting_round(&self, wave: WaveNumber) -> RoundNumber {
        let leader_round = self.leader_round(wave);
        let decision_round = self.decision_round(wave);
        (leader_round + 1).max(decision_round - 1)
    }

    /// Return the decision round of the specified wave.
    #[inline]
    fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        let wave_length = self.wave_length;
        wave * wave_length + wave_length - 1 + self.round_offset
    }

    /// The leader-elect protocol is offset by `leader_offset` to ensure that different
    /// committers with different leader offsets elect different leaders for the same round number.
    /// This function returns `None` if there are no leaders for the specified round.
    pub(crate) fn elect_leader(&self, round: RoundNumber) -> Option<Authority> {
        let wave = self.wave_number(round);
        if self.leader_round(wave) != round {
            return None;
        }

        let offset = self.leader_offset as RoundNumber;
        Some(self.leader_elector.elect_leader(round + offset))
    }

    /// Find which block is supported at (author, round) by the given block.
    /// Blocks can indirectly reference multiple other blocks at (author, round), but only one
    /// block at (author, round)  will be supported by the given block. If block A supports B at
    /// (author, round), it is guaranteed that any processed block by the same author that directly
    /// or indirectly includes. A will also support B at (author, round).
    fn find_support(
        &self,
        (author, round): (Authority, RoundNumber),
        from: &Data<Block>,
    ) -> Option<BlockReference> {
        if from.round() < round {
            return None;
        }
        for include in from.includes() {
            // Weak links may point to blocks with lower round numbers than strong links.
            if include.round() < round {
                continue;
            }
            if include.author_round() == (author, round) {
                return Some(*include);
            }
            let include = self
                .block_reader
                .get_block(*include)
                .expect("We should have the whole sub-dag by now");
            if let Some(support) = self.find_support((author, round), &include) {
                return Some(support);
            }
        }
        None
    }

    /// Check whether the specified block (`potential_certificate`) is a vote for
    /// the specified leader (`leader_block`).
    fn is_vote(&self, potential_vote: &Data<Block>, leader_block: &Data<Block>) -> bool {
        let (author, round) = leader_block.author_round();
        self.find_support((author, round), potential_vote) == Some(*leader_block.reference())
    }

    /// Check whether the specified block (`potential_certificate`) is a certificate for
    /// the specified leader (`leader_block`).
    fn is_certificate(
        &self,
        potential_certificate: &Data<Block>,
        leader_block: &Data<Block>,
        quorum: Stake,
    ) -> bool {
        let mut votes_stake_aggregator = StakeAggregator::new(quorum);
        for reference in potential_certificate.includes() {
            let potential_vote = self
                .block_reader
                .get_block(*reference)
                .expect("We should have the whole sub-dag by now");

            if self.is_vote(&potential_vote, leader_block) {
                tracing::trace!("[{self}] {potential_vote:?} is a vote for {leader_block:?}");
                if votes_stake_aggregator.add(reference.authority, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Decide the status of a target leader from the specified anchor. We commit the target leader
    /// if it has a certified link to the anchor. Otherwise, we skip the target leader.
    fn decide_leader_from_anchor(
        &self,
        anchor: &Data<Block>,
        leader: Authority,
        leader_round: RoundNumber,
    ) -> LeaderStatus {
        // Get the block(s) proposed by the leader. There could be more than one leader block
        // per round (produced by a Byzantine leader).
        let leader_blocks = self
            .block_reader
            .get_blocks_at_authority_round(leader, leader_round);

        // Get all blocks that could be potential certificates for the target leader. These blocks
        // are in the decision round of the target leader and are linked to the anchor.
        let wave = self.wave_number(leader_round);
        let decision_round = self.decision_round(wave);
        let decision_blocks = self.block_reader.get_blocks_by_round(decision_round);

        // Find the certified leader block (at most one).
        let mut certified = leader_blocks.into_iter().filter(|leader_block| {
            decision_blocks
                .iter()
                .filter(|block| self.block_reader.linked(anchor, block))
                .any(|potential_certificate| {
                    self.is_certificate(potential_certificate, leader_block, self.weak_quorum)
                })
        });
        let first = certified.next();
        if certified.next().is_some() {
            panic!("More than one certified block at wave {wave} from leader {leader}")
        }

        match first {
            Some(block) => LeaderStatus::IndirectCommit(block.clone()),
            None => LeaderStatus::IndirectSkip(leader, leader_round),
        }
    }

    /// Check whether the specified leader has enough blames (that is, 2f+1 non-votes) to be
    /// directly skipped.
    fn enough_leader_blame(
        &self,
        voting_round: RoundNumber,
        leader: Authority,
        leader_round: RoundNumber,
    ) -> bool {
        let voting_blocks = self.block_reader.get_blocks_by_round(voting_round);

        let mut blame_stake_aggregator = StakeAggregator::new(self.strong_quorum);
        for voting_block in &voting_blocks {
            let voter = voting_block.reference().authority;
            if self
                .find_support((leader, leader_round), voting_block)
                .is_none()
            {
                tracing::trace!(
                    "[{self}] {voting_block:?} is a blame for leader {}",
                    leader.with_round(leader_round)
                );
                if blame_stake_aggregator.add(voter, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Check whether the specified leader has enough support (that is, 2f+1 certificates)
    /// to be directly committed.
    fn enough_leader_support(
        &self,
        decision_round: RoundNumber,
        leader_block: &Data<Block>,
    ) -> bool {
        let decision_blocks = self.block_reader.get_blocks_by_round(decision_round);

        let mut certificate_stake_aggregator = StakeAggregator::new(self.strong_quorum);
        for decision_block in &decision_blocks {
            let authority = decision_block.reference().authority;
            if self.is_certificate(decision_block, leader_block, self.strong_quorum) {
                tracing::trace!(
                    "[{self}] {decision_block:?} is a certificate for leader {leader_block:?}"
                );
                if certificate_stake_aggregator.add(authority, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Apply the indirect decision rule to the specified leader to see whether we can
    /// indirect-commit or indirect-skip it.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(leader = %leader.with_round(leader_round))
    )]
    pub(crate) fn try_indirect_decide<'a>(
        &self,
        leader: Authority,
        leader_round: RoundNumber,
        leaders: impl Iterator<Item = &'a LeaderStatus>,
    ) -> LeaderStatus {
        // The anchor is the first committed leader with round higher than the decision round of the
        // target leader. We must stop the iteration upon encountering an undecided leader.
        let anchors = leaders.filter(|x| leader_round + self.wave_length <= x.round());

        for anchor in anchors {
            tracing::trace!(
                "[{self}] Trying to indirect-decide {} using anchor {anchor}",
                leader.with_round(leader_round),
            );
            match anchor {
                LeaderStatus::DirectCommit(anchor) | LeaderStatus::IndirectCommit(anchor) => {
                    return self.decide_leader_from_anchor(anchor, leader, leader_round);
                }
                LeaderStatus::DirectSkip(..) | LeaderStatus::IndirectSkip(..) => (),
                LeaderStatus::Undecided(..) => break,
            }
        }

        LeaderStatus::Undecided(leader, leader_round)
    }

    /// Apply the direct decision rule to the specified leader to see whether we can direct-commit
    /// or direct-skip it.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(leader = %leader.with_round(leader_round))
    )]
    pub(crate) fn try_direct_decide(
        &self,
        leader: Authority,
        leader_round: RoundNumber,
    ) -> LeaderStatus {
        let wave = self.wave_number(leader_round);
        let voting_round = self.voting_round(wave);
        let decision_round = self.decision_round(wave);

        // Check whether the leader has enough blame. That is, whether there are 2f+1 non-votes
        // for that leader (which ensure there will never be a certificate for that leader).
        if self.enough_leader_blame(voting_round, leader, leader_round) {
            return LeaderStatus::DirectSkip(leader, leader_round);
        }

        // Check whether the leader(s) has enough support. That is, whether there are 2f+1
        // certificates over the leader. Note that there could be more than one leader block
        // (created by Byzantine leaders).
        let leader_blocks = self
            .block_reader
            .get_blocks_at_authority_round(leader, leader_round);
        let mut supported = leader_blocks
            .into_iter()
            .filter(|l| self.enough_leader_support(decision_round, l));
        let first = supported.next();
        if supported.next().is_some() {
            panic!(
                "[{self}] More than one certified block for {}",
                leader.with_round(leader_round)
            )
        }

        first
            .map(LeaderStatus::DirectCommit)
            .unwrap_or_else(|| LeaderStatus::Undecided(leader, leader_round))
    }
}

impl Display for BaseCommitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Committer-L{}-R{}",
            self.leader_offset, self.round_offset
        )
    }
}
