// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Display, sync::Arc};

use crate::{block_store::BlockStore, consensus::DEFAULT_WAVE_LENGTH};
use crate::{
    committee::{Committee, QuorumThreshold, StakeAggregator},
    data::Data,
    types::{RoundNumber, StatementBlock},
};
use crate::{
    metrics::Metrics,
    types::{AuthorityIndex, BlockReference},
};

use super::{Committer, LeaderStatus};

/// The consensus protocol operates in 'waves'. Each wave is composed of a leader round, at least one
/// voting round, and one decision round.
type WaveNumber = u64;

pub struct BaseCommitterOptions {
    /// The length of a wave (minimum 3)
    pub wave_length: u64,
    /// The offset used in the leader-election protocol. THis is used by the multi-committer to ensure
    /// that each [`BaseCommitter`] instance elects a different leader.
    pub leader_offset: u64,
    /// The offset of the first wave. This is used by the pipelined committer to ensure that each
    /// [`BaseCommitter`] instances operates on a different view of the dag.
    pub round_offset: u64,
}

impl Default for BaseCommitterOptions {
    fn default() -> Self {
        Self {
            wave_length: DEFAULT_WAVE_LENGTH,
            leader_offset: 0,
            round_offset: 0,
        }
    }
}

/// The [`BaseCommitter`] contains the bare bone commit logic. Once instantiated, the method `try_commit`
/// can be called at any time and any number of times (it is idempotent) to return extensions to the commit
/// sequence.
pub struct BaseCommitter {
    /// The committee information
    committee: Arc<Committee>,
    /// Keep all block data
    block_store: BlockStore,
    /// The options used by this committer
    options: BaseCommitterOptions,
    /// Metrics information
    metrics: Arc<Metrics>,
}

impl BaseCommitter {
    /// We need at least one leader round, one voting round, and one decision round.
    pub const MINIMUM_WAVE_LENGTH: u64 = 3;

    pub fn new(committee: Arc<Committee>, block_store: BlockStore, metrics: Arc<Metrics>) -> Self {
        Self {
            committee,
            block_store,
            options: BaseCommitterOptions::default(),
            metrics,
        }
    }

    pub fn with_options(mut self, options: BaseCommitterOptions) -> Self {
        assert!(options.wave_length >= Self::MINIMUM_WAVE_LENGTH);
        self.options = options;
        self
    }

    pub fn leader_offset(&self) -> u64 {
        self.options.leader_offset
    }

    /// The leader-elect protocol is offset by `leader_offset` to ensure that different committers
    /// with different leader offsets elect different leaders for the same round number.
    pub fn elect_leader(&self, round: RoundNumber) -> AuthorityIndex {
        let offset = self.options.leader_offset as RoundNumber;
        self.committee.elect_leader(round + offset)
    }

    /// Return the wave in which the specified round belongs.
    fn wave_number(&self, round: RoundNumber) -> WaveNumber {
        round.saturating_sub(self.options.round_offset) / self.options.wave_length
    }

    /// Return the highest wave number that can (potentially) be committed given the highest
    /// known round number.
    fn hightest_committable_wave(&self, highest_round: RoundNumber) -> WaveNumber {
        let wave = self.wave_number(highest_round);
        if highest_round == self.decision_round(wave) {
            wave
        } else {
            wave.saturating_sub(1)
        }
    }

    /// Return the leader round of the specified wave number. The leader round is always the first
    /// round of the wave.
    fn leader_round(&self, wave: WaveNumber) -> RoundNumber {
        wave * self.options.wave_length + self.options.round_offset
    }

    /// Return the decision round of the specified wave. The decision round is always the last
    /// round of the wave.
    fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        let wave_length = self.options.wave_length;
        wave * wave_length + wave_length - 1 + self.options.round_offset
    }

    /// Find which block is supported at (author, round) by the given block.
    /// Blocks can indirectly reference multiple other blocks at (author, round), but only one block at
    /// (author, round)  will be supported by the given block. If block A supports B at (author, round),
    /// it is guaranteed that any processed block by the same author that directly or indirectly includes
    /// A will also support B at (author, round).
    fn find_support(
        &self,
        (author, round): (AuthorityIndex, RoundNumber),
        from: &Data<StatementBlock>,
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
                .block_store
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
    fn is_vote(
        &self,
        potential_vote: &Data<StatementBlock>,
        leader_block: &Data<StatementBlock>,
    ) -> bool {
        let (author, round) = leader_block.author_round();
        self.find_support((author, round), potential_vote) == Some(*leader_block.reference())
    }

    /// Check whether the specified block (`potential_certificate`) is a certificate for
    /// the specified leader (`leader_block`).
    fn is_certificate(
        &self,
        potential_certificate: &Data<StatementBlock>,
        leader_block: &Data<StatementBlock>,
    ) -> bool {
        let mut votes_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for reference in potential_certificate.includes() {
            let potential_vote = self
                .block_store
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

    /// Check whether the specified leader has enough blames (that is, 2f+1 non-votes) to be
    /// directly skipped.
    fn enough_leader_blame(&self, voting_round: RoundNumber, leader: AuthorityIndex) -> bool {
        let voting_blocks = self.block_store.get_blocks_by_round(voting_round);

        let mut blame_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for voting_block in &voting_blocks {
            let voter = voting_block.reference().authority;
            if voting_block
                .includes()
                .iter()
                .all(|include| include.authority != leader)
            {
                tracing::trace!(
                    "[{self}] {voting_block:?} is a blame for leader v{leader:?} of round {}",
                    voting_round - 1
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
        leader_block: &Data<StatementBlock>,
    ) -> bool {
        let decision_blocks = self.block_store.get_blocks_by_round(decision_round);

        let mut certificate_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for decision_block in &decision_blocks {
            let authority = decision_block.reference().authority;
            if self.is_certificate(decision_block, leader_block) {
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

    /// Apply the direct commit rule to the specified leader to see whether we can direct-commit or
    /// direct-skip it.
    fn try_direct_commit(
        &self,
        leader: AuthorityIndex,
        leader_round: RoundNumber,
        decision_round: RoundNumber,
    ) -> LeaderStatus {
        // Check whether the leader has enough blame. That is, whether there are 2f+1 non-votes
        // for that leader (which ensure there will never be a certificate for that leader).
        let voting_round = leader_round + 1;
        if self.enough_leader_blame(voting_round, leader) {
            return LeaderStatus::Skip(leader, leader_round);
        }

        // Check whether the leader(s) has enough support. That is, whether there are 2f+1
        // certificates over the leader. Note that there could be more than one leader block
        // (created by Byzantine leaders).
        let leader_blocks = self
            .block_store
            .get_blocks_at_authority_round(leader, leader_round);
        let mut leaders_with_enough_support: Vec<_> = leader_blocks
            .into_iter()
            .filter(|l| self.enough_leader_support(decision_round, l))
            .map(LeaderStatus::Commit)
            .collect();

        // There can be at most one leader with enough support for each round, otherwise it means
        // the BFT assumption is broken.
        if leaders_with_enough_support.len() > 1 {
            panic!("More than one certified block at round {leader_round} from leader {leader}")
        }

        leaders_with_enough_support
            .pop()
            .unwrap_or_else(|| LeaderStatus::Undecided(leader, leader_round))
    }

    pub fn try_direct_decide(&self, round: RoundNumber) -> Option<LeaderStatus> {
        let wave = self.wave_number(round);
        if self.leader_round(wave) != round {
            return None;
        }
        let leader = self.elect_leader(round);
        let decision_round = self.decision_round(wave);
        Some(self.try_direct_commit(leader, round, decision_round))
    }

    pub fn try_indirect_decide(
        &self,
        target: LeaderStatus,
        leaders: &[LeaderStatus],
    ) -> LeaderStatus {
        let anchors = leaders
            .iter()
            .filter(|x| target.round() + self.options.wave_length <= x.round());

        for anchor in anchors {
            match anchor {
                LeaderStatus::Commit(anchor) => {
                    return self.decide_past_leader(anchor, target.round(), target.authority());
                }
                LeaderStatus::Skip(..) => (),
                LeaderStatus::Undecided(..) => break,
            }
        }

        return target;
    }

    fn decide_past_leader(
        &self,
        anchor: &Data<StatementBlock>,
        target_round: RoundNumber,
        target_authority: AuthorityIndex,
    ) -> LeaderStatus {
        let wave = self.wave_number(anchor.round());

        todo!()
    }

    /// Output an ordered list of leader blocks (that we should commit in order). This function is
    /// used by the indirect commit rule to determine which past leader blocks can be committed.
    pub fn decide_past_leaders(
        &self,
        last_committed_wave: WaveNumber,
        anchor: Data<StatementBlock>,
    ) -> Vec<LeaderStatus> {
        let mut to_commit = Vec::new();
        let latest_wave = self.wave_number(anchor.round());
        let mut current_leader_block = anchor;

        let earliest_wave = last_committed_wave + 1;
        for w in (earliest_wave..latest_wave).rev() {
            // Get the block(s) proposed by the previous leader. There could be more than one
            // leader block per round (produced by a Byzantine leader).
            let leader_round = self.leader_round(w);
            let leader = self.elect_leader(leader_round);
            let leader_blocks = self
                .block_store
                .get_blocks_at_authority_round(leader, leader_round);

            // Get all blocks that could be potential certificates for these leader blocks, if
            // they are ancestors of the current leader.
            let decision_round = self.decision_round(w);
            let decision_blocks = self.block_store.get_blocks_by_round(decision_round);
            let potential_certificates: Vec<_> = decision_blocks
                .iter()
                .filter(|block| self.block_store.linked(&current_leader_block, block))
                .collect();

            // Use those potential certificates to determine which (if any) of the previous leader
            // blocks can be committed.
            let mut certified_leader_blocks: Vec<_> = leader_blocks
                .into_iter()
                .filter(|leader_block| {
                    potential_certificates.iter().any(|potential_certificate| {
                        self.is_certificate(potential_certificate, leader_block)
                    })
                })
                .collect();

            // There can be at most one certified leader, otherwise it means the BFT assumption
            // is broken.
            if certified_leader_blocks.len() > 1 {
                panic!("More than one certified block at wave {w} from leader {leader}")
            }

            // We skip the previous leader if it has no certificates that are ancestors of current
            // leader. Otherwise we proceed to the next iteration using the previous leader as anchor.
            match certified_leader_blocks.pop() {
                Some(certified_leader_block) => {
                    to_commit.push(LeaderStatus::Commit(certified_leader_block.clone()));
                    current_leader_block = certified_leader_block;
                }
                None => {
                    to_commit.push(LeaderStatus::Skip(leader, leader_round));
                }
            }
        }
        to_commit
    }

    /// Apply the indirect commit rule. That is, we try to commit any eligible past leader (recursively)
    /// that we did not already commit.
    fn try_indirect_commit(
        &self,
        last_committed_wave: WaveNumber,
        leader_block: Data<StatementBlock>,
    ) -> Vec<LeaderStatus> {
        let sequence: Vec<_> = self
            .decide_past_leaders(last_committed_wave, leader_block)
            .into_iter()
            .rev()
            .collect();

        self.update_metrics(&sequence, "indirect");

        sequence
    }

    /// Update metrics.
    fn update_metrics(&self, sequence: &[LeaderStatus], direct_or_indirect: &str) {
        for leader in sequence {
            let status = match leader {
                LeaderStatus::Commit(_) => format!("{direct_or_indirect}-commit"),
                LeaderStatus::Skip(_, _) => format!("{direct_or_indirect}-skip"),
                LeaderStatus::Undecided(_, _) => continue,
            };
            let authority = leader.authority().to_string();
            self.metrics
                .committed_leaders_total
                .with_label_values(&[&authority, &status])
                .inc();
        }
    }
}

impl Committer for BaseCommitter {
    type LastCommitted = RoundNumber;

    fn try_commit(
        &self,
        last_committed_round: Self::LastCommitted,
    ) -> (Vec<LeaderStatus>, Self::LastCommitted) {
        let mut last_committed_wave = self.wave_number(last_committed_round);
        let highest_round = self.block_store.highest_round();
        let highest_wave = self.hightest_committable_wave(highest_round);

        // Undecided leaders may be overwritten by decided leaders in the next wave.
        let mut sequence = BTreeMap::new();
        for wave in (last_committed_wave + 1)..=highest_wave {
            let leader_round = self.leader_round(wave);
            let decision_round = self.decision_round(wave);

            tracing::debug!(
                "[{self}] Trying to commit ( \
                    wave: {wave}, \
                    leader_round: {leader_round}, \
                    last committed wave: {last_committed_wave} \
                )"
            );

            // Check whether the leader(s) has enough support to be skipped or committed.
            let leader = self.elect_leader(leader_round);
            let anchor = self.try_direct_commit(leader, leader_round, decision_round);
            match anchor {
                // We can direct-commit this leader. We first check the indirect commit rule
                // to see if we can commit any past leader.
                LeaderStatus::Commit(ref block) => {
                    tracing::debug!("[{self}] Leader {block} is direct-committed");
                    let commits = self.try_indirect_commit(last_committed_wave, block.clone());
                    sequence.extend(commits.into_iter().map(|x| (x.round(), x)));
                }
                // We can safely direct-skip this leader.
                LeaderStatus::Skip(leader, round) => {
                    tracing::debug!("[{self}] Leader v{leader} at round {round} is direct-skipped");
                }
                // We cannot commit this leader yet. We will try again in the next round.
                LeaderStatus::Undecided(leader, round) => {
                    tracing::debug!("[{self}] Leader v{leader} at round {round} is undecided");
                }
            }

            if anchor.is_decided() {
                self.update_metrics(&[anchor.clone()], "direct");
                last_committed_wave = wave;
            }
            sequence.insert(anchor.round(), anchor);
        }

        (
            sequence.into_values().collect(),
            self.leader_round(last_committed_wave),
        )
    }

    fn leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        let wave = self.wave_number(round);
        let leader_round = self.leader_round(wave);
        if round != leader_round {
            return vec![];
        }
        vec![self.elect_leader(round)]
    }
}

impl Display for BaseCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BaseCommitter(L{},R{})",
            self.options.leader_offset, self.options.round_offset
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::test_util::{build_dag, build_dag_layer, TestBlockWriter};
    use crate::test_util::{committee, test_metrics};

    /// Commit one leader.
    #[test]
    #[tracing_test::traced_test]
    fn direct_commit() {
        let committee = committee(4);

        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        if let LeaderStatus::Commit(ref block) = sequence[0] {
            assert_eq!(block.author(), committee.elect_leader(DEFAULT_WAVE_LENGTH))
        } else {
            panic!("Expected a committed leader")
        };
    }

    /// Ensure idempotent replies.
    #[test]
    #[tracing_test::traced_test]
    fn idempotence() {
        let committee = committee(4);

        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = DEFAULT_WAVE_LENGTH;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }

    /// Commit one by one each leader as the dag progresses in ideal conditions.
    #[test]
    #[tracing_test::traced_test]
    fn multiple_direct_commit() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut last_committed_round = 0;
        for n in 1..=10 {
            let enough_blocks = wave_length * (n + 1) - 1;
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, enough_blocks);

            let committer = BaseCommitter::new(
                committee.clone(),
                block_writer.into_block_store(),
                test_metrics(),
            );

            let sequence = committer.try_commit(last_committed_round);

            assert_eq!(sequence.len(), 1);
            tracing::info!("Commit sequence: {sequence:?}");

            let leader_round = n as u64 * wave_length;
            if let LeaderStatus::Commit(ref block) = sequence[0] {
                assert_eq!(block.author(), committee.elect_leader(leader_round));
            } else {
                panic!("Expected a committed leader")
            }

            last_committed_round = leader_round;
        }
    }

    /// Commit 10 leaders in a row (9 of them are committed recursively).
    #[test]
    #[tracing_test::traced_test]
    fn indirect_commit() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let n = 10;
        let enough_blocks = wave_length * (n + 1) - 1;
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, enough_blocks);

        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), n as usize);
        for (i, leader_block) in sequence.iter().enumerate() {
            let leader_round = (i as u64 + 1) * wave_length;
            if let LeaderStatus::Commit(ref block) = leader_block {
                assert_eq!(block.author(), committee.elect_leader(leader_round));
            } else {
                panic!("Expected a committed leader")
            };
        }
    }

    /// Do not commit anything if we are still in the first wave.
    #[test]
    #[tracing_test::traced_test]
    fn no_genesis_commit() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let first_commit_round = 2 * wave_length - 1;
        for r in 0..first_commit_round {
            let mut block_writer = TestBlockWriter::new(&committee);
            build_dag(&committee, &mut block_writer, None, r);

            let committer = BaseCommitter::new(
                committee.clone(),
                block_writer.into_block_store(),
                test_metrics(),
            );

            let last_committed_round = 0;
            let sequence = committer.try_commit(last_committed_round);
            tracing::info!("Commit sequence: {sequence:?}");
            assert!(sequence.is_empty());
        }
    }

    /// We directly skip the leader if it is missing.
    #[test]
    #[tracing_test::traced_test]
    fn no_leader() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to finish wave 0.
        let decision_round_0 = wave_length - 1;
        let references = build_dag(&committee, &mut block_writer, None, decision_round_0);

        // Add enough blocks to reach the decision round of wave 1 (but without its leader).
        let leader_round_1 = wave_length;
        let leader_1 = committee.elect_leader(leader_round_1);

        let connections = committee
            .authorities()
            .filter(|&authority| authority != leader_1)
            .map(|authority| (authority, references.clone()));
        let references = build_dag_layer(connections.collect(), &mut block_writer);

        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            decision_round_1,
        );

        // Ensure no blocks are committed.
        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        if let LeaderStatus::Skip(leader, round) = sequence[0] {
            assert_eq!(leader, leader_1);
            assert_eq!(round, leader_round_1);
        } else {
            panic!("Expected to directly skip the leader");
        }
    }

    /// We directly skip the leader if it has enough blame.
    #[test]
    #[tracing_test::traced_test]
    fn direct_skip() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 1.
        let leader_round_1 = wave_length;
        let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

        // Filter out that leader.
        let references_1_without_leader: Vec<_> = references_1
            .into_iter()
            .filter(|x| x.authority != committee.elect_leader(leader_round_1))
            .collect();

        // Add enough blocks to reach the decision round of wave 1.
        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references_1_without_leader),
            decision_round_1,
        );

        // Ensure the leader is skipped.
        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        if let LeaderStatus::Skip(leader, round) = sequence[0] {
            assert_eq!(leader, committee.elect_leader(leader_round_1));
            assert_eq!(round, leader_round_1);
        } else {
            panic!("Expected to directly skip the leader");
        }
    }

    /// Commit the leaders of wave 1 and 3 while the leader of wave 2 is missing.
    #[test]
    #[tracing_test::traced_test]
    fn indirect_skip() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 2.
        let leader_round_2 = 2 * wave_length;
        let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

        // Filter out that leader.
        let leader_2 = committee.elect_leader(leader_round_2);
        let references_2_without_leader: Vec<_> = references_2
            .iter()
            .cloned()
            .filter(|x| x.authority != leader_2)
            .collect();

        // Create a dag layer where only one authority votes for the leader of wave 2.
        let mut authorities = committee.authorities();
        let leader_connection = vec![(authorities.next().unwrap(), references_2)];
        let non_leader_connections: Vec<_> = authorities
            .take((committee.quorum_threshold() - 1) as usize)
            .map(|authority| (authority, references_2_without_leader.clone()))
            .collect();

        let connections = leader_connection.into_iter().chain(non_leader_connections);
        let references = build_dag_layer(connections.collect(), &mut block_writer);

        // Add enough blocks to reach the decision round of wave 3.
        let decision_round_3 = 4 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            decision_round_3,
        );

        // Ensure we commit the leaders of wave 1 and 3
        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), 3);

        // Ensure we commit the leader of wave 1.
        let leader_round_1 = wave_length;
        let leader_1 = committee.elect_leader(leader_round_1);
        if let LeaderStatus::Commit(ref block) = sequence[0] {
            assert_eq!(block.author(), leader_1);
        } else {
            panic!("Expected a committed leader")
        };

        // Ensure we skip the leader of wave 2.
        let leader_round_2 = 2 * wave_length;
        if let LeaderStatus::Skip(leader, round) = sequence[1] {
            assert_eq!(leader, leader_2);
            assert_eq!(round, leader_round_2);
        } else {
            panic!("Expected a skipped leader")
        }

        // Ensure we commit the leader of wave 3.
        let leader_round_3 = 3 * wave_length;
        let leader_3 = committee.elect_leader(leader_round_3);
        if let LeaderStatus::Commit(ref block) = sequence[2] {
            assert_eq!(block.author(), leader_3);
        } else {
            panic!("Expected a committed leader")
        }
    }

    /// If there is no leader with enough support nor blame, we commit nothing.
    #[test]
    #[tracing_test::traced_test]
    fn undecided() {
        let committee = committee(4);
        let wave_length = DEFAULT_WAVE_LENGTH;

        let mut block_writer = TestBlockWriter::new(&committee);

        // Add enough blocks to reach the leader of wave 1.
        let leader_round_1 = wave_length;
        let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

        // Filter out that leader.
        let references_1_without_leader: Vec<_> = references_1
            .iter()
            .cloned()
            .filter(|x| x.authority != committee.elect_leader(leader_round_1))
            .collect();

        // Create a dag layer where only one authority votes for the leader of wave 1.
        let mut authorities = committee.authorities();
        let leader_connection = vec![(authorities.next().unwrap(), references_1)];
        let non_leader_connections: Vec<_> = authorities
            .take((committee.quorum_threshold() - 1) as usize)
            .map(|authority| (authority, references_1_without_leader.clone()))
            .collect();

        let connections = leader_connection.into_iter().chain(non_leader_connections);
        let references = build_dag_layer(connections.collect(), &mut block_writer);

        // Add enough blocks to reach the decision round of wave 1.
        let decision_round_1 = 2 * wave_length - 1;
        build_dag(
            &committee,
            &mut block_writer,
            Some(references),
            decision_round_1,
        );

        // Ensure no blocks are committed.
        let committer = BaseCommitter::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        );

        let last_committed_round = 0;
        let sequence = committer.try_commit(last_committed_round);
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }
}
