// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use crate::{committer::Committer, leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::BlockReference,
    consensus::LeaderStatus,
    metrics::Metrics,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee},
};

/// Commit the leaders of the first wave.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let wave_length = 3;
    for leader_count in 1..committee.len() {
        let (mut storage, _) =
            Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);
        build_dag(&committee, &mut storage, None, 5);

        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            Protocol {
                strong_quorum: 2 * committee.total_stake() / 3 + 1,
                weak_quorum: 2 * committee.total_stake() / 3 + 1,
                wave_length: 3,
                leader_count: NonZeroUsize::new(leader_count).unwrap(),
                pipeline: false,
                leader_wait: false,
                require_crypto: false,
            },
        );

        let last_committed = BlockReference::new_test(0, 0);
        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), leader_count);
        for (i, leader) in sequence.iter().enumerate() {
            if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = leader
            {
                let leader_round = wave_length;
                let leader_offset = i as u64;
                let expected = leader_elector.elect_leader(leader_round + leader_offset);
                assert_eq!(block.author(), expected);
            } else {
                panic!("Expected a committed leader")
            };
        }
    }
}

/// Ensure idempotent replies.
#[test]
#[tracing_test::traced_test]
fn idempotence() {
    let committee = committee(4);
    for leader_count in 1..committee.len() {
        let (mut storage, _) =
            Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);
        build_dag(&committee, &mut storage, None, 5);

        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            Protocol {
                strong_quorum: 2 * committee.total_stake() / 3 + 1,
                weak_quorum: 2 * committee.total_stake() / 3 + 1,
                wave_length: 3,
                leader_count: NonZeroUsize::new(leader_count).unwrap(),
                pipeline: false,
                leader_wait: false,
                require_crypto: false,
            },
        );

        // Commit one block.
        let last_committed = BlockReference::new_test(0, 0);
        let committed = committer.try_commit(last_committed).collect::<Vec<_>>();

        // Ensure we don't commit it again.
        let last = committed.into_iter().last().unwrap();
        let last_committed = BlockReference::new_test(last.authority().as_u64(), last.round());
        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }
}

/// Commit one by one each wave as the dag progresses in ideal conditions.
#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let mut last_committed = BlockReference::new_test(0, 0);
    for n in 1..=10 {
        let enough_blocks = wave_length * (n + 1) - 1;
        let (mut storage, _) =
            Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);
        build_dag(&committee, &mut storage, None, enough_blocks);

        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            Protocol {
                strong_quorum: 2 * committee.total_stake() / 3 + 1,
                weak_quorum: 2 * committee.total_stake() / 3 + 1,
                wave_length: 3,
                leader_count: NonZeroUsize::new(leader_count).unwrap(),
                pipeline: false,
                leader_wait: false,
                require_crypto: false,
            },
        );

        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), leader_count);

        let leader_round = n * wave_length;
        for (i, leader) in sequence.iter().enumerate() {
            if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = leader
            {
                let leader_offset = i as u64;
                let expected = leader_elector.elect_leader(leader_round + leader_offset);
                assert_eq!(block.author(), expected);
            } else {
                panic!("Expected a committed leader")
            };
        }

        let last = sequence.iter().last().unwrap();
        last_committed = BlockReference::new_test(last.authority().as_u64(), last.round());
    }
}

/// Commit the leaders of the first wave assuming the very first leader is already committed.
#[test]
#[tracing_test::traced_test]
fn direct_commit_partial_round() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let first_leader_round = wave_length;
    let first_leader = leader_elector.elect_leader(first_leader_round);
    let last_committed = BlockReference::new_test(first_leader.as_u64(), first_leader_round);

    let enough_blocks = 2 * wave_length - 1;
    let (mut storage, _) =
        Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);
    build_dag(&committee, &mut storage, None, enough_blocks);

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol {
            strong_quorum: 2 * committee.total_stake() / 3 + 1,
            weak_quorum: 2 * committee.total_stake() / 3 + 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(leader_count).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        },
    );

    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), leader_count - 1);

    for (i, leader) in sequence.iter().enumerate() {
        if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = leader {
            let leader_offset = (i + 1) % committee.len();
            let expected = leader_elector.elect_leader(first_leader_round + leader_offset as u64);
            assert_eq!(block.author(), expected);
        } else {
            panic!("Expected a committed leader")
        };
    }
}

/// Commit 10 leaders in a row (calling the committer after adding them).
#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let n = 10;
    let enough_blocks = wave_length * (n + 1) - 1;
    let (mut storage, _) =
        Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);
    build_dag(&committee, &mut storage, None, enough_blocks);

    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol {
            strong_quorum: 2 * committee.total_stake() / 3 + 1,
            weak_quorum: 2 * committee.total_stake() / 3 + 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(leader_count).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        },
    );

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), leader_count * n as usize);
    for (i, leaders) in sequence.chunks(leader_count).enumerate() {
        let leader_round = (i as u64 + 1) * wave_length;
        for (j, leader) in leaders.iter().enumerate() {
            if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = leader
            {
                let leader_offset = j as u64;
                let expected = leader_elector.elect_leader(leader_round + leader_offset);
                assert_eq!(block.author(), expected);
            } else {
                panic!("Expected a committed leader")
            }
        }
    }
}

/// Do not commit anything if we are still in the first wave.
#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    let committee = committee(4);
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let first_commit_round = 2 * wave_length - 1;
    for r in 0..first_commit_round {
        let (mut storage, _) =
            Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);
        build_dag(&committee, &mut storage, None, r);

        let mut committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            Protocol {
                strong_quorum: 2 * committee.total_stake() / 3 + 1,
                weak_quorum: 2 * committee.total_stake() / 3 + 1,
                wave_length: 3,
                leader_count: NonZeroUsize::new(leader_count).unwrap(),
                pipeline: false,
                leader_wait: false,
                require_crypto: false,
            },
        );

        let last_committed = BlockReference::new_test(0, 0);
        let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }
}

/// We directly skip a leader that has enough blames and commit the others
#[test]
#[tracing_test::traced_test]
fn no_leader() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let (mut storage, _) =
        Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);

    // Add enough blocks to finish wave 0.
    let decision_round_0 = wave_length - 1;
    let references = build_dag(&committee, &mut storage, None, decision_round_0);

    // Add enough blocks to reach the decision round of wave 1 (but without its leader).
    let leader_round_1 = wave_length;
    let leader_1 = leader_elector.elect_leader(leader_round_1);

    let connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1)
        .map(|authority| (authority, references.clone()));
    let references = build_dag_layer(connections.collect(), &mut storage);

    let decision_round_1 = 2 * wave_length - 1;
    build_dag(&committee, &mut storage, Some(references), decision_round_1);

    // Ensure the omitted leader is skipped and the others are committed.
    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol {
            strong_quorum: 2 * committee.total_stake() / 3 + 1,
            weak_quorum: 2 * committee.total_stake() / 3 + 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(leader_count).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        },
    );

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), leader_count);
    for (i, leader) in sequence.iter().enumerate() {
        let leader_round = wave_length;
        let leader_offset = i as u64;
        let expected_leader = leader_elector.elect_leader(leader_round + leader_offset);
        if i == 0 {
            if let LeaderStatus::DirectSkip(leader, round)
            | LeaderStatus::IndirectSkip(leader, round) = sequence[i]
            {
                assert_eq!(leader, expected_leader);
                assert_eq!(round, leader_round_1);
            } else {
                panic!("Expected to directly skip the leader");
            }
        } else if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) =
            leader
        {
            assert_eq!(block.author(), expected_leader);
        } else {
            panic!("Expected a committed leader")
        }
    }
}

/// We directly skip the leader if it has enough blame.
#[test]
#[tracing_test::traced_test]
fn direct_skip() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let (mut storage, _) =
        Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);

    // Add enough blocks to reach the first leader of wave 1.
    let leader_round_1 = wave_length;
    let references_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    // Filter out that leader.
    let references_without_leader_1: Vec<_> = references_1
        .into_iter()
        .filter(|x| x.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    // Add enough blocks to reach the decision round of wave 1.
    let decision_round_1 = 2 * wave_length - 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_without_leader_1),
        decision_round_1,
    );

    // Ensure that the first leader of wave 1 is skipped.
    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol {
            strong_quorum: 2 * committee.total_stake() / 3 + 1,
            weak_quorum: 2 * committee.total_stake() / 3 + 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(leader_count).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        },
    );

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), leader_count);
    for (i, leader) in sequence.iter().enumerate() {
        let leader_round = wave_length;
        let leader_offset = i as u64;
        let expected_leader = leader_elector.elect_leader(leader_round + leader_offset);
        if i == 0 {
            if let LeaderStatus::DirectSkip(leader, round)
            | LeaderStatus::IndirectSkip(leader, round) = sequence[i]
            {
                assert_eq!(leader, expected_leader);
                assert_eq!(round, leader_round_1);
            } else {
                panic!("Expected to directly skip the leader");
            }
        } else if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) =
            leader
        {
            assert_eq!(block.author(), expected_leader);
        } else {
            panic!("Expected a committed leader")
        }
    }
}

/// Indirect-commit the first leader.
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let one_fault = total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let (mut storage, _) =
        Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);

    // Add enough blocks to reach the leaders of wave 1.
    let leader_round_1 = wave_length;
    let references_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    // Filter out the first leader of wave 1.
    let references_without_leader_1: Vec<_> = references_1
        .iter()
        .cloned()
        .filter(|x| x.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    // Only 2f+1 validators vote for the that leader.
    let connections_with_leader_1 = committee
        .authorities()
        .take(strong_quorum as usize)
        .map(|authority| (authority, references_1.clone()))
        .collect();
    let references_with_votes_for_leader_1 =
        build_dag_layer(connections_with_leader_1, &mut storage);

    let connections_without_leader_1 = committee
        .authorities()
        .skip(strong_quorum as usize)
        .map(|authority| (authority, references_without_leader_1.clone()))
        .collect();
    let references_without_votes_for_leader_1 =
        build_dag_layer(connections_without_leader_1, &mut storage);

    // Only f+1 validators certify that leader.
    let mut references_3 = Vec::new();

    let connections_with_votes_for_leader_1 = committee
        .authorities()
        .take(one_fault as usize)
        .map(|authority| (authority, references_with_votes_for_leader_1.clone()))
        .collect();
    references_3.extend(build_dag_layer(
        connections_with_votes_for_leader_1,
        &mut storage,
    ));

    let references: Vec<_> = references_without_votes_for_leader_1
        .into_iter()
        .chain(references_with_votes_for_leader_1)
        .take(strong_quorum as usize)
        .collect();
    let connections_without_votes_for_leader_1 = committee
        .authorities()
        .skip(one_fault as usize)
        .map(|authority| (authority, references.clone()))
        .collect();
    references_3.extend(build_dag_layer(
        connections_without_votes_for_leader_1,
        &mut storage,
    ));

    // Add enough blocks to decide the leaders of wave 2.
    let decision_round_3 = 3 * wave_length - 1;
    build_dag(
        &committee,
        &mut storage,
        Some(references_3),
        decision_round_3,
    );

    // Ensure we commit the 1st leader.
    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol {
            strong_quorum: 2 * committee.total_stake() / 3 + 1,
            weak_quorum: 2 * committee.total_stake() / 3 + 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(leader_count).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        },
    );

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 2 * leader_count);

    let leader_round = wave_length;
    let leader = leader_elector.elect_leader(leader_round);
    if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
        sequence[0]
    {
        assert_eq!(block.author(), leader);
    } else {
        panic!("Expected a committed leader")
    };
}

/// Commit the leaders of wave 1, skip the first leader of wave 2, and commit the leaders of wave 3.
#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let one_fault = total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let (mut storage, _) =
        Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);

    // Add enough blocks to reach the leaders of wave 2.
    let leader_round_2 = 2 * wave_length;
    let references_2 = build_dag(&committee, &mut storage, None, leader_round_2);

    // Filter out the first leader of wave 2.
    let leader_2 = leader_elector.elect_leader(leader_round_2);
    let references_without_leader_2: Vec<_> = references_2
        .iter()
        .cloned()
        .filter(|x| x.authority != leader_2)
        .collect();

    // Only f+1 validators connect to that leader.
    let mut references = Vec::new();

    let connections_with_leader_2 = committee
        .authorities()
        .take(one_fault as usize)
        .map(|authority| (authority, references_2.clone()))
        .collect();
    references.extend(build_dag_layer(connections_with_leader_2, &mut storage));

    let connections_without_leader_2 = committee
        .authorities()
        .skip(one_fault as usize)
        .map(|authority| (authority, references_without_leader_2.clone()))
        .collect();
    references.extend(build_dag_layer(connections_without_leader_2, &mut storage));

    // Add enough blocks to reach the decision round of the the third wave.
    let decision_round_3 = 4 * wave_length - 1;
    build_dag(&committee, &mut storage, Some(references), decision_round_3);

    // Ensure we commit the leaders of wave 1 and 3
    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol {
            strong_quorum: 2 * committee.total_stake() / 3 + 1,
            weak_quorum: 2 * committee.total_stake() / 3 + 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(leader_count).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        },
    );

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 3 * leader_count);

    // Ensure we commit the leaders of wave 1.
    for (n, status) in sequence.iter().take(leader_count).enumerate() {
        let leader_round_1 = wave_length;
        let leader_offset = n as u64;
        let leader_1 = leader_elector.elect_leader(leader_round_1 + leader_offset);
        if let LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) = status {
            assert_eq!(block.author(), leader_1);
        } else {
            panic!("Expected a committed leader")
        };
    }

    // Ensure we skip the first leader of wave 2 but commit the other leaders of wave 2.
    let leader_round_2 = 2 * wave_length;
    if let LeaderStatus::DirectSkip(leader, round) | LeaderStatus::IndirectSkip(leader, round) =
        sequence[leader_count]
    {
        assert_eq!(leader, leader_2);
        assert_eq!(round, leader_round_2);
    } else {
        panic!("Expected a skipped leader")
    }

    for n in 0..leader_count {
        let leader_round_2 = 2 * wave_length;
        let leader_offset = n as u64;
        if n == 0 {
            if let LeaderStatus::DirectSkip(leader, round)
            | LeaderStatus::IndirectSkip(leader, round) = sequence[leader_count + n]
            {
                assert_eq!(leader, leader_2);
                assert_eq!(round, leader_round_2);
            } else {
                panic!("Expected a skipped leader")
            }
        } else {
            let leader_2 = leader_elector.elect_leader(leader_round_2 + leader_offset);
            if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
                sequence[leader_count + n]
            {
                assert_eq!(block.author(), leader_2);
            } else {
                panic!("Expected a committed leader")
            }
        }
    }

    // Ensure we commit the leaders of wave 3.
    for n in 0..leader_count {
        let leader_round_3 = 3 * wave_length;
        let leader_offset = n as u64;
        let leader_3 = leader_elector.elect_leader(leader_round_3 + leader_offset);
        if let LeaderStatus::DirectCommit(ref block) | LeaderStatus::IndirectCommit(ref block) =
            sequence[2 * leader_count + n]
        {
            assert_eq!(block.author(), leader_3);
        } else {
            panic!("Expected a committed leader")
        }
    }
}

/// If there is no leader with enough support nor blame, we commit nothing.
#[test]
#[tracing_test::traced_test]
fn undecided() {
    let committee = committee(4);
    let leader_elector = LeaderElector::new(committee.len());
    let total = committee.total_stake();
    let strong_quorum = 2 * total / 3 + 1;
    let wave_length = 3;
    let leader_count = strong_quorum as usize;

    let (mut storage, _) =
        Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);

    // Add enough blocks to reach the leaders of wave 1.
    let leader_round_1 = wave_length;
    let references_1 = build_dag(&committee, &mut storage, None, leader_round_1);

    // Filter out the first leader of wave 1.
    let references_1_without_leader: Vec<_> = references_1
        .iter()
        .cloned()
        .filter(|x| x.authority != leader_elector.elect_leader(leader_round_1))
        .collect();

    // Create a dag layer where only one authority votes for that leader.
    let mut authorities = committee.authorities();
    let leader_connection = vec![(authorities.next().unwrap(), references_1)];
    let non_leader_connections: Vec<_> = authorities
        .take((strong_quorum - 1) as usize)
        .map(|authority| (authority, references_1_without_leader.clone()))
        .collect();

    let connections = leader_connection.into_iter().chain(non_leader_connections);
    let references = build_dag_layer(connections.collect(), &mut storage);

    // Add enough blocks to reach the decision round of wave 1.
    let decision_round_1 = 2 * wave_length - 1;
    build_dag(&committee, &mut storage, Some(references), decision_round_1);

    // Ensure no blocks are committed.
    let mut committer = Committer::new(
        committee.clone(),
        storage.block_reader().clone(),
        Protocol {
            strong_quorum: 2 * committee.total_stake() / 3 + 1,
            weak_quorum: 2 * committee.total_stake() / 3 + 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(leader_count).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: false,
        },
    );

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed).collect::<Vec<_>>();
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}
