// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    consensus::{
        universal_committer::UniversalCommitterBuilder, LeaderStatus, DEFAULT_WAVE_LENGTH,
    },
    test_util::{build_dag, build_dag_layer, committee, test_metrics, TestBlockWriter},
    types::{BlockReference, StatementBlock},
};

/// Commit one leader.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);
    build_dag(&committee, &mut block_writer, None, wave_length);

    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .with_pipeline(true)
    .build();

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::Commit(ref block) = sequence[0] {
        assert_eq!(block.author(), committee.elect_leader(1));
    } else {
        panic!("Expected a committed leader")
    };
}

/// Ensure idempotent replies.
#[test]
#[tracing_test::traced_test]
fn idempotence() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);
    build_dag(&committee, &mut block_writer, None, 5);

    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .with_pipeline(true)
    .build();

    // Commit one block.
    let last_committed = BlockReference::new_test(0, 0);
    let committed = committer.try_commit(last_committed);

    // Ensure we don't commit it again.
    let max = committed.into_iter().max().unwrap();
    let last_committed = BlockReference::new_test(max.authority(), max.round());
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}

/// Commit one by one each leader as the dag progresses in ideal conditions.
#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut last_committed = BlockReference::new_test(0, 0);
    for n in 1..=10 {
        let enough_blocks = n + (wave_length - 1);
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, enough_blocks);

        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_pipeline(true)
        .build();

        let sequence = committer.try_commit(last_committed);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), 1);
        let leader_round = n as u64;
        if let LeaderStatus::Commit(ref block) = sequence[0] {
            assert_eq!(block.author(), committee.elect_leader(leader_round));
        } else {
            panic!("Expected a committed leader")
        }

        let max = sequence.into_iter().max().unwrap();
        last_committed = BlockReference::new_test(max.authority(), max.round());
    }
}

/// Commit 10 leaders in a row (calling the committer after adding them).
#[test]
#[tracing_test::traced_test]
fn indirect_commit_late_call() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let n = 10;
    let enough_blocks = n + (wave_length - 1);
    let mut block_writer = TestBlockWriter::new(&committee);
    build_dag(&committee, &mut block_writer, None, enough_blocks);

    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .with_pipeline(true)
    .build();

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), n as usize);
    for (i, leader_block) in sequence.iter().enumerate() {
        let leader_round = 1 + i as u64;
        if let LeaderStatus::Commit(ref block) = leader_block {
            assert_eq!(block.author(), committee.elect_leader(leader_round));
        } else {
            panic!("Expected a committed leader")
        };
    }
}

/// Commit the first 3 leaders, skip the 4th, and commit the next 3 leaders.
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks to reach the 1st leader.
    let leader_round_1 = 1;
    let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

    // Filter out that leader.
    let references_1_without_leader: Vec<_> = references_1
        .iter()
        .cloned()
        .filter(|x| x.authority != committee.elect_leader(leader_round_1))
        .collect();

    // Only 2f+1 validators vote for the 1st leader.
    let connections_with_leader_1 = committee
        .authorities()
        .take(committee.quorum_threshold() as usize)
        .map(|authority| (authority, references_1.clone()))
        .collect();
    let references_2_with_votes_for_leader_1 =
        build_dag_layer(connections_with_leader_1, &mut block_writer);

    let connections_without_leader_1 = committee
        .authorities()
        .skip(committee.quorum_threshold() as usize)
        .map(|authority| (authority, references_1_without_leader.clone()))
        .collect();
    let references_2_without_votes_for_leader_1 =
        build_dag_layer(connections_without_leader_1, &mut block_writer);

    // Only f+1 validators certify the 1st leader.
    let mut references_3 = Vec::new();

    let connections_with_votes_for_leader_1 = committee
        .authorities()
        .take(committee.validity_threshold() as usize)
        .map(|authority| (authority, references_2_with_votes_for_leader_1.clone()))
        .collect();
    references_3.extend(build_dag_layer(
        connections_with_votes_for_leader_1,
        &mut block_writer,
    ));

    let references: Vec<_> = references_2_without_votes_for_leader_1
        .into_iter()
        .chain(references_2_with_votes_for_leader_1.into_iter())
        .take(committee.quorum_threshold() as usize)
        .collect();
    let connections_without_votes_for_leader_1 = committee
        .authorities()
        .skip(committee.validity_threshold() as usize)
        .map(|authority| (authority, references.clone()))
        .collect();
    references_3.extend(build_dag_layer(
        connections_without_votes_for_leader_1,
        &mut block_writer,
    ));

    // Add enough blocks to decide the 5th leader (the second leader may be skipped
    // so we add enough blocks to recursively decide it).
    let decision_round_3 = 2 * wave_length + 1;
    build_dag(
        &committee,
        &mut block_writer,
        Some(references_3),
        decision_round_3,
    );

    // Ensure we commit the first 2 leaders.
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .with_pipeline(true)
    .build();

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 5);

    // Ensure we commit the first 3 leaders.
    let leader_round = 1;
    let leader = committee.elect_leader(leader_round);
    if let LeaderStatus::Commit(ref block) = sequence[0] {
        assert_eq!(block.author(), leader);
    } else {
        panic!("Expected a committed leader")
    };
}

/// Do not commit anything if we are still in the first wave.
#[test]
#[tracing_test::traced_test]
fn no_genesis_commit() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let first_commit_round = wave_length - 1;
    for r in 0..first_commit_round {
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, r);

        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_pipeline(true)
        .build();

        let last_committed = BlockReference::new_test(0, 0);
        let sequence = committer.try_commit(last_committed);
        tracing::info!("Commit sequence: {sequence:?}");
        assert!(sequence.is_empty());
    }
}

/// We do not commit anything if we miss the first leader.
#[test]
#[tracing_test::traced_test]
fn no_leader() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks to reach the decision round of the first leader (but without the leader).
    let leader_round_1 = 1;
    let leader_1 = committee.elect_leader(leader_round_1);

    let genesis: Vec<_> = committee
        .authorities()
        .map(|authority| *StatementBlock::new_genesis(authority).reference())
        .collect();
    let connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1)
        .map(|authority| (authority, genesis.clone()));
    let references = build_dag_layer(connections.collect(), &mut block_writer);

    let decision_round_1 = wave_length;
    build_dag(
        &committee,
        &mut block_writer,
        Some(references),
        decision_round_1,
    );

    // Ensure no blocks are committed.
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .with_pipeline(true)
    .build();

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed);
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

    // Add enough blocks to reach the first leader.
    let leader_round_1 = 1;
    let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

    // Filter out that leader.
    let references_1_without_leader: Vec<_> = references_1
        .into_iter()
        .filter(|x| x.authority != committee.elect_leader(leader_round_1))
        .collect();

    // Add enough blocks to reach the decision round of the first leader.
    let decision_round_1 = wave_length;
    build_dag(
        &committee,
        &mut block_writer,
        Some(references_1_without_leader),
        decision_round_1,
    );

    // Ensure the omitted leader is skipped.
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .with_pipeline(true)
    .build();

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::Skip(leader, round) = sequence[0] {
        assert_eq!(leader, committee.elect_leader(leader_round_1));
        assert_eq!(round, leader_round_1);
    } else {
        panic!("Expected to directly skip the leader");
    }
}

/// Commit the first 3 leaders, skip the 4th, and commit the next 3 leaders.
#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks to reach the 4th leader.
    let leader_round_4 = wave_length + 1;
    let references_4 = build_dag(&committee, &mut block_writer, None, leader_round_4);

    // Filter out that leader.
    let references_4_without_leader: Vec<_> = references_4
        .iter()
        .cloned()
        .filter(|x| x.authority != committee.elect_leader(leader_round_4))
        .collect();

    // Only f+1 validators connect to the 4th leader.
    let mut references_5 = Vec::new();

    let connections_with_leader_4 = committee
        .authorities()
        .take(committee.validity_threshold() as usize)
        .map(|authority| (authority, references_4.clone()))
        .collect();
    references_5.extend(build_dag_layer(
        connections_with_leader_4,
        &mut block_writer,
    ));

    let connections_without_leader_4 = committee
        .authorities()
        .skip(committee.validity_threshold() as usize)
        .map(|authority| (authority, references_4_without_leader.clone()))
        .collect();
    references_5.extend(build_dag_layer(
        connections_without_leader_4,
        &mut block_writer,
    ));

    // Add enough blocks to reach the decision round of the 7th leader.
    let decision_round_7 = 3 * wave_length;
    build_dag(
        &committee,
        &mut block_writer,
        Some(references_5),
        decision_round_7,
    );

    // Ensure we commit the first 3 leaders, skip the 4th, and commit the last 2 leaders.
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .with_pipeline(true)
    .build();

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 7);

    // Ensure we commit the first 3 leaders.
    for i in 0..=2 {
        let leader_round = i + 1;
        let leader = committee.elect_leader(leader_round);
        if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
            assert_eq!(block.author(), leader);
        } else {
            panic!("Expected a committed leader")
        };
    }

    // Ensure we skip the leader of wave 1 (first pipeline) but commit the others.
    if let LeaderStatus::Skip(leader, round) = sequence[3] {
        assert_eq!(leader, committee.elect_leader(leader_round_4));
        assert_eq!(round, leader_round_4);
    } else {
        panic!("Expected a skipped leader")
    }

    for i in 4..=6 {
        let leader_round = i + 1;
        let leader = committee.elect_leader(leader_round);
        if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
            assert_eq!(block.author(), leader);
        } else {
            panic!("Expected a committed leader")
        };
    }
}

// /// Commit all leaders of wave 1; then miss the first leader of wave 2 (first pipeline) and
//     /// ensure the other leaders of wave 2 are not committed.
//     #[test]
//     #[tracing_test::traced_test]
//     fn unblocked_pipeline_through_direct_skip() {
//         let committee = committee(4);
//         let wave_length = DEFAULT_WAVE_LENGTH;

//         let mut block_writer = TestBlockWriter::new(&committee);

//         // Add enough blocks to reach the leader of wave 2, as seen by the first pipeline.
//         let leader_round_2 = 2 * wave_length;
//         let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

//         // Filter out the first leader of wave 2.
//         let references_2_without_leader: Vec<_> = references_2
//             .into_iter()
//             .filter(|x| x.authority != committee.elect_leader(leader_round_2))
//             .collect();

//         // Add enough blocks to reach the voting round of wave 3, as seen by the first pipeline.
//         let voting_round_3 = (4 * wave_length - 1) - 1;
//         build_dag(
//             &committee,
//             &mut block_writer,
//             Some(references_2_without_leader),
//             voting_round_3,
//         );

//         // Ensure we commit the leaders of waves 1 and 2, except the leader we skipped.
//         let committer = PipelinedCommitterBuilder::new(
//             committee.clone(),
//             block_writer.into_block_store(),
//             test_metrics(),
//         )
//         .with_wave_length(wave_length)
//         .build();

//         let last_committed_round = 0;
//         let sequence = committer.try_commit(last_committed_round);
//         tracing::info!("Commit sequence: {sequence:?}");
//         assert_eq!(sequence.len(), 6);

//         // Ensure we commit the leaders of wave 1.
//         for i in 0..=2 {
//             let leader_round_1 = wave_length + i;
//             let leader_1 = committee.elect_leader(leader_round_1);
//             if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
//                 assert_eq!(block.author(), leader_1);
//             } else {
//                 panic!("Expected a committed leader")
//             };
//         }

//         // Ensure we skip the first leader of wave 2.
//         let leader_round_2 = 2 * wave_length;
//         if let LeaderStatus::Skip(leader, round) = sequence[3] {
//             assert_eq!(leader, committee.elect_leader(leader_round_2));
//             assert_eq!(round, leader_round_2);
//         } else {
//             panic!("Expected to skip the leader")
//         };

//         // Ensure we commit the leaders of wave 2.
//         for i in 4..=5 {
//             let leader_round_2 = wave_length + i;
//             let leader_2 = committee.elect_leader(leader_round_2);
//             if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
//                 assert_eq!(block.author(), leader_2);
//             } else {
//                 panic!("Expected a committed leader")
//             };
//         }
//     }

//     /// Commit all leaders of wave 1; then miss the first leader of wave 2 (first pipeline) and
//     /// ensure the other leaders of wave 2 are not committed.
//     #[test]
//     #[tracing_test::traced_test]
//     fn blocked_pipeline() {
//         let committee = committee(4);
//         let wave_length = DEFAULT_WAVE_LENGTH;

//         let mut block_writer = TestBlockWriter::new(&committee);

//         // Add enough blocks to reach the leader of wave 2, as seen by the first pipeline.
//         let leader_round_2 = 2 * wave_length;
//         let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

//         // Filter out the first leader of wave 2.
//         let references_2_without_leader: Vec<_> = references_2
//             .iter()
//             .cloned()
//             .filter(|x| x.authority != committee.elect_leader(leader_round_2))
//             .collect();

//         // Create a dag layer where only one authority votes for that leader.
//         let mut authorities = committee.authorities();
//         let leader_connection = vec![(authorities.next().unwrap(), references_2)];
//         let non_leader_connections: Vec<_> = authorities
//             .take((committee.quorum_threshold() - 1) as usize)
//             .map(|authority| (authority, references_2_without_leader.clone()))
//             .collect();

//         let connections = leader_connection.into_iter().chain(non_leader_connections);
//         let references = build_dag_layer(connections.collect(), &mut block_writer);

//         // Add enough blocks to reach the voting round of wave 3, as seen by the first pipeline.
//         let voting_round_3 = (4 * wave_length - 1) - 1;
//         build_dag(
//             &committee,
//             &mut block_writer,
//             Some(references),
//             voting_round_3,
//         );

//         // Ensure we commit the leaders of wave 1 and 3, as seen by the first pipeline.
//         let committer = PipelinedCommitterBuilder::new(
//             committee.clone(),
//             block_writer.into_block_store(),
//             test_metrics(),
//         )
//         .with_wave_length(wave_length)
//         .build();

//         let last_committed_round = 0;
//         let sequence = committer.try_commit(last_committed_round);
//         tracing::info!("Commit sequence: {sequence:?}");
//         assert_eq!(sequence.len(), 3);

//         // Ensure we commit the leaders of wave 1.
//         for i in 0..=2 {
//             let leader_round_1 = wave_length + i;
//             let leader_1 = committee.elect_leader(leader_round_1);
//             if let LeaderStatus::Commit(ref block) = sequence[i as usize] {
//                 assert_eq!(block.author(), leader_1);
//             } else {
//                 panic!("Expected a committed leader")
//             };
//         }
//     }
