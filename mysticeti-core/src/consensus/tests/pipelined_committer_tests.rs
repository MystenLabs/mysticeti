// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    consensus::{
        universal_committer::UniversalCommitterBuilder, LeaderStatus, DEFAULT_WAVE_LENGTH,
    },
    test_util::{build_dag, committee, test_metrics, TestBlockWriter},
    types::BlockReference,
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

/// Commit 10 leaders in a row (9 of them are committed recursively).
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
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

/// Do not commit anything if we are still in the first wave. Remember that the first leader
/// has round = wave_length.
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
