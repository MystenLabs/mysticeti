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

    let mut block_writer = TestBlockWriter::new(&committee);
    build_dag(&committee, &mut block_writer, None, 5);

    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .build();

    let last_committed = BlockReference::new_test(0, 0);
    let sequence = committer.try_commit(last_committed);
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

    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .build();

    // Commit one block.
    let last_committed = BlockReference::new_test(0, 0);
    let committed = committer.try_commit(last_committed).pop().unwrap();

    // Ensure we don't commit it again.
    let last_committed = BlockReference::new_test(committed.authority(), committed.round());
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
        let enough_blocks = wave_length * (n + 1) - 1;
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, enough_blocks);

        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .build();

        let sequence = committer.try_commit(last_committed);
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), 1);

        let leader_round = n as u64 * wave_length;
        if let LeaderStatus::Commit(ref block) = sequence[0] {
            assert_eq!(block.author(), committee.elect_leader(leader_round));
            last_committed = BlockReference::new_test(block.author(), leader_round);
        } else {
            panic!("Expected a committed leader")
        }
    }
}
