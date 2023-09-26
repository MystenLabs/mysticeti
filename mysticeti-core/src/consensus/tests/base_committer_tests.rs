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
