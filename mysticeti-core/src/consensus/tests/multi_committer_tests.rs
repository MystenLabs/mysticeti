// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    consensus::{
        universal_committer::UniversalCommitterBuilder, LeaderStatus, DEFAULT_WAVE_LENGTH,
    },
    test_util::{build_dag, committee, test_metrics, TestBlockWriter},
    types::BlockReference,
};

/// Commit the leaders of the first wave.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;
    for number_of_leaders in 1..committee.len() {
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(&committee, &mut block_writer, None, 5);

        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .with_number_of_leaders(number_of_leaders)
        .build();

        let last_committed = BlockReference::new_test(0, 0);
        let sequence = committer.try_commit(last_committed);
        tracing::info!("Commit sequence: {sequence:?}");

        assert_eq!(sequence.len(), number_of_leaders);
        for (i, leader) in sequence.iter().enumerate() {
            if let LeaderStatus::Commit(block) = leader {
                let leader_round = wave_length;
                let leader_offset = i as u64;
                let expected = committee.elect_leader(leader_round + leader_offset);
                assert_eq!(block.author(), expected);
            } else {
                panic!("Expected a committed leader")
            };
        }
    }
}
