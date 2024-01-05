// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::types::AuthorityRound;
use crate::{
    consensus::{
        universal_committer::UniversalCommitterBuilder, LeaderStatus, DEFAULT_WAVE_LENGTH,
    },
    test_util::{build_dag, build_dag_layer, committee, test_metrics, TestBlockWriter},
};

/// Commit one leader.
#[test]
#[tracing_test::traced_test]
fn direct_commit() {
    // even stake
    let committee = committee(4);

    let mut block_writer = TestBlockWriter::new(&committee);

    // build fully connected fully connected dag with empty blocks
    // adding 8 rounds to the dag so that we have 2 completed waves
    // and one incomplete wave. The universal committer should skip
    // the potential leaders in r6 because there is no way to get
    // enough certificates for r6 leaders without completing wave 3.
    build_dag(&committee, &mut block_writer, None, 7);

    // Create committer without pipelining and only 1 leader per round
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .build();

    // genesis cert will not be included in commit sequence,
    // marking it as last decided
    let last_decided = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_decided);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::Commit(ref block) = sequence[0] {
        assert_eq!(
            block.author(),
            committee.elect_leader(DEFAULT_WAVE_LENGTH, 0)
        )
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
    let all_blocks = build_dag(&committee, &mut block_writer, None, 5);
    let last_round_blocks = all_blocks
        .iter()
        .filter(|b| b.round == 5)
        .cloned()
        .collect::<Vec<_>>();

    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.block_store(),
        test_metrics(),
    )
    .build();

    // Commit one block.
    let last_decided = AuthorityRound::new(0, 0);
    let sequenced = committer.try_commit(last_decided);
    assert_eq!(sequenced.len(), 1);

    // Ensure we don't commit it again.
    // add more rounds first , so we have something to commit after the last_committed
    build_dag(&committee, &mut block_writer, Some(last_round_blocks), 8);

    let max = sequenced.into_iter().max().unwrap();
    let last_committed = AuthorityRound::new(max.authority(), max.round());
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 1);
    assert_eq!(sequence.first().unwrap().round(), 6);
}

/// Commit one by one each leader as the dag progresses in ideal conditions.
#[test]
#[tracing_test::traced_test]
fn multiple_direct_commit() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut last_decided = AuthorityRound::new(0, 0);
    for n in 1..=10 {
        // genesis blocks are ignored in commit sequence and round is zero indexed
        // i.e. first leader to be committed will be in wave1/round3
        let round_with_enough_blocks = wave_length * (n + 1) - 1;
        let mut block_writer = TestBlockWriter::new(&committee);
        build_dag(
            &committee,
            &mut block_writer,
            None,
            round_with_enough_blocks,
        );

        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .build();

        let sequence = committer.try_commit(last_decided);
        tracing::info!("Commit sequence: {sequence:?}");
        assert_eq!(sequence.len(), 1);

        let leader_round = n as u64 * wave_length;
        if let LeaderStatus::Commit(ref block) = sequence[0] {
            assert_eq!(block.author(), committee.elect_leader(leader_round, 0));
        } else {
            panic!("Expected a committed leader")
        }

        let max = sequence.iter().max().unwrap();
        last_decided = AuthorityRound::new(max.authority(), max.round());
    }
}

/// Commit 10 leaders in a row (calling the committer after adding them).
#[test]
#[tracing_test::traced_test]
fn direct_commit_late_call() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let num_waves = 10;
    let round_with_enough_blocks = wave_length * (num_waves + 1) - 1;
    let mut block_writer = TestBlockWriter::new(&committee);
    build_dag(
        &committee,
        &mut block_writer,
        None,
        round_with_enough_blocks,
    );

    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .build();

    let last_decided = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_decided);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), num_waves as usize);
    for (i, leader_block) in sequence.iter().enumerate() {
        let leader_round = (i as u64 + 1) * wave_length;
        if let LeaderStatus::Commit(ref block) = leader_block {
            assert_eq!(block.author(), committee.elect_leader(leader_round, 0));
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

        let committer = UniversalCommitterBuilder::new(
            committee.clone(),
            block_writer.into_block_store(),
            test_metrics(),
        )
        .with_wave_length(wave_length)
        .build();

        let last_committed = AuthorityRound::new(0, 0);
        let sequence = committer.try_commit(last_committed);
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

    // Add enough blocks to reach the decision round of the first leader (but without the leader).
    let leader_round_1 = wave_length;
    let leader_1 = committee.elect_leader(leader_round_1, 0);

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
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .build();

    let last_decided = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_decided);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::Skip(leader) = sequence[0] {
        assert_eq!(leader.authority, leader_1);
        assert_eq!(leader.round, leader_round_1);
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
    let leader_round_1 = wave_length;
    let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

    // Filter out that leader.
    let references_without_leader_1: Vec<_> = references_1
        .into_iter()
        .filter(|x| x.authority != committee.elect_leader(leader_round_1, 0))
        .collect();

    // Add enough blocks to reach the decision round of the first leader.
    let decision_round_1 = 2 * wave_length - 1;
    build_dag(
        &committee,
        &mut block_writer,
        Some(references_without_leader_1),
        decision_round_1,
    );

    // Ensure the leader is skipped.
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .build();

    let last_committed = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::Skip(leader) = sequence[0] {
        assert_eq!(leader.authority, committee.elect_leader(leader_round_1, 0));
        assert_eq!(leader.round, leader_round_1);
    } else {
        panic!("Expected to directly skip the leader");
    }
}

/// Indirect-commit the first leader.
#[test]
#[tracing_test::traced_test]
fn indirect_commit() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks to reach the 1st leader.
    let leader_round_1 = wave_length;
    let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

    // Filter out that leader.
    let references_without_leader_1: Vec<_> = references_1
        .iter()
        .cloned()
        .filter(|x| x.authority != committee.elect_leader(leader_round_1, 0))
        .collect();

    // Only 2f+1 validators vote for the 1st leader.
    let connections_with_leader_1 = committee
        .authorities()
        .take(committee.quorum_threshold() as usize)
        .map(|authority| (authority, references_1.clone()))
        .collect();
    let references_with_votes_for_leader_1 =
        build_dag_layer(connections_with_leader_1, &mut block_writer);

    let connections_without_leader_1 = committee
        .authorities()
        .skip(committee.quorum_threshold() as usize)
        .map(|authority| (authority, references_without_leader_1.clone()))
        .collect();
    let references_without_votes_for_leader_1 =
        build_dag_layer(connections_without_leader_1, &mut block_writer);

    // Only f+1 validators certify the 1st leader.
    let mut references_3 = Vec::new();

    let connections_with_votes_for_leader_1 = committee
        .authorities()
        .take(committee.validity_threshold() as usize)
        .map(|authority| (authority, references_with_votes_for_leader_1.clone()))
        .collect();
    println!(
        "connections_with_votes_for_leader_1 {:#?}",
        connections_with_votes_for_leader_1
    );
    references_3.extend(build_dag_layer(
        connections_with_votes_for_leader_1,
        &mut block_writer,
    ));
    println!("{:#?}", references_3);

    let references: Vec<_> = references_without_votes_for_leader_1
        .into_iter()
        .chain(references_with_votes_for_leader_1.into_iter())
        .take(committee.quorum_threshold() as usize)
        .collect();

    println!("references{:#?}", references);
    let connections_without_votes_for_leader_1 = committee
        .authorities()
        .skip(committee.validity_threshold() as usize)
        .map(|authority| (authority, references.clone()))
        .collect();

    println!(
        "connections_without_votes_for_leader_1 {:#?}",
        connections_without_votes_for_leader_1
    );
    references_3.extend(build_dag_layer(
        connections_without_votes_for_leader_1,
        &mut block_writer,
    ));
    println!("{:#?}", references_3);

    // Ensure we mark the 1st leader as undecided resulting in no leaders sequenced
    // because there is also no anchor that is either commit or undecided.

    // let committer = UniversalCommitterBuilder::new(
    //     committee.clone(),
    //     block_writer.into_block_store(),
    //     test_metrics(),
    // )
    // .with_wave_length(wave_length)
    // .build();
    // let last_decided = AuthorityRound::new(0, 0);
    // let sequence = committer.try_commit(last_decided);
    // tracing::info!("Commit sequence: {sequence:?}");
    // assert!(sequence.is_empty());
    // let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks to decide the 2nd leader.
    let decision_round_3 = 3 * wave_length - 1;
    build_dag(
        &committee,
        &mut block_writer,
        Some(references_3),
        decision_round_3,
    );

    // 2nd leader or C6 has the necessary votes/certs to be directly commited
    // after this leaders of previous rounds are checked. When we get to the 1st leader
    // at round 3 or D3. We see that we cannot direct commit and it is marked as
    // undecided. But this time we have a committed anchor so we check if there is a certified
    // link from the anchor (c6) to the undecided leader (d3). There is a certified link
    // through A5 with votes A4,B4,C4. So we can mark this leader as commit indirectly.

    // Ensure we commit the 1st leader.
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .build();

    let last_decided = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_decided);
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 2);

    let leader_round = wave_length;
    let leader = committee.elect_leader(leader_round, 0);
    if let LeaderStatus::Commit(ref block) = sequence[0] {
        assert_eq!(block.author(), leader);
    } else {
        panic!("Expected a committed leader")
    };
}

/// Commit the first leader, skip the 2nd, and commit the 3rd leader.
#[test]
#[tracing_test::traced_test]
fn indirect_skip() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks to reach the 2nd leader.
    let leader_round_2 = 2 * wave_length;
    let references_2 = build_dag(&committee, &mut block_writer, None, leader_round_2);

    // Filter out that leader.
    let leader_2 = committee.elect_leader(leader_round_2, 0);
    let references_without_leader_2: Vec<_> = references_2
        .iter()
        .cloned()
        .filter(|x| x.authority != leader_2)
        .collect();

    // Only f+1 validators connect to the 2nd leader.
    let mut references = Vec::new();

    let connections_with_leader_2 = committee
        .authorities()
        .take(committee.validity_threshold() as usize)
        .map(|authority| (authority, references_2.clone()))
        .collect();
    println!("connections_with_leader_2 {:#?}", connections_with_leader_2);

    references.extend(build_dag_layer(
        connections_with_leader_2,
        &mut block_writer,
    ));

    let connections_without_leader_2 = committee
        .authorities()
        .skip(committee.validity_threshold() as usize)
        .map(|authority| (authority, references_without_leader_2.clone()))
        .collect();
    println!(
        "connections_without_leader_2 {:#?}",
        connections_without_leader_2
    );

    references.extend(build_dag_layer(
        connections_without_leader_2,
        &mut block_writer,
    ));

    // Add enough blocks to reach the decision round of the 3rd leader.
    let decision_round_3 = 4 * wave_length - 1;
    build_dag(
        &committee,
        &mut block_writer,
        Some(references),
        decision_round_3,
    );

    // question(arun): why do we mark undecided if there are <2f+1 blames
    // and all the blocks have been received for the round. We should check
    // if there are ever enough possible blocks left to be able to generate the
    // number of votes needed. In this case there will not be enough votes to create
    // a certified link. So we should mark the leader as skip.
    // answer: multiple blocks can be received per round, due to byzantine behavior
    // or equivocation. So we need to wait for a leader with strong support to make
    // the decision.

    // Ensure we commit the leaders of wave 1 and 3
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .build();

    let last_committed = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");
    assert_eq!(sequence.len(), 3);

    // wave 1 leader is committed directly
    // Ensure we commit the leader of wave 1.
    let leader_round_1 = wave_length;
    let leader_1 = committee.elect_leader(leader_round_1, 0);
    if let LeaderStatus::Commit(ref block) = sequence[0] {
        assert_eq!(block.author(), leader_1);
    } else {
        panic!("Expected a committed leader")
    };

    // wave 2 leader is undecided directly and then skipped indirectly because
    // of lack of certified links
    // Ensure we skip the 2nd leader.
    let leader_round_2 = 2 * wave_length;
    if let LeaderStatus::Skip(leader) = sequence[1] {
        assert_eq!(leader.authority, leader_2);
        assert_eq!(leader.round, leader_round_2);
    } else {
        panic!("Expected a skipped leader")
    }

    // wave 3 leader is committed directly
    // Ensure we commit the 3rd leader.
    let leader_round_3 = 3 * wave_length;
    let leader_3 = committee.elect_leader(leader_round_3, 0);
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

    // Add enough blocks to reach the first leader.
    let leader_round_1 = wave_length;
    let references_1 = build_dag(&committee, &mut block_writer, None, leader_round_1);

    // Filter out that leader.
    let references_without_leader_1: Vec<_> = references_1
        .iter()
        .cloned()
        .filter(|x| x.authority != committee.elect_leader(leader_round_1, 0))
        .collect();

    // Create a dag layer where only one authority votes for the first leader.
    let mut authorities = committee.authorities();
    let leader_connection = vec![(authorities.next().unwrap(), references_1)];
    println!("leader_connection {:#?}", leader_connection);
    let non_leader_connections: Vec<_> = authorities
        .take((committee.quorum_threshold() - 1) as usize)
        .map(|authority| (authority, references_without_leader_1.clone()))
        .collect();
    println!("non_leader_connections {:#?}", non_leader_connections);

    let connections = leader_connection.into_iter().chain(non_leader_connections);
    let references = build_dag_layer(connections.collect(), &mut block_writer);

    // Add enough blocks to reach the decision round of the first leader.
    let decision_round_1 = 2 * wave_length - 1;
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
    .build();

    let last_committed = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_committed);
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}

// todo add test for mutliple blocks from authority per round
// todo investigate why only the "most recent" block seems to be used

#[test]
#[tracing_test::traced_test]
fn add_non_support_blocks_after_support_blocks() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks till round 3

    let leader_1_round = wave_length;
    let leader_1_references = build_dag(&committee, &mut block_writer, None, leader_1_round);

    // add blocks till round 5 (wave 1 decision round)
    let decision_round_1 = 2 * wave_length - 1;
    build_dag(
        &committee,
        &mut block_writer,
        Some(leader_1_references.clone()),
        decision_round_1,
    );

    // add block layer for round 4 with no votes for leader d3

    // Filter out leader 1 from round 3 (wave 1).
    let leader_1 = committee.elect_leader(leader_1_round, 0);
    println!("leader_1 = {leader_1:#?}");
    // A3 B3 C3
    let references_without_leader_1: Vec<_> = leader_1_references
        .into_iter()
        .filter(|x| x.authority != leader_1)
        .collect();
    println!("references_without_leader_1 = {references_without_leader_1:#?}");
    // NV-B4 (A3 B3 C3) NV-C4 (A3 B3 C3)
    // cant write mutiple block to my own index (i.e. block writer is operating as auth 0)
    // so writing multiple blocks to a4 would panic
    let voting_1_round_connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1 && authority != 0)
        .map(|authority| (authority, references_without_leader_1.clone()));
    let r1_votes_references =
        build_dag_layer(voting_1_round_connections.collect(), &mut block_writer);
    println!("r1 votes = {r1_votes_references:#?}");

    // add decision round blocks to link to these non vote/cert blocks
    let decision_1_round_connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1 && authority != 0)
        .map(|authority| (authority, r1_votes_references.clone()));
    let r1_decision_references =
        build_dag_layer(decision_1_round_connections.collect(), &mut block_writer);
    println!("r1 decisions = {r1_decision_references:#?}");

    // question: even though there are 3 blocks for auhtority A/B/C at round 4 that are non votes for
    // leader d3. There were new blocks added that are votes after. They should be the ones
    // used to get the 2f+1 votes needed to direct commit the block. why is this not happening?

    // try commit leader 1 in round 3 (d3)
    // "non support" blocks are used instead of support blocks so leader is undecided
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .build();

    let last_decided = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_decided);
    tracing::info!("Commit sequence: {sequence:?}");
    assert!(sequence.is_empty());
}
#[test]
#[tracing_test::traced_test]
fn add_non_support_blocks_before_support_blocks() {
    let committee = committee(4);
    let wave_length = DEFAULT_WAVE_LENGTH;

    let mut block_writer = TestBlockWriter::new(&committee);

    // Add enough blocks till round 3

    let leader_1_round = wave_length;
    let leader_1_references = build_dag(&committee, &mut block_writer, None, leader_1_round);

    // add block layer for round 4 with no votes for leader d3

    // Filter out leader 1 from round 3 (wave 1).
    let leader_1 = committee.elect_leader(leader_1_round, 0);
    println!("leader_1 = {leader_1:#?}");
    // A3 B3 C3
    let references_without_leader_1: Vec<_> = leader_1_references
        .clone()
        .into_iter()
        .filter(|x| x.authority != leader_1)
        .collect();
    println!("references_without_leader_1 = {references_without_leader_1:#?}");
    // NV-B4 (A3 B3 C3) NV-C4 (A3 B3 C3)
    // cant write mutiple block to my own index (i.e. block writer is operating as auth 0)
    // so writing multiple blocks to a4 would panic
    let voting_1_round_connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1 && authority != 0)
        .map(|authority| (authority, references_without_leader_1.clone()));
    let r1_votes_references =
        build_dag_layer(voting_1_round_connections.collect(), &mut block_writer);
    println!("r1 votes = {r1_votes_references:#?}");

    // add decision round blocks to link to these non vote/cert blocks
    let decision_1_round_connections = committee
        .authorities()
        .filter(|&authority| authority != leader_1 && authority != 0)
        .map(|authority| (authority, r1_votes_references.clone()));
    let r1_decision_references =
        build_dag_layer(decision_1_round_connections.collect(), &mut block_writer);
    println!("r1 decisions = {r1_decision_references:#?}");

    // add blocks till round 5 but dont use references from block layer above
    // this should add another layer of blocks for round 4 that inlcude the votes
    // for d3
    let decision_round_1 = 2 * wave_length - 1;
    build_dag(
        &committee,
        &mut block_writer,
        Some(leader_1_references),
        decision_round_1,
    );

    // question: even though there are 3 blocks for auhtority A/B/C at round 4 that are non votes for
    // leader d3. There were new blocks added that are votes after. They should be the ones
    // used to get the 2f+1 votes needed to direct commit the block. why is this not happening?

    // try commit leader 1 in round 3 (d3)
    // "non support" blocks are used instead of support blocks so leader is undecided
    let committer = UniversalCommitterBuilder::new(
        committee.clone(),
        block_writer.into_block_store(),
        test_metrics(),
    )
    .with_wave_length(wave_length)
    .build();

    let last_decided = AuthorityRound::new(0, 0);
    let sequence = committer.try_commit(last_decided);
    tracing::info!("Commit sequence: {sequence:?}");

    assert_eq!(sequence.len(), 1);
    if let LeaderStatus::Commit(ref block) = sequence[0] {
        assert_eq!(block.author(), leader_1)
    } else {
        panic!("Expected a committed leader")
    };
}
