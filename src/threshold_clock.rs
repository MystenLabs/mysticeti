use std::collections::HashSet;

use crate::types::MetaStatementBlock;

// A block is threshold clock valid if:
// - all included blocks have a sequence number lower than the block sequence number.
// - the set of authorities with blocks included has a quorum in the current committee.
pub fn threshold_clock_valid(block: &MetaStatementBlock) -> bool {
    // get a committee from the creator of the block
    let committee = block.get_authority().get_committee();
    let sequence_number = block.get_reference().1;

    // Ensure all includes have a sequence number smaller than the block sequence number
    for include in block.get_includes() {
        if include.1 >= block.get_reference().1 {
            return false;
        }
    }

    // Collect the authorities with included blocks at sequence_number  - 1
    let mut authorities_with_includes = HashSet::new();
    for include in block.get_includes() {
        if include.1 == sequence_number - 1 {
            authorities_with_includes.insert(include.0.clone());
        }
    }

    // Ensure the set of authorities with includes has a quorum in the current committee
    // Unless this is the zero blocks, that start the epoch
    sequence_number == 0
        || committee.is_quorum(committee.get_total_stake(&authorities_with_includes))
}

#[cfg(test)]
mod tests {

    use crate::types::Committee;

    use super::*;

    // Make a committee with 4 authorities each with Stake 1, and a block with 3 includes at sequence number zero
    // check that if the includes are blocks the threshold_clock_valid returns false, but if it is only base statements
    // it succeeds
    #[test]
    fn test_threshold_clock_valid() {
        let committee = Committee::new(0, vec![1, 1, 1, 1]);
        let auth0 = committee.get_rich_authority(0);
        let auth1 = committee.get_rich_authority(1);
        let auth2 = committee.get_rich_authority(2);
        let auth3 = committee.get_rich_authority(3);

        // Make a block from authority 0 with no includes
        let block0 = MetaStatementBlock::new_for_testing(&auth0, 0);
        assert!(threshold_clock_valid(&block0));

        // make another block from authority 1 at sequence number 0 including block0
        let block1 =
            MetaStatementBlock::new_for_testing(&auth1, 0).extend_with(block0.into_include());
        assert!(!threshold_clock_valid(&block1)); // fails because block0 is at sequence number 0

        // Make a block at sequence number 1 including block1
        let block2 =
            MetaStatementBlock::new_for_testing(&auth2, 1).extend_with(block1.into_include());
        assert!(!threshold_clock_valid(&block2)); // fails because not enough sequence number 0 blocks included

        // Now we make 3 blocks at round 0 and include them in a block at round 1
        let block0 = MetaStatementBlock::new_for_testing(&auth0, 0);
        let block1 = MetaStatementBlock::new_for_testing(&auth1, 0);
        let block2 = MetaStatementBlock::new_for_testing(&auth2, 0);
        let block3 = MetaStatementBlock::new_for_testing(&auth3, 1)
            .extend_with(block0.into_include())
            .extend_with(block1.into_include())
            .extend_with(block2.into_include());

        // Are the 3 authroities a quorum
        let committee = auth0.get_committee();
        assert!(committee.is_quorum(
            committee.get_total_stake(
                &[auth0.clone(), auth1.clone(), auth2.clone()]
                    .into_iter()
                    .collect()
            )
        ));

        assert!(threshold_clock_valid(&block3)); // succeeds because there is a quorum of sequence number 0 blocks
    }
}
