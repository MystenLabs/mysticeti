use std::cmp::Ordering;

use crate::committee::{Committee, QuorumThreshold, StakeAggregator};
use crate::types::{BlockReference, RoundNumber, StatementBlock};

// A block is threshold clock valid if:
// - all included blocks have a round number lower than the block round number.
// - the set of authorities with blocks included has a quorum in the current committee.
pub fn threshold_clock_valid_non_genesis(block: &StatementBlock, committee: &Committee) -> bool {
    // get a committee from the creator of the block
    let round_number = block.reference().round;
    assert!(round_number > 0);

    // Ensure all includes have a round number smaller than the block round number
    for include in block.includes() {
        if include.round >= block.reference().round {
            return false;
        }
    }

    let mut aggregator = StakeAggregator::<QuorumThreshold>::new();
    let mut is_quorum = false;
    // Collect the authorities with included blocks at round_number  - 1
    for include in block.includes() {
        if include.round == round_number - 1 {
            is_quorum = aggregator.add(include.authority, committee);
        }
    }

    // Ensure the set of authorities with includes has a quorum in the current committee
    is_quorum
}

pub struct ThresholdClockAggregator {
    aggregator: StakeAggregator<QuorumThreshold>,
    round: RoundNumber,
}

impl ThresholdClockAggregator {
    pub fn new(round: RoundNumber) -> Self {
        Self {
            aggregator: StakeAggregator::new(),
            round,
        }
    }

    pub fn add_block(&mut self, block: BlockReference, committee: &Committee) {
        match block.round.cmp(&self.round) {
            // Blocks with round less then what we currently build are irrelevant here
            Ordering::Less => {}
            // If we processed block for round r, we also have stored 2f+1 blocks from r-1
            Ordering::Greater => {
                self.aggregator.clear();
                self.aggregator.add(block.authority, committee);
                self.round = block.round;
            }
            Ordering::Equal => {
                if self.aggregator.add(block.authority, committee) {
                    self.aggregator.clear();
                    // We have seen 2f+1 blocks for current round, advance
                    self.round = block.round + 1;
                }
            }
        }
        if block.round > self.round {
            // If we processed block for round r, we also have stored 2f+1 blocks from r-1
            self.round = block.round;
        }
    }

    pub fn get_round(&self) -> RoundNumber {
        self.round
    }
}

#[cfg(test)]
mod tests {

    use crate::types::Dag;

    use super::*;

    // Make a committee with 4 authorities each with Stake 1, and a block with 3 includes at round number zero
    // check that if the includes are blocks the threshold_clock_valid returns false, but if it is only base statements
    // it succeeds
    #[test]
    fn test_threshold_clock_valid() {
        let committee = Committee::new_test(vec![1, 1, 1, 1]);
        assert!(!threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[]"),
            &committee
        ));
        assert!(!threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[A0, B0]"),
            &committee
        ));
        assert!(threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[A0, B0, C0]"),
            &committee
        ));
        assert!(threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[A0, B0, C0, D0]"),
            &committee
        ));
        assert!(!threshold_clock_valid_non_genesis(
            &Dag::draw_block("A2:[A1, B1, C0, D0]"),
            &committee
        ));
        assert!(threshold_clock_valid_non_genesis(
            &Dag::draw_block("A2:[A1, B1, C1, D0]"),
            &committee
        ));
    }

    #[test]
    fn test_threshold_clock_aggregator() {
        let committee = Committee::new_test(vec![1, 1, 1, 1]);
        let mut aggregator = ThresholdClockAggregator::new(0);

        aggregator.add_block(BlockReference::new_test(0, 0), &committee);
        assert_eq!(aggregator.get_round(), 0);
        aggregator.add_block(BlockReference::new_test(0, 1), &committee);
        assert_eq!(aggregator.get_round(), 1);
        aggregator.add_block(BlockReference::new_test(1, 0), &committee);
        assert_eq!(aggregator.get_round(), 1);
        aggregator.add_block(BlockReference::new_test(1, 1), &committee);
        assert_eq!(aggregator.get_round(), 1);
        aggregator.add_block(BlockReference::new_test(2, 1), &committee);
        assert_eq!(aggregator.get_round(), 2);
        aggregator.add_block(BlockReference::new_test(3, 1), &committee);
        assert_eq!(aggregator.get_round(), 2);
    }
}
