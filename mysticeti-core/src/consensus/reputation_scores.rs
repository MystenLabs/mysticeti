use crate::committee::Committee;
use crate::types::AuthorityIndex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ReputationScores {
    /// Holds the score for every authority. If an authority is not amongst
    /// the records of the map then we assume that its score is zero.
    pub scores_per_authority: HashMap<AuthorityIndex, u64>,
    /// When true it notifies us that those scores will be the last updated scores of the
    /// current schedule before they get reset for the next schedule and start
    /// scoring from the beginning. In practice we can leverage this information to
    /// use the scores during the next schedule until the next final ones are calculated.
    pub final_of_schedule: bool,
}

impl ReputationScores {
    /// Creating a new ReputationScores instance pre-populating the authorities entries with
    /// zero score value.
    pub fn new(committee: &Arc<Committee>) -> Self {
        let scores_per_authority = committee
            .authorities()
            .map(|index| (index, 0_u64))
            .collect();

        Self {
            scores_per_authority,
            ..Default::default()
        }
    }
    /// Adds the provided `score` to the existing score for the provided `authority`
    pub fn add_score(&mut self, authority: AuthorityIndex, score: u64) {
        self.scores_per_authority
            .entry(authority)
            .and_modify(|value| *value += score)
            .or_insert(score);
    }

    pub fn total_authorities(&self) -> u64 {
        self.scores_per_authority.len() as u64
    }

    pub fn all_zero(&self) -> bool {
        !self.scores_per_authority.values().any(|e| *e > 0)
    }

    // Returns the authorities in score descending order.
    pub fn authorities_by_score_desc(&self) -> Vec<(AuthorityIndex, u64)> {
        let mut authorities: Vec<_> = self
            .scores_per_authority
            .iter()
            .map(|(authority, score)| (*authority, *score))
            .collect();

        authorities.sort_by(|a1, a2| {
            match a2.1.cmp(&a1.1) {
                Ordering::Equal => {
                    // we resolve the score equality deterministically by ordering in authority
                    // identifier order descending.
                    a2.0.cmp(&a1.0)
                }
                result => result,
            }
        });

        authorities
    }
}
