use std::{collections::HashSet, fmt::Formatter, hash::Hasher, sync::Arc};

pub type Authority = RichAuthority;
pub type Transaction = u64;
pub type TransactionId = u64;
pub type RoundNumber = u64;
pub type BlockDigest = u64;
pub type BlockReference = (Authority, RoundNumber, BlockDigest);
pub type Signature = u64;

#[derive(Clone, PartialEq)]
pub enum Vote {
    Accept,
    Reject(Option<TransactionId>),
}

#[derive(Clone, PartialEq)]
pub enum BaseStatement {
    /// Authority Shares a transactions, without accepting it or not.
    Share(TransactionId, Transaction),
    /// Authority votes to accept or reject a transaction.
    Vote(TransactionId, Vote),
}

#[derive(Clone, PartialEq)]
pub enum MetaStatement {
    /// State a base statement
    Base(BaseStatement),
    // Include statements from another authority, by reference
    Include(BlockReference),
}

#[derive(Clone)]
pub struct MetaStatementBlock {
    reference: BlockReference,

    //  A list of block references to other blocks that this block includes
    //  Note that the order matters: if a reference to two blocks from the same round and same authority
    //  are included, then the first reference is the one that this block conceptually votes for.
    includes: Vec<BlockReference>,

    // A list of base statements in order.
    base_statements: Vec<BaseStatement>,
    _signature: Signature,
}

impl MetaStatementBlock {
    #[cfg(test)]
    pub fn new_for_testing(authority: &Authority, round: RoundNumber) -> Self {
        MetaStatementBlock::new(authority, round, vec![])
    }

    pub fn new(authority: &Authority, round: RoundNumber, contents: Vec<MetaStatement>) -> Self {
        let mut includes: Vec<BlockReference> = vec![];
        let mut base_statements: Vec<BaseStatement> = vec![];
        contents.into_iter().for_each(|item| match item {
            MetaStatement::Base(base) => base_statements.push(base),
            MetaStatement::Include(block_ref) => includes.push(block_ref),
        });

        MetaStatementBlock {
            reference: (authority.clone(), round, 0),
            includes,
            base_statements,
            _signature: 0,
        }
    }

    pub fn get_authority(&self) -> &Authority {
        &self.reference.0
    }

    pub fn get_includes(&self) -> &Vec<BlockReference> {
        &self.includes
    }

    pub fn get_reference(&self) -> &BlockReference {
        &self.reference
    }

    pub fn get_base_statements(&self) -> &Vec<BaseStatement> {
        &self.base_statements
    }

    pub fn into_include(&self) -> MetaStatement {
        MetaStatement::Include(self.get_reference().clone())
    }

    pub fn extend_with(mut self, item: MetaStatement) -> Self {
        match item {
            MetaStatement::Base(base) => self.base_statements.push(base),
            MetaStatement::Include(block_ref) => self.includes.push(block_ref),
        }
        self
    }

    pub fn extend_inplace(&mut self, item: MetaStatement) {
        match item {
            MetaStatement::Base(base) => self.base_statements.push(base),
            MetaStatement::Include(block_ref) => self.includes.push(block_ref),
        }
    }
}

pub type Stake = u64;
pub type CommitteeId = u64;

// clone() is not implemented for Arc, so we need to implement it ourselves
#[derive(Clone)]
pub struct RichAuthority {
    index: usize,              // Index of the authority in the committee
    committee: Arc<Committee>, // Reference to the committee
}

impl std::fmt::Debug for RichAuthority {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RichAuthority({})", self.index)
    }
}

impl PartialEq for RichAuthority {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.committee.committee_id == other.committee.committee_id
    }
}

impl Eq for RichAuthority {}

impl std::hash::Hash for RichAuthority {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.index.hash(state);
        self.committee.committee_id.hash(state);
    }
}

impl RichAuthority {
    pub fn get_stake(&self) -> Stake {
        self.committee.authorities[self.index]
    }

    // Get a reference to the committee
    pub fn get_committee(&self) -> &Arc<Committee> {
        &self.committee
    }
}

pub struct Committee {
    committee_id: CommitteeId, // Unique identifier for the committee
    authorities: Vec<Stake>,   // List of authorities and their stakes
    validity_threshold: Stake, // The minimum stake required for validity
    quorum_threshold: Stake,   // The minimum stake required for quorum
}

impl Committee {
    // What this function does:
    // 1. Creates a new committee with the given committee_id and authorities
    // 2. Ensures that the list of authorities is not empty
    // 3. Ensures that all stakes are positive
    // 4. Calculates the validity threshold and quorum threshold
    // 5. Returns the committee
    pub fn new(committee_id: CommitteeId, authorities: Vec<Stake>) -> Arc<Self> {
        // Ensure the list is not empty
        assert!(!authorities.is_empty());

        // Ensure all stakes are positive
        assert!(authorities.iter().all(|stake| *stake > 0));

        let total_stake: Stake = authorities.iter().sum();
        let validity_threshold = total_stake / 3;
        let quorum_threshold = 2 * total_stake / 3;
        Arc::new(Committee {
            committee_id,
            authorities,
            validity_threshold,
            quorum_threshold,
        })
    }

    pub fn get_authorities(&self) -> &Vec<Stake> {
        &self.authorities
    }

    pub fn is_valid(&self, amount: Stake) -> bool {
        amount > *self.get_validity_threshold()
    }

    pub fn is_quorum(&self, amount: Stake) -> bool {
        amount > *self.get_quorum_threshold()
    }

    pub fn get_validity_threshold(&self) -> &Stake {
        &self.validity_threshold
    }

    pub fn get_quorum_threshold(&self) -> &Stake {
        &self.quorum_threshold
    }

    pub fn get_rich_authority(self: &Arc<Self>, index: usize) -> RichAuthority {
        RichAuthority {
            index,
            committee: self.clone(),
        }
    }

    /// Take a list of rich authorities and return the total stake to which they
    /// correspond in the committee.
    pub fn get_total_stake(&self, authorities: &HashSet<RichAuthority>) -> Stake {
        let mut total_stake = 0;
        for authority in authorities {
            total_stake += self.authorities[authority.index];
        }
        total_stake
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// Write a test that makes a committee with 4 authorities each with Stake 1
    #[test]
    fn test_committee() {
        let committee = Committee::new(0, vec![1, 1, 1, 1]);
        assert_eq!(committee.get_authorities().len(), 4);
        assert_eq!(*committee.get_validity_threshold(), 1);
        assert_eq!(*committee.get_quorum_threshold(), 2);

        // Check is_valid and is_quorum
        assert!(committee.is_valid(2));
        assert!(committee.is_quorum(3));
        assert!(!committee.is_valid(1));
        assert!(!committee.is_quorum(2));
    }

    /// Make a committee of 4 authorities, each with Stake 1, and check that you get authority 1
    /// and equality when you clone it holds.
    #[test]
    fn test_rich_authority() {
        let committee = Committee::new(0, vec![1, 1, 1, 1]);
        let rich_authority = committee.get_rich_authority(1);
        let rich_authority_clone = rich_authority.clone();
        assert!(rich_authority == rich_authority_clone);
    }

    // Test get_total_stake: make a committee of 4 authorities, each with Stake 1. Then get a list of them, with repetitions. And calculate the total stake. Check that the result is the same without repetitions.
    #[test]
    fn test_get_total_stake() {
        let committee = Committee::new(0, vec![1, 1, 1, 1]);
        let rich_authority = committee.get_rich_authority(0);
        let rich_authority_clone = rich_authority.clone();
        let rich_authority2 = committee.get_rich_authority(1);
        let rich_authority3 = committee.get_rich_authority(2);
        let rich_authority4 = committee.get_rich_authority(3);
        let authorities = vec![
            rich_authority,
            rich_authority_clone,
            rich_authority2,
            rich_authority3,
            rich_authority4,
        ];
        let authorities = authorities.into_iter().collect();
        assert_eq!(committee.get_total_stake(&authorities), 4);
    }

    // Make two different committees and check that the equality of rich authorities does not hold.
    #[test]
    fn test_rich_authority_equality() {
        let committee = Committee::new(0, vec![1, 1, 1, 1]);
        let committee2 = Committee::new(1, vec![1, 1, 1, 1]);
        let rich_authority = committee.get_rich_authority(0);
        let rich_authority2 = committee2.get_rich_authority(0);
        assert!(rich_authority != rich_authority2);

        // But for a new committee with the same committee id, the equality should hold.
        let committee3 = Committee::new(0, vec![1, 1, 1, 1]);
        let rich_authority3 = committee3.get_rich_authority(0);
        assert!(rich_authority == rich_authority3);
    }
}
