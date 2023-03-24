pub type Authority = u16;
pub type Transaction = u64;
pub type TransactionId = u64;
pub type SequenceNumber = u64;
pub type SequenceDigest = u64;
pub type BlockReference = (Authority, SequenceNumber, SequenceDigest);
pub type Signature = u64;


#[derive(Clone)]
pub enum Vote {
    Accept,
    Reject(Option<TransactionId>),
}

#[derive(Clone)]
pub enum BaseStatement {
    /// Authority Shares a transactions, without accepting it or not.
    Share(TransactionId, Transaction),
    /// Authority votes to accept or reject a transaction.
    Vote(TransactionId, Vote),
}

#[derive(Clone)]
pub enum MetaStatement {
    /// State a base statement
    Base(BaseStatement),
    // Include statements from another authority, by reference
    Include(BlockReference),
}

#[derive(Default, Clone)]
pub struct MetaStatementBlock(BlockReference, Vec<MetaStatement>, Signature);

impl MetaStatementBlock {
    #[cfg(test)]
    pub fn new_for_testing(authority: Authority, sequence: SequenceNumber) -> Self {
        MetaStatementBlock((authority, sequence, 0), vec![], 0)
    }

    pub fn get_authority(&self) -> &Authority {
        &self.0 .0
    }

    pub fn get_includes(&self) -> Vec<&BlockReference> {
        self.1
            .iter()
            .filter_map(|item| {
                if let MetaStatement::Include(reference) = item {
                    Some(reference)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_reference(&self) -> &BlockReference {
        &self.0
    }

    pub fn all_items(&self) -> &Vec<MetaStatement> {
        &self.1
    }

    pub fn into_include(&self) -> MetaStatement {
        MetaStatement::Include(*self.get_reference())
    }

    pub fn extend_with(mut self, item: MetaStatement) -> Self {
        self.1.push(item);
        self
    }

    pub fn extend_inplace(&mut self, item: MetaStatement) {
        self.1.push(item);
    }
}
