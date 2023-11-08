// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;

use crate::types::StatementBlock;

/// The interfaces to verify the legitimacy of a statement block's contents.
#[async_trait]
pub trait BlockVerifier: Send + Sync + 'static {
    type Error: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static;
    /// Determines if a statement block's content is valid.
    async fn verify(&self, b: &StatementBlock) -> Result<(), Self::Error>;
}

/// Simple validator that accepts all transactions and batches.
#[derive(Clone)]
pub struct AcceptAllBlockVerifier;

#[async_trait]
impl BlockVerifier for AcceptAllBlockVerifier {
    type Error = eyre::Report;

    async fn verify(&self, _b: &StatementBlock) -> Result<(), Self::Error> {
        Ok(())
    }
}
