// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::types::{
    AuthorityIndex, BaseStatement, BlockReference, RoundNumber, TimestampNs, Transaction, Vote,
};
use blake2::Blake2b;
use digest::Digest;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize, Default, Hash)]
pub struct TransactionDigest([u8; 32]); // todo - serialize performance

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize, Default, Hash)]
pub struct BlockDigest([u8; 32]); // todo - serialize performance

type TransactionHasher = Blake2b<digest::consts::U32>;
type BlockHasher = Blake2b<digest::consts::U32>;

impl TransactionDigest {
    pub fn new(transaction: &Transaction) -> Self {
        Self::from(transaction.data())
    }

    pub fn from(data: &[u8]) -> Self {
        let mut hasher = TransactionHasher::default();
        hasher.update(data);
        Self(hasher.finalize().into())
    }
}

impl BlockDigest {
    pub fn new(
        authority: AuthorityIndex,
        round: RoundNumber,
        includes: &[BlockReference],
        statements: &[BaseStatement],
        meta_creation_time_ns: TimestampNs,
    ) -> Self {
        let mut hasher = BlockHasher::default();
        hasher.update(authority.to_le_bytes());
        hasher.update(round.to_le_bytes());
        // todo - we might want a generic interface to hash BlockReference/BaseStatement
        for include in includes {
            hasher.update(include.authority.to_le_bytes());
            hasher.update(include.round.to_le_bytes());
            hasher.update(include.digest.as_ref());
        }
        for statement in statements {
            match statement {
                BaseStatement::Share(id, _) => {
                    hasher.update([0]);
                    hasher.update(id.as_ref());
                }
                BaseStatement::Vote(id, Vote::Accept) => {
                    hasher.update([1]);
                    hasher.update(id.as_ref());
                }
                BaseStatement::Vote(id, Vote::Reject(None)) => {
                    hasher.update([2]);
                    hasher.update(id.as_ref());
                }
                BaseStatement::Vote(id, Vote::Reject(Some(other))) => {
                    hasher.update([3]);
                    hasher.update(id.as_ref());
                    hasher.update(other.as_ref());
                }
            }
        }
        hasher.update(meta_creation_time_ns.to_le_bytes());
        Self(hasher.finalize().into())
    }
}

impl AsRef<[u8]> for BlockDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for TransactionDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for TransactionDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", hex::encode(self.0))
    }
}

impl fmt::Display for TransactionDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", hex::encode(&self.0[..4]))
    }
}
