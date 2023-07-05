// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::types::Transaction;
use blake2::Blake2b;
use digest::Digest;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize, Default, Hash)]
pub struct TransactionHash([u8; 32]); // todo - serialize performance

type TransactionHasher = Blake2b<digest::consts::U32>;

impl TransactionHash {
    pub fn new(transaction: &Transaction) -> Self {
        Self::from(transaction.data())
    }

    pub fn from(data: &[u8]) -> Self {
        let mut hasher = TransactionHasher::default();
        hasher.update(data);
        Self(hasher.finalize().into())
    }
}

impl fmt::Debug for TransactionHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", hex::encode(self.0))
    }
}

impl fmt::Display for TransactionHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", hex::encode(&self.0[..4]))
    }
}
