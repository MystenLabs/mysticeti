// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::serde::{ByteRepr, BytesVisitor};
use crate::types::{
    AuthorityIndex, BaseStatement, BlockReference, RoundNumber, TimestampNs, Transaction, Vote,
};
use blake2::Blake2b;
use digest::Digest;
use ed25519_consensus::Signature;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

pub const SIGNATURE_SIZE: usize = 64;
pub const TRANSACTION_DIGEST_SIZE: usize = 32;
pub const BLOCK_DIGEST_SIZE: usize = 32;

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Default, Hash)]
pub struct TransactionDigest([u8; TRANSACTION_DIGEST_SIZE]);

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Default, Hash)]
pub struct BlockDigest([u8; BLOCK_DIGEST_SIZE]);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct PublicKey(ed25519_consensus::VerificationKey);

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Hash)]
pub struct SignatureBytes([u8; SIGNATURE_SIZE]);

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
            hasher.update(include.digest);
        }
        for statement in statements {
            match statement {
                BaseStatement::Share(id, _) => {
                    hasher.update([0]);
                    hasher.update(id);
                }
                BaseStatement::Vote(id, Vote::Accept) => {
                    hasher.update([1]);
                    hasher.update(id);
                }
                BaseStatement::Vote(id, Vote::Reject(None)) => {
                    hasher.update([2]);
                    hasher.update(id);
                }
                BaseStatement::Vote(id, Vote::Reject(Some(other))) => {
                    hasher.update([3]);
                    hasher.update(id);
                    hasher.update(other);
                }
            }
        }
        hasher.update(meta_creation_time_ns.to_le_bytes());
        Self(hasher.finalize().into())
    }
}

impl PublicKey {
    pub fn verify_block(
        &self,
        digest: BlockDigest,
        signature: SignatureBytes,
    ) -> Result<(), ed25519_consensus::Error> {
        let signature = Signature::from(signature.0);
        self.0.verify(&signature, digest.as_ref())
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

impl fmt::Debug for BlockDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", hex::encode(self.0))
    }
}

impl fmt::Display for BlockDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", hex::encode(&self.0[..4]))
    }
}

impl Default for SignatureBytes {
    fn default() -> Self {
        Self([0u8; 64])
    }
}

impl ByteRepr for SignatureBytes {
    fn try_copy_from_slice<E: de::Error>(v: &[u8]) -> Result<Self, E> {
        if v.len() != SIGNATURE_SIZE {
            return Err(E::custom(format!("Invalid signature length: {}", v.len())));
        }
        let mut inner = [0u8; SIGNATURE_SIZE];
        inner.copy_from_slice(v);
        Ok(Self(inner))
    }
}

impl Serialize for SignatureBytes {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for SignatureBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(BytesVisitor::new())
    }
}

impl ByteRepr for TransactionDigest {
    fn try_copy_from_slice<E: de::Error>(v: &[u8]) -> Result<Self, E> {
        if v.len() != TRANSACTION_DIGEST_SIZE {
            return Err(E::custom(format!(
                "Invalid transaction digest length: {}",
                v.len()
            )));
        }
        let mut inner = [0u8; TRANSACTION_DIGEST_SIZE];
        inner.copy_from_slice(v);
        Ok(Self(inner))
    }
}

impl Serialize for TransactionDigest {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for TransactionDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(BytesVisitor::new())
    }
}

impl ByteRepr for BlockDigest {
    fn try_copy_from_slice<E: de::Error>(v: &[u8]) -> Result<Self, E> {
        if v.len() != BLOCK_DIGEST_SIZE {
            return Err(E::custom(format!(
                "Invalid block digest length: {}",
                v.len()
            )));
        }
        let mut inner = [0u8; BLOCK_DIGEST_SIZE];
        inner.copy_from_slice(v);
        Ok(Self(inner))
    }
}

impl Serialize for BlockDigest {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for BlockDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(BytesVisitor::new())
    }
}

pub fn dummy_public_key() -> PublicKey {
    PublicKey(ed25519_consensus::SigningKey::from([0u8; 32]).verification_key())
}
