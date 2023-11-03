// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::serde::{ByteRepr, BytesVisitor};
#[cfg(not(test))]
use crate::types::Vote;
use crate::types::{
    AuthorityIndex, BaseStatement, BlockReference, EpochStatus, RoundNumber, StatementBlock,
    TimestampNs,
};
use digest::Digest;
#[cfg(not(test))]
use ed25519_consensus::Signature;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use zeroize::Zeroize;

pub const SIGNATURE_SIZE: usize = 64;
pub const BLOCK_DIGEST_SIZE: usize = 32;

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Default, Hash)]
pub struct BlockDigest([u8; BLOCK_DIGEST_SIZE]);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct PublicKey(pub ed25519_consensus::VerificationKey);

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Hash)]
pub struct SignatureBytes([u8; SIGNATURE_SIZE]);

// Box ensures value is not copied in memory when Signer itself is moved around for better security
pub struct Signer(pub Box<ed25519_consensus::SigningKey>);

#[cfg(not(test))]
type BlockHasher = blake2::Blake2b<digest::consts::U32>;

impl BlockDigest {
    #[cfg(not(test))]
    pub fn new(
        authority: AuthorityIndex,
        round: RoundNumber,
        includes: &[BlockReference],
        statements: &[BaseStatement],
        meta_creation_time_ns: TimestampNs,
        epoch_marker: EpochStatus,
        signature: &SignatureBytes,
    ) -> Self {
        let mut hasher = BlockHasher::default();
        Self::digest_without_signature(
            &mut hasher,
            authority,
            round,
            includes,
            statements,
            meta_creation_time_ns,
            epoch_marker,
        );
        hasher.update(signature);
        Self(hasher.finalize().into())
    }

    #[cfg(test)]
    pub fn new(
        _authority: AuthorityIndex,
        _round: RoundNumber,
        _includes: &[BlockReference],
        _statements: &[BaseStatement],
        _meta_creation_time_ns: TimestampNs,
        _epoch_marker: EpochStatus,
        _signature: &SignatureBytes,
    ) -> Self {
        Default::default()
    }

    /// There is a bit of a complexity around what is considered block digest and what is being signed
    ///
    /// * Block signature covers all the fields in the block, except for signature and reference.digest
    /// * Block digest(e.g. block.reference.digest) covers all the above **and** block signature
    ///
    /// This is not very beautiful, but it allows to optimize block synchronization,
    /// by skipping signature verification for all the descendants of the certified block.
    #[cfg(not(test))]
    fn digest_without_signature(
        hasher: &mut BlockHasher,
        authority: AuthorityIndex,
        round: RoundNumber,
        includes: &[BlockReference],
        statements: &[BaseStatement],
        meta_creation_time_ns: TimestampNs,
        epoch_marker: EpochStatus,
    ) {
        authority.crypto_hash(hasher);
        round.crypto_hash(hasher);
        for include in includes {
            include.crypto_hash(hasher);
        }
        for statement in statements {
            match statement {
                BaseStatement::Share(tx) => {
                    [0].crypto_hash(hasher);
                    tx.crypto_hash(hasher);
                }
                BaseStatement::Vote(id, Vote::Accept) => {
                    [1].crypto_hash(hasher);
                    id.crypto_hash(hasher);
                }
                BaseStatement::Vote(id, Vote::Reject(None)) => {
                    [2].crypto_hash(hasher);
                    id.crypto_hash(hasher);
                }
                BaseStatement::Vote(id, Vote::Reject(Some(other))) => {
                    [3].crypto_hash(hasher);
                    id.crypto_hash(hasher);
                    other.crypto_hash(hasher);
                }
                BaseStatement::VoteRange(range) => {
                    [4].crypto_hash(hasher);
                    range.crypto_hash(hasher);
                }
            }
        }
        meta_creation_time_ns.crypto_hash(hasher);
        epoch_marker.crypto_hash(hasher);
    }
}

pub trait AsBytes {
    // This is pretty much same as AsRef<[u8]>
    //
    // We need this separate trait because we want to impl CryptoHash
    // for primitive types(u64, etc) and types like XxxDigest that implement AsRef<[u8]>.
    //
    // Rust unfortunately does not allow to impl trait for AsRef<[u8]> and primitive types like u64.
    //
    // While AsRef<[u8]> is not implemented for u64, it seem to be reserved in compiler,
    // so `impl CryptoHash for u64` and `impl<T: AsRef<[u8]>> CryptoHash for T` collide.
    fn as_bytes(&self) -> &[u8];
}

impl<const N: usize> AsBytes for [u8; N] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

pub trait CryptoHash {
    fn crypto_hash(&self, state: &mut impl Digest);
}

impl CryptoHash for u64 {
    fn crypto_hash(&self, state: &mut impl Digest) {
        state.update(self.to_be_bytes());
    }
}

impl CryptoHash for u128 {
    fn crypto_hash(&self, state: &mut impl Digest) {
        state.update(self.to_be_bytes());
    }
}

impl<T: AsBytes> CryptoHash for T {
    fn crypto_hash(&self, state: &mut impl Digest) {
        state.update(self.as_bytes());
    }
}

impl PublicKey {
    #[cfg(not(test))]
    pub fn verify_block(&self, block: &StatementBlock) -> Result<(), ed25519_consensus::Error> {
        let signature = Signature::from(block.signature().0);
        let mut hasher = BlockHasher::default();
        BlockDigest::digest_without_signature(
            &mut hasher,
            block.author(),
            block.round(),
            block.includes(),
            block.statements(),
            block.meta_creation_time_ns(),
            block.epoch_changed(),
        );
        let digest: [u8; BLOCK_DIGEST_SIZE] = hasher.finalize().into();
        self.0.verify(&signature, digest.as_ref())
    }

    #[cfg(test)]
    pub fn verify_block(&self, _block: &StatementBlock) -> Result<(), ed25519_consensus::Error> {
        Ok(())
    }
}

impl Signer {
    #[cfg(not(test))]
    pub fn sign_block(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
        includes: &[BlockReference],
        statements: &[BaseStatement],
        meta_creation_time_ns: TimestampNs,
        epoch_marker: EpochStatus,
    ) -> SignatureBytes {
        let mut hasher = BlockHasher::default();
        BlockDigest::digest_without_signature(
            &mut hasher,
            authority,
            round,
            includes,
            statements,
            meta_creation_time_ns,
            epoch_marker,
        );
        let digest: [u8; BLOCK_DIGEST_SIZE] = hasher.finalize().into();
        let signature = self.0.sign(digest.as_ref());
        SignatureBytes(signature.to_bytes())
    }

    #[cfg(test)]
    pub fn sign_block(
        &self,
        _authority: AuthorityIndex,
        _round: RoundNumber,
        _includes: &[BlockReference],
        _statements: &[BaseStatement],
        _meta_creation_time_ns: TimestampNs,
        _epoch_marker: EpochStatus,
    ) -> SignatureBytes {
        Default::default()
    }

    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.0.verification_key())
    }
}

impl AsRef<[u8]> for BlockDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for SignatureBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsBytes for BlockDigest {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsBytes for SignatureBytes {
    fn as_bytes(&self) -> &[u8] {
        &self.0
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

impl fmt::Debug for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signer(public_key={:?})", self.public_key())
    }
}

impl fmt::Display for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signer(public_key={:?})", self.public_key())
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

impl Drop for Signer {
    fn drop(&mut self) {
        self.0.zeroize()
    }
}

pub fn dummy_signer() -> Signer {
    Signer(Box::new(ed25519_consensus::SigningKey::from([0u8; 32])))
}

pub fn dummy_public_key() -> PublicKey {
    dummy_signer().public_key()
}
