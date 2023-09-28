// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use minibytes::Bytes;
use serde::de::{DeserializeOwned, Error};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Data<T> carries both the value and it's serialized bytes.
/// When Data is created, it's value is serialized into a cache variable.
/// When Data is serialized, instead of serializing a value we use a cached serialized bytes.
/// When Data is deserialized, cache is initialized with the bytes that used to deserialized value.
///
/// Note that cache always stores data serialized in a single format (bincode).
/// When data is serialized, instead of serializing the value, the byte array is written into target serializer.
/// This means that serialize(T) != serialize(Data<T>), e.g. Data<T> is not a transparent wrapper.
#[derive(Clone)]
pub struct Data<T>(Arc<DataInner<T>>);

struct DataInner<T> {
    t: T,
    serialized: Bytes, // this is serialized as bincode regardless of underlining serialization
}

pub static IN_MEMORY_BLOCKS: AtomicUsize = AtomicUsize::new(0);
pub static IN_MEMORY_BLOCKS_BYTES: AtomicUsize = AtomicUsize::new(0);

impl<T: Serialize + DeserializeOwned> Data<T> {
    pub fn new(t: T) -> Self {
        let serialized = bincode::serialize(&t).expect("Serialization should not fail");
        let serialized: Bytes = serialized.into();
        IN_MEMORY_BLOCKS.fetch_add(1, Ordering::Relaxed);
        IN_MEMORY_BLOCKS_BYTES.fetch_add(serialized.len(), Ordering::Relaxed);
        Self(Arc::new(DataInner { t, serialized }))
    }

    // Important - use Data::from_bytes,
    // rather then Data::deserialize to avoid mem copy of serialized representation
    pub fn from_bytes(bytes: Bytes) -> bincode::Result<Self> {
        IN_MEMORY_BLOCKS.fetch_add(1, Ordering::Relaxed);
        IN_MEMORY_BLOCKS_BYTES.fetch_add(bytes.len(), Ordering::Relaxed);
        let t = bincode::deserialize(&bytes)?;
        let inner = DataInner {
            t,
            serialized: bytes,
        };
        Ok(Self(Arc::new(inner)))
    }

    pub fn serialized_bytes(&self) -> &Bytes {
        &self.0.serialized
    }
}

impl<T> Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.t
    }
}

impl<T: Serialize> Serialize for Data<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0.serialized)
    }
}

impl<T> Drop for DataInner<T> {
    fn drop(&mut self) {
        IN_MEMORY_BLOCKS.fetch_sub(1, Ordering::Relaxed);
        IN_MEMORY_BLOCKS_BYTES.fetch_sub(self.serialized.len(), Ordering::Relaxed);
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Data<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized = Vec::<u8>::deserialize(deserializer)?;
        let Ok(t) = bincode::deserialize(&serialized) else {
            return Err(D::Error::custom("Failed to deserialized inner bytes"));
        };
        IN_MEMORY_BLOCKS.fetch_add(1, Ordering::Relaxed);
        IN_MEMORY_BLOCKS_BYTES.fetch_add(serialized.len(), Ordering::Relaxed);
        let serialized = serialized.into();
        Ok(Self(Arc::new(DataInner { t, serialized })))
    }
}

impl<T: fmt::Debug> fmt::Debug for Data<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.t.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for Data<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.t.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for Data<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.t == other.0.t
    }
}

impl<T: Eq> Eq for Data<T> {}

impl<T: Hash> Hash for Data<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.t.hash(state)
    }
}
