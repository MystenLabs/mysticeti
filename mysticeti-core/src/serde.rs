// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::de;
use std::fmt;
use std::marker::PhantomData;

pub trait ByteRepr: Sized {
    fn try_copy_from_slice<E: de::Error>(v: &[u8]) -> Result<Self, E>;

    fn try_from_vec<E: de::Error>(v: Vec<u8>) -> Result<Self, E> {
        Self::try_copy_from_slice(&v)
    }
}

pub struct BytesVisitor<T>(PhantomData<T>);

impl<T> BytesVisitor<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<'de, T: ByteRepr> de::Visitor<'de> for BytesVisitor<T> {
    type Value = T;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("byte slice")
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        T::try_copy_from_slice(v.as_bytes())
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        T::try_copy_from_slice(v)
    }

    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
        T::try_copy_from_slice(v)
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        T::try_from_vec(v)
    }
}
