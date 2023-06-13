// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
#[cfg(feature = "simulator")]
mod simulated;
#[path = "tokio.rs"]
mod tokio_mod;

#[cfg(not(feature = "simulator"))]
pub use tokio_mod::*;

#[cfg(feature = "simulator")]
pub use simulated::*;
