// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "simulator")]
mod simulated;
#[cfg(not(feature = "simulator"))]
mod spawned;

#[cfg(not(feature = "simulator"))]
pub use spawned::*;

#[cfg(feature = "simulator")]
pub use simulated::*;
