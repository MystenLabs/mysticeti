// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(not(feature = "simulator"))]
mod simulated;
#[cfg(feature = "simulator")]
mod spawned;

#[cfg(feature = "simulator")]
pub use spawned::*;

#[cfg(not(feature = "simulator"))]
pub use simulated::*;
