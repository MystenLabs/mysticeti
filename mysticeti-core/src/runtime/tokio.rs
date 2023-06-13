// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};
pub use tokio::runtime::Handle;
pub use tokio::task::JoinError;
pub use tokio::task::JoinHandle;
pub use tokio::time::sleep;

#[allow(dead_code)]
pub struct TimeInstant(Instant);

#[allow(dead_code)]
impl TimeInstant {
    pub fn now() -> Self {
        Self(Instant::now())
    }

    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}
