// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub use crate::v2::future_simulator::JoinError;
pub use crate::v2::future_simulator::JoinHandle;
use crate::v2::future_simulator::Sleep;
use std::future::Future;
use std::time::Duration;

pub struct Handle;

use crate::v2::future_simulator::simulator_spawn;

impl Handle {
    pub fn current() -> Self {
        Self
    }

    pub fn spawn<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
        &self,
        f: F,
    ) -> JoinHandle<R> {
        simulator_spawn(f)
    }
}

pub fn sleep(duration: Duration) -> Sleep {
    Sleep::new(duration)
}
