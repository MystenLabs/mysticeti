// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub use crate::future_simulator::JoinError;
pub use crate::future_simulator::JoinHandle;
use crate::future_simulator::{SimulatorContext, Sleep};
use std::future::Future;
use std::time::Duration;

pub struct Handle;

use crate::future_simulator::simulator_spawn;

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

#[derive(Clone)]
pub struct TimeInstant(Duration);

impl TimeInstant {
    pub fn now() -> Self {
        Self(SimulatorContext::time())
    }

    pub fn elapsed(&self) -> Duration {
        SimulatorContext::time() - self.0
    }
}

pub fn timestamp_utc() -> Duration {
    SimulatorContext::time()
}

#[allow(dead_code)]
pub struct TimeInterval(Duration);

#[allow(dead_code)]
impl TimeInterval {
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }

    pub async fn tick(&mut self) -> TimeInstant {
        sleep(self.0);
        TimeInstant::now()
    }
}
