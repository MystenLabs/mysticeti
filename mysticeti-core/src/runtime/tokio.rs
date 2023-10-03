// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, SystemTime};
pub use tokio::runtime::Handle;
pub use tokio::task::JoinError;
pub use tokio::task::JoinHandle;
pub use tokio::time::{sleep, Instant};
use tokio::time::{Interval, MissedTickBehavior};

#[allow(dead_code)]
#[derive(Clone)]
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

#[allow(dead_code)]
pub fn timestamp_utc() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

#[allow(dead_code)]
pub struct TimeInterval(Interval);

#[allow(dead_code)]
impl TimeInterval {
    pub fn new(duration: Duration) -> Self {
        let mut interval = tokio::time::interval(duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Burst);
        Self(interval)
    }

    pub async fn tick(&mut self) -> TimeInstant {
        TimeInstant(self.0.tick().await)
    }
}
