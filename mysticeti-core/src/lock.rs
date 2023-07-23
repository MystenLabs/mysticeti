// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::metrics::{UtilizationTimer, UtilizationTimerExt};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use prometheus::IntCounter;
use std::ops::{Deref, DerefMut};

pub struct MonitoredRwLock<V> {
    lock: RwLock<V>,
    wlock_util: IntCounter,
    wlock_wait: IntCounter,
}

impl<V> MonitoredRwLock<V> {
    pub fn new(v: V, wlock_util: IntCounter, wlock_wait: IntCounter) -> Self {
        let lock = RwLock::new(v);
        Self {
            lock,
            wlock_util,
            wlock_wait,
        }
    }

    pub fn write(&self) -> MonitoredRwLockWriteGuard<V> {
        let guard = {
            let _timer = self.wlock_wait.utilization_timer();
            self.lock.write()
        };
        let _timer = self.wlock_util.utilization_timer();
        MonitoredRwLockWriteGuard { guard, _timer }
    }

    pub fn read(&self) -> RwLockReadGuard<V> {
        self.lock.read()
    }

    pub fn into_inner(self) -> V {
        self.lock.into_inner()
    }
}

pub struct MonitoredRwLockWriteGuard<'a, V> {
    guard: RwLockWriteGuard<'a, V>,
    _timer: UtilizationTimer<'a>,
}

impl<'a, V> Deref for MonitoredRwLockWriteGuard<'a, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, V> DerefMut for MonitoredRwLockWriteGuard<'a, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}
