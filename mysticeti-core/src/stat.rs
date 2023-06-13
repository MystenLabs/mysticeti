// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{AddAssign, Div};

#[derive(Default)]
pub struct PreciseHistogram<T> {
    points: Vec<T>,
    sum: T,
}

impl<T: Ord + AddAssign + Div<u32, Output = T> + Copy> PreciseHistogram<T> {
    pub fn observe(&mut self, point: T) {
        self.points.push(point);
        self.sum += point;
    }

    pub fn avg(&self) -> Option<T> {
        if self.points.len() == 0 {
            return None;
        }
        Some(self.sum / self.points.len() as u32)
    }

    pub fn pct(&self, pct1000: usize) -> Option<T> {
        if self.points.is_empty() {
            return None;
        }
        let mut points = self.points.clone();
        points.sort();
        Some(*points.get(self.pct1000_index(pct1000)).unwrap())
    }

    fn pct1000_index(&self, pct1000: usize) -> usize {
        debug_assert!(pct1000 < 1000);
        self.points.len() * pct1000 / 1000
    }
}
