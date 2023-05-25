// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::prelude::StdRng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Duration;

pub struct Simulator<S: SimulatorState> {
    states: Vec<S>,
    time: Duration,
    events: BinaryHeap<ScheduledEvent<S::Event>>,
    rng: StdRng,
}

pub struct Scheduler<'a, E> {
    events: &'a mut BinaryHeap<ScheduledEvent<E>>,
    rng: &'a mut StdRng,
    time: Duration,
}

pub trait SimulatorState {
    type Event;

    fn handle_event(&mut self, scheduler: Scheduler<Self::Event>, event: Self::Event);
}

impl<S: SimulatorState> Simulator<S> {
    pub fn new(states: Vec<S>, rng: StdRng) -> Self {
        Self {
            states,
            time: Default::default(),
            events: Default::default(),
            rng,
        }
    }

    pub fn schedule_event(&mut self, after: Duration, state: usize, event: S::Event) {
        Scheduler {
            events: &mut self.events,
            time: self.time,
            rng: &mut self.rng,
        }
        .schedule_event(after, state, event);
    }

    /// returns true if complete
    pub fn run_one(&mut self) -> bool {
        if let Some(event) = self.events.pop() {
            self.time = event.time;
            self.run_event(event.state, event.event);
        }
        self.events.is_empty()
    }

    pub fn states(&self) -> &[S] {
        &self.states
    }

    pub fn time(&self) -> Duration {
        self.time
    }

    fn run_event(&mut self, state: usize, event: S::Event) {
        let scheduler = Scheduler {
            events: &mut self.events,
            time: self.time,
            rng: &mut self.rng,
        };
        let state = &mut self.states[state];
        state.handle_event(scheduler, event);
    }
}

impl<'a, E> Scheduler<'a, E> {
    pub fn schedule_event(&mut self, after: Duration, state: usize, event: E) {
        self.events.push(ScheduledEvent {
            time: self.time + after,
            state,
            event,
        });
    }

    #[allow(dead_code)]
    pub fn time_ms(&self) -> u128 {
        self.time.as_millis()
    }

    pub fn rng(&mut self) -> &mut StdRng {
        self.rng
    }
}

struct ScheduledEvent<E> {
    time: Duration,
    state: usize,
    event: E,
}

// PartialOrd and Ord are implemented as inverse order on self.time
// This is because rust's BinaryHeap is a max heap, and we care about events with lowest time
impl<E> PartialOrd for ScheduledEvent<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.time.partial_cmp(&self.time)
    }
}

impl<E> PartialEq for ScheduledEvent<E> {
    fn eq(&self, other: &Self) -> bool {
        self.time.eq(&other.time)
    }
}

impl<E> Ord for ScheduledEvent<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.cmp(&self.time)
    }
}

impl<E> Eq for ScheduledEvent<E> {}

#[cfg(test)]
mod test {
    use super::*;
    use rand::SeedableRng;

    impl SimulatorState for u64 {
        type Event = u64;

        fn handle_event(&mut self, _scheduler: Scheduler<Self::Event>, event: Self::Event) {
            *self += event;
        }
    }

    #[test]
    pub fn test_simulator() {
        let mut simulator = Simulator::new(vec![0u64, 0u64], StdRng::from_seed(Default::default()));
        simulator.schedule_event(Duration::from_secs(3), 0, 2);
        simulator.schedule_event(Duration::from_secs(1), 1, 4);
        simulator.schedule_event(Duration::from_secs(4), 0, 8);

        assert!(!simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(1));
        assert_eq!(simulator.states, vec![0, 4]);
        simulator.schedule_event(Duration::from_secs(1), 1, 1);

        assert!(!simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(2));
        assert_eq!(simulator.states, vec![0, 5]);

        assert!(!simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(3));
        assert_eq!(simulator.states, vec![2, 5]);

        assert!(simulator.run_one());
        assert_eq!(simulator.time, Duration::from_secs(4));
        assert_eq!(simulator.states, vec![10, 5]);
    }
}
