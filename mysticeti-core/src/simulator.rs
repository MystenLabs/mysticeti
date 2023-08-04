// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::prelude::StdRng;
use rand::SeedableRng;
use std::any::Any;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Duration;

pub struct Simulator<S: SimulatorState>
where
    S::Event: 'static,
{
    states: Vec<S>,
    time: Duration,
    events: BinaryHeap<ScheduledEvent<S::Event>>,
    rng: Option<StdRng>,
}

pub struct Scheduler<E> {
    events: Vec<ScheduledEvent<E>>,
    rng: StdRng,
    time: Duration,
}

pub trait SimulatorState {
    type Event;

    fn handle_event(&mut self, event: Self::Event);
}

impl<S: SimulatorState + 'static> Simulator<S>
where
    S::Event: 'static,
{
    pub fn new(states: Vec<S>, rng: StdRng) -> Self {
        Self {
            states,
            time: Default::default(),
            events: Default::default(),
            rng: Some(rng),
        }
    }

    pub fn schedule_event(&mut self, after: Duration, state: usize, event: S::Event) {
        self.events.push(ScheduledEvent {
            time: self.time.saturating_add(after),
            state,
            event,
        });
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

    #[allow(dead_code)]
    pub fn states_mut(&mut self) -> &mut [S] {
        &mut self.states
    }

    pub fn time(&self) -> Duration {
        self.time
    }

    fn run_event(&mut self, state: usize, event: S::Event) {
        Scheduler::<S::Event>::enter(self.time, self.rng.take().unwrap());
        // Exiting scheduler in the drop handler to make sure it rungs even if handle_event panics
        let guard = SchedulerEnterGuard { simulator: self };
        let state = &mut guard.simulator.states[state];
        state.handle_event(event);
    }
}

struct SchedulerEnterGuard<'a, E: SimulatorState + 'static> {
    simulator: &'a mut Simulator<E>,
}

impl<'a, E: SimulatorState + 'static> Drop for SchedulerEnterGuard<'a, E> {
    fn drop(&mut self) {
        let (events, rng) = Scheduler::exit();
        self.simulator.rng = Some(rng);
        self.simulator.events.extend(events);
    }
}

thread_local! {
    static SCHEDULER: RefCell<Option<Box<dyn Any>>> = RefCell::new(None);
}

impl<E: 'static> Scheduler<E> {
    pub fn schedule_event(after: Duration, state: usize, event: E) -> Duration {
        Self::with(|scheduler| {
            let time = scheduler.time.saturating_add(after);
            scheduler.events.push(ScheduledEvent { time, state, event });
            time
        })
    }

    pub fn with_rng<R, F: FnOnce(&mut StdRng) -> R>(f: F) -> R {
        Self::with(|scheduler| f(&mut scheduler.rng))
    }

    #[allow(dead_code)]
    pub fn time() -> Duration {
        Self::with(|scheduler| scheduler.time)
    }

    fn with<R, F: FnOnce(&mut Self) -> R>(f: F) -> R {
        SCHEDULER.with(|cell| {
            let mut o = cell.borrow_mut();
            let o = o.as_mut().unwrap();
            f(o.downcast_mut().unwrap())
        })
    }

    fn enter(time: Duration, rng: StdRng) {
        let scheduler = Self {
            time,
            rng,
            events: vec![],
        };
        SCHEDULER.with(|cell| {
            let mut o = cell.borrow_mut();
            assert!(o.is_none());
            *o = Some(Box::new(scheduler));
        });
    }

    fn exit() -> (Vec<ScheduledEvent<E>>, StdRng) {
        SCHEDULER.with(|cell| {
            let mut o = cell.borrow_mut();
            let scheduler = o.take().unwrap();
            let scheduler: Box<Scheduler<E>> = scheduler.downcast().unwrap();
            let scheduler = *scheduler;
            (scheduler.events, scheduler.rng)
        })
    }
}

impl<S: SimulatorState> Drop for Simulator<S>
where
    S::Event: 'static,
{
    /// Drop simulator states in the context of the scheduler
    /// This is mostly needed for future_simulator because some futures trigger waker while dropping
    fn drop(&mut self) {
        Scheduler::<S::Event>::enter(self.time, self.rng.take().unwrap());
        self.states.clear();
        Scheduler::<S::Event>::exit(); // all scheduled events are ignored
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

    impl SimulatorState for u64 {
        type Event = u64;

        fn handle_event(&mut self, event: Self::Event) {
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
