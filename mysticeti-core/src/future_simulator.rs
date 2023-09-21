// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::simulator::{Scheduler, Simulator, SimulatorState};
use crate::types::AuthorityIndex;
use futures::FutureExt;
use rand::prelude::StdRng;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake};
use std::time::Duration;
use tokio::select;
use tokio::sync::{oneshot, Notify};

#[derive(Default)]
pub struct SimulatedExecutorState {
    tasks: HashMap<usize, Task>,
    next_id: usize,
}

pub struct JoinHandle<R> {
    ch: Pin<Box<oneshot::Receiver<Result<R, JoinError>>>>,
    abort: Arc<Notify>,
}

struct Task {
    f: Pin<Box<dyn Future<Output = ()>>>,
    node: Option<AuthorityIndex>,
}

impl SimulatedExecutorState {
    pub fn run<F: Future<Output = ()> + Send + 'static>(rng: StdRng, f: F) {
        let mut simulator = Self::new_simulator(rng);
        Self::block_on(&mut simulator, f);
    }

    fn new_simulator(rng: StdRng) -> Simulator<SimulatedExecutorState> {
        Simulator::new(vec![Self::default()], rng)
    }

    fn spawn_at<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
        simulator: &mut Simulator<SimulatedExecutorState>,
        f: F,
    ) -> JoinHandle<R> {
        let state = &mut simulator.states_mut()[0];
        let (task, join_handle) = make_task(f);
        let task = Task {
            f: task,
            node: None,
        };
        let task_id = state.create_task(task);
        // todo - randomize delay
        simulator.schedule_event(
            Duration::from_nanos(1),
            0,
            ExecutorStateEvent::Wake(task_id),
        );
        join_handle
    }

    /// Spawn the task on simulator and run simulator until it completes
    fn block_on<F: Future<Output = ()> + Send + 'static>(
        simulator: &mut Simulator<SimulatedExecutorState>,
        f: F,
    ) {
        let jh = Self::spawn_at(simulator, f);
        Self::run_until_complete(simulator, jh);
    }

    /// Run the simulation until the spawned task completes
    fn run_until_complete(
        simulator: &mut Simulator<SimulatedExecutorState>,
        mut jh: JoinHandle<()>,
    ) {
        loop {
            assert!(!simulator.run_one());
            match jh.check_complete() {
                Ok(()) => {
                    break;
                }
                Err(njh) => {
                    jh = njh;
                }
            }
        }
    }

    fn create_task(&mut self, task: Task) -> usize {
        let task_id = self.next_id;
        self.next_id += 1;
        let p = self.tasks.insert(task_id, task);
        assert!(p.is_none());
        task_id
    }
}

pub fn simulator_spawn<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
    f: F,
) -> JoinHandle<R> {
    SimulatorContext::with(|context| {
        let (task, join_handle) = make_task(f);
        let task = Task {
            f: task,
            node: context.current_node,
        };
        context.spawned.push(task);
        join_handle
    })
}

thread_local! {
    static CONTEXT: RefCell<Option<SimulatorContext>> = RefCell::new(None);
}

pub struct SimulatorContext {
    spawned: Vec<Task>,
    task_id: usize,
    current_node: Option<AuthorityIndex>,
}

impl SimulatorContext {
    pub fn new(task_id: usize, current_node: Option<AuthorityIndex>) -> Self {
        Self {
            spawned: Default::default(),
            task_id,
            current_node,
        }
    }

    pub fn with<R, F: FnOnce(&mut SimulatorContext) -> R>(f: F) -> R {
        CONTEXT.with(|ctx| {
            f(ctx
                .borrow_mut()
                .as_mut()
                .expect("Not running in simulator context"))
        })
    }

    pub fn task_id() -> usize {
        CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .expect("Not running in simulator context")
                .task_id
        })
    }

    pub fn current_node() -> Option<AuthorityIndex> {
        CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .expect("Not running in simulator context")
                .current_node
        })
    }

    pub fn enter(self) {
        CONTEXT.with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            assert!(ctx.is_none(), "Can not re-enter simulator context");
            *ctx = Some(self);
        })
    }

    pub fn exit() -> Self {
        CONTEXT.with(|ctx| {
            ctx.borrow_mut()
                .take()
                .expect("Not running in simulator context - can not exit")
        })
    }

    pub fn with_rng<R, F: FnOnce(&mut StdRng) -> R>(f: F) -> R {
        Scheduler::<ExecutorStateEvent>::with_rng(f)
    }

    #[allow(dead_code)]
    pub fn time() -> Duration {
        Scheduler::<ExecutorStateEvent>::time()
    }
}

fn make_task<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
    f: F,
) -> (Pin<Box<dyn Future<Output = ()>>>, JoinHandle<R>) {
    let (s, r) = oneshot::channel();
    let abort = Arc::new(Notify::new());
    let task = task(f, s, abort.clone());
    let join_handle = JoinHandle {
        ch: Box::pin(r),
        abort,
    };
    let task = task.boxed();
    (task, join_handle)
}

#[derive(Debug)]
pub struct JoinError;

async fn task<F: Future>(
    f: F,
    ch: oneshot::Sender<Result<F::Output, JoinError>>,
    abort: Arc<Notify>,
) {
    select! {
        r = f => {
            ch.send(Ok(r)).ok();
        }
        _ = abort.notified() => {
            ch.send(Err(JoinError)).ok();
        }
    }
}

impl<R> JoinHandle<R> {
    pub fn abort(&self) {
        self.abort.notify_one();
    }

    /// todo - this fn is not great(does not check try_recv error type), need to be rewritten
    // This is organized as consume/return self to avoid re-entrance on success since channel state is mutated in this case
    fn check_complete(mut self) -> Result<(), Self> {
        if self.ch.try_recv().is_ok() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl<R> Future for JoinHandle<R> {
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.ch.as_mut().poll(cx).map(|res| match res {
            Ok(res) => res,
            Err(_) => Err(JoinError),
        })
    }
}

pub enum ExecutorStateEvent {
    Wake(usize),
}

impl SimulatorState for SimulatedExecutorState {
    type Event = ExecutorStateEvent;

    fn handle_event(&mut self, event: Self::Event) {
        match event {
            ExecutorStateEvent::Wake(task_id) => {
                let Entry::Occupied(mut oc) = self.tasks.entry(task_id) else {
                    return;
                };
                let waker = Arc::new(Waker(task_id));
                let waker = waker.into();
                let mut context = Context::from_waker(&waker);
                let task = oc.get_mut();
                SimulatorContext::new(task_id, task.node).enter();
                if let Poll::Ready(()) = task.f.as_mut().poll(&mut context) {
                    oc.remove();
                }
                let context = SimulatorContext::exit();
                for task in context.spawned {
                    let id = self.create_task(task);
                    // todo - randomize scheduling
                    Scheduler::schedule_event(
                        Duration::from_nanos(id as u64),
                        0,
                        ExecutorStateEvent::Wake(id),
                    );
                }
            }
        }
    }
}

struct Waker(usize);

impl Wake for Waker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // todo - randomize scheduling
        Scheduler::schedule_event(
            Duration::from_nanos(self.0 as u64),
            0,
            ExecutorStateEvent::Wake(self.0),
        );
    }
}

pub enum Sleep {
    Created(Duration),
    WaitingUntil(Duration),
}

impl Sleep {
    pub fn new(duration: Duration) -> Self {
        Self::Created(duration)
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ptr = self.get_mut();
        match ptr {
            Sleep::Created(duration) => {
                let ready = Scheduler::schedule_event(
                    *duration,
                    0,
                    ExecutorStateEvent::Wake(SimulatorContext::task_id()),
                );
                *ptr = Sleep::WaitingUntil(ready);
                Poll::Pending
            }
            Sleep::WaitingUntil(deadline) => {
                if Scheduler::<ExecutorStateEvent>::time() >= *deadline {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

pub struct OverrideNodeContext {
    prev: Option<AuthorityIndex>,
}

impl OverrideNodeContext {
    pub fn enter(new: Option<AuthorityIndex>) -> Self {
        let prev = CONTEXT.with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            let ctx = ctx.as_mut().expect("Not running in simulator context");
            let prev = ctx.current_node;
            ctx.current_node = new;
            prev
        });
        Self { prev }
    }
}

impl Drop for OverrideNodeContext {
    fn drop(&mut self) {
        CONTEXT.with(|ctx| {
            let mut ctx = ctx.borrow_mut();
            let ctx = ctx.as_mut().expect("Not running in simulator context");
            ctx.current_node = self.prev;
        });
    }
}
