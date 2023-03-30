// A quick and dirty discrete event simulator we can use to test different
// scenarios in the mysticeti protocol. It allows us to (1) read current time,
// (2) schedule events to happen at a certain time, and (3) use all of the usual
// async channels wakers based stuff.

use futures::channel::oneshot;
use futures::executor::LocalPool;
use futures::executor::LocalSpawner;
use futures::task::LocalSpawnExt;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex;

// Define a time type as a u64
pub type Time = u64;

// A Map that holds the sending end of a one shot channel keyed by the time
// it needs to be sent.
type EventMap = BTreeMap<Time, oneshot::Sender<()>>;

struct DSimExecutor {
    current_time: AtomicU64,
    spawner: LocalSpawner,
    waiting_events: Arc<Mutex<EventMap>>,
}

impl DSimExecutor {
    pub fn new(local_pool: &LocalPool) -> Self {
        DSimExecutor {
            current_time: AtomicU64::new(0),
            spawner: local_pool.spawner(),
            waiting_events: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    // Add a future to the local pool
    pub fn spawn_local(&self, future: impl futures::Future<Output = ()> + 'static) {
        self.spawner.spawn_local(future).unwrap();
    }

    // Get the current time
    pub fn get_time(&self) -> Time {
        self.current_time.load(std::sync::atomic::Ordering::Relaxed)
    }

    // A future that waits until a future time
    pub async fn wait_until(&self, time: Time) {
        let (tx, rx) = oneshot::channel();
        {
            // Ensure the given time is in the future
            assert!(time > self.get_time());

            let mut res = self.waiting_events.try_lock().unwrap();
            let _ = res.insert(time, tx);
            drop(res);
        }
        let _ = rx.await;
    }

    // Run the local pool to completion
    pub fn run(&self, local_pool: &mut LocalPool) {
        local_pool.run_until_stalled();
        loop {
            let mut lock = self.waiting_events.try_lock().unwrap();

            if let Some(lowest) = lock.keys().next().map(|x| *x) {
                // Update the current time
                self.current_time
                    .store(lowest, std::sync::atomic::Ordering::Relaxed);

                // Send the next event
                let tx = lock.remove(&lowest).unwrap();
                let _ = tx.send(());
                // Now we manually drop the lock, to allow tasks to get it.
                drop(lock);

                print!("Sent event at time {}\n", lowest);
                local_pool.run_until_stalled();
            } else {
                break;
            }
        }
        local_pool.run_until_stalled();
    }
}

// Make a local thread pool and schedule two tasks to run to completion
#[test]
pub fn example_executor() {
    // Use a futures::LocalPool to run the tasks
    let mut pool = futures::executor::LocalPool::new();

    // Get a DSimExecutor in an Arc so we can share it between tasks
    let executor = Arc::new(DSimExecutor::new(&pool));

    // Schedule a task for time 100
    let executor1 = executor.clone();
    executor.spawn_local(async move {
        println!("Task 1 at time {}", executor1.get_time());
        executor1.wait_until(100).await;
        println!("done with task 1");
    });

    // Schedule a task for time 200
    let executor2 = executor.clone();
    executor.spawn_local(async move {
        println!("Task 2 at time {}", executor2.get_time());
        executor2.wait_until(200).await;
        println!("done with task 2");
    });

    // Run the tasks to completion
    executor.run(&mut pool);
}

// Make a local thread pool and schedule two tasks to run to completion
#[test]
pub fn example_executor_spawn_in_spawn() {
    // Use a futures::LocalPool to run the tasks
    let mut pool = futures::executor::LocalPool::new();

    // Get a DSimExecutor in an Arc so we can share it between tasks
    let executor = Arc::new(DSimExecutor::new(&pool));

    // Schedule a task for time 100
    let executor1 = executor.clone();
    executor.spawn_local(async move {
        println!("Task 1 at time {}", executor1.get_time());
        executor1.wait_until(100).await;
        println!("done with task 1");
    });

    // Schedule a task for time 200
    let executor2 = executor.clone();
    executor.spawn_local(async move {
        println!("Task 2 at time {}", executor2.get_time());
        executor2.wait_until(200).await;
        println!("done with task 2");

        let executor3 = executor2.clone();
        executor2.spawn_local(async move {
            println!("Task 3 at time {}", executor3.get_time());
            executor3.wait_until(300).await;
            println!("done with task 2");
        });

        // And lets also try to wait inline.
        executor2.wait_until(500).await;
    });

    // Run the tasks to completion
    executor.run(&mut pool);
}
