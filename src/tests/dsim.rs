// A quick and dirty discrete event simulator we can use to test different
// scenarios in the mysticeti protocol. It allows us to (1) read current time,
// (2) schedule events to happen at a certain time, and (3) use all of the usual
// async channels wakers based stuff.

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::executor::LocalPool;
use futures::executor::LocalSpawner;
use futures::task::LocalSpawnExt;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use rand::Rng;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex;

use crate::node::Node;
use crate::threshold_clock::get_highest_threshold_clock_round_number;
use crate::types::BaseStatement;
use crate::types::Committee;
use crate::types::MetaStatementBlock;
use crate::types::Transaction;
use crate::types::TransactionId;
use crate::types::Vote;

// Define a time type as a u64
pub type Time = u64;

// A Map that holds the sending end of a one shot channel keyed by the time
// it needs to be sent.
type EventMap = BTreeMap<Time, Vec<oneshot::Sender<()>>>;

pub struct DSimExecutor {
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
            let _ = res.entry(time).or_insert_with(Vec::new).push(tx);
            drop(res);
        }
        let _ = rx.await;
    }

    // Run the local pool to completion
    pub fn run(&self, local_pool: &mut LocalPool) {
        local_pool.run_until_stalled();
        loop {
            let mut lock = self.waiting_events.try_lock().unwrap();

            if let Some(lowest) = lock.keys().next().copied() {
                // Update the current time
                self.current_time
                    .store(lowest, std::sync::atomic::Ordering::Relaxed);

                // Send the next event
                let tx_list = lock.remove(&lowest).unwrap();
                for tx in tx_list {
                    let _ = tx.send(());
                }
                // Now we manually drop the lock, to allow tasks to get it.
                drop(lock);

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
pub fn example_executor_same_time() {
    // Use a futures::LocalPool to run the tasks
    let mut pool = futures::executor::LocalPool::new();

    // Make an atomic counter to keep track of how many tasks have run
    let counter = Arc::new(AtomicU64::new(0));

    // Get a DSimExecutor in an Arc so we can share it between tasks
    let executor = Arc::new(DSimExecutor::new(&pool));

    // Schedule a task for time 100
    let local_counter = counter.clone();
    let executor1 = executor.clone();
    executor.spawn_local(async move {
        println!("Task 1 at time {}", executor1.get_time());
        executor1.wait_until(100).await;
        println!("done with task 1");

        // Increment the counter
        local_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });

    // Schedule a task for time 100 (same time as task 1)
    let local_counter = counter.clone();
    let executor2 = executor.clone();
    executor.spawn_local(async move {
        println!("Task 2 at time {}", executor2.get_time());
        executor2.wait_until(100).await;
        println!("done with task 2");

        // increment the counter
        local_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });

    // Run the tasks to completion
    executor.run(&mut pool);

    // Check that both tasks ran
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);
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

#[test]
pub fn example_run_four_nodes_no_delays() {
    // Use a futures::LocalPool to run the tasks
    let mut pool = futures::executor::LocalPool::new();

    // Get a DSimExecutor in an Arc so we can share it between tasks
    let executor = Arc::new(DSimExecutor::new(&pool));

    // Make 4 mpsc channels to simulate the network of the 4 nodes
    let (tx1, rx1) = futures::channel::mpsc::channel(100);
    let (tx2, rx2) = futures::channel::mpsc::channel(100);
    let (tx3, rx3) = futures::channel::mpsc::channel(100);
    let (tx4, rx4) = futures::channel::mpsc::channel(100);

    let network_sender = vec![tx1, tx2, tx3, tx4];
    let mut network_receiver = vec![rx1, rx2, rx3, rx4];

    // make a committee of 4 authorities
    let committee = Committee::new(0, vec![1, 1, 1, 1]);

    for auth_seq in 0..4 {
        let executor = executor.clone();
        let mut network_sender = network_sender.clone();
        let mut network_receiver = network_receiver.pop().unwrap();
        let committee = committee.clone();
        let authority = committee.get_rich_authority(auth_seq);

        executor.spawn_local(async move {
            let mut node = Node::new(authority);
            println!("Node {:?} started", node.auth);
            loop {
                // Assert that for this node we are ready to emit the next block
                let quorum_round = get_highest_threshold_clock_round_number(
                    committee.as_ref(),
                    node.block_manager.get_blocks_processed_by_round(),
                    0,
                );

                println!(
                    "{:?} {}",
                    quorum_round,
                    node.block_manager.next_round_number()
                );
                if quorum_round.is_some()
                    && quorum_round.clone().unwrap() >= node.block_manager.next_round_number() - 1
                {
                    // We are ready to emit the next block
                    let next_block_ref = node
                        .block_manager
                        .seal_next_block(node.auth.clone(), quorum_round.unwrap() + 1);
                    let next_block = node
                        .block_manager
                        .get_blocks_processed()
                        .get(&next_block_ref)
                        .unwrap();

                    // Send the block signature to all the other nodes
                    println!(
                        "Node {:?} sending block {:?}",
                        node.auth,
                        next_block.get_reference()
                    );
                    for tx in network_sender.iter_mut() {
                        let _ = tx.send(next_block.clone()).await;
                    }

                    if node.block_manager.next_round_number() > 100 {
                        break;
                    }
                }

                // Wait for the next block to be received
                println!("Node {:?} waiting for block", node.auth);
                let block = network_receiver.next().await.unwrap();
                println!(
                    "Node {:?} received block {:?}",
                    node.auth,
                    block.get_reference()
                );
                // process the next block
                node.block_manager.add_blocks([block].into_iter().collect());
            }
        });
    }

    // Run the tasks to completion
    executor.run(&mut pool);
}

fn delay_send(
    executor: Arc<DSimExecutor>,
    mut channel: mpsc::Sender<MetaStatementBlock>,
    block: MetaStatementBlock,
    max_delay: u64,
) {
    // Generate a random integer from 1 to delay
    let mut rng = rand::thread_rng();
    let delay = rng.gen_range(1..max_delay);

    let exec2 = executor.clone();
    executor.spawn_local(async move {
        exec2.wait_until(exec2.get_time() + delay).await;
        let _ = channel.send(block).await;
    });
}

fn generate_transactions(
    executor: Arc<DSimExecutor>,
    max_delay: u64,
) -> mpsc::Receiver<(TransactionId, Transaction)> {
    // Make a channel
    let (mut tx, rx) = futures::channel::mpsc::channel(100);
    let inner_executor = executor.clone();

    // Make a random initeger u64
    let mut rng = rand::thread_rng();
    let mut tx_id = rng.gen::<u64>();

    executor.spawn_local(async move {
        loop {
            // Generate a random integer from 1 to delay
            let mut rng = rand::thread_rng();
            let delay = rng.gen_range(1..max_delay);

            inner_executor
                .wait_until(inner_executor.get_time() + delay)
                .await;

            // increment tx_id
            tx_id = tx_id + 1;

            let ret = tx.send((tx_id, tx_id)).await;
            // Exit loop on error
            if ret.is_err() {
                break;
            }
        }
    });

    rx
}

async fn delay_vote(
    tx_id: TransactionId,
    max_delay: u64,
    executor: Arc<DSimExecutor>,
) -> TransactionId {
    // Generate a random integer from 1 to delay
    let mut rng = rand::thread_rng();
    let delay = rng.gen_range(1..max_delay);

    executor.wait_until(executor.get_time() + delay).await;
    tx_id
}

#[test]
pub fn example_run_four_nodes_with_delays() {
    // Use a futures::LocalPool to run the tasks
    let mut pool = futures::executor::LocalPool::new();

    // Get a DSimExecutor in an Arc so we can share it between tasks
    let executor = Arc::new(DSimExecutor::new(&pool));

    // Make 4 mpsc channels to simulate the network of the 4 nodes
    let (tx1, rx1) = futures::channel::mpsc::channel(100);
    let (tx2, rx2) = futures::channel::mpsc::channel(100);
    let (tx3, rx3) = futures::channel::mpsc::channel(100);
    let (tx4, rx4) = futures::channel::mpsc::channel(100);

    let network_sender = vec![tx1, tx2, tx3, tx4];
    let mut network_receiver = vec![rx1, rx2, rx3, rx4];

    // make a committee of 4 authorities
    let committee = Committee::new(0, vec![1, 1, 1, 1]);

    for auth_seq in 0..4 {
        let executor = executor.clone();
        let mut network_sender = network_sender.clone();
        let mut network_receiver = network_receiver.pop().unwrap();
        let committee = committee.clone();
        let authority = committee.get_rich_authority(auth_seq);

        let inner_executor = executor.clone();
        executor.spawn_local(async move {
            let mut node = Node::new(authority);

            // transactions receiver
            let mut tx_receiver = generate_transactions(inner_executor.clone(), 100);

            // Make a FuturesUnordered to run the tasks
            let mut tasks = futures::stream::FuturesUnordered::new();

            println!("Node {:?} started", node.auth);
            loop {
                // Assert that for this node we are ready to emit the next block
                let quorum_round = get_highest_threshold_clock_round_number(
                    committee.as_ref(),
                    node.block_manager.get_blocks_processed_by_round(),
                    0,
                );

                println!(
                    "{:?} {}",
                    quorum_round,
                    node.block_manager.next_round_number()
                );
                if quorum_round.is_some()
                    && quorum_round.clone().unwrap() >= node.block_manager.next_round_number() - 1
                {
                    // We are ready to emit the next block
                    let next_block_ref = node
                        .block_manager
                        .seal_next_block(node.auth.clone(), quorum_round.unwrap() + 1);
                    let next_block = node
                        .block_manager
                        .get_blocks_processed()
                        .get(&next_block_ref)
                        .unwrap();

                    // Send the block signature to all the other nodes
                    println!(
                        "Node {:?} sending block {:?} at time {}",
                        node.auth,
                        next_block.get_reference(),
                        inner_executor.get_time()
                    );
                    for tx in network_sender.iter_mut() {
                        let exec3 = inner_executor.clone();
                        delay_send(exec3, tx.clone(), next_block.clone(), 100);
                    }

                    if node.block_manager.next_round_number() > 100 {
                        break;
                    }
                }

                // select! between receiving a block and receiving a transaction
                futures::select! {
                    // Receive a transaction
                    tx = tx_receiver.next().fuse() => {
                        if let Some((tx_id, tx)) = tx {
                            println!("Node {:?} inserts transaction {:?}", node.auth, tx_id);
                            node.block_manager.add_base_statements(&node.auth, vec![BaseStatement::Share(tx_id, tx)]);
                        } else {
                            break;
                        }
                    },
                    // Receive a block
                    block = network_receiver.next().fuse() => {
                        if let Some(block) = block {
                            println!("Node {:?} received block {:?}", node.auth, block.get_reference());
                            let results = node.block_manager.add_blocks([block].into_iter().collect());

                            // For each transaction in the results add a delay_vote future into the tasks
                            for tx_id in results.get_newly_added_transactions() {
                                let exec2 = inner_executor.clone();
                                let task = delay_vote(*tx_id, 100, exec2);
                                tasks.push(task);
                            }
                        } else {
                            break;
                        }
                    },
                    // Receive a vote
                    tx_id = tasks.select_next_some() => {
                            println!("Node {:?} inserts vote for transaction {:?}", node.auth, tx_id);
                            node.block_manager.add_base_statements(&node.auth, vec![BaseStatement::Vote(tx_id, Vote::Accept)]);
                    }

                }
            }
        });
    }

    // Run the tasks to completion
    executor.run(&mut pool);
}
