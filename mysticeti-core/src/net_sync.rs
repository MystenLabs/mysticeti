use crate::committee::Committee;
use crate::core::Core;
use crate::network::{Connection, Network, NetworkMessage};
use crate::runtime;
use crate::runtime::Handle;
use crate::runtime::{JoinError, JoinHandle};
use crate::syncer::{CommitObserver, Syncer, SyncerSignals};
use crate::types::format_authority_index;
use crate::types::{AuthorityIndex, RoundNumber};
use crate::wal::WalSyncer;
use crate::{
    block_handler::BlockHandler,
    metrics::{MetricReporter, Metrics},
};
use futures::future::join_all;
use parking_lot::RwLock;
use rand::{seq::IteratorRandom, thread_rng, Rng, RngCore};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot, Notify};

pub struct NetworkSyncer<H: BlockHandler, C: CommitObserver> {
    inner: Arc<NetworkSyncerInner<H, C>>,
    main_task: JoinHandle<()>,
    syncer_task: oneshot::Receiver<()>,
    stop: mpsc::Receiver<()>,
    _timeout_calculator_task: JoinHandle<()>,
}

pub struct NetworkSyncerInner<H: BlockHandler, C: CommitObserver> {
    syncer: RwLock<Syncer<H, Arc<Notify>, C>>,
    notify: Arc<Notify>,
    committee: Arc<Committee>,
    stop: mpsc::Sender<()>,
    tx_timeout_config: mpsc::Sender<oneshot::Sender<Duration>>,
    reporter: RwLock<MetricReporter>,
}

impl<H: BlockHandler + 'static, C: CommitObserver + 'static> NetworkSyncer<H, C> {
    const DEFAULT_LEADER_TIMEOUT: Duration = Duration::from_secs(1);

    pub fn start(
        network: Network,
        mut core: Core<H>,
        commit_period: u64,
        mut commit_observer: C,
        metrics: Arc<Metrics>,
        reporter: MetricReporter,
    ) -> Self {
        let handle = Handle::current();
        let notify = Arc::new(Notify::new());
        // todo - ugly, probably need to merge syncer and core
        let (committed, state) = core.take_recovered_committed_blocks();
        commit_observer.recover_committed(committed, state);
        let committee = core.committee().clone();
        let wal_syncer = core.wal_syncer();
        let mut syncer = Syncer::new(
            core,
            commit_period,
            notify.clone(),
            commit_observer,
            metrics,
        );
        syncer.force_new_block(0);
        let syncer = RwLock::new(syncer);
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        stop_sender.try_send(()).unwrap(); // occupy the only available permit, so that all other calls to send() will block
        let (tx_timeout_config, rx_timeout_config) = mpsc::channel(1);
        let inner = Arc::new(NetworkSyncerInner {
            notify,
            syncer,
            committee,
            stop: stop_sender.clone(),
            tx_timeout_config,
            reporter: RwLock::new(reporter),
        });

        let main_task = handle.spawn(Self::run(network, inner.clone()));
        let syncer_task = AsyncWalSyncer::start(wal_syncer, stop_sender);
        let timeout_calculator_task = handle.spawn(Self::timeout_calculator_task(
            inner.clone(),
            rx_timeout_config,
        ));

        Self {
            inner,
            main_task,
            stop: stop_receiver,
            syncer_task,
            _timeout_calculator_task: timeout_calculator_task,
        }
    }

    pub async fn shutdown(self) -> NetworkSyncerInner<H, C> {
        drop(self.stop);
        // todo - wait for network shutdown as well
        self.main_task.await.ok();
        self.syncer_task.await.ok();
        let Ok(inner) = Arc::try_unwrap(self.inner) else {
            panic!("Shutdown failed - not all resources are freed after main task is completed");
        };
        inner
    }

    async fn run(mut network: Network, inner: Arc<NetworkSyncerInner<H, C>>) {
        let mut connections: HashMap<usize, JoinHandle<Option<()>>> = HashMap::new();
        let handle = Handle::current();
        let leader_timeout_task = handle.spawn(Self::leader_timeout_task(inner.clone()));
        while let Some(connection) = inner.recv_or_stopped(network.connection_receiver()).await {
            let peer_id = connection.peer_id;
            if let Some(task) = connections.remove(&peer_id) {
                // wait until previous sync task completes
                task.await.ok();
            }
            let task = handle.spawn(Self::connection_task(connection, inner.clone()));
            connections.insert(peer_id, task);
        }
        join_all(
            connections
                .into_values()
                .chain([leader_timeout_task].into_iter()),
        )
        .await;
    }

    async fn connection_task(
        mut connection: Connection,
        inner: Arc<NetworkSyncerInner<H, C>>,
    ) -> Option<()> {
        let last_seen = inner
            .syncer
            .read()
            .last_seen_by_authority(connection.peer_id as AuthorityIndex);
        connection
            .sender
            .send(NetworkMessage::SubscribeOwnFrom(last_seen))
            .await
            .ok()?;
        let handle = Handle::current();
        let mut subscribe_handler: Option<JoinHandle<Option<()>>> = None;
        let peer = format_authority_index(connection.peer_id as AuthorityIndex);
        while let Some(message) = inner.recv_or_stopped(&mut connection.receiver).await {
            match message {
                NetworkMessage::SubscribeOwnFrom(round) => {
                    tracing::debug!("{} subscribed from round {}", peer, round);
                    if let Some(send_blocks_handler) = subscribe_handler.take() {
                        send_blocks_handler.abort();
                        send_blocks_handler.await.ok();
                    }
                    subscribe_handler = Some(handle.spawn(Self::send_blocks(
                        connection.sender.clone(),
                        inner.clone(),
                        round,
                    )));
                }
                NetworkMessage::Block(block) => {
                    tracing::debug!("Received {} from {}", block.reference(), peer);
                    if let Err(e) = block.verify(&inner.committee) {
                        tracing::warn!(
                            "Rejected incorrect block {} from {}: {:?}",
                            block.reference(),
                            peer,
                            e
                        );
                        // Terminate connection on receiving incorrect block
                        return None;
                    }
                    inner.syncer.write().add_blocks(vec![block]);
                }
            }
        }
        if let Some(subscribe_handler) = subscribe_handler.take() {
            subscribe_handler.abort();
            subscribe_handler.await.ok();
        }
        None
    }

    async fn send_blocks(
        to: mpsc::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<H, C>>,
        mut round: RoundNumber,
    ) -> Option<()> {
        loop {
            let notified = inner.notify.notified();
            let blocks = inner.syncer.read().get_own_blocks(round, 10);
            for block in blocks {
                round = block.round();
                to.send(NetworkMessage::Block(block)).await.ok()?;
            }
            notified.await
        }
    }

    async fn leader_timeout_task(inner: Arc<NetworkSyncerInner<H, C>>) -> Option<()> {
        let mut leader_timeout = Self::DEFAULT_LEADER_TIMEOUT;
        loop {
            let notified = inner.notify.notified();
            let round = inner
                .syncer
                .read()
                .last_own_block()
                .map(|b| b.round())
                .unwrap_or_default();
            select! {
                _sleep = runtime::sleep(leader_timeout) => {
                    tracing::debug!("Timeout {round}");
                    // todo - more then one round timeout can happen, need to fix this
                    inner.syncer.write().force_new_block(round);
                }
                _notified = notified => {
                    // restart loop
                    let (sender, receiver) = oneshot::channel();
                    inner.tx_timeout_config.send(sender).await.unwrap();
                    leader_timeout = receiver.await.unwrap();

                }
                _stopped = inner.stopped() => {
                    return None;
                }
            }
        }
    }

    /// Adjust the leader timeout
    async fn timeout_calculator_task(
        inner: Arc<NetworkSyncerInner<H, C>>,
        mut rx_timeout_config: mpsc::Receiver<oneshot::Sender<Duration>>,
    ) {
        // Possible timeout values
        let actions = (1000..10_000).step_by(1000).map(Duration::from_millis);
        // let actions = vec![500, 1_000, 3_000, 5_000, 10_000]
        //     .into_iter()
        //     .map(Duration::from_millis);
        let mut count: HashMap<Duration, usize> = actions.clone().map(|x| (x, 0)).collect();
        let mut av: HashMap<Duration, f64> = actions.clone().map(|x| (x, 0.0)).collect();
        // epsilon
        let epsilon = 0.1;

        fn reward(latency: Duration, _c: usize) -> f64 {
            // todo
            -(latency.as_millis() as f64)
        }

        let mut timeout = Self::DEFAULT_LEADER_TIMEOUT;
        let mut i = 0;
        loop {
            tokio::select! {
                Some(sender) = rx_timeout_config.recv() => {
                    i += 1;
                    if i % 3 != 0 {
                        sender.send(timeout).unwrap();
                        continue;
                    }

                    // update
                    let tx_commit_avg = {
                        inner.reporter.write().clear();
                        inner.reporter.write().receive_all();
                        // inner.reporter.read().transaction_committed_latency.avg()
                        inner.reporter.write().transaction_committed_latency.pct(900)
                    };

                    // if let Some(latency) = tx_commit_avg {
                    //     *count.get_mut(&timeout).unwrap() += 1;
                    //     let c = count.get(&timeout).unwrap().clone();
                    //     let old_av = av.get(&timeout).unwrap().clone();
                    //     let rew = reward(latency, c);
                    //     let new_av = old_av + (rew - old_av) / (c as f64);
                    //     *av.get_mut(&timeout).unwrap() = new_av;

                    //     // pick next timeout value
                    //     if thread_rng().gen_range(0.0..1.0) < epsilon {
                    //         // explore
                    //         timeout = actions.clone().choose(&mut thread_rng()).unwrap().clone();
                    //     } else {
                    //         // exploit
                    //         timeout = av.iter().max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()).unwrap().0.clone();
                    //     }
                    // }

                    *count.get_mut(&timeout).unwrap() += 1;
                    let c = count.get(&timeout).unwrap().clone();
                    let old_av = av.get(&timeout).unwrap().clone();

                    let rew = match tx_commit_avg {
                        Some(latency) => reward(latency, c),
                        None => -50_000.0,
                    };

                    let new_av = old_av + (rew - old_av) / (c as f64);
                    *av.get_mut(&timeout).unwrap() = new_av;

                    // pick next timeout value
                    if thread_rng().gen_range(0.0..1.0) < epsilon {
                        // explore
                        timeout = actions.clone().choose(&mut thread_rng()).unwrap().clone();
                    } else {
                        // exploit
                        timeout = av.iter().max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()).unwrap().0.clone();
                    }

                    // let timeout = Duration::from_millis(1000);
                    // println!("timeout: {:?}", timeout);
                    sender.send(timeout).unwrap();
                }
                _stopped = inner.stopped() => {

                    println!();
                    let mut values: Vec<_> = av.iter().collect();
                    values.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());
                    let str = values.iter().map(|(a, b)| format!("{},{b}", a.as_millis())).collect::<Vec<_>>().join("\n");
                    println!("{}", str);
                    println!();

                    let int = thread_rng().next_u64();
                    std::fs::write(format!("data-{int}.txt"), str).unwrap();

                    return;
                }
            }
        }
    }

    pub async fn await_completion(self) -> Result<(), JoinError> {
        self.main_task.await
    }
}

impl<H: BlockHandler + 'static, C: CommitObserver + 'static> NetworkSyncerInner<H, C> {
    // Returns None either if channel is closed or NetworkSyncerInner receives stop signal
    async fn recv_or_stopped<T>(&self, channel: &mut mpsc::Receiver<T>) -> Option<T> {
        select! {
            stopped = self.stop.send(()) => {
                assert!(stopped.is_err());
                None
            }
            data = channel.recv() => {
                data
            }
        }
    }

    async fn stopped(&self) {
        let res = self.stop.send(()).await;
        assert!(res.is_err());
    }
}

impl SyncerSignals for Arc<Notify> {
    fn new_block_ready(&mut self) {
        self.notify_waiters();
    }
}

pub struct AsyncWalSyncer {
    wal_syncer: WalSyncer,
    stop: mpsc::Sender<()>,
    _sender: oneshot::Sender<()>,
}

impl AsyncWalSyncer {
    #[cfg(not(feature = "simulator"))]
    pub fn start(wal_syncer: WalSyncer, stop: mpsc::Sender<()>) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let this = Self {
            wal_syncer,
            stop,
            _sender: sender,
        };
        std::thread::Builder::new()
            .name("wal-syncer".to_string())
            .spawn(move || this.run())
            .expect("Failed to spawn wal-syncer");
        receiver
    }

    #[cfg(feature = "simulator")]
    pub fn start(_wal_syncer: WalSyncer, _stop: mpsc::Sender<()>) -> oneshot::Receiver<()> {
        oneshot::channel().1
    }

    pub fn run(mut self) {
        loop {
            if tokio::runtime::Handle::current().block_on(self.wait_next()) {
                return;
            }
            self.wal_syncer.sync().expect("Failed to sync wal");
        }
    }

    // Returns true to stop the task
    async fn wait_next(&mut self) -> bool {
        select! {
            _wait = runtime::sleep(Duration::from_secs(1)) => {
                false
            }
            _signal = self.stop.send(()) => {
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::{check_commits, network_syncers};
    use std::time::Duration;

    #[tokio::test]
    async fn test_network_sync() {
        let network_syncers = network_syncers(4).await;
        println!("Started");
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let inner = network_syncer.shutdown().await;
            let syncer = inner.syncer.into_inner();
            syncers.push(syncer);
        }

        check_commits(&syncers);
    }
}

#[cfg(test)]
#[cfg(feature = "simulator")]
mod sim_tests {
    use crate::{
        block_handler::TestBlockHandler,
        test_util::{check_commits, print_stats, rng_at_seed, simulated_network_syncers},
    };
    use crate::{
        block_handler::TestCommitHandler,
        future_simulator::{OverrideNodeContext, SimulatedExecutorState},
    };
    use crate::{runtime, test_util::test_metrics};
    use crate::{simulated_network::SimulatedNetwork, simulator_tracing::setup_simulator_tracing};
    use std::time::Duration;

    use super::NetworkSyncer;

    #[test]
    fn test_network_sync_sim_all_up() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_sync_sim_all_up_async());
    }

    async fn test_network_sync_sim_all_up_async() {
        let (simulated_network, network_syncers) = simulated_network_syncers(10);
        simulated_network.connect_all().await;
        runtime::sleep(Duration::from_secs(20)).await;
        let mut syncers = vec![];
        let mut reporters = vec![];
        for network_syncer in network_syncers {
            let inner = network_syncer.shutdown().await;
            let syncer = inner.syncer.into_inner();
            syncers.push(syncer);
            let reporter = inner.reporter;
            reporters.push(reporter.into_inner());
        }

        check_commits(&syncers);
        print_stats(&syncers, &mut reporters);
    }

    #[test]
    fn test_network_sync_sim_one_down() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_sync_sim_one_down_async());
    }

    // All peers except for peer A are connected in this test
    // Peer A is disconnected from everything
    async fn test_network_sync_sim_one_down_async() {
        let (simulated_network, network_syncers) = simulated_network_syncers(10);
        simulated_network.connect_some(|a, _b| a != 0).await;
        println!("Started");
        runtime::sleep(Duration::from_secs(40)).await;
        println!("Done");
        let mut syncers = vec![];
        let mut reporters = vec![];
        for network_syncer in network_syncers {
            let inner = network_syncer.shutdown().await;
            let syncer = inner.syncer.into_inner();
            syncers.push(syncer);
            let reporter = inner.reporter;
            reporters.push(reporter.into_inner());
        }

        check_commits(&syncers);
        print_stats(&syncers, &mut reporters);
    }

    // TEST
    // $ cargo test -p mysticeti-core --features simulator test_rl -- --nocapture
    // parameters:
    const EXPERIMENT_DURATION: Duration = Duration::from_secs(120_000);
    const EXPERIMENT_LATENCY_RANGE: std::ops::Range<Duration> =
        Duration::from_millis(4000)..Duration::from_millis(4100);

    #[test]
    fn test_rl_network_sync() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_rl_network_sync_sim());
    }

    async fn test_rl_network_sync_sim() {
        let (simulated_network, network_syncers) = custom_simulated_network_syncers(10);
        simulated_network.connect_all().await;

        println!("Started");
        runtime::sleep(EXPERIMENT_DURATION).await;
        println!("Done");

        let mut syncers = vec![];
        let mut reporters = vec![];
        for network_syncer in network_syncers {
            let inner = network_syncer.shutdown().await;
            let syncer = inner.syncer.into_inner();
            syncers.push(syncer);
            let reporter = inner.reporter;
            reporters.push(reporter.into_inner());
        }

        // check_commits(&syncers);
        print_stats(&syncers, &mut reporters);
    }

    fn custom_simulated_network_syncers(
        n: usize,
    ) -> (
        SimulatedNetwork,
        Vec<NetworkSyncer<TestBlockHandler, TestCommitHandler>>,
    ) {
        let (committee, cores, reporters) = crate::test_util::committee_and_cores(n);
        let (mut simulated_network, networks) = SimulatedNetwork::new(&committee);
        simulated_network.set_latency_range(EXPERIMENT_LATENCY_RANGE);
        let mut network_syncers = vec![];
        for ((network, core), reporter) in networks
            .into_iter()
            .zip(cores.into_iter())
            .zip(reporters.into_iter())
        {
            let commit_handler = TestCommitHandler::new(
                committee.clone(),
                core.block_handler().transaction_time.clone(),
                core.metrics.clone(),
            );
            let node_context = OverrideNodeContext::enter(Some(core.authority()));
            let network_syncer =
                NetworkSyncer::start(network, core, 3, commit_handler, test_metrics(), reporter);
            drop(node_context);
            network_syncers.push(network_syncer);
        }
        (simulated_network, network_syncers)
    }
}
