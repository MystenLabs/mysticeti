use crate::core::Core;
use crate::core_thread::CoreThreadDispatcher;
use crate::network::{Connection, Network, NetworkMessage};
use crate::runtime;
use crate::runtime::Handle;
use crate::runtime::{JoinError, JoinHandle};
use crate::syncer::{CommitObserver, Syncer, SyncerSignals};
use crate::types::format_authority_index;
use crate::types::AuthorityIndex;
use crate::wal::WalSyncer;
use crate::{block_handler::BlockHandler, metrics::Metrics};
use crate::{block_store::BlockStore, synchronizer::BlockDisseminator};
use crate::{committee::Committee, synchronizer::BlockFetcher};
use futures::future::join_all;
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
}

pub struct NetworkSyncerInner<H: BlockHandler, C: CommitObserver> {
    pub syncer: CoreThreadDispatcher<H, Arc<Notify>, C>,
    pub block_store: BlockStore,
    pub notify: Arc<Notify>,
    committee: Arc<Committee>,
    stop: mpsc::Sender<()>,
}

impl<H: BlockHandler + 'static, C: CommitObserver + 'static> NetworkSyncer<H, C> {
    pub fn start(
        network: Network,
        mut core: Core<H>,
        commit_period: u64,
        mut commit_observer: C,
        metrics: Arc<Metrics>,
    ) -> Self {
        let authority_index = core.authority();
        let handle = Handle::current();
        let notify = Arc::new(Notify::new());
        // todo - ugly, probably need to merge syncer and core
        let (committed, state) = core.take_recovered_committed_blocks();
        commit_observer.recover_committed(committed, state);
        let committee = core.committee().clone();
        let wal_syncer = core.wal_syncer();
        let block_store = core.block_store().clone();
        let mut syncer = Syncer::new(
            core,
            commit_period,
            notify.clone(),
            commit_observer,
            metrics.clone(),
        );
        syncer.force_new_block(0);
        let syncer = CoreThreadDispatcher::start(syncer);
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        stop_sender.try_send(()).unwrap(); // occupy the only available permit, so that all other calls to send() will block
        let inner = Arc::new(NetworkSyncerInner {
            notify,
            syncer,
            block_store,
            committee,
            stop: stop_sender.clone(),
        });
        let block_fetcher = Arc::new(BlockFetcher::new(authority_index, inner.clone()));
        let main_task = handle.spawn(Self::run(network, inner.clone(), block_fetcher));
        let syncer_task = AsyncWalSyncer::start(wal_syncer, stop_sender);
        Self {
            inner,
            main_task,
            stop: stop_receiver,
            syncer_task,
        }
    }

    pub async fn shutdown(self) -> Syncer<H, Arc<Notify>, C> {
        drop(self.stop);
        // todo - wait for network shutdown as well
        self.main_task.await.ok();
        self.syncer_task.await.ok();
        let Ok(inner) = Arc::try_unwrap(self.inner) else {
            panic!("Shutdown failed - not all resources are freed after main task is completed");
        };
        inner.syncer.stop()
    }

    async fn run(
        mut network: Network,
        inner: Arc<NetworkSyncerInner<H, C>>,
        block_fetcher: Arc<BlockFetcher>,
    ) {
        let mut connections: HashMap<usize, JoinHandle<Option<()>>> = HashMap::new();
        let handle = Handle::current();
        let leader_timeout_task = handle.spawn(Self::leader_timeout_task(inner.clone()));
        let cleanup_task = handle.spawn(Self::cleanup_task(inner.clone()));
        while let Some(connection) = inner.recv_or_stopped(network.connection_receiver()).await {
            let peer_id = connection.peer_id;
            if let Some(task) = connections.remove(&peer_id) {
                // wait until previous sync task completes
                task.await.ok();
            }

            let sender = connection.sender.clone();
            let authority = peer_id as AuthorityIndex;
            block_fetcher.register_authority(authority, sender).await;

            let task = handle.spawn(Self::connection_task(
                connection,
                inner.clone(),
                block_fetcher.clone(),
            ));
            connections.insert(peer_id, task);
        }
        join_all(
            connections
                .into_values()
                .chain([leader_timeout_task, cleanup_task].into_iter()),
        )
        .await;
        Arc::try_unwrap(block_fetcher)
            .unwrap_or_else(|_| panic!("Failed to drop all connections"))
            .shutdown()
            .await;
    }

    async fn connection_task(
        mut connection: Connection,
        inner: Arc<NetworkSyncerInner<H, C>>,
        block_fetcher: Arc<BlockFetcher>,
    ) -> Option<()> {
        let last_seen = inner
            .block_store
            .last_seen_by_authority(connection.peer_id as AuthorityIndex);
        connection
            .sender
            .send(NetworkMessage::SubscribeOwnFrom(last_seen))
            .await
            .ok()?;

        let mut disseminator = BlockDisseminator::new(connection.sender.clone(), inner.clone());

        let peer = format_authority_index(connection.peer_id as AuthorityIndex);
        while let Some(message) = inner.recv_or_stopped(&mut connection.receiver).await {
            match message {
                NetworkMessage::SubscribeOwnFrom(round) => {
                    disseminator.disseminate_own_blocks(round).await
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
                        break;
                    }
                    inner.syncer.add_blocks(vec![block]).await;
                }
                NetworkMessage::RequestBlocks(references) => {
                    if disseminator.send_blocks(references).await.is_none() {
                        break;
                    }
                }
                NetworkMessage::BlockNotFound(_references) => {
                    // TODO: leverage this signal to request blocks from other peers
                }
            }
        }
        disseminator.cleanup().await;
        let id = connection.peer_id as AuthorityIndex;
        block_fetcher.remove_authority(id).await;
        None
    }

    async fn leader_timeout_task(inner: Arc<NetworkSyncerInner<H, C>>) -> Option<()> {
        let leader_timeout = Duration::from_secs(1);
        loop {
            let notified = inner.notify.notified();
            let round = inner
                .block_store
                .last_own_block_ref()
                .map(|b| b.round())
                .unwrap_or_default();
            select! {
                _sleep = runtime::sleep(leader_timeout) => {
                    tracing::debug!("Timeout {round}");
                    // todo - more then one round timeout can happen, need to fix this
                    inner.syncer.force_new_block(round).await;
                }
                _notified = notified => {
                    // restart loop
                }
                _stopped = inner.stopped() => {
                    return None;
                }
            }
        }
    }

    async fn cleanup_task(inner: Arc<NetworkSyncerInner<H, C>>) -> Option<()> {
        let cleanup_interval = Duration::from_secs(10);
        loop {
            select! {
                _sleep = runtime::sleep(cleanup_interval) => {
                    // Keep read lock for everything else
                    inner.syncer.cleanup().await;
                }
                _stopped = inner.stopped() => {
                    return None;
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
    runtime: tokio::runtime::Handle,
}

impl AsyncWalSyncer {
    #[cfg(not(feature = "simulator"))]
    pub fn start(wal_syncer: WalSyncer, stop: mpsc::Sender<()>) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let this = Self {
            wal_syncer,
            stop,
            _sender: sender,
            runtime: tokio::runtime::Handle::current(),
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
        let runtime = self.runtime.clone();
        loop {
            if runtime.block_on(self.wait_next()) {
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
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        check_commits(&syncers);
    }
}

#[cfg(test)]
#[cfg(feature = "simulator")]
mod sim_tests {
    use crate::future_simulator::SimulatedExecutorState;
    use crate::runtime;
    use crate::simulator_tracing::setup_simulator_tracing;
    use crate::test_util::{check_commits, print_stats, rng_at_seed, simulated_network_syncers};
    use std::time::Duration;

    #[test]
    fn test_network_sync_sim_all_up() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_sync_sim_all_up_async());
    }

    async fn test_network_sync_sim_all_up_async() {
        let (simulated_network, network_syncers, mut reporters) = simulated_network_syncers(10);
        simulated_network.connect_all().await;
        runtime::sleep(Duration::from_secs(20)).await;
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
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
        let (simulated_network, network_syncers, mut reporters) = simulated_network_syncers(10);
        simulated_network.connect_some(|a, _b| a != 0).await;
        println!("Started");
        runtime::sleep(Duration::from_secs(40)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        check_commits(&syncers);
        print_stats(&syncers, &mut reporters);
    }

    #[test]
    fn test_network_partition() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_partition_async());
    }

    // All peers except for peer A are connected in this test. Peer A is disconnected from everyone
    // except for peer B. This test ensures that A eventually manages to commit by syncing with B.
    async fn test_network_partition_async() {
        let (simulated_network, network_syncers, mut reporters) = simulated_network_syncers(10);
        // Disconnect all A from all peers except for B.
        simulated_network
            .connect_some(|a, b| a != 0 || (a == 0 && b == 1))
            .await;

        println!("Started");
        runtime::sleep(Duration::from_secs(40)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        // Ensure no conflicts.
        check_commits(&syncers);
        print_stats(&syncers, &mut reporters);
    }

    #[test]
    fn test_network_synchronize() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_synchronize_async());
    }

    // All peers except for peer A are connected in this test. Peer A is disconnected from everyone
    // except for peer B. This test ensures that A eventually manages to commit by syncing with B.
    async fn test_network_synchronize_async() {
        let (simulated_network, network_syncers, mut reporters) = simulated_network_syncers(10);
        // Disconnect all A from all peers except for B.
        simulated_network
            .connect_some(|a, b| a != 0 || (a == 0 && b == 1))
            .await;

        println!("Started");
        runtime::sleep(Duration::from_secs(40)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        // Ensure no conflicts.
        check_commits(&syncers);
        print_stats(&syncers, &mut reporters);
    }
}
