use crate::block_validator::BlockVerifier;
use crate::commit_observer::CommitObserver;
use crate::config::SynchronizerParameters;
use crate::core::Core;
use crate::core_thread::CoreThreadDispatcher;
use crate::data::Data;
use crate::network::{Connection, Network, NetworkMessage};
use crate::runtime::Handle;
use crate::runtime::{self, timestamp_utc};
use crate::runtime::{JoinError, JoinHandle};
use crate::syncer::{Signals, Syncer};
use crate::types::{AuthorityIndex, BlockReference, StatementBlock};
use crate::types::{AuthoritySet, RoundNumber};
use crate::wal::WalSyncer;
use crate::{block_handler::BlockHandler, metrics::Metrics};
use crate::{block_store::BlockStore, synchronizer::BlockDisseminator};
use crate::{committee::Committee, synchronizer::BlockFetcher};
use futures::future::join_all;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc, oneshot, Notify};

/// The maximum number of blocks that can be requested in a single message.
pub const MAXIMUM_BLOCK_REQUEST: usize = 50;

struct ConnectedAuthorities {
    metrics: Arc<Metrics>,
    authorities: AuthoritySet,
    count: u64,
}

impl ConnectedAuthorities {
    fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            count: 0,
            authorities: AuthoritySet::default(),
        }
    }

    fn insert(&mut self, authority_index: AuthorityIndex) {
        if self.authorities.insert(authority_index) {
            self.count += 1;
            self.metrics.connected_nodes.set(self.count as i64);
        }
    }

    fn remove(&mut self, authority_index: AuthorityIndex) {
        if self.authorities.remove(authority_index) {
            self.count -= 1;
            self.metrics.connected_nodes.set(self.count as i64);
        }
    }
}

pub struct NetworkSyncer<H: BlockHandler, C: CommitObserver> {
    inner: Arc<NetworkSyncerInner<H, C>>,
    main_task: JoinHandle<()>,
    syncer_task: oneshot::Receiver<()>,
    stop: mpsc::Receiver<()>,
}

pub struct NetworkSyncerInner<H: BlockHandler, C: CommitObserver> {
    pub syncer: CoreThreadDispatcher<H, Signals, C>,
    pub block_store: BlockStore,
    pub notify: Arc<Notify>,
    committee: Arc<Committee>,
    stop: mpsc::Sender<()>,
    epoch_close_signal: mpsc::Sender<()>,
    pub epoch_closing_time: Arc<AtomicU64>,
    connected_authorities: Arc<Mutex<ConnectedAuthorities>>,
}

impl<H: BlockHandler + 'static, C: CommitObserver + 'static> NetworkSyncer<H, C> {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        network: Network,
        core: Core<H>,
        commit_period: u64,
        commit_observer: C,
        shutdown_grace_period: Duration,
        block_verifier: impl BlockVerifier,
        metrics: Arc<Metrics>,
        leader_timeout: Duration,
        parameters: SynchronizerParameters,
        cleanup_enabled: bool,
    ) -> Self {
        let authority_index = core.authority();
        let handle = Handle::current();
        let committee = core.committee().clone();
        let wal_syncer = core.wal_syncer();
        let block_store = core.block_store().clone();
        let epoch_closing_time = core.epoch_closing_time();
        let notify = Arc::new(Notify::new());
        let (round_advanced_sender, round_advanced_receiver) =
            tokio::sync::watch::channel(0 as RoundNumber);
        let signals = Signals::new(notify.clone(), round_advanced_sender);
        let mut syncer = Syncer::new(
            core,
            commit_period,
            signals,
            commit_observer,
            metrics.clone(),
        );
        syncer.force_new_block(1, Default::default());
        let syncer = CoreThreadDispatcher::start(syncer);
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        stop_sender.try_send(()).unwrap(); // occupy the only available permit, so that all other calls to send() will block
        let (epoch_sender, epoch_receiver) = mpsc::channel(1);
        epoch_sender.try_send(()).unwrap(); // occupy the only available permit, so that all other calls to send() will block
        let connected_authorities =
            Arc::new(Mutex::new(ConnectedAuthorities::new(metrics.clone())));
        let inner = Arc::new(NetworkSyncerInner {
            syncer,
            block_store,
            notify,
            committee: committee.clone(),
            stop: stop_sender.clone(),
            epoch_close_signal: epoch_sender.clone(),
            epoch_closing_time,
            connected_authorities,
        });
        let block_fetcher = Arc::new(BlockFetcher::start(
            authority_index,
            inner.clone(),
            metrics.clone(),
            committee,
        ));
        let main_task = handle.spawn(Self::run(
            network,
            inner.clone(),
            epoch_receiver,
            shutdown_grace_period,
            block_fetcher,
            Arc::new(block_verifier),
            metrics.clone(),
            leader_timeout,
            parameters,
            cleanup_enabled,
            round_advanced_receiver,
        ));
        let syncer_task = AsyncWalSyncer::start(wal_syncer, stop_sender, epoch_sender);
        Self {
            inner,
            main_task,
            stop: stop_receiver,
            syncer_task,
        }
    }

    pub async fn shutdown(self) -> Syncer<H, Signals, C> {
        drop(self.stop);
        // todo - wait for network shutdown as well
        self.main_task.await.ok();
        self.syncer_task.await.ok();
        let Ok(inner) = Arc::try_unwrap(self.inner) else {
            panic!("Shutdown failed - not all resources are freed after main task is completed");
        };
        inner.syncer.stop()
    }

    #[allow(clippy::too_many_arguments)]
    async fn run(
        mut network: Network,
        inner: Arc<NetworkSyncerInner<H, C>>,
        epoch_close_signal: mpsc::Receiver<()>,
        shutdown_grace_period: Duration,
        block_fetcher: Arc<BlockFetcher>,
        block_verifier: Arc<impl BlockVerifier>,
        metrics: Arc<Metrics>,
        leader_timeout: Duration,
        parameters: SynchronizerParameters,
        cleanup_enabled: bool,
        round_advanced_receiver: Receiver<RoundNumber>,
    ) {
        let mut connections: HashMap<usize, JoinHandle<Option<()>>> = HashMap::new();
        let handle = Handle::current();
        let leader_timeout_task = handle.spawn(Self::leader_timeout_task(
            inner.clone(),
            epoch_close_signal,
            shutdown_grace_period,
            leader_timeout,
            round_advanced_receiver,
        ));
        let cleanup_task = if cleanup_enabled {
            handle.spawn(Self::cleanup_task(inner.clone()))
        } else {
            handle.spawn(async { None })
        };
        while let Some(connection) = inner.recv_or_stopped(network.connection_receiver()).await {
            let peer_id = connection.peer_id;
            if let Some(task) = connections.remove(&peer_id) {
                // wait until previous sync task completes
                task.await.ok();
            }

            let sender = connection.sender.clone();
            let authority = peer_id as AuthorityIndex;
            block_fetcher
                .register_authority(
                    authority,
                    sender,
                    connection.latency_last_value_receiver.clone(),
                )
                .await;
            inner.connected_authorities.lock().insert(authority);

            let task = handle.spawn(Self::connection_task(
                connection,
                inner.clone(),
                block_fetcher.clone(),
                block_verifier.clone(),
                metrics.clone(),
                parameters.clone(),
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
        network.shutdown().await;
    }

    async fn connection_task(
        mut connection: Connection,
        inner: Arc<NetworkSyncerInner<H, C>>,
        block_fetcher: Arc<BlockFetcher>,
        block_verifier: Arc<impl BlockVerifier>,
        metrics: Arc<Metrics>,
        parameters: SynchronizerParameters,
    ) -> Option<()> {
        let last_seen = inner
            .block_store
            .last_seen_by_authority(connection.peer_id as AuthorityIndex);
        connection
            .sender
            .send(NetworkMessage::SubscribeOwnFrom(last_seen))
            .await
            .ok()?;

        let mut disseminator = BlockDisseminator::new(
            connection.sender.clone(),
            inner.clone(),
            metrics.clone(),
            parameters,
        );

        let authority = inner
            .committee
            .authority_safe(connection.peer_id as AuthorityIndex);
        while let Some(message) = inner.recv_or_stopped(&mut connection.receiver).await {
            match message {
                NetworkMessage::SubscribeOwnFrom(round) => {
                    disseminator.disseminate_own_blocks(round).await
                }
                NetworkMessage::Blocks(blocks) => {
                    if let Ok(missing_blocks) =
                        Self::process_blocks(&inner, &block_verifier, &metrics, blocks).await
                    {
                        // we only want to request missing blocks when a validator is sending us their block
                        // proposals, and not during a bulk catchup via our request (RequestBlocks) to avoid
                        // overwhelming the peer.
                        Self::request_missing_blocks(missing_blocks, &connection.sender);
                    } else {
                        break;
                    }
                }
                NetworkMessage::RequestBlocks(references) => {
                    if references.len() > MAXIMUM_BLOCK_REQUEST {
                        // Terminate connection on receiving invalid message.
                        break;
                    }
                    if disseminator
                        .send_blocks(authority, references)
                        .await
                        .is_none()
                    {
                        break;
                    }
                }
                NetworkMessage::RequestBlocksResponse(blocks) => {
                    if Self::process_blocks(&inner, &block_verifier, &metrics, blocks)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                NetworkMessage::BlockNotFound(_references) => {
                    // TODO: leverage this signal to request blocks from other peers
                }
            }
        }
        disseminator.shutdown().await;
        let authority = connection.peer_id as AuthorityIndex;
        inner.connected_authorities.lock().remove(authority);
        block_fetcher.remove_authority(authority).await;
        None
    }

    async fn process_blocks(
        inner: &Arc<NetworkSyncerInner<H, C>>,
        block_verifier: &Arc<impl BlockVerifier>,
        metrics: &Arc<Metrics>,
        blocks: Vec<Data<StatementBlock>>,
    ) -> Result<Vec<BlockReference>, eyre::Report> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let now = timestamp_utc();
        let processed = inner
            .syncer
            .processed(blocks.iter().map(|block| *block.reference()).collect())
            .await;

        let mut to_process = Vec::new();
        for block in blocks.into_iter() {
            // skip the processing if already processed.
            if processed.contains(block.reference()) {
                tracing::debug!("Skip block {} as is already processed", block.reference());
                continue;
            }

            let hostname = inner.committee.authority_safe(block.author()).hostname();

            tracing::debug!("Received {} from {}", block.reference(), hostname);

            metrics
                .block_receive_latency
                .with_label_values(&[&hostname])
                .observe(
                    now.checked_sub(block.meta_creation_time())
                        .unwrap_or_default()
                        .as_secs_f64(),
                );

            // Verify blocks based on consensus rules
            if let Err(e) = block.verify(&inner.committee) {
                tracing::warn!(
                    "Rejected incorrect block {} based on consensus rules from {}: {:?}",
                    block.reference(),
                    hostname,
                    e
                );
                // Terminate connection on receiving incorrect block
                return Err(e);
            }
            // Verify blocks based on customized validation rules
            if let Err(e) = block_verifier.verify(&block).await {
                tracing::warn!(
                    "Rejected incorrect block {} based on validation rules from {}: {:?}",
                    block.reference(),
                    hostname,
                    e
                );
                // Terminate connection on receiving incorrect block
                return Err(eyre::Report::msg(""));
            }

            to_process.push(block);
        }

        if !to_process.is_empty() {
            let connected_authorities = inner.connected_authorities.lock().authorities.clone();
            return Ok(inner
                .syncer
                .add_blocks(to_process, connected_authorities)
                .await);
        }

        Ok(vec![])
    }

    fn request_missing_blocks(
        missing_blocks: Vec<BlockReference>,
        sender: &mpsc::Sender<NetworkMessage>,
    ) {
        if !missing_blocks.is_empty() {
            for blocks in missing_blocks.chunks(MAXIMUM_BLOCK_REQUEST) {
                if let Ok(permit) = sender.try_reserve() {
                    permit.send(NetworkMessage::RequestBlocks(blocks.to_vec()))
                }
            }
        }
    }

    async fn leader_timeout_task(
        inner: Arc<NetworkSyncerInner<H, C>>,
        mut epoch_close_signal: mpsc::Receiver<()>,
        shutdown_grace_period: Duration,
        leader_timeout: Duration,
        mut round_advanced_receiver: Receiver<RoundNumber>,
    ) -> Option<()> {
        let mut round = *round_advanced_receiver.borrow();
        let mut timed_out = false;

        loop {
            let closing_time = inner.epoch_closing_time.load(Ordering::Relaxed);
            let shutdown_duration = if closing_time != 0 {
                shutdown_grace_period.saturating_sub(
                    timestamp_utc().saturating_sub(Duration::from_millis(closing_time)),
                )
            } else {
                Duration::MAX
            };
            if Duration::is_zero(&shutdown_duration) {
                return None;
            }
            select! {
                _sleep = runtime::sleep(leader_timeout), if !timed_out => {
                    tracing::debug!("Timeout {round}");
                    let connected_authorities = inner.connected_authorities.lock().authorities.clone();
                    inner.syncer.force_new_block(round, connected_authorities).await;
                    timed_out = true;
                }
                _changed = round_advanced_receiver.changed() => {
                    // restart loop as we have received a new quorum
                    round = *round_advanced_receiver.borrow();
                    timed_out = false;
                }
                _epoch_shutdown = runtime::sleep(shutdown_duration) => {
                    tracing::info!("Shutting down sync after epoch close");
                    epoch_close_signal.close();
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
            closed = self.epoch_close_signal.send(()) => {
                assert!(closed.is_err());
                None
            }
            data = channel.recv() => {
                data
            }
        }
    }

    async fn stopped(&self) {
        select! {
            stopped = self.stop.send(()) => {
                assert!(stopped.is_err());
            }
            closed = self.epoch_close_signal.send(()) => {
                assert!(closed.is_err());
            }
        }
    }
}

pub struct AsyncWalSyncer {
    wal_syncer: WalSyncer,
    stop: mpsc::Sender<()>,
    epoch_signal: mpsc::Sender<()>,
    _sender: oneshot::Sender<()>,
    runtime: tokio::runtime::Handle,
}

impl AsyncWalSyncer {
    #[cfg(not(feature = "simulator"))]
    pub fn start(
        wal_syncer: WalSyncer,
        stop: mpsc::Sender<()>,
        epoch_signal: mpsc::Sender<()>,
    ) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let this = Self {
            wal_syncer,
            stop,
            epoch_signal,
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
    pub fn start(
        _wal_syncer: WalSyncer,
        _stop: mpsc::Sender<()>,
        _epoch_signal: mpsc::Sender<()>,
    ) -> oneshot::Receiver<()> {
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
            _ = self.epoch_signal.send(()) => {
                // might need to sync wal completely before shutting down
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
    use super::NetworkSyncer;
    use crate::block_handler::TestBlockHandler;
    use crate::commit_observer::TestCommitObserver;
    use crate::config::Parameters;
    use crate::finalization_interpreter::FinalizationInterpreter;
    use crate::future_simulator::SimulatedExecutorState;
    use crate::runtime;
    use crate::simulator_tracing::setup_simulator_tracing;
    use crate::syncer::{Signals, Syncer};
    use crate::test_util::{
        check_commits, print_stats, rng_at_seed, simulated_network_syncers,
        simulated_network_syncers_with_epoch_duration,
    };
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[test]
    fn test_epoch_close() {
        SimulatedExecutorState::run(rng_at_seed(0), test_epoch_close_async());
    }

    async fn test_epoch_close_async() {
        let n = 4;
        let rounds_in_epoch = 3000;
        let (simulated_network, network_syncers, _) =
            simulated_network_syncers_with_epoch_duration(n, rounds_in_epoch);
        simulated_network.connect_all().await;
        let syncers = wait_for_epoch_to_close(network_syncers).await;
        for syncer in syncers.iter() {
            if syncer.core().epoch_closed() {
                ()
            } else {
                panic!("Epoch should have closed!")
            }
        }
    }

    async fn wait_for_epoch_to_close(
        network_syncers: Vec<NetworkSyncer<TestBlockHandler, TestCommitObserver>>,
    ) -> Vec<Syncer<TestBlockHandler, Signals, TestCommitObserver>> {
        let mut any_closed = false;
        while !any_closed {
            for net_sync in network_syncers.iter() {
                if net_sync.inner.epoch_closing_time.load(Ordering::Relaxed) != 0 {
                    any_closed = true;
                }
            }
            runtime::sleep(Duration::from_secs(10)).await;
        }
        runtime::sleep(Parameters::DEFAULT_SHUTDOWN_GRACE_PERIOD).await;
        let mut syncers = vec![];
        for net_sync in network_syncers {
            let syncer = net_sync.shutdown().await;
            syncers.push(syncer);
        }
        syncers
    }
    #[test]
    fn test_exact_commits_in_epoch() {
        SimulatedExecutorState::run(rng_at_seed(0), test_exact_commits_in_epoch_async());
    }

    async fn test_exact_commits_in_epoch_async() {
        let n = 4;
        let rounds_in_epoch = 3000;
        let (simulated_network, network_syncers, mut reporters) =
            simulated_network_syncers_with_epoch_duration(n, rounds_in_epoch);
        simulated_network.connect_all().await;
        let syncers = wait_for_epoch_to_close(network_syncers).await;
        let canonical_commit_seq = syncers[0].commit_observer().committed_leaders().clone();
        for syncer in &syncers {
            let commit_seq = syncer.commit_observer().committed_leaders().clone();
            assert_eq!(canonical_commit_seq, commit_seq);
        }
        print_stats(&syncers, &mut reporters);
    }

    #[test]
    fn test_finalization_epoch_safety() {
        SimulatedExecutorState::run(rng_at_seed(0), test_finalization_safety_async());
    }

    async fn test_finalization_safety_async() {
        // todo - no cleanup of block store
        let n = 4;
        let rounds_in_epoch = 10;
        let (simulated_network, network_syncers, mut reporters) =
            simulated_network_syncers_with_epoch_duration(n, rounds_in_epoch);
        simulated_network.connect_all().await;
        let syncers = wait_for_epoch_to_close(network_syncers).await;
        for syncer in &syncers {
            let block_store = syncer.core().block_store();
            let committee = syncer.core().committee().clone();
            let latest_committed_leader =
                syncer.commit_observer().committed_leaders().last().unwrap();

            println!(
                "Num of Committed leaders: {:?}",
                syncer.commit_observer().committed_leaders()
            );

            let mut finalization_interpreter = FinalizationInterpreter::new(block_store, committee);
            let finalized_tx_certifying_blocks =
                finalization_interpreter.finalized_tx_certifying_blocks();

            for (_, certificates) in finalized_tx_certifying_blocks {
                // check if at least one certificate is committed
                let mut committed = false;
                for certifying_block in certificates {
                    if block_store.linked(
                        &block_store.get_block(*latest_committed_leader).unwrap(),
                        &block_store.get_block(certifying_block).unwrap(),
                    ) {
                        committed = true;
                        break;
                    }
                }
                assert!(committed);
            }
        }
        print_stats(&syncers, &mut reporters);
    }

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
}
