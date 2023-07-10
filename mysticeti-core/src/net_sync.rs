use crate::committee::Committee;
use crate::core::Core;
use crate::network::{Connection, Network, NetworkMessage};
use crate::runtime;
use crate::runtime::Handle;
use crate::runtime::{JoinError, JoinHandle};
use crate::syncer::{CommitObserver, Syncer, SyncerSignals};
use crate::types::format_authority_index;
use crate::types::{AuthorityIndex, RoundNumber};
use crate::{block_handler::BlockHandler, metrics::Metrics};
use futures::future::join_all;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, Notify};

pub struct NetworkSyncer<H: BlockHandler, C: CommitObserver> {
    inner: Arc<NetworkSyncerInner<H, C>>,
    main_task: JoinHandle<()>,
    stop: mpsc::Receiver<()>,
}

struct NetworkSyncerInner<H: BlockHandler, C: CommitObserver> {
    syncer: RwLock<Syncer<H, Arc<Notify>, C>>,
    notify: Arc<Notify>,
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
        let handle = Handle::current();
        let notify = Arc::new(Notify::new());
        // todo - ugly, probably need to merge syncer and core
        let (committed, state) = core.take_recovered_committed_blocks();
        commit_observer.recover_committed(committed, state);
        let committee = core.committee().clone();
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
        let inner = Arc::new(NetworkSyncerInner {
            notify,
            syncer,
            committee,
            stop: stop_sender,
        });
        let main_task = handle.spawn(Self::run(network, inner.clone()));
        Self {
            inner,
            main_task,
            stop: stop_receiver,
        }
    }

    pub async fn shutdown(self) -> Syncer<H, Arc<Notify>, C> {
        drop(self.stop);
        // todo - wait for network shutdown as well
        self.main_task.await.ok();
        let Ok(inner) = Arc::try_unwrap(self.inner) else {
            panic!("Shutdown failed - not all resources are freed after main task is completed");
        };
        inner.syncer.into_inner()
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
        let leader_timeout = Duration::from_secs(1);
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
}
