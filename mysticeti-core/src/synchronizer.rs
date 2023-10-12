// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, env, sync::Arc, time::Duration};

use futures::future::join_all;
use rand::{seq::SliceRandom, thread_rng};
use tokio::sync::mpsc;

use crate::{
    block_handler::BlockHandler,
    metrics::Metrics,
    net_sync::{self, NetworkSyncerInner},
    network::NetworkMessage,
    runtime::{sleep, timestamp_utc, Handle, JoinHandle},
    syncer::CommitObserver,
    types::{AuthorityIndex, BlockReference, RoundNumber},
};

// TODO: A central controller will eventually dynamically update these parameters.
pub struct SynchronizerParameters {
    /// The maximum number of helpers (across all nodes).
    pub absolute_maximum_helpers: usize,
    /// The maximum number of helpers per authority.
    pub maximum_helpers_per_authority: usize,
    /// The number of blocks to send in a single batch.
    pub batch_size: usize,
    /// The sampling precision with which to re-evaluate the sync strategy.
    pub sample_precision: Duration,
    /// The grace period ofter which to eagerly sync missing blocks.
    pub grace_period: Duration,
    /// The interval at which to send stream blocks authored by other nodes.
    pub stream_interval: Duration,
    /// Threshold number of missing block from an authority to open a new stream.
    pub new_stream_threshold: usize,
}

impl Default for SynchronizerParameters {
    fn default() -> Self {
        Self {
            absolute_maximum_helpers: 10,
            maximum_helpers_per_authority: 2,
            batch_size: 10,
            sample_precision: Duration::from_secs(5),
            grace_period: Duration::from_secs(15),
            stream_interval: Duration::from_secs(1),
            new_stream_threshold: 10,
        }
    }
}

pub struct BlockDisseminator<H: BlockHandler, C: CommitObserver> {
    /// The sender to the network.
    sender: mpsc::Sender<NetworkMessage>,
    /// The inner state of the network syncer.
    inner: Arc<NetworkSyncerInner<H, C>>,
    /// The handle of the task disseminating our own blocks.
    own_blocks: Option<JoinHandle<Option<()>>>,
    /// The handles of tasks disseminating other nodes' blocks.
    other_blocks: Vec<JoinHandle<Option<()>>>,
    /// The parameters of the synchronizer.
    parameters: SynchronizerParameters,
    /// Metrics.
    metrics: Arc<Metrics>,
}

impl<H, C> BlockDisseminator<H, C>
where
    H: BlockHandler + 'static,
    C: CommitObserver + 'static,
{
    pub fn new(
        sender: mpsc::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<H, C>>,
        parameters: SynchronizerParameters,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            sender,
            inner,
            own_blocks: None,
            other_blocks: Vec::new(),
            parameters,
            metrics,
        }
    }

    pub async fn shutdown(mut self) {
        let mut waiters = Vec::with_capacity(1 + self.other_blocks.len());
        if let Some(handle) = self.own_blocks.take() {
            handle.abort();
            waiters.push(handle);
        }
        for handle in self.other_blocks {
            handle.abort();
            waiters.push(handle);
        }
        join_all(waiters).await;
    }

    pub async fn send_blocks(
        &mut self,
        peer: AuthorityIndex,
        references: Vec<BlockReference>,
    ) -> Option<()> {
        let mut missing = Vec::new();
        for reference in references {
            let stored_block = self.inner.block_store.get_block(reference);
            let found = stored_block.is_some();
            match stored_block {
                // TODO: Should we be able to send more than one block in a single network message?
                Some(block) => self.sender.send(NetworkMessage::Block(block)).await.ok()?,
                None => missing.push(reference),
            }
            self.metrics
                .block_sync_requests_received
                .with_label_values(&[&peer.to_string(), &found.to_string()])
                .inc();
        }
        self.sender
            .send(NetworkMessage::BlockNotFound(missing))
            .await
            .ok()
    }

    pub async fn disseminate_own_blocks(&mut self, round: RoundNumber) {
        if let Some(existing) = self.own_blocks.take() {
            existing.abort();
            existing.await.ok();
        }

        let handle = Handle::current().spawn(Self::stream_own_blocks(
            self.sender.clone(),
            self.inner.clone(),
            round,
            self.parameters.batch_size,
        ));
        self.own_blocks = Some(handle);
    }

    async fn stream_own_blocks(
        to: mpsc::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<H, C>>,
        mut round: RoundNumber,
        batch_size: usize,
    ) -> Option<()> {
        loop {
            let notified = inner.notify.notified();
            let blocks = inner.block_store.get_own_blocks(round, batch_size);
            for block in blocks {
                round = block.round();
                to.send(NetworkMessage::Block(block)).await.ok()?;
            }
            notified.await
        }
    }

    // TODO:
    // * There should be a new protocol message that indicate when we should stop this task.
    // * Decide when to subscribe to a stream versus requesting specific blocks by ids.
    #[allow(dead_code)]
    pub fn disseminate_others_blocks(&mut self, round: RoundNumber, author: AuthorityIndex) {
        if self.other_blocks.len() >= self.parameters.maximum_helpers_per_authority {
            return;
        }

        let handle = Handle::current().spawn(Self::stream_others_blocks(
            self.sender.clone(),
            self.inner.clone(),
            round,
            author,
            self.parameters.batch_size,
            self.parameters.stream_interval,
        ));
        self.other_blocks.push(handle);
    }

    async fn stream_others_blocks(
        to: mpsc::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<H, C>>,
        mut round: RoundNumber,
        author: AuthorityIndex,
        batch_size: usize,
        stream_interval: Duration,
    ) -> Option<()> {
        loop {
            let blocks = inner
                .block_store
                .get_others_blocks(round, author, batch_size);
            for block in blocks {
                round = block.round();
                to.send(NetworkMessage::Block(block)).await.ok()?;
            }
            sleep(stream_interval).await;
        }
    }
}

enum BlockFetcherMessage {
    RegisterAuthority(AuthorityIndex, mpsc::Sender<NetworkMessage>),
    RemoveAuthority(AuthorityIndex),
}

pub struct BlockFetcher {
    sender: mpsc::Sender<BlockFetcherMessage>,
    handle: JoinHandle<Option<()>>,
}

impl BlockFetcher {
    pub fn start<B, C>(
        id: AuthorityIndex,
        inner: Arc<NetworkSyncerInner<B, C>>,
        metrics: Arc<Metrics>,
    ) -> Self
    where
        B: BlockHandler + 'static,
        C: CommitObserver + 'static,
    {
        let (sender, receiver) = mpsc::channel(100);
        let worker = BlockFetcherWorker::new(id, inner, receiver, metrics);
        let handle = Handle::current().spawn(worker.run());
        Self { sender, handle }
    }

    pub async fn register_authority(
        &self,
        authority: AuthorityIndex,
        sender: mpsc::Sender<NetworkMessage>,
    ) {
        self.sender
            .send(BlockFetcherMessage::RegisterAuthority(authority, sender))
            .await
            .ok();
    }

    pub async fn remove_authority(&self, authority: AuthorityIndex) {
        self.sender
            .send(BlockFetcherMessage::RemoveAuthority(authority))
            .await
            .ok();
    }

    pub async fn shutdown(self) {
        self.handle.abort();
        self.handle.await.ok();
    }
}

struct BlockFetcherWorker<B: BlockHandler, C: CommitObserver> {
    id: AuthorityIndex,
    inner: Arc<NetworkSyncerInner<B, C>>,
    receiver: mpsc::Receiver<BlockFetcherMessage>,
    senders: HashMap<AuthorityIndex, mpsc::Sender<NetworkMessage>>,
    parameters: SynchronizerParameters,
    metrics: Arc<Metrics>,
    /// Hold a timestamp of when blocks were first considered missing.
    missing: HashMap<BlockReference, Duration>,
    enable: bool,
}

impl<B, C> BlockFetcherWorker<B, C>
where
    B: BlockHandler + 'static,
    C: CommitObserver + 'static,
{
    pub fn new(
        id: AuthorityIndex,
        inner: Arc<NetworkSyncerInner<B, C>>,
        receiver: mpsc::Receiver<BlockFetcherMessage>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let enable = env::var("USE_SYNCER").is_ok();
        Self {
            id,
            inner,
            receiver,
            senders: Default::default(),
            parameters: Default::default(),
            metrics,
            missing: Default::default(),
            enable,
        }
    }

    async fn run(mut self) -> Option<()> {
        loop {
            tokio::select! {
                _ = sleep(self.parameters.sample_precision) => self.sync_strategy().await,
                message = self.receiver.recv() => {
                    match message {
                        Some(BlockFetcherMessage::RegisterAuthority(authority, sender)) => {
                            self.senders.insert(authority, sender);
                        },
                        Some(BlockFetcherMessage::RemoveAuthority(authority)) => {
                            self.senders.remove(&authority);
                        },
                        None => return None,
                    }
                }
            }
        }
    }

    /// A simple and naive strategy that requests missing blocks from random peers.
    async fn sync_strategy(&mut self) {
        if self.enable {
            return;
        }

        let now = timestamp_utc();
        let mut to_request = Vec::new();
        let missing_blocks = self.inner.syncer.get_missing_blocks().await;
        for (authority, missing) in missing_blocks.into_iter().enumerate() {
            self.metrics
                .missing_blocks
                .with_label_values(&[&authority.to_string()])
                .set(missing.len() as i64);

            for reference in missing {
                let time = self.missing.entry(reference).or_insert(now);
                if now.checked_sub(*time).unwrap_or_default() >= self.parameters.grace_period {
                    to_request.push(reference);
                    self.missing.remove(&reference); // todo - ensure we receive the block
                }
            }
        }
        self.missing.retain(|_, time| {
            now.checked_sub(*time).unwrap_or_default() < self.parameters.grace_period
        });

        // TODO: If we are missing many blocks from the same authority
        // (`missing.len() > self.parameters.new_stream_threshold`), it is likely that
        // we have a network partition. We should try to find an other peer from which
        // to (temporarily) sync the blocks from that authority.

        for chunks in to_request.chunks(net_sync::MAXIMUM_BLOCK_REQUEST) {
            let Some((peer, permit)) = self.sample_peer(&[self.id]) else {
                break;
            };
            let message = NetworkMessage::RequestBlocks(chunks.to_vec());
            permit.send(message);

            self.metrics
                .block_sync_requests_sent
                .with_label_values(&[&peer.to_string()])
                .inc();
        }
    }

    fn sample_peer(
        &self,
        except: &[AuthorityIndex],
    ) -> Option<(AuthorityIndex, mpsc::Permit<NetworkMessage>)> {
        let mut senders = self
            .senders
            .iter()
            .filter(|&(index, _)| !except.contains(index))
            .collect::<Vec<_>>();

        senders.shuffle(&mut thread_rng());

        for (peer, sender) in senders {
            if let Ok(permit) = sender.try_reserve() {
                return Some((*peer, permit));
            }
        }
        None
    }
}
