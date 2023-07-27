// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use futures::future::join_all;
use tokio::sync::mpsc;

use crate::{
    block_handler::BlockHandler,
    net_sync::NetworkSyncerInner,
    network::NetworkMessage,
    runtime::{sleep, Handle, JoinHandle},
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
    /// The interval at which to send stream blocks authored by other nodes.
    pub stream_interval: Duration,
    /// Threshold number of missing block from an authority to open a new stream.
    pub new_stream_threshold: usize,
    /// The maximum number of blocks to request in a single request.
    pub maximum_block_request: usize,
}

impl Default for SynchronizerParameters {
    fn default() -> Self {
        Self {
            absolute_maximum_helpers: 10,
            maximum_helpers_per_authority: 2,
            batch_size: 10,
            sample_precision: Duration::from_secs(5),
            stream_interval: Duration::from_secs(1),
            new_stream_threshold: 10,
            maximum_block_request: 10,
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
}

impl<H, C> BlockDisseminator<H, C>
where
    H: BlockHandler + 'static,
    C: CommitObserver + 'static,
{
    pub fn new(sender: mpsc::Sender<NetworkMessage>, inner: Arc<NetworkSyncerInner<H, C>>) -> Self {
        Self {
            sender,
            inner,
            own_blocks: None,
            other_blocks: Vec::new(),
            parameters: SynchronizerParameters::default(),
        }
    }

    pub async fn cleanup(mut self) {
        let mut waiters = Vec::new();
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

    pub async fn send_blocks(&mut self, mut references: Vec<BlockReference>) -> Option<()> {
        // Ensure that we don't send too many blocks from a single request.
        references.truncate(self.parameters.maximum_block_request);

        let mut missing = Vec::new();
        for reference in references {
            match self.inner.block_store.get_block(reference) {
                Some(block) => self.sender.send(NetworkMessage::Block(block)).await.ok()?,
                None => missing.push(reference),
            }
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

// enum BlockFetcherMessage {
//     RegisterAuthority(AuthorityIndex, mpsc::Sender<NetworkMessage>),
//     RemoveAuthority(AuthorityIndex),
// }

// pub struct BlockFetcher {
//     sender: mpsc::Sender<BlockFetcherMessage>,
//     handle: JoinHandle<Option<()>>,
// }

// impl BlockFetcher {
//     pub fn new<B, C>(inner: Arc<NetworkSyncerInner<B, C>>) -> Self
//     where
//         B: BlockHandler + 'static,
//         C: CommitObserver + 'static,
//     {
//         let (sender, receiver) = mpsc::channel(100);
//         let worker = BlockFetcherWorker::new(inner, receiver);
//         let handle = Handle::current().spawn(worker.run());
//         Self { sender, handle }
//     }

//     pub async fn register_authority(
//         &self,
//         authority: AuthorityIndex,
//         sender: mpsc::Sender<NetworkMessage>,
//     ) {
//         self.sender
//             .send(BlockFetcherMessage::RegisterAuthority(authority, sender))
//             .await
//             .ok();
//     }

//     pub async fn remove_authority(&self, authority: AuthorityIndex) {
//         self.sender
//             .send(BlockFetcherMessage::RemoveAuthority(authority))
//             .await
//             .ok();
//     }

//     pub async fn shutdown(self) {
//         self.handle.abort();
//         self.handle.await.ok();
//     }
// }

// struct BlockFetcherWorker<B: BlockHandler, C: CommitObserver> {
//     id: AuthorityIndex,
//     inner: Arc<NetworkSyncerInner<B, C>>,
//     receiver: mpsc::Receiver<BlockFetcherMessage>,
//     senders: HashMap<AuthorityIndex, mpsc::Sender<NetworkMessage>>,
//     parameters: SynchronizerParameters,
// }

// impl<B, C> BlockFetcherWorker<B, C>
// where
//     B: BlockHandler + 'static,
//     C: CommitObserver + 'static,
// {
//     pub fn new(
//         inner: Arc<NetworkSyncerInner<B, C>>,
//         receiver: mpsc::Receiver<BlockFetcherMessage>,
//     ) -> Self {
//         let id = inner.syncer.read().core().authority();
//         Self {
//             id,
//             inner,
//             receiver,
//             senders: Default::default(),
//             parameters: Default::default(),
//         }
//     }

//     async fn run(mut self) -> Option<()> {
//         loop {
//             tokio::select! {
//                 _ = sleep(self.parameters.sample_precision) => {
//                     for (peer, message) in self.sync_strategy().await {
//                         let sender = self.senders.get(&peer).unwrap();
//                         sender.send(message).await.ok()?;
//                     }
//                 },
//                 message = self.receiver.recv() => {
//                     match message {
//                         Some(BlockFetcherMessage::RegisterAuthority(authority, sender)) => {
//                             self.senders.insert(authority, sender);
//                         },
//                         Some(BlockFetcherMessage::RemoveAuthority(authority)) => {
//                             self.senders.remove(&authority);
//                         },
//                         None => return None,
//                     }
//                 }
//             }
//         }
//     }

//     /// A simple and naive strategy that requests missing blocks from random peers.
//     async fn sync_strategy(&self) -> Vec<(u64, NetworkMessage)> {
//         let mut to_request = Vec::new();
//         let syncer = self.inner.syncer.read();
//         let missing_blocks = syncer.core().block_manager().missing_blocks();
//         for missing in missing_blocks.values() {
//             // TODO: If we are missing many blocks from the same authority
//             // (`missing.len() > self.parameters.new_stream_threshold`), it is likely that
//             // we have a network partition. We should try to find an other peer from which
//             // to (temporarily) sync the blocks from that authority.

//             to_request.extend(missing.iter().cloned().collect::<Vec<_>>());
//         }

//         let mut messages = Vec::new();
//         for chunks in to_request.chunks(self.parameters.maximum_block_request) {
//             let Some(peer) = self.sample_peer(&[self.id]) else { break };
//             let message = NetworkMessage::RequestBlocks(chunks.to_vec());
//             messages.push((peer, message));
//         }
//         messages
//     }

//     fn sample_peer(&self, except: &[AuthorityIndex]) -> Option<AuthorityIndex> {
//         self.senders
//             .keys()
//             .filter(|&index| !except.contains(index))
//             .collect::<Vec<_>>()
//             .choose(&mut thread_rng())
//             .map(|index| **index)
//     }
// }
