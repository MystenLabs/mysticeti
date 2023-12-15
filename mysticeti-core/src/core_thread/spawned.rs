// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block_handler::BlockHandler;
use crate::commit_observer::CommitObserver;
use crate::metrics::{Metrics, UtilizationTimerExt};
use crate::syncer::{Syncer, SyncerSignals};
use crate::types::AuthoritySet;
use crate::types::{RoundNumber, StatementBlock};
use crate::{data::Data, types::BlockReference};
use std::sync::Arc;
use std::{collections::HashSet, thread};
use tokio::sync::{mpsc, oneshot};

pub struct CoreThreadDispatcher<H: BlockHandler, S: SyncerSignals, C: CommitObserver> {
    sender: mpsc::Sender<CoreThreadCommand>,
    join_handle: thread::JoinHandle<Syncer<H, S, C>>,
    metrics: Arc<Metrics>,
}

pub struct CoreThread<H: BlockHandler, S: SyncerSignals, C: CommitObserver> {
    syncer: Syncer<H, S, C>,
    receiver: mpsc::Receiver<CoreThreadCommand>,
}

enum CoreThreadCommand {
    AddBlocks(Vec<Data<StatementBlock>>, AuthoritySet, oneshot::Sender<()>),
    ForceNewBlock(RoundNumber, AuthoritySet, oneshot::Sender<()>),
    Cleanup(oneshot::Sender<()>),
    /// Request missing blocks that need to be synched.
    GetMissing(oneshot::Sender<Vec<HashSet<BlockReference>>>),
    Processed(
        Vec<BlockReference>,
        oneshot::Sender<HashSet<BlockReference>>,
    ),
}

impl<H: BlockHandler + 'static, S: SyncerSignals + 'static, C: CommitObserver + 'static>
    CoreThreadDispatcher<H, S, C>
{
    pub fn start(syncer: Syncer<H, S, C>) -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let metrics = syncer.core().metrics.clone();
        let core_thread = CoreThread { syncer, receiver };
        let join_handle = thread::Builder::new()
            .name("mysticeti-core".to_string())
            .spawn(move || core_thread.run())
            .unwrap();
        Self {
            sender,
            join_handle,
            metrics,
        }
    }

    pub fn stop(self) -> Syncer<H, S, C> {
        drop(self.sender);
        self.join_handle.join().unwrap()
    }

    pub async fn add_blocks(
        &self,
        blocks: Vec<Data<StatementBlock>>,
        connected_authorities: AuthoritySet,
    ) {
        let (sender, receiver) = oneshot::channel();
        self.send(CoreThreadCommand::AddBlocks(
            blocks,
            connected_authorities,
            sender,
        ))
        .await;
        receiver.await.expect("core thread is not expected to stop");
    }

    pub async fn force_new_block(&self, round: RoundNumber, connected_authorities: AuthoritySet) {
        let (sender, receiver) = oneshot::channel();
        self.send(CoreThreadCommand::ForceNewBlock(
            round,
            connected_authorities,
            sender,
        ))
        .await;
        receiver.await.expect("core thread is not expected to stop");
    }

    pub async fn cleanup(&self) {
        let (sender, receiver) = oneshot::channel();
        self.send(CoreThreadCommand::Cleanup(sender)).await;
        receiver.await.expect("core thread is not expected to stop");
    }

    pub async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        let (sender, receiver) = oneshot::channel();
        self.send(CoreThreadCommand::GetMissing(sender)).await;
        receiver.await.expect("core thread is not expected to stop")
    }

    pub async fn processed(&self, refs: Vec<BlockReference>) -> HashSet<BlockReference> {
        let (sender, receiver) = oneshot::channel();
        self.send(CoreThreadCommand::Processed(refs, sender)).await;
        receiver.await.expect("core thread is not expected to stop")
    }
    async fn send(&self, command: CoreThreadCommand) {
        self.metrics.core_lock_enqueued.inc();
        if self.sender.send(command).await.is_err() {
            panic!("core thread is not expected to stop");
        }
    }
}

impl<H: BlockHandler, S: SyncerSignals, C: CommitObserver> CoreThread<H, S, C> {
    pub fn run(mut self) -> Syncer<H, S, C> {
        tracing::info!("Started core thread with tid {}", gettid::gettid());
        let metrics = self.syncer.core().metrics.clone();
        while let Some(command) = self.receiver.blocking_recv() {
            let _timer = metrics.core_lock_util.utilization_timer();
            metrics.core_lock_dequeued.inc();
            match command {
                CoreThreadCommand::AddBlocks(blocks, connected_authorities, sender) => {
                    self.syncer.add_blocks(blocks, connected_authorities);
                    sender.send(()).ok();
                }
                CoreThreadCommand::ForceNewBlock(round, connected_authorities, sender) => {
                    self.syncer.force_new_block(round, connected_authorities);
                    sender.send(()).ok();
                }
                CoreThreadCommand::Cleanup(sender) => {
                    self.syncer.core().cleanup();
                    sender.send(()).ok();
                }
                CoreThreadCommand::GetMissing(sender) => {
                    let missing = self.syncer.core().block_manager().missing_blocks();
                    sender.send(missing.to_vec()).ok();
                }
                CoreThreadCommand::Processed(refs, sender) => {
                    let result = refs
                        .into_iter()
                        .filter(|block_id| {
                            self.syncer
                                .core()
                                .block_manager()
                                .exists_or_pending(*block_id)
                        })
                        .collect();
                    sender.send(result).ok();
                }
            }
        }
        self.syncer
    }
}
