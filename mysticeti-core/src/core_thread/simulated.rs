// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use crate::block_handler::BlockHandler;
use crate::commit_observer::CommitObserver;
use crate::data::Data;
use crate::metrics::{LatencyHistogramVecExt, Metrics};
use crate::syncer::{Syncer, SyncerSignals};
use crate::types::{AuthoritySet, BlockReference};
use crate::types::{RoundNumber, StatementBlock};
use parking_lot::Mutex;

pub struct CoreThreadDispatcher<H: BlockHandler, S: SyncerSignals, C: CommitObserver> {
    syncer: Mutex<Syncer<H, S, C>>,
    metrics: Arc<Metrics>,
}

impl<H: BlockHandler + 'static, S: SyncerSignals + 'static, C: CommitObserver + 'static>
    CoreThreadDispatcher<H, S, C>
{
    pub fn start(syncer: Syncer<H, S, C>) -> Self {
        Self {
            metrics: syncer.core().metrics.clone(),
            syncer: Mutex::new(syncer),
        }
    }

    pub fn stop(self) -> Syncer<H, S, C> {
        self.syncer.into_inner()
    }

    pub async fn add_blocks(
        &self,
        blocks: Vec<Data<StatementBlock>>,
        connected_authorities: AuthoritySet,
    ) -> Vec<BlockReference> {
        let _guard = self
            .metrics
            .code_scope_latency
            .latency_histogram(&["CoreThreadDispatcher::add_blocks"]);
        self.syncer.lock().add_blocks(blocks, connected_authorities)
    }

    pub async fn force_new_block(&self, round: RoundNumber, connected_authorities: AuthoritySet) {
        let _guard = self
            .metrics
            .code_scope_latency
            .latency_histogram(&["CoreThreadDispatcher::force_new_block"]);
        self.syncer
            .lock()
            .force_new_block(round, connected_authorities);
    }

    pub async fn cleanup(&self) {
        let _guard = self
            .metrics
            .code_scope_latency
            .latency_histogram(&["CoreThreadDispatcher::cleanup"]);
        self.syncer.lock().core().cleanup();
    }

    pub async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        let _guard = self
            .metrics
            .code_scope_latency
            .latency_histogram(&["CoreThreadDispatcher::get_missing_blocks"]);
        self.syncer
            .lock()
            .core()
            .block_manager()
            .missing_blocks()
            .to_vec()
    }
}
