// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use crate::block_handler::BlockHandler;
use crate::data::Data;
use crate::syncer::{CommitObserver, Syncer, SyncerSignals};
use crate::types::AuthorityIndex;
use crate::types::BlockReference;
use crate::types::{RoundNumber, StatementBlock};
use parking_lot::Mutex;

pub struct CoreThreadDispatcher<H: BlockHandler, S: SyncerSignals, C: CommitObserver> {
    syncer: Mutex<Syncer<H, S, C>>,
}

impl<H: BlockHandler + 'static, S: SyncerSignals + 'static, C: CommitObserver + 'static>
    CoreThreadDispatcher<H, S, C>
{
    pub fn start(syncer: Syncer<H, S, C>) -> Self {
        Self {
            syncer: Mutex::new(syncer),
        }
    }

    pub fn stop(self) -> Syncer<H, S, C> {
        self.syncer.into_inner()
    }

    pub async fn add_blocks(&self, blocks: Vec<Data<StatementBlock>>) {
        self.syncer.lock().add_blocks(blocks);
    }

    pub async fn force_new_block(&self, round: RoundNumber) {
        self.syncer.lock().force_new_block(round);
    }

    pub async fn cleanup(&self) {
        self.syncer.lock().core().cleanup();
    }

    pub async fn get_missing_blocks(&self) -> HashMap<AuthorityIndex, HashSet<BlockReference>> {
        self.syncer
            .lock()
            .core()
            .block_manager()
            .missing_blocks()
            .clone()
    }
}
