// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use crate::block_handler::BlockHandler;
use crate::commit_observer::CommitObserver;
use crate::data::Data;
use crate::syncer::{Syncer, SyncerSignals};
use crate::types::{AuthoritySet, BlockReference};
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

    pub async fn add_blocks(
        &self,
        blocks: Vec<Data<StatementBlock>>,
        connected_authorities: AuthoritySet,
    ) {
        self.syncer.lock().add_blocks(blocks, connected_authorities);
    }

    pub async fn force_new_block(&self, round: RoundNumber, connected_authorities: AuthoritySet) {
        self.syncer
            .lock()
            .force_new_block(round, connected_authorities);
    }

    pub async fn cleanup(&self) {
        self.syncer.lock().core().cleanup();
    }

    pub async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        self.syncer
            .lock()
            .core()
            .block_manager()
            .missing_blocks()
            .to_vec()
    }

    pub async fn processed(&self, refs: Vec<BlockReference>) -> HashSet<BlockReference> {
        let lock = self.syncer.lock();
        refs.into_iter()
            .filter_map(|block_id| {
                if lock.core().block_manager().exists_or_pending(block_id) {
                    Some(block_id)
                } else {
                    None
                }
            })
            .collect()
    }
}
