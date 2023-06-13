// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::data::Data;
use crate::types::{BlockDigest, BlockReference, RoundNumber, StatementBlock};
use crate::AuthorityIndex;
use parking_lot::RwLock;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct BlockStore {
    inner: Arc<RwLock<BlockStoreInner>>,
}

#[derive(Default)]
pub struct BlockStoreInner {
    blocks: HashMap<RoundNumber, HashMap<(AuthorityIndex, BlockDigest), Data<StatementBlock>>>,
    highest_round: RoundNumber,
}

impl BlockStore {
    pub fn insert_block(&self, block: Data<StatementBlock>) {
        let mut inner = self.inner.write();
        inner.highest_round = max(inner.highest_round, block.round());
        let map = inner.blocks.entry(block.round()).or_default();
        map.insert((block.author(), block.digest()), block);
    }

    pub fn get_block(&self, reference: BlockReference) -> Option<Data<StatementBlock>> {
        self.inner
            .read()
            .blocks
            .get(&reference.round)?
            .get(&(reference.authority, reference.digest))
            .cloned()
    }

    pub fn get_blocks_by_round(&self, round: RoundNumber) -> Vec<Data<StatementBlock>> {
        let inner = self.inner.read();
        let Some(blocks) = inner.blocks.get(&round) else { return vec![]; };
        blocks.values().cloned().collect()
    }

    pub fn get_blocks_at_authority_round(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
    ) -> Vec<Data<StatementBlock>> {
        let inner = self.inner.read();
        let Some(blocks) = inner.blocks.get(&round) else { return vec![]; };
        blocks
            .values()
            .filter(|block| block.reference().authority == authority)
            .cloned()
            .collect()
    }

    pub fn block_exists_at_authority_round(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
    ) -> bool {
        let inner = self.inner.read();
        let Some(blocks) = inner.blocks.get(&round) else { return false; };
        blocks
            .values()
            .any(|block| block.reference().authority == authority)
    }

    pub fn block_exists(&self, reference: BlockReference) -> bool {
        self.inner.read().block_exists(reference)
    }

    pub fn len_expensive(&self) -> usize {
        let inner = self.inner.read();
        inner.blocks.values().map(HashMap::len).sum()
    }

    pub fn highest_round(&self) -> RoundNumber {
        self.inner.read().highest_round
    }
}

impl BlockStoreInner {
    pub fn block_exists(&self, reference: BlockReference) -> bool {
        let Some(blocks) = self.blocks.get(&reference.round) else { return false; };
        blocks.contains_key(&(reference.authority, reference.digest))
    }
}
