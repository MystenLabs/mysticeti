// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::data::Data;
use crate::types::{BlockDigest, BlockReference, RoundNumber, StatementBlock};
use crate::wal::{WalPosition, WalReader, WalWriter};
use crate::AuthorityIndex;
use parking_lot::RwLock;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct BlockStore {
    inner: Arc<RwLock<BlockStoreInner>>,
    block_wal_reader: Arc<WalReader>,
}

#[derive(Default)]
struct BlockStoreInner {
    index: HashMap<RoundNumber, HashMap<(AuthorityIndex, BlockDigest), IndexEntry>>,
    highest_round: RoundNumber,
}

pub trait BlockWriter {
    fn insert_block(&mut self, block: Data<StatementBlock>);
}

#[derive(Clone)]
enum IndexEntry {
    #[allow(dead_code)]
    WalPosition(WalPosition),
    Loaded(Data<StatementBlock>),
}

impl BlockStore {
    pub fn new(block_wal_reader: Arc<WalReader>) -> Self {
        Self {
            block_wal_reader,
            inner: Default::default(), // todo - read wal
        }
    }

    pub fn insert_block(&self, block: Data<StatementBlock>, _position: WalPosition) {
        let mut inner = self.inner.write();
        inner.highest_round = max(inner.highest_round, block.round());
        let map = inner.index.entry(block.round()).or_default();
        map.insert((block.author(), block.digest()), IndexEntry::Loaded(block));
    }

    pub fn get_block(&self, reference: BlockReference) -> Option<Data<StatementBlock>> {
        let entry = self.inner.read().get_block(reference);
        entry.map(|pos| self.read_index(pos))
    }

    pub fn get_blocks_by_round(&self, round: RoundNumber) -> Vec<Data<StatementBlock>> {
        let entries = self.inner.read().get_blocks_by_round(round);
        self.read_index_vec(entries)
    }

    pub fn get_blocks_at_authority_round(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
    ) -> Vec<Data<StatementBlock>> {
        let entries = self
            .inner
            .read()
            .get_blocks_at_authority_round(authority, round);
        self.read_index_vec(entries)
    }

    pub fn block_exists_at_authority_round(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
    ) -> bool {
        let inner = self.inner.read();
        let Some(blocks) = inner.index.get(&round) else { return false; };
        blocks
            .keys()
            .any(|(block_authority, _)| *block_authority == authority)
    }

    pub fn block_exists(&self, reference: BlockReference) -> bool {
        self.inner.read().block_exists(reference)
    }

    pub fn len_expensive(&self) -> usize {
        let inner = self.inner.read();
        inner.index.values().map(HashMap::len).sum()
    }

    pub fn highest_round(&self) -> RoundNumber {
        self.inner.read().highest_round
    }

    fn read_index(&self, entry: IndexEntry) -> Data<StatementBlock> {
        match entry {
            IndexEntry::WalPosition(position) => {
                let data = self
                    .block_wal_reader
                    .read(position)
                    .expect("Failed to read wal");
                // todo - avoid copy of data
                bincode::deserialize(&data).expect("Failed to deserialize data from wal")
            }
            IndexEntry::Loaded(block) => block,
        }
    }

    fn read_index_vec(&self, entries: Vec<IndexEntry>) -> Vec<Data<StatementBlock>> {
        entries
            .into_iter()
            .map(|pos| self.read_index(pos))
            .collect()
    }
}

impl BlockStoreInner {
    pub fn block_exists(&self, reference: BlockReference) -> bool {
        let Some(blocks) = self.index.get(&reference.round) else { return false; };
        blocks.contains_key(&(reference.authority, reference.digest))
    }

    pub fn get_blocks_at_authority_round(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
    ) -> Vec<IndexEntry> {
        let Some(blocks) = self.index.get(&round) else { return vec![]; };
        blocks
            .iter()
            .filter_map(|((a, _), entry)| {
                if *a == authority {
                    Some(entry.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_blocks_by_round(&self, round: RoundNumber) -> Vec<IndexEntry> {
        let Some(blocks) = self.index.get(&round) else { return vec![]; };
        blocks.values().cloned().collect()
    }

    pub fn get_block(&self, reference: BlockReference) -> Option<IndexEntry> {
        self.index
            .get(&reference.round)?
            .get(&(reference.authority, reference.digest))
            .cloned()
    }
}

impl BlockWriter for (&mut WalWriter, &BlockStore) {
    fn insert_block(&mut self, block: Data<StatementBlock>) {
        let pos = self
            .0
            .write(&bincode::serialize(&block).expect("Serialization failed"))
            .expect("Writing to wal failed");
        self.1.insert_block(block, pos);
    }
}
