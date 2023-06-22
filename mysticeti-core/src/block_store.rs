// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::commit_interpreter::CommittedSubDag;
use crate::data::Data;
use crate::state::{RecoveredState, RecoveredStateBuilder};
use crate::types::{AuthorityIndex, BlockDigest, BlockReference, RoundNumber, StatementBlock};
use crate::wal::{Tag, WalPosition, WalReader, WalWriter};
use minibytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashMap;
use std::io::IoSlice;
use std::sync::Arc;
use std::time::Instant;

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
    fn insert_block(&mut self, block: Data<StatementBlock>) -> WalPosition;
    fn insert_own_block(&mut self, block: &OwnBlockData);
}

#[derive(Clone)]
enum IndexEntry {
    #[allow(dead_code)]
    WalPosition(WalPosition),
    Loaded(Data<StatementBlock>),
}

impl BlockStore {
    pub fn open(block_wal_reader: Arc<WalReader>, wal_writer: &WalWriter) -> RecoveredState {
        let mut inner = BlockStoreInner::default();
        let mut builder = RecoveredStateBuilder::new();
        let mut replay_started: Option<Instant> = None;
        for (pos, (tag, data)) in block_wal_reader.iter_until(wal_writer) {
            if replay_started.is_none() {
                replay_started = Some(Instant::now());
                tracing::info!("Wal is not empty, starting replay");
            }
            let block = match tag {
                WAL_ENTRY_BLOCK => {
                    let block = Data::<StatementBlock>::from_bytes(data)
                        .expect("Failed to deserialize data from wal");
                    builder.block(pos, &block);
                    block
                }
                WAL_ENTRY_PAYLOAD => {
                    builder.payload(pos, data);
                    continue;
                }
                WAL_ENTRY_OWN_BLOCK => {
                    let (own_block_data, own_block) = OwnBlockData::from_bytes(data)
                        .expect("Failed to deserialized own block data from wal");
                    builder.own_block(own_block_data);
                    own_block
                }
                WAL_ENTRY_STATE => {
                    builder.state(data);
                    continue;
                }
                WAL_ENTRY_COMMIT => {
                    let commit_data = bincode::deserialize(&data)
                        .expect("Failed to deserialized commit data from wal");
                    builder.commit_data(commit_data);
                    continue;
                }
                _ => panic!("Unknown wal tag {tag} at position {pos}"),
            };
            inner.add_to_index(block);
        }
        if let Some(replay_started) = replay_started {
            tracing::info!("Wal replay completed in {:?}", replay_started.elapsed());
        } else {
            tracing::info!("Wal is empty, will start from genesis");
        }
        let this = Self {
            block_wal_reader,
            inner: Arc::new(RwLock::new(inner)),
        };
        builder.build(this)
    }

    pub fn insert_block(&self, block: Data<StatementBlock>, _position: WalPosition) {
        self.inner.write().add_to_index(block);
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
                let (tag, data) = self
                    .block_wal_reader
                    .read(position)
                    .expect("Failed to read wal");
                // todo - handle own block data
                assert_eq!(tag, WAL_ENTRY_BLOCK);
                Data::from_bytes(data).expect("Failed to deserialize data from wal")
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

    pub fn add_to_index(&mut self, block: Data<StatementBlock>) {
        self.highest_round = max(self.highest_round, block.round());
        let map = self.index.entry(block.round()).or_default();
        map.insert((block.author(), block.digest()), IndexEntry::Loaded(block));
    }
}

pub const WAL_ENTRY_BLOCK: Tag = 1;
pub const WAL_ENTRY_PAYLOAD: Tag = 2;
pub const WAL_ENTRY_OWN_BLOCK: Tag = 3;
pub const WAL_ENTRY_STATE: Tag = 4;
pub const WAL_ENTRY_COMMIT: Tag = 5;

impl BlockWriter for (&mut WalWriter, &BlockStore) {
    fn insert_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        let pos = self
            .0
            .write(WAL_ENTRY_BLOCK, block.serialized_bytes())
            .expect("Writing to wal failed");
        self.1.insert_block(block, pos);
        pos
    }

    fn insert_own_block(&mut self, data: &OwnBlockData) {
        let block_pos = data.write_to_wal(self.0);
        self.1.insert_block(data.block.clone(), block_pos);
    }
}

// This data structure has a special serialization in/from Bytes, see OwnBlockData::from_bytes/write_to_wal
pub struct OwnBlockData {
    pub next_entry: WalPosition,
    pub block: Data<StatementBlock>,
}

const OWN_BLOCK_HEADER_SIZE: usize = 8;

impl OwnBlockData {
    // A bit of custom serialization to minimize data copy, relies on own_block_serialization_test
    pub fn from_bytes(bytes: Bytes) -> bincode::Result<(OwnBlockData, Data<StatementBlock>)> {
        let next_entry = &bytes[..OWN_BLOCK_HEADER_SIZE];
        let next_entry: WalPosition = bincode::deserialize(next_entry)?;
        let block = bytes.slice(OWN_BLOCK_HEADER_SIZE..);
        let block = Data::<StatementBlock>::from_bytes(block)?;
        let own_block_data = OwnBlockData {
            next_entry,
            block: block.clone(),
        };
        Ok((own_block_data, block))
    }

    pub fn write_to_wal(&self, writer: &mut WalWriter) -> WalPosition {
        let header = bincode::serialize(&self.next_entry).expect("Serialization failed");
        let header = IoSlice::new(&header);
        let block = IoSlice::new(self.block.serialized_bytes());
        writer
            .writev(WAL_ENTRY_OWN_BLOCK, &[header, block])
            .expect("Writing to wal failed")
    }
}

#[derive(Serialize, Deserialize)]
pub struct CommitData {
    pub leader: BlockReference,
    // All committed blocks, including the leader
    pub sub_dag: Vec<BlockReference>,
}

impl From<&CommittedSubDag> for CommitData {
    fn from(value: &CommittedSubDag) -> Self {
        let sub_dag = value.blocks.iter().map(|b| *b.reference()).collect();
        Self {
            leader: value.anchor,
            sub_dag,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn own_block_serialization_test() {
        let next_entry = WalPosition::default();
        let serialized = bincode::serialize(&next_entry).unwrap();
        assert_eq!(serialized.len(), OWN_BLOCK_HEADER_SIZE);
    }
}
