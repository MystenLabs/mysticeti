use crate::block_store::{BlockStore, OwnBlockData};
use crate::core::MetaStatement;
use crate::data::Data;
use crate::types::{BlockReference, StatementBlock};
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::collections::{BTreeMap, VecDeque};

pub struct RecoveredState {
    pub block_store: BlockStore,
    pub last_own_block: Option<OwnBlockData>,
    pub pending: VecDeque<(WalPosition, MetaStatement)>,
}

#[derive(Default)]
pub struct RecoveredStateBuilder {
    pending: BTreeMap<WalPosition, RawMetaStatement>,
    last_own_block: Option<OwnBlockData>,
}

impl RecoveredStateBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block(&mut self, pos: WalPosition, block: &Data<StatementBlock>) {
        self.pending
            .insert(pos, RawMetaStatement::Include(*block.reference()));
    }

    pub fn payload(&mut self, pos: WalPosition, payload: Bytes) {
        self.pending.insert(pos, RawMetaStatement::Payload(payload));
    }

    pub fn own_block(&mut self, own_block_data: OwnBlockData) {
        // Edge case of WalPosition::MAX is automatically handled here, empty map is returned
        self.pending = self.pending.split_off(&own_block_data.next_entry);
        self.last_own_block = Some(own_block_data);
    }

    pub fn build(self, block_store: BlockStore) -> RecoveredState {
        let pending = self
            .pending
            .into_iter()
            .map(|(pos, raw)| (pos, raw.into_meta_statement()))
            .collect();
        RecoveredState {
            pending,
            last_own_block: self.last_own_block,
            block_store,
        }
    }
}

enum RawMetaStatement {
    Include(BlockReference),
    Payload(Bytes),
}

impl RawMetaStatement {
    fn into_meta_statement(self) -> MetaStatement {
        match self {
            RawMetaStatement::Include(include) => MetaStatement::Include(include),
            RawMetaStatement::Payload(payload) => MetaStatement::Payload(
                bincode::deserialize(&payload).expect("Failed to deserialize payload"),
            ),
        }
    }
}
