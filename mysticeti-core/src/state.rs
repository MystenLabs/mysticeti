use crate::block_store::{BlockStore, CommitData, OwnBlockData};
use crate::core::MetaStatement;
use crate::data::Data;
use crate::types::{BlockReference, StatementBlock};
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::collections::{BTreeMap, HashSet, VecDeque};

pub struct RecoveredState {
    pub block_store: BlockStore,
    pub last_own_block: Option<OwnBlockData>,
    pub pending: VecDeque<(WalPosition, MetaStatement)>,
    pub state: Option<Bytes>,
    pub unprocessed_blocks: Vec<Data<StatementBlock>>,

    pub last_committed_leader: Option<BlockReference>,
    pub committed_blocks: HashSet<BlockReference>,
    pub committed_state: Option<Bytes>,
}

#[derive(Default)]
pub struct RecoveredStateBuilder {
    pending: BTreeMap<WalPosition, RawMetaStatement>,
    last_own_block: Option<OwnBlockData>,
    state: Option<Bytes>,
    unprocessed_blocks: Vec<Data<StatementBlock>>,

    last_committed_leader: Option<BlockReference>,
    committed_blocks: HashSet<BlockReference>,
    committed_state: Option<Bytes>,
}

impl RecoveredStateBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block(&mut self, pos: WalPosition, block: &Data<StatementBlock>) {
        self.pending
            .insert(pos, RawMetaStatement::Include(*block.reference()));
        self.unprocessed_blocks.push(block.clone());
    }

    pub fn payload(&mut self, pos: WalPosition, payload: Bytes) {
        self.pending.insert(pos, RawMetaStatement::Payload(payload));
    }

    pub fn own_block(&mut self, own_block_data: OwnBlockData) {
        // Edge case of WalPosition::MAX is automatically handled here, empty map is returned
        self.pending = self.pending.split_off(&own_block_data.next_entry);
        self.unprocessed_blocks.push(own_block_data.block.clone());
        self.last_own_block = Some(own_block_data);
    }

    pub fn state(&mut self, state: Bytes) {
        self.state = Some(state);
        self.unprocessed_blocks.clear();
    }

    pub fn commit_data(&mut self, commits: Vec<CommitData>, committed_state: Bytes) {
        for commit_data in commits {
            self.last_committed_leader = Some(commit_data.leader);
            self.committed_blocks
                .extend(commit_data.sub_dag.into_iter());
        }
        self.committed_state = Some(committed_state);
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
            state: self.state,
            unprocessed_blocks: self.unprocessed_blocks,
            last_committed_leader: self.last_committed_leader,
            committed_blocks: self.committed_blocks,
            committed_state: self.committed_state,
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
