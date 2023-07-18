use crate::committee::ProcessedTransactionHandler;
use crate::runtime;
use crate::types::TransactionId;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::Path;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct TransactionLog {
    ch: UnboundedSender<Vec<TransactionId>>,
}

impl TransactionLog {
    pub fn start(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let (sender, receiver) = unbounded_channel();
        runtime::Handle::current().spawn(Self::run(file, receiver));
        Ok(Self { ch: sender })
    }

    async fn run(mut file: File, mut receiver: UnboundedReceiver<Vec<TransactionId>>) {
        while let Some(id) = receiver.recv().await {
            writeln!(file, "{:?}", id).expect("Failed to write to transaction log");
        }
    }
}

impl ProcessedTransactionHandler<TransactionId> for TransactionLog {
    fn transaction_processed(&mut self, k: TransactionId) {
        self.ch.send(vec![k]).ok();
    }
}
