// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::{rngs::StdRng, Rng, SeedableRng};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{channel, Receiver};

use crate::runtime::JoinHandle;
use crate::{
    crypto::AsBytes,
    runtime,
    runtime::timestamp_utc,
    types::{AuthorityIndex, Transaction},
};

pub struct TransactionGeneratorHandle {
    stop: mpsc::Sender<()>,
    handle: JoinHandle<()>,
}

impl TransactionGeneratorHandle {
    pub async fn shutdown(self) {
        self.stop.send(()).await.ok();
        self.handle.await.ok();
    }
}

pub struct TransactionGenerator {
    sender: mpsc::Sender<Vec<Transaction>>,
    rng: StdRng,
    transactions_per_second: usize,
    transaction_size: usize,
    initial_delay: Duration,
}

impl TransactionGenerator {
    pub const DEFAULT_TRANSACTION_SIZE: usize = 512;
    const TARGET_BLOCK_INTERVAL: Duration = Duration::from_millis(100);

    pub fn start(
        sender: mpsc::Sender<Vec<Transaction>>,
        seed: AuthorityIndex,
        transactions_per_second: usize,
        transaction_size: usize,
        initial_delay: Duration,
    ) -> TransactionGeneratorHandle {
        assert!(transaction_size >= 8 + 8); // 8 bytes timestamp + 8 bytes random
        let (stop, rx_stop) = channel(1);
        let handle = runtime::Handle::current().spawn(
            Self {
                sender,
                rng: StdRng::seed_from_u64(seed),
                transactions_per_second,
                transaction_size,
                initial_delay,
            }
            .run(rx_stop),
        );

        TransactionGeneratorHandle { stop, handle }
    }

    pub async fn run(mut self, mut rx_stop: Receiver<()>) {
        let transactions_per_100ms = (self.transactions_per_second + 9) / 10;

        let mut counter = 0;
        let mut random: u64 = self.rng.gen(); // 8 bytes
        let zeros = vec![0u8; self.transaction_size - 8 - 8]; // 8 bytes timestamp + 8 bytes random

        let mut interval = runtime::TimeInterval::new(Self::TARGET_BLOCK_INTERVAL);
        runtime::sleep(self.initial_delay).await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut block = Vec::with_capacity(transactions_per_100ms);
                    let timestamp = (timestamp_utc().as_millis() as u64).to_le_bytes();

                    for _ in 0..transactions_per_100ms {
                        random += counter;

                        let mut transaction = Vec::with_capacity(self.transaction_size);
                        transaction.extend_from_slice(&timestamp); // 8 bytes
                        transaction.extend_from_slice(&random.to_le_bytes()); // 8 bytes
                        transaction.extend_from_slice(&zeros[..]);

                        block.push(Transaction::new(transaction));
                        counter += 1;
                    }

                    if self.sender.send(block).await.is_err() {
                        break;
                    }
                },
                _ = rx_stop.recv() => {
                    tracing::info!("Shutting down transactions generator");
                    return;
                }
            }
        }
    }

    pub fn extract_timestamp(transaction: &Transaction) -> Duration {
        let bytes = transaction.as_bytes()[0..8]
            .try_into()
            .expect("Transactions should be at least 8 bytes");
        Duration::from_millis(u64::from_le_bytes(bytes))
    }
}
