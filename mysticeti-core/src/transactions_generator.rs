// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{cmp::min, time::Duration};
use tokio::sync::mpsc;

use crate::{
    crypto::AsBytes,
    runtime,
    runtime::timestamp_utc,
    types::{AuthorityIndex, Transaction},
};

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
    ) {
        assert!(transaction_size >= 8 + 8); // 8 bytes timestamp + 8 bytes random
        runtime::Handle::current().spawn(
            Self {
                sender,
                rng: StdRng::seed_from_u64(seed),
                transactions_per_second,
                transaction_size,
                initial_delay,
            }
            .run(),
        );
    }

    pub async fn run(mut self) {
        // The max block size is dilated by the WAL entry size (we leave 100 bytes for serialization
        // overhead). Exceeding this limit will cause the block to be rejected by the validator.
        let transactions_per_block_interval = (self.transactions_per_second + 9) / 10;
        let max_block_size = (crate::wal::MAX_ENTRY_SIZE / 2 - 100) / self.transaction_size;
        let target_block_size = min(max_block_size, transactions_per_block_interval);

        let mut counter = 0;
        let mut random: u64 = self.rng.gen(); // 8 bytes
        let zeros = vec![0u8; self.transaction_size - 8 - 8]; // 8 bytes timestamp + 8 bytes random

        let mut interval = runtime::TimeInterval::new(Self::TARGET_BLOCK_INTERVAL);
        runtime::sleep(self.initial_delay).await;
        loop {
            interval.tick().await;
            let timestamp = (timestamp_utc().as_millis() as u64).to_le_bytes();

            let mut block = Vec::with_capacity(target_block_size);
            for _ in 0..transactions_per_block_interval {
                random += counter;

                let mut transaction = Vec::with_capacity(self.transaction_size);
                transaction.extend_from_slice(&timestamp); // 8 bytes
                transaction.extend_from_slice(&random.to_le_bytes()); // 8 bytes
                transaction.extend_from_slice(&zeros[..]);

                block.push(Transaction::new(transaction));
                counter += 1;

                if block.len() >= max_block_size {
                    if self.sender.send(block).await.is_err() {
                        return;
                    }
                    block = Vec::with_capacity(target_block_size)
                }
            }

            if !block.is_empty() && self.sender.send(block).await.is_err() {
                return;
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
