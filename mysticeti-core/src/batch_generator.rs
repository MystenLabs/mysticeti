// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::{rngs::StdRng, Rng, SeedableRng};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::{
    runtime,
    runtime::timestamp_utc,
    types::{AuthorityIndex, Transaction},
};

pub struct BatchGenerator {
    sender: mpsc::Sender<Vec<Transaction>>,
    rng: StdRng,
    transactions_per_second: usize,
    transaction_size: usize,
    initial_delay: Duration,
}

impl BatchGenerator {
    pub const DEFAULT_TRANSACTION_SIZE: usize = 512;
    const TARGET_BLOCK_INTERVAL: Duration = Duration::from_millis(100);

    pub fn start(
        sender: mpsc::Sender<Vec<Transaction>>,
        seed: AuthorityIndex,
        transactions_per_second: usize,
        transaction_size: usize,
        initial_delay: Duration,
    ) {
        assert!(transaction_size >= 16 + 8); // 16 bytes timestamp + 8 bytes random
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
        let transactions_per_100ms = (self.transactions_per_second + 9) / 10;
        let mut block = Vec::with_capacity(transactions_per_100ms);
        let mut transaction = Vec::with_capacity(self.transaction_size);

        let mut counter = 0;
        let mut random: u64 = self.rng.gen(); // 8 bytes
        let zeros = vec![0u8; self.transaction_size - 16 - 8]; // 16 bytes timestamp + 8 bytes random

        let mut interval = runtime::TimeInterval::new(Self::TARGET_BLOCK_INTERVAL);
        runtime::sleep(self.initial_delay).await;
        loop {
            interval.tick().await;

            let timestamp = timestamp_utc().as_millis().to_le_bytes();

            for _ in 0..transactions_per_100ms {
                random += counter;

                transaction.clear();
                transaction.extend_from_slice(&timestamp); // 16 bytes
                transaction.extend_from_slice(&random.to_le_bytes()); // 8 bytes
                transaction.extend_from_slice(&zeros[..]);

                block.push(Transaction::new(transaction.clone()));
                counter += 1;
            }

            if self.sender.send(block.clone()).await.is_err() {
                break;
            }
            block.clear();
        }
    }
}
