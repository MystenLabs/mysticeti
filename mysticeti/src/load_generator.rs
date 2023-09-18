// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, time::Duration};

use clap::Parser;
use eyre::{eyre, Context, Result};
use futures::future::join_all;
use mysticeti_core::{
    config::{Parameters, Print},
    network::{NetworkMessage, Worker},
    types::Transaction,
};
use rand::Rng;
use tokio::{
    net::TcpStream,
    sync::mpsc,
    time::{interval, sleep, Instant},
};
use tracing_subscriber::{filter::LevelFilter, fmt, EnvFilter};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The index of the target node.
    #[clap(short, long, value_name = "INT")]
    target: u64,

    /// The rate of transactions per second.
    #[clap(short, long, value_name = "INT")]
    rate: u64,

    /// The size of each transaction in bytes.
    #[clap(short, long, value_name = "INT")]
    size: usize,

    /// Path to the file holding the public validator parameters (such as network addresses).
    #[clap(long, value_name = "FILE")]
    parameters_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    fmt().with_env_filter(filter).init();

    let args = Args::parse();
    let generator = LoadGenerator::new(args.target, args.rate, args.size, args.parameters_path)?;
    generator.wait().await;
    generator.run().await?;

    Ok(())
}

pub struct LoadGenerator {
    target_id: u64,
    target_address: SocketAddr,
    transaction_size: usize,
    transaction_rate: u64,
    all_nodes: Vec<SocketAddr>,
}

impl LoadGenerator {
    pub fn new(target: u64, rate: u64, size: usize, parameters_path: String) -> Result<Self> {
        let parameters = Parameters::load(&parameters_path).wrap_err(format!(
            "Failed to load parameters file '{parameters_path}'"
        ))?;

        Ok(Self {
            target_id: target,
            target_address: parameters
                .network_address(target)
                .ok_or_else(|| eyre!("Target node not found"))?,
            transaction_size: size,
            transaction_rate: rate,
            all_nodes: parameters.all_network_addresses().collect(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        // The transaction size must be at least 8 bytes to ensure all txs are different.
        if self.transaction_size < 8 {
            return Err(eyre!("Transaction size must be at least 8 bytes"));
        }

        let (connection_sender, mut connection_receiver) = mpsc::channel(1);
        let (_tcp_sender, tcp_receiver) = mpsc::unbounded_channel();

        let worker = Worker {
            peer: self.target_address,
            peer_id: self.target_id as usize,
            connection_sender,
            bind_addr: "127.0.0.1:0".parse().unwrap(), // Unused
            active_immediately: true,
            latency_sender: None,
        };
        tokio::spawn(worker.run(tcp_receiver));

        while let Some(connection) = connection_receiver.recv().await {
            tracing::info!(
                "Client connected to peer {} ({})",
                self.target_id,
                self.target_address
            );
            self.send_transactions(connection.sender).await;
        }
        Ok(())
    }

    async fn send_transactions(&self, sender: mpsc::Sender<NetworkMessage>) {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let burst = self.transaction_rate / PRECISION;
        let mut tx = Vec::with_capacity(self.transaction_size);
        let zeros = vec![0u8; self.transaction_size - 8];
        let mut r: u64 = rand::thread_rng().gen();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        tracing::info!(
            "Start sending transactions to peer {} ({})",
            self.target_id,
            self.target_address
        );
        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for _ in 0..burst {
                r += 1; // Ensures all clients send different txs.
                tx.extend_from_slice(&r.to_le_bytes());
                tx.extend_from_slice(&zeros[..]);

                let message = NetworkMessage::Transactions(vec![Transaction::new(tx.clone())]);
                tx.clear();
                if let Err(e) = sender.send(message).await {
                    tracing::warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                tracing::warn!("Transaction rate too high for this client");
            }
        }
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        tracing::info!("Waiting for all nodes to be online...");
        join_all(self.all_nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
