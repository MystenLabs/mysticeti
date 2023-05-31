// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::v2::committee::Committee;
use crate::v2::future_simulator::SimulatorContext;
use crate::v2::network::{Connection, Network};
use crate::v2::runtime;
use rand::Rng;
use std::fmt::Debug;
use std::ops::Range;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct SimulatedNetwork {
    senders: Vec<mpsc::Sender<Connection>>,
}

impl SimulatedNetwork {
    const LATENCY_RANGE: Range<Duration> = Duration::from_millis(10)..Duration::from_millis(20);

    pub fn new(committee: &Committee) -> (SimulatedNetwork, Vec<Network>) {
        let (networks, senders): (Vec<_>, Vec<_>) = committee
            .authorities()
            .map(|_| {
                let (connection_sender, connection_receiver) = mpsc::channel(16);
                (
                    Network::new_from_raw(connection_receiver),
                    connection_sender,
                )
            })
            .unzip();
        (Self { senders }, networks)
    }

    pub async fn connect_all(&self) {
        for a in 0..self.senders.len() {
            for b in a + 1..self.senders.len() {
                self.connect(a, b).await
            }
        }
    }

    pub async fn connect(&self, a: usize, b: usize) {
        let (a_sender, a_receiver) = Self::latency_channel();
        let (b_sender, b_receiver) = Self::latency_channel();
        let a_connection = Connection {
            peer_id: b,
            sender: b_sender,
            receiver: a_receiver,
        };
        let b_connection = Connection {
            peer_id: a,
            sender: a_sender,
            receiver: b_receiver,
        };
        let a = &self.senders[a];
        let b = &self.senders[b];
        a.send(a_connection).await.ok();
        b.send(b_connection).await.ok();
    }

    fn latency_channel<T: Send + 'static + Debug>() -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        let (buf_sender, mut buf_receiver) = mpsc::channel(16);
        let (sender, receiver) = mpsc::channel(16);
        runtime::Handle::current().spawn(async move {
            while let Some(message) = buf_receiver.recv().await {
                let latency = SimulatorContext::with_rng(|rng| rng.gen_range(Self::LATENCY_RANGE));
                // println!("{} {:?} lat {latency:?}", SimulatorContext::time().as_millis(), message);
                runtime::sleep(latency).await;
                // println!("{} snd {:?} lat {latency:?}", SimulatorContext::time().as_millis(), message);
                if sender.send(message).await.is_err() {
                    return;
                }
            }
        });
        (buf_sender, receiver)
    }
}
