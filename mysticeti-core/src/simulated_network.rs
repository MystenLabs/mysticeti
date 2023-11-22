// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::committee::Committee;
use crate::future_simulator::SimulatorContext;
use crate::network::{Connection, Network};
use crate::test_util::test_metrics;
use crate::{metered_channel, runtime};
use rand::Rng;
use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::ops::Range;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct SimulatedNetwork {
    senders: Vec<mpsc::Sender<Connection>>,
}

impl SimulatedNetwork {
    // This is one way latency distribution, e.g. 1/2 RTT
    const LATENCY_RANGE: Range<Duration> = Duration::from_millis(50)..Duration::from_millis(100);

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

    /// Connects some peers, for which given should_connect function returns true
    pub async fn connect_some<F: Fn(usize, usize) -> bool>(&self, should_connect: F) {
        for a in 0..self.senders.len() {
            for b in a + 1..self.senders.len() {
                if should_connect(a, b) {
                    self.connect(a, b).await
                }
            }
        }
    }

    pub async fn connect(&self, a: usize, b: usize) {
        let (a_sender, a_receiver) = Self::latency_channel();
        let (b_sender, b_receiver) = Self::latency_channel();
        let (_al_sender, al_receiver) = tokio::sync::watch::channel(Duration::from_secs(0));
        let (_bl_sender, bl_receiver) = tokio::sync::watch::channel(Duration::from_secs(0));
        let a_connection = Connection {
            peer_id: b,
            peer: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            sender: b_sender,
            receiver: a_receiver,
            latency_last_value_receiver: al_receiver,
        };
        let b_connection = Connection {
            peer_id: a,
            peer: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            sender: a_sender,
            receiver: b_receiver,
            latency_last_value_receiver: bl_receiver,
        };
        let a = &self.senders[a];
        let b = &self.senders[b];
        a.send(a_connection).await.ok();
        b.send(b_connection).await.ok();
    }

    fn latency_channel<T: Send + 'static + Debug>(
    ) -> (metered_channel::Sender<T>, metered_channel::Receiver<T>) {
        let metrics = test_metrics();

        let channel_messages_total = metrics
            .channel_messages_total
            .with_label_values(&["in", ""])
            .clone();

        let (buf_sender, mut buf_receiver) =
            metered_channel::channel(16, channel_messages_total.clone());
        let (sender, receiver) = metered_channel::channel(16, channel_messages_total);
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
