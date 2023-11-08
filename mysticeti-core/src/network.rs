// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::stat::HistogramSender;
use crate::types::{AuthorityIndex, RoundNumber, StatementBlock};
use crate::{config::Parameters, data::Data, runtime};
use crate::{
    metrics::{print_network_address_table, Metrics},
    types::BlockReference,
};
use futures::future::{select, select_all, Either};
use futures::FutureExt;
use rand::prelude::ThreadRng;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const PING_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    SubscribeOwnFrom(RoundNumber), // subscribe from round number excluding
    Block(Data<StatementBlock>),
    /// Request a few specific block references (this is not indented for large requests).
    RequestBlocks(Vec<BlockReference>),
    /// Indicate that a requested block is not found.
    BlockNotFound(Vec<BlockReference>),
}

pub struct Network {
    connection_receiver: mpsc::Receiver<Connection>,
    stop: Option<mpsc::Sender<()>>,
    server_handle: Option<JoinHandle<()>>,
}

pub struct Connection {
    pub peer_id: usize,
    pub sender: mpsc::Sender<NetworkMessage>,
    pub receiver: mpsc::Receiver<NetworkMessage>,
}

impl Network {
    #[cfg(feature = "simulator")]
    pub(crate) fn new_from_raw(connection_receiver: mpsc::Receiver<Connection>) -> Self {
        Self {
            connection_receiver,
            stop: None,
            server_handle: None,
        }
    }

    pub async fn load(
        parameters: &Parameters,
        our_id: AuthorityIndex,
        local_addr: SocketAddr,
        metrics: Arc<Metrics>,
    ) -> Self {
        let addresses = parameters.all_network_addresses().collect::<Vec<_>>();
        print_network_address_table(&addresses);
        Self::from_socket_addresses(&addresses, our_id as usize, local_addr, metrics).await
    }

    pub fn connection_receiver(&mut self) -> &mut mpsc::Receiver<Connection> {
        &mut self.connection_receiver
    }

    pub async fn from_socket_addresses(
        addresses: &[SocketAddr],
        our_id: usize,
        local_addr: SocketAddr,
        metrics: Arc<Metrics>,
    ) -> Self {
        if our_id >= addresses.len() {
            panic!(
                "our_id {our_id} is larger then address length {}",
                addresses.len()
            );
        }
        let server = TcpListener::bind(local_addr)
            .await
            .expect("Failed to bind to local socket");
        let mut worker_senders: HashMap<SocketAddr, mpsc::UnboundedSender<TcpStream>> =
            HashMap::default();
        let handle = Handle::current();
        let (connection_sender, connection_receiver) = mpsc::channel(16);
        let translation_mode = source_address_translation_mode(addresses);
        for (id, address) in addresses.iter().enumerate() {
            if id == our_id {
                continue;
            }
            let (sender, receiver) = mpsc::unbounded_channel();
            assert!(
                worker_senders
                    .insert(translation_mode.translate_source_address(*address), sender)
                    .is_none(),
                "Duplicated address {} in list",
                address
            );
            handle.spawn(
                Worker {
                    peer: *address,
                    peer_id: id,
                    connection_sender: connection_sender.clone(),
                    bind_addr: bind_addr(local_addr),
                    active_immediately: id < our_id,
                    latency_sender: metrics.connection_latency_sender.get(id).expect("Can not locate connection_latency_sender metric - did you initialize metrics with correct committee?").clone()
                }
                .run(receiver),
            );
        }
        let (stop, rx_stop) = tokio::sync::mpsc::channel(1);
        let server_handle = handle.spawn(async {
            Server {
                server,
                worker_senders,
                translation_mode,
            }
            .run(rx_stop)
            .await
        });
        Self {
            connection_receiver,
            stop: Some(stop),
            server_handle: Some(server_handle),
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send(()).await.ok();
        }
        if let Some(handle) = self.server_handle.take() {
            handle.await.ok();
        }
    }
}

struct Server {
    server: TcpListener,
    worker_senders: HashMap<SocketAddr, mpsc::UnboundedSender<TcpStream>>,
    translation_mode: SourceAddressTranslationMode,
}

impl Server {
    async fn run(self, mut stop: Receiver<()>) {
        loop {
            tokio::select! {
                result = self.server.accept() => {
                    let (socket, original_remote_peer) = result.expect("accept failed");
                    let remote_peer = remote_to_local_port(original_remote_peer);
                    let remote_peer = self.translation_mode.translate_source_address(remote_peer);
                    if let Some(sender) = self.worker_senders.get(&remote_peer) {
                        sender.send(socket).ok();
                    } else {
                        tracing::warn!("Dropping connection from unknown peer {remote_peer}(actual socket address {original_remote_peer})");
                    }
                }
                _ = stop.recv() => {
                    tracing::info!("Shutting down network");
                    return;
                }
            }
        }
    }
}

// just ignore these two functions for now :)
fn remote_to_local_port(mut remote_peer: SocketAddr) -> SocketAddr {
    match &mut remote_peer {
        SocketAddr::V4(v4) => {
            v4.set_port(v4.port() / 10);
        }
        SocketAddr::V6(v6) => {
            v6.set_port(v6.port() / 10);
        }
    }
    remote_peer
}

fn bind_addr(mut local_peer: SocketAddr) -> SocketAddr {
    match &mut local_peer {
        SocketAddr::V4(v4) => {
            v4.set_port(v4.port() * 10);
        }
        SocketAddr::V6(v6) => {
            v6.set_port(v6.port() * 10);
        }
    }
    local_peer
}

struct Worker {
    peer: SocketAddr,
    peer_id: usize,
    connection_sender: mpsc::Sender<Connection>,
    bind_addr: SocketAddr,
    active_immediately: bool,
    latency_sender: HistogramSender<Duration>,
}

struct WorkerConnection {
    sender: mpsc::Sender<NetworkMessage>,
    receiver: mpsc::Receiver<NetworkMessage>,
    peer_id: usize,
    latency_sender: HistogramSender<Duration>,
}

impl Worker {
    const ACTIVE_HANDSHAKE: u64 = 0xFEFE0000;
    const PASSIVE_HANDSHAKE: u64 = 0x0000AEAE;
    const MAX_SIZE: u32 = 16 * 1024 * 1024;

    async fn run(self, mut receiver: mpsc::UnboundedReceiver<TcpStream>) -> Option<()> {
        let initial_delay = if self.active_immediately {
            Duration::ZERO
        } else {
            sample_delay(Duration::from_secs(1)..Duration::from_secs(5))
        };
        let mut work = self.connect_and_handle(initial_delay, self.peer).boxed();
        loop {
            match select(work, receiver.recv().boxed()).await {
                Either::Left((_work, _receiver)) => {
                    let delay = sample_delay(Duration::from_secs(1)..Duration::from_secs(5));
                    work = self.connect_and_handle(delay, self.peer).boxed();
                }
                Either::Right((received, _work)) => {
                    if let Some(received) = received {
                        tracing::debug!("Replaced connection for {}", self.peer_id);
                        work = self.handle_passive_stream(received).boxed();
                    } else {
                        // Channel closed, server is terminated
                        return None;
                    }
                }
            }
        }
    }

    async fn connect_and_handle(&self, delay: Duration, peer: SocketAddr) -> io::Result<()> {
        // this is critical to avoid race between active and passive connections
        runtime::sleep(delay).await;
        let mut stream = loop {
            let socket = if self.bind_addr.is_ipv4() {
                TcpSocket::new_v4().unwrap()
            } else {
                TcpSocket::new_v6().unwrap()
            };
            #[cfg(unix)]
            socket.set_reuseport(true).unwrap();
            socket.bind(self.bind_addr).unwrap();
            match socket.connect(peer).await {
                Ok(stream) => break stream,
                Err(_err) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };
        stream.set_nodelay(true)?;
        stream.write_u64(Self::ACTIVE_HANDSHAKE).await?;
        let handshake = stream.read_u64().await?;
        if handshake != Self::PASSIVE_HANDSHAKE {
            tracing::warn!("Invalid passive handshake: {handshake}");
            return Ok(());
        }
        let Some(connection) = self.make_connection().await else {
            // todo - pass signal to break the main loop
            return Ok(());
        };
        Self::handle_stream(stream, connection).await
    }

    async fn handle_passive_stream(&self, mut stream: TcpStream) -> io::Result<()> {
        stream.set_nodelay(true)?;
        stream.write_u64(Self::PASSIVE_HANDSHAKE).await?;
        let handshake = stream.read_u64().await?;
        if handshake != Self::ACTIVE_HANDSHAKE {
            tracing::warn!("Invalid active handshake: {handshake}");
            return Ok(());
        }
        let Some(connection) = self.make_connection().await else {
            // todo - pass signal to break the main loop
            return Ok(());
        };
        Self::handle_stream(stream, connection).await
    }

    async fn handle_stream(stream: TcpStream, connection: WorkerConnection) -> io::Result<()> {
        let WorkerConnection {
            sender,
            receiver,
            peer_id,
            latency_sender,
        } = connection;
        tracing::debug!("Connected to {}", peer_id);
        let (reader, writer) = stream.into_split();
        let (pong_sender, pong_receiver) = mpsc::channel(16);
        let write_fut =
            Self::handle_write_stream(writer, receiver, pong_receiver, latency_sender).boxed();
        let read_fut = Self::handle_read_stream(reader, sender, pong_sender).boxed();
        let (r, _, _) = select_all([write_fut, read_fut]).await;
        tracing::debug!("Disconnected from {}", peer_id);
        r
    }

    async fn handle_write_stream(
        mut writer: OwnedWriteHalf,
        mut receiver: mpsc::Receiver<NetworkMessage>,
        mut pong_receiver: mpsc::Receiver<i64>,
        latency_sender: HistogramSender<Duration>,
    ) -> io::Result<()> {
        let start = Instant::now();
        let mut ping_deadline = start + PING_INTERVAL;
        loop {
            select! {
                _deadline = tokio::time::sleep_until(ping_deadline) => {
                    ping_deadline += PING_INTERVAL;
                    let ping_time = start.elapsed().as_micros() as i64;
                    // because we wait for PING_INTERVAL the interval it can't be 0
                    assert!(ping_time > 0);
                    let ping = encode_ping(ping_time);
                    writer.write_all(&ping).await?;
                }
                received = pong_receiver.recv() => {
                    // We have an embedded ping-pong protocol for measuring RTT:
                    //
                    // Every PING_INTERVAL node emits a "ping", positive number encoding some local time
                    // On receiving positive ping, node replies with "pong" which is negative number (e.g. "ping".neg())
                    // On receiving negative number we can calculate RTT(by negating it again and getting original ping time)
                    // todo - we trust remote peer here, might want to enforce ping (not critical for safety though)

                    let Some(ping) = received else {return Ok(())}; // todo - pass signal? (pong_sender closed)
                    if ping == 0 {
                        tracing::warn!("Invalid ping: {ping}");
                        return Ok(());
                    }
                    if ping > 0 {
                        match ping.checked_neg() {
                            Some(pong) => {
                                let pong = encode_ping(pong);
                                writer.write_all(&pong).await?;
                            },
                            None => {
                                tracing::warn!("Invalid ping: {ping}");
                                return Ok(());
                            }
                        }
                    } else {
                        match ping.checked_neg().and_then(|n|u64::try_from(n).ok()) {
                            Some(our_ping) => {
                                let time = start.elapsed().as_micros() as u64;
                                match time.checked_sub(our_ping) {
                                    Some(delay) => {
                                        latency_sender.observe(Duration::from_micros(delay));
                                    },
                                    None => {
                                        tracing::warn!("Invalid ping: {ping}, greater then current time {time}");
                                        return Ok(());
                                    }
                                }

                            },
                            None => {
                                tracing::warn!("Invalid pong: {ping}");
                                return Ok(());
                            }
                        }
                    }
                }
                received = receiver.recv() => {
                    // todo - pass signal to break main loop
                    let Some(message) = received else {return Ok(())};
                    let serialized = bincode::serialize(&message).expect("Serialization should not fail");
                    writer.write_u32(serialized.len() as u32).await?;
                    writer.write_all(&serialized).await?;
                }
            }
        }
    }

    async fn handle_read_stream(
        mut stream: OwnedReadHalf,
        sender: mpsc::Sender<NetworkMessage>,
        pong_sender: mpsc::Sender<i64>,
    ) -> io::Result<()> {
        // stdlib has a special fast implementation for generating n-size byte vectors,
        // see impl SpecFromElem for u8
        // Note that Box::new([0u8; Self::MAX_SIZE as usize]); does not work with large MAX_SIZE
        let mut buf = vec![0u8; Self::MAX_SIZE as usize].into_boxed_slice();
        loop {
            let size = stream.read_u32().await?;
            if size > Self::MAX_SIZE {
                tracing::warn!("Invalid size: {size}");
                return Ok(());
            }
            if size == 0 {
                // ping message
                let buf = &mut buf[..PING_SIZE - 4 /*Already read size(u32)*/];
                let read = stream.read_exact(buf).await?;
                assert_eq!(read, buf.len());
                let pong = decode_ping(buf);
                if pong_sender.send(pong).await.is_err() {
                    return Ok(()); // write stream closed
                }
                continue;
            }
            let buf = &mut buf[..size as usize];
            let read = stream.read_exact(buf).await?;
            assert_eq!(read, buf.len());
            match bincode::deserialize::<NetworkMessage>(buf) {
                Ok(message) => {
                    if sender.send(message).await.is_err() {
                        // todo - pass signal to break main loop
                        return Ok(());
                    }
                }
                Err(err) => {
                    tracing::warn!("Failed to deserialize: {}", err);
                    return Ok(());
                }
            }
        }
    }

    async fn make_connection(&self) -> Option<WorkerConnection> {
        let (network_in_sender, network_in_receiver) = mpsc::channel(16);
        let (network_out_sender, network_out_receiver) = mpsc::channel(16);
        let connection = Connection {
            peer_id: self.peer_id,
            sender: network_out_sender,
            receiver: network_in_receiver,
        };
        self.connection_sender.send(connection).await.ok()?;
        Some(WorkerConnection {
            sender: network_in_sender,
            receiver: network_out_receiver,
            peer_id: self.peer_id,
            latency_sender: self.latency_sender.clone(),
        })
    }
}

// This is a bit ugly, but luckily it should go away when we use noise/tls authentication instead of network addresses
enum SourceAddressTranslationMode {
    IpAndPort,
    IpOnly,
}

impl SourceAddressTranslationMode {
    pub fn translate_source_address(&self, mut addr: SocketAddr) -> SocketAddr {
        match self {
            SourceAddressTranslationMode::IpAndPort => addr,
            SourceAddressTranslationMode::IpOnly => {
                addr.set_port(0);
                addr
            }
        }
    }
}

// Check if all addresses in the list has unique IPs and use IP address only to match peers in that case
fn source_address_translation_mode(addresses: &[SocketAddr]) -> SourceAddressTranslationMode {
    let ips = addresses.iter().map(SocketAddr::ip).collect::<HashSet<_>>();
    if ips.len() == addresses.len() {
        SourceAddressTranslationMode::IpOnly
    } else {
        SourceAddressTranslationMode::IpAndPort
    }
}

fn sample_delay(range: Range<Duration>) -> Duration {
    ThreadRng::default().gen_range(range)
}

const PING_SIZE: usize = 12;
fn encode_ping(message: i64) -> [u8; PING_SIZE] {
    let mut m = [0u8; 12];
    m[4..].copy_from_slice(&message.to_le_bytes());
    m
}

fn decode_ping(message: &[u8]) -> i64 {
    let mut m = [0u8; 8];
    m.copy_from_slice(message); // asserts message.len() == 8
    i64::from_le_bytes(m)
}

#[cfg(test)]
mod test {
    use crate::committee::Committee;
    use crate::metrics::Metrics;
    use crate::test_util::networks_and_addresses;
    use prometheus::Registry;
    use std::collections::HashSet;

    #[ignore]
    #[tokio::test]
    async fn network_connect_test() {
        let committee = Committee::new_test(vec![1, 1, 1]);
        let metrics: Vec<_> = committee
            .authorities()
            .map(|_| Metrics::new(&Registry::default(), Some(&committee)).0)
            .collect();
        let (networks, addresses) = networks_and_addresses(&metrics).await;
        for (mut network, address) in networks.into_iter().zip(addresses.iter()) {
            let mut waiting_peers: HashSet<_> = HashSet::from_iter(addresses.iter().copied());
            waiting_peers.remove(address);
            while let Some(connection) = network.connection_receiver.recv().await {
                let peer = &addresses[connection.peer_id];
                eprintln!("{address} connected to {peer}");
                waiting_peers.remove(peer);
                if waiting_peers.len() == 0 {
                    break;
                }
            }
        }
    }
}
