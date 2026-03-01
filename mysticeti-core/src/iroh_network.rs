// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Iroh-based transport for Mysticeti: QUIC connections with optional node discovery.

use crate::stat::HistogramSender;
use crate::types::AuthorityIndex;
use crate::config::Parameters;
use crate::metrics::{print_network_address_table, Metrics};
use async_trait::async_trait;
use eyre::{Context, Result};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use rand::prelude::ThreadRng;
use rand::Rng;
use std::collections::HashMap;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
#[allow(unused_imports)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::network::{Connection, NetworkMessage, Transport};
use crate::runtime;

const PING_INTERVAL: Duration = Duration::from_secs(30);
const PING_SIZE: usize = 12;
const MAX_SIZE: u32 = 16 * 1024 * 1024;

fn encode_ping(message: i64) -> [u8; PING_SIZE] {
    let mut m = [0u8; PING_SIZE];
    m[4..].copy_from_slice(&message.to_le_bytes());
    m
}

fn decode_ping(message: &[u8]) -> i64 {
    let mut m = [0u8; 8];
    m.copy_from_slice(message);
    i64::from_le_bytes(m)
}

fn sample_delay(range: std::ops::Range<Duration>) -> Duration {
    ThreadRng::default().gen_range(range)
}

/// Parse hex string to iroh PublicKey (32 bytes).
fn parse_endpoint_id(hex_str: &str) -> Result<iroh::PublicKey> {
    let bytes: [u8; 32] = hex::decode(hex_str)
        .context("decode iroh_endpoint_id hex")?
        .try_into()
        .map_err(|_| eyre::eyre!("iroh_endpoint_id must be 32 bytes (64 hex chars)"))?;
    iroh::PublicKey::from_bytes(&bytes).map_err(|e| eyre::eyre!("invalid iroh public key: {:?}", e))
}

/// Build EndpointAddr for a peer from config (static: id + optional relay + direct from network_address).
fn peer_endpoint_addr(id: &crate::config::Identifier) -> Result<EndpointAddr> {
    let hex_id = id
        .iroh_endpoint_id
        .as_ref()
        .ok_or_else(|| eyre::eyre!("missing iroh_endpoint_id for peer"))?;
    let public_key = parse_endpoint_id(hex_id)?;
    let mut addr = EndpointAddr::new(public_key);
    if let Some(ref url) = id.iroh_relay_url {
        let relay_url =
            iroh::RelayUrl::from_str(url).map_err(|e| eyre::eyre!("invalid iroh_relay_url: {:?}", e))?;
        addr = addr.with_relay_url(relay_url);
    }
    addr = addr.with_ip_addr(id.network_address);
    Ok(addr)
}

/// Load Iroh SecretKey from a file (32 bytes raw).
pub fn load_secret_key(path: &std::path::Path) -> Result<iroh::SecretKey> {
    let bytes = std::fs::read(path).context("read iroh secret key file")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| eyre::eyre!("iroh secret key file must be exactly 32 bytes"))?;
    Ok(iroh::SecretKey::from_bytes(&bytes))
}

/// Generate a new Iroh secret key, write it to `path`, and return the EndpointId (public key) as hex.
pub fn generate_and_write_secret_key(path: &std::path::Path) -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).context("getrandom for key")?;
    let secret_key = iroh::SecretKey::from_bytes(&bytes);
    let public_key = secret_key.public();
    std::fs::write(path, secret_key.to_bytes()).context("write iroh secret key file")?;
    Ok(hex::encode(public_key.as_bytes()))
}

/// Parts of a worker connection needed to run the stream (connection is sent to NetworkSyncer).
struct StreamHandler {
    sender: mpsc::Sender<NetworkMessage>,
    receiver: mpsc::Receiver<NetworkMessage>,
    peer_id: usize,
    latency_sender: HistogramSender<Duration>,
    latency_last_value_sender: tokio::sync::watch::Sender<Duration>,
}

async fn make_worker_connection(peer_id: usize, metrics: &Metrics) -> Option<(Connection, StreamHandler)> {
    let (network_in_sender, network_in_receiver) = mpsc::channel(1_000);
    let (network_out_sender, network_out_receiver) = mpsc::channel(1_000);
    let (latency_last_value_sender, latency_last_value_receiver) =
        tokio::sync::watch::channel(Duration::from_millis(0));
    let connection = Connection {
        peer_id,
        sender: network_out_sender.clone(),
        receiver: network_in_receiver,
        latency_last_value_receiver,
    };
    let handler = StreamHandler {
        sender: network_in_sender,
        receiver: network_out_receiver,
        peer_id,
        latency_sender: metrics.connection_latency_sender.get(peer_id)?.clone(),
        latency_last_value_sender,
    };
    Some((connection, handler))
}

async fn run_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    worker: StreamHandler,
    network_connection_max_latency: Duration,
) -> io::Result<()> {
    let StreamHandler {
        sender,
        mut receiver,
        peer_id,
        latency_sender,
        latency_last_value_sender,
    } = worker;
    tracing::info!(peer_id, "iroh stream connected");
    let (pong_sender, mut pong_receiver) = mpsc::channel(150);
    let start = Instant::now();
    let mut ping_deadline = start + PING_INTERVAL;

    let write_fut = async {
        loop {
            select! {
                _ = tokio::time::sleep_until(ping_deadline) => {
                    ping_deadline += PING_INTERVAL;
                    let ping_time = start.elapsed().as_micros() as i64;
                    let ping = encode_ping(ping_time);
                    send.write_all(&0u32.to_le_bytes()).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    send.write_all(&ping[4..]).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    send.flush().await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                }
                received = pong_receiver.recv() => {
                    let Some(ping): Option<i64> = received else { return Ok::<_, io::Error>(()) };
                    if ping == 0 {
                        return Ok(());
                    }
                    if ping > 0 {
                        if let Some(pong) = ping.checked_neg() {
                            let pong = encode_ping(pong);
                            send.write_all(&0u32.to_le_bytes()).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                            send.write_all(&pong[4..]).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                            send.flush().await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                        }
                    } else {
                        if let Some(our_ping) = ping.checked_neg().and_then(|n| u64::try_from(n).ok()) {
                            let time = start.elapsed().as_micros() as u64;
                            if let Some(delay) = time.checked_sub(our_ping) {
                                let d = Duration::from_micros(delay);
                                latency_sender.observe(d);
                                let _ = latency_last_value_sender.send(d);
                                if d >= network_connection_max_latency {
                                    tracing::warn!("High latency connection: {:?}. Breaking.", d);
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                received = receiver.recv() => {
                    let Some(message) = received else { return Ok(()) };
                    let serialized = bincode::serialize(&message).expect("serialize");
                    let len = serialized.len() as u32;
                    send.write_all(&len.to_le_bytes()).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    send.write_all(&serialized).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    send.flush().await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                }
            }
        }
    };

    let read_fut = async {
        let mut buf = vec![0u8; MAX_SIZE as usize];
        loop {
            let mut len_buf = [0u8; 4];
            recv.read_exact(&mut len_buf).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let size = u32::from_le_bytes(len_buf);
            if size > MAX_SIZE {
                tracing::warn!("Invalid size: {}", size);
                return Ok(());
            }
            if size == 0 {
                let mut ping_buf = [0u8; 8];
                recv.read_exact(&mut ping_buf).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                let pong = decode_ping(&ping_buf);
                if pong_sender.try_reserve().is_ok() {
                    let _ = pong_sender.send(pong);
                }
                continue;
            }
            let buf = &mut buf[..size as usize];
            recv.read_exact(buf).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            match bincode::deserialize::<NetworkMessage>(buf) {
                Ok(message) => {
                    let variant = match &message {
                        NetworkMessage::SubscribeOwnFrom(r) => format!("SubscribeOwnFrom({r})"),
                        NetworkMessage::Blocks(b) => format!("Blocks({})", b.len()),
                        NetworkMessage::RequestBlocks(r) => format!("RequestBlocks({})", r.len()),
                        NetworkMessage::RequestBlocksResponse(r) => {
                            format!("RequestBlocksResponse({})", r.len())
                        }
                        NetworkMessage::BlockNotFound(n) => format!("BlockNotFound({})", n.len()),
                    };
                    tracing::debug!(peer_id, variant, "iroh received app message");
                    if sender.send(message).await.is_err() {
                        return Ok(());
                    }
                }
                Err(err) => {
                    tracing::warn!("Failed to deserialize: {}", err);
                    return Ok(());
                }
            }
        }
    };

    let result = tokio::select! {
        r = write_fut => r,
        r = read_fut => r,
    };
    tracing::info!(
        peer_id,
        error = ?result.as_ref().err(),
        "iroh stream ended"
    );
    result
}

pub struct IrohNetwork {
    connection_receiver: mpsc::Receiver<Connection>,
    stop: Option<mpsc::Sender<()>>,
    main_handle: Option<JoinHandle<()>>,
}

impl IrohNetwork {
    const ALPN: &'static [u8] = b"mysticeti/1";

    /// Build Iroh transport from parameters. Requires `parameters.use_iroh()` and valid
    /// iroh_endpoint_id for all identifiers. Optional: pass secret_key for a fixed node identity.
    /// When `relay_mode` is `None`, uses default (public relay). Use `Some(RelayMode::Disabled)` for
    /// local tests with direct connectivity only.
    pub async fn load(
        parameters: &Parameters,
        our_id: AuthorityIndex,
        local_addr: std::net::SocketAddr,
        metrics: Arc<Metrics>,
        secret_key: Option<iroh::SecretKey>,
        relay_mode: Option<RelayMode>,
    ) -> Result<Self> {
        if !parameters.use_iroh() {
            eyre::bail!("Iroh transport requires iroh_endpoint_id for all identifiers");
        }
        let addresses: Vec<std::net::SocketAddr> = parameters.all_network_addresses().collect();
        print_network_address_table(&addresses);

        let mode = relay_mode
            .or_else(|| {
                std::env::var_os("MYSTICETI_IROH_DISABLE_RELAY").map(|_| RelayMode::Disabled)
            })
            .unwrap_or(RelayMode::Default);
        let mut builder = Endpoint::empty_builder(mode).alpns(vec![Self::ALPN.to_vec()]);
        if let Some(ref sk) = secret_key {
            builder = builder.secret_key(sk.clone());
        }
        // Bind to the configured port so peers can connect via the addresses in identifiers.
        builder = builder
            .clear_ip_transports()
            .bind_addr(local_addr)
            .map_err(|e| eyre::eyre!("iroh bind_addr: {:?}", e))?;
        let ep = builder.bind().await.map_err(|e| eyre::eyre!("iroh bind: {:?}", e))?;

        let (connection_sender, connection_receiver) = mpsc::channel(16);
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel::<()>(1);

        let peer_ids: HashMap<iroh::PublicKey, usize> = parameters
            .identifiers
            .iter()
            .enumerate()
            .filter_map(|(i, id)| {
                id.iroh_endpoint_id
                    .as_ref()
                    .and_then(|hex| parse_endpoint_id(hex).ok().map(|pk| (pk, i)))
            })
            .collect();

        let max_latency = parameters.network_connection_max_latency;
        let main_handle = tokio::spawn({
            let ep = ep.clone();
            let connection_sender = connection_sender.clone();
            let metrics = metrics.clone();
            let parameters = parameters.clone();
            async move {
                let accept_handle = tokio::spawn({
                    let ep = ep.clone();
                    let connection_sender = connection_sender.clone();
                    let metrics = metrics.clone();
                    let peer_ids = peer_ids.clone();
                    let our_id = our_id;
                    async move {
                        loop {
                            match ep.accept().await {
                                None => break,
                                Some(incoming) => {
                                    if let Ok(accepting) = incoming.accept() {
                                        match accepting.await {
                                            Ok(conn) => {
                                                let remote_id = conn.remote_id();
                                                if let Some(&peer_id) = peer_ids.get(&remote_id) {
                                                    if let Ok((send, recv)) = conn.accept_bi().await {
                                                        if let Some((connection, handler)) =
                                                            make_worker_connection(peer_id, &metrics).await
                                                        {
                                                            tracing::info!(
                                                                our_id = our_id,
                                                                peer_id,
                                                                "iroh accepted connection, handing to syncer"
                                                            );
                                                            let _ = connection_sender.send(connection).await;
                                                            tokio::spawn(async move {
                                                                let _ = run_stream(send, recv, handler, max_latency).await;
                                                            });
                                                        }
                                                    }
                                                } else {
                                                    tracing::warn!("dropping iroh connection from unknown peer {}", remote_id);
                                                }
                                            }
                                            Err(e) => tracing::debug!("iroh accept handshake: {:?}", e),
                                        }
                                    }
                                }
                            }
                        }
                    }
                });

                let worker_handles: Vec<_> = parameters
                    .identifiers
                    .iter()
                    .enumerate()
                    .filter(|(id, _)| *id != our_id as usize)
                    .map(|(id, id_cfg)| {
                        let addr = peer_endpoint_addr(id_cfg).expect("peer addr");
                        let peer_id = id;
                        let connection_sender = connection_sender.clone();
                        let ep = ep.clone();
                        let metrics = metrics.clone();
                        let initial_delay = if id < our_id as usize {
                            Duration::ZERO
                        } else {
                            sample_delay(Duration::from_secs(1)..Duration::from_secs(5))
                        };
                        tokio::spawn(async move {
                            runtime::sleep(initial_delay).await;
                            loop {
                                if let Ok(conn) = ep.connect(addr.clone(), IrohNetwork::ALPN).await {
                                    if let Ok((send, recv)) = conn.open_bi().await {
                                        if let Some((connection, handler)) = make_worker_connection(peer_id, &metrics).await {
                                            tracing::info!(
                                                our_id = our_id,
                                                peer_id,
                                                "iroh dialed peer, handing connection to syncer"
                                            );
                                            let _ = connection_sender.send(connection).await;
                                            let _ = run_stream(send, recv, handler, max_latency).await;
                                        }
                                    }
                                }
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        })
                    })
                    .collect();

                let _ = stop_rx.recv().await;
                ep.close().await;
                accept_handle.abort();
                for h in worker_handles {
                    h.abort();
                }
            }
        });

        Ok(Self {
            connection_receiver,
            stop: Some(stop_tx),
            main_handle: Some(main_handle),
        })
    }

    pub fn connection_receiver(&mut self) -> &mut Receiver<Connection> {
        &mut self.connection_receiver
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(()).await;
        }
        if let Some(h) = self.main_handle.take() {
            let _ = h.await;
        }
    }
}

#[async_trait]
impl Transport for IrohNetwork {
    fn connection_receiver(&mut self) -> &mut Receiver<Connection> {
        &mut self.connection_receiver
    }

    async fn shutdown(self) {
        IrohNetwork::shutdown(self).await
    }
}

#[cfg(all(test, feature = "iroh"))]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Minimal wire test: two Iroh endpoints, one sends SubscribeOwnFrom(0), the other receives it.
    /// Confirms framing (4-byte len + bincode) and flush deliver the message.
    #[tokio::test]
    async fn iroh_wire_send_receive_subscribe_own_from() {
        std::env::set_var("MYSTICETI_IROH_DISABLE_RELAY", "1");

        let dir = std::env::temp_dir().join("iroh_wire_test");
        let _ = std::fs::create_dir_all(&dir);
        let path_a = dir.join("a.key");
        let path_b = dir.join("b.key");
        generate_and_write_secret_key(&path_a).expect("gen key a");
        generate_and_write_secret_key(&path_b).expect("gen key b");
        let key_a = load_secret_key(&path_a).expect("load key a");
        let key_b = load_secret_key(&path_b).expect("load key b");
        let addr_a: SocketAddr = "0.0.0.0:29100".parse().unwrap();
        let addr_b: SocketAddr = "0.0.0.0:29101".parse().unwrap();
        let peer_addr_a: SocketAddr = "127.0.0.1:29100".parse().unwrap();

        let ep_a = Endpoint::empty_builder(RelayMode::Disabled)
            .alpns(vec![IrohNetwork::ALPN.to_vec()])
            .secret_key(key_a.clone())
            .clear_ip_transports()
            .bind_addr(addr_a)
            .map_err(|e| eyre::eyre!("bind: {:?}", e))
            .unwrap()
            .bind()
            .await
            .map_err(|e| eyre::eyre!("bind await: {:?}", e))
            .unwrap();

        let ep_b = Endpoint::empty_builder(RelayMode::Disabled)
            .alpns(vec![IrohNetwork::ALPN.to_vec()])
            .secret_key(key_b.clone())
            .clear_ip_transports()
            .bind_addr(addr_b)
            .map_err(|e| eyre::eyre!("bind: {:?}", e))
            .unwrap()
            .bind()
            .await
            .map_err(|e| eyre::eyre!("bind await: {:?}", e))
            .unwrap();

        let received: Arc<Mutex<Option<NetworkMessage>>> = Arc::new(Mutex::new(None));

        let recv_handle = {
            let ep_a = ep_a.clone();
            let received = received.clone();
            tokio::spawn(async move {
                let incoming = ep_a.accept().await.expect("accept one (incoming)");
                let accepting = incoming.accept().expect("accept");
                let conn = accepting.await.expect("handshake");
                let (_send, mut recv) = conn.accept_bi().await.expect("accept_bi (incoming stream from connector)");
                let mut len_buf = [0u8; 4];
                recv.read_exact(&mut len_buf).await.expect("read len");
                let size = u32::from_le_bytes(len_buf);
                assert!(size <= MAX_SIZE, "size {} > MAX_SIZE", size);
                let mut buf = vec![0u8; size as usize];
                recv.read_exact(&mut buf).await.expect("read body");
                let msg: NetworkMessage = bincode::deserialize(&buf).expect("deserialize");
                *received.lock().await = Some(msg);
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        let addr = EndpointAddr::new(key_a.public()).with_ip_addr(peer_addr_a);
        let conn = ep_b.connect(addr, IrohNetwork::ALPN).await.expect("connect");
        let (mut send, _recv) = conn.open_bi().await.expect("open_bi");
        let msg = NetworkMessage::SubscribeOwnFrom(0);
        let serialized = bincode::serialize(&msg).expect("serialize");
        let len = serialized.len() as u32;
        send.write_all(&len.to_le_bytes()).await.expect("write len");
        send.write_all(&serialized).await.expect("write body");
        send.flush().await.expect("flush");

        recv_handle.await.expect("receiver task");

        let got = received.lock().await.take().expect("one message");
        match &got {
            NetworkMessage::SubscribeOwnFrom(r) => assert_eq!(*r, 0),
            _ => panic!("expected SubscribeOwnFrom(0), got {:?}", got),
        }

        ep_a.close().await;
        ep_b.close().await;
    }
}
