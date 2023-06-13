// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::types::{AuthorityIndex, RoundNumber, StatementBlock};
use crate::{config::Parameters, data::Data};
use futures::future::{select, select_all, Either};
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    SubscribeOwnFrom(RoundNumber), // subscribe from round number excluding
    Block(Data<StatementBlock>),
}

pub struct Network {
    connection_receiver: mpsc::Receiver<Connection>,
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
        }
    }

    #[allow(dead_code)]
    pub async fn load(
        parameters: &Parameters,
        our_id: AuthorityIndex,
        local_addr: SocketAddr,
    ) -> Self {
        let mut addresses = vec![];
        for address in parameters.all_network_addresses() {
            addresses.push(address);
        }
        Self::from_socket_addresses(&addresses, our_id as usize, local_addr).await
    }

    pub fn connection_receiver(&mut self) -> &mut mpsc::Receiver<Connection> {
        &mut self.connection_receiver
    }

    pub async fn from_socket_addresses(
        addresses: &[SocketAddr],
        our_id: usize,
        local_addr: SocketAddr,
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
        for (id, address) in addresses.iter().enumerate() {
            if id == our_id {
                continue;
            }
            let (sender, receiver) = mpsc::unbounded_channel();
            assert!(
                worker_senders.insert(*address, sender).is_none(),
                "Duplicated address {} in list",
                address
            );
            handle.spawn(
                Worker {
                    peer: *address,
                    peer_id: id,
                    connection_sender: connection_sender.clone(),
                    bind_addr: bind_addr(local_addr),
                }
                .run(receiver),
            );
        }
        handle.spawn(
            Server {
                server,
                worker_senders,
            }
            .run(),
        );
        Self {
            connection_receiver,
        }
    }
}

struct Server {
    server: TcpListener,
    worker_senders: HashMap<SocketAddr, mpsc::UnboundedSender<TcpStream>>,
}

impl Server {
    async fn run(self) {
        loop {
            let (socket, remote_peer) = self.server.accept().await.expect("Accept failed");
            let remote_peer = remote_to_local_port(remote_peer);
            if let Some(sender) = self.worker_senders.get(&remote_peer) {
                sender.send(socket).ok();
            } else {
                eprintln!("Dropping connection from unknown peer {remote_peer}");
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
}

struct WorkerConnection {
    sender: mpsc::Sender<NetworkMessage>,
    receiver: mpsc::Receiver<NetworkMessage>,
}

impl Worker {
    const ACTIVE_HANDSHAKE: u64 = 0xFEFE0000;
    const PASSIVE_HANDSHAKE: u64 = 0x0000AEAE;
    const MAX_SIZE: u32 = 1024 * 1024;

    async fn run(self, mut receiver: mpsc::UnboundedReceiver<TcpStream>) -> Option<()> {
        let mut work = self.connect_and_handle(self.peer).boxed();
        loop {
            match select(work, receiver.recv().boxed()).await {
                Either::Left((_work, _receiver)) => {
                    // restart work (todo - need sleep / wait?)
                    work = self.connect_and_handle(self.peer).boxed();
                }
                Either::Right((received, _work)) => {
                    if let Some(received) = received {
                        work = self.handle_passive_stream(received).boxed();
                    } else {
                        // Channel closed, server is terminated
                        return None;
                    }
                }
            }
        }
    }

    async fn connect_and_handle(&self, peer: SocketAddr) -> io::Result<()> {
        let mut stream = loop {
            let socket = if self.bind_addr.is_ipv4() {
                TcpSocket::new_v4().unwrap()
            } else {
                TcpSocket::new_v6().unwrap()
            };
            socket.set_reuseport(true).unwrap();
            socket.bind(self.bind_addr).unwrap();
            match socket.connect(peer).await {
                Ok(stream) => break stream,
                Err(_err) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };
        stream.write_u64(Self::ACTIVE_HANDSHAKE).await?;
        let handshake = stream.read_u64().await?;
        if handshake != Self::PASSIVE_HANDSHAKE {
            eprintln!("Invalid passive handshake: {handshake}");
            return Ok(());
        }
        let Some(connection) = self.make_connection().await else {
            // todo - pass signal to break the main loop
            return Ok(());
        };
        Self::handle_stream(stream, connection).await
    }

    async fn handle_passive_stream(&self, mut stream: TcpStream) -> io::Result<()> {
        stream.write_u64(Self::PASSIVE_HANDSHAKE).await?;
        let handshake = stream.read_u64().await?;
        if handshake != Self::ACTIVE_HANDSHAKE {
            eprintln!("Invalid active handshake: {handshake}");
            return Ok(());
        }
        let Some(connection) = self.make_connection().await else {
            // todo - pass signal to break the main loop
            return Ok(());
        };
        Self::handle_stream(stream, connection).await
    }

    async fn handle_stream(stream: TcpStream, connection: WorkerConnection) -> io::Result<()> {
        let WorkerConnection { sender, receiver } = connection;
        let (reader, writer) = stream.into_split();
        let write_fut = Self::handle_write_stream(writer, receiver).boxed();
        let read_fut = Self::handle_read_stream(reader, sender).boxed();
        let (r, _, _) = select_all([write_fut, read_fut]).await;
        r
    }

    async fn handle_write_stream(
        mut writer: OwnedWriteHalf,
        mut receiver: mpsc::Receiver<NetworkMessage>,
    ) -> io::Result<()> {
        // todo - pass signal to break main loop
        while let Some(message) = receiver.recv().await {
            let serialized = bincode::serialize(&message).expect("Serialization should not fail");
            writer.write_u32(serialized.len() as u32).await?;
            let written = writer.write(&serialized).await?;
            assert_eq!(written, serialized.len());
        }
        Ok(())
    }

    async fn handle_read_stream(
        mut stream: OwnedReadHalf,
        sender: mpsc::Sender<NetworkMessage>,
    ) -> io::Result<()> {
        let mut buf = Box::new([0u8; Self::MAX_SIZE as usize]);
        loop {
            let size = stream.read_u32().await?;
            if size > Self::MAX_SIZE {
                eprintln!("Invalid size: {size}");
                return Ok(());
            }
            let buf = &mut buf[..size as usize];
            let read = stream.read(buf).await?;
            assert_eq!(read, buf.len());
            match bincode::deserialize::<NetworkMessage>(buf) {
                Ok(message) => {
                    if sender.send(message).await.is_err() {
                        // todo - pass signal to break main loop
                        return Ok(());
                    }
                }
                Err(err) => {
                    eprintln!("Failed to deserialize: {}", err);
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
        })
    }
}

#[cfg(test)]
mod test {
    use crate::test_util::networks_and_addresses;
    use std::collections::HashSet;

    #[ignore]
    #[tokio::test]
    async fn network_connect_test() {
        let (networks, addresses) = networks_and_addresses(3).await;
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
