// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::types::{AuthorityIndex, KeyPair, PublicKey, RoundNumber};

pub trait Print: Serialize + DeserializeOwned {
    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), std::io::Error> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(&path, content)
    }

    fn load<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let content = fs::read_to_string(&path)?;
        let object = serde_json::from_str(&content)?;
        Ok(object)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Identifier {
    pub public_key: PublicKey,
    pub network_address: SocketAddr,
    pub metrics_address: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct Parameters {
    identifiers: Vec<Identifier>,
    wave_length: RoundNumber,
    leader_timeout: Duration,
}

impl Parameters {
    pub const DEFAULT_FILENAME: &'static str = "parameters.json";

    pub const DEFAULT_WAVE_LENGTH: RoundNumber = 3;
    pub const DEFAULT_LEADER_TIMEOUT: Duration = Duration::from_secs(2);

    pub const BENCHMARK_PORT_OFFSET: u16 = 10_000;
    pub const BENCHMARK_METRICS_PORT_OFFSET: u16 = 1000;

    pub fn new_for_benchmarks(ips: Vec<IpAddr>) -> Self {
        let mut identifiers = Vec::new();
        for (i, ip) in ips.into_iter().enumerate() {
            let public_key = i as PublicKey;
            let network_port = Self::BENCHMARK_PORT_OFFSET + i as u16;
            let metrics_port = Self::BENCHMARK_METRICS_PORT_OFFSET + network_port;
            let network_address = SocketAddr::new(ip, network_port);
            let metrics_address = SocketAddr::new(ip, metrics_port);
            identifiers.push(Identifier {
                public_key,
                network_address,
                metrics_address,
            });
        }
        Self {
            identifiers,
            wave_length: Self::DEFAULT_WAVE_LENGTH,
            leader_timeout: Self::DEFAULT_LEADER_TIMEOUT,
        }
    }

    /// Return all network addresses (including our own) in the order of the authority index.
    pub fn all_network_addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.identifiers.iter().map(|id| id.network_address)
    }

    pub fn network_address(&self, authority: AuthorityIndex) -> Option<SocketAddr> {
        self.identifiers
            .get(authority as usize)
            .map(|id| id.network_address)
    }

    pub fn metrics_address(&self, authority: AuthorityIndex) -> Option<SocketAddr> {
        self.identifiers
            .get(authority as usize)
            .map(|id| id.metrics_address)
    }

    pub fn wave_length(&self) -> RoundNumber {
        self.wave_length
    }
}

impl Print for Parameters {}

#[derive(Serialize, Deserialize)]
pub struct PrivateConfig {
    authority_index: AuthorityIndex,
    keypair: KeyPair,
    storage_path: PathBuf,
}

impl PrivateConfig {
    pub fn new_for_benchmarks(authority_index: AuthorityIndex) -> Self {
        // TODO: Once we have a crypto library, generate a keypair from a fixed seed.
        tracing::warn!("Generating a predictable keypair for benchmarking");
        Self {
            authority_index,
            keypair: 0,
            storage_path: ["storage", &authority_index.to_string()].iter().collect(),
        }
    }

    pub fn default_filename(authority: AuthorityIndex) -> PathBuf {
        ["private", &format!("{authority}.json")].iter().collect()
    }
}

impl Print for PrivateConfig {}
