// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::types::{AuthorityIndex, KeyPair};

pub trait Print: Serialize + DeserializeOwned {
    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), std::io::Error> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(&path, content)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Parameters {
    network_address: HashMap<AuthorityIndex, SocketAddr>,
    metrics_address: HashMap<AuthorityIndex, SocketAddr>,
    leader_timeout: Duration,
}

impl Parameters {
    pub const DEFAULT_FILENAME: &'static str = "parameters.json";

    pub const BENCHMARK_PORT_OFFSET: u16 = 10_000;
    pub const BENCHMARK_METRICS_PORT_OFFSET: u16 = 1000;

    pub const DEFAULT_LEADER_TIMEOUT: Duration = Duration::from_secs(2);

    pub fn new_for_benchmarks(ips: Vec<IpAddr>) -> Self {
        let mut network_address = HashMap::new();
        let mut metrics_address = HashMap::new();
        for (i, ip) in ips.into_iter().enumerate() {
            let authority = i as AuthorityIndex;
            let network_port = Self::BENCHMARK_PORT_OFFSET + i as u16;
            let metrics_port = Self::BENCHMARK_METRICS_PORT_OFFSET + network_port;
            let network_addr = SocketAddr::new(ip, network_port);
            let metrics_addr = SocketAddr::new(ip, metrics_port);
            network_address.insert(authority, network_addr);
            metrics_address.insert(authority, metrics_addr);
        }
        Self {
            network_address,
            metrics_address,
            leader_timeout: Self::DEFAULT_LEADER_TIMEOUT,
        }
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
    pub const DEFAULT_FILENAME: &'static str = "private.json";

    pub fn new_for_benchmarks(authority_index: AuthorityIndex) -> Self {
        // TODO: Once we have a crypto library, generate a keypair from a fixed seed.
        tracing::warn!("Generating a predictable keypair for benchmarking");
        Self {
            authority_index,
            keypair: 0,
            storage_path: ["storage", &authority_index.to_string()].iter().collect(),
        }
    }
}

impl Print for PrivateConfig {}
