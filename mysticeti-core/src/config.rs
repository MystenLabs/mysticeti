// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs, io,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use crate::crypto::dummy_public_key;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::types::{AuthorityIndex, KeyPair, PublicKey, RoundNumber};

pub trait Print: Serialize + DeserializeOwned {
    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), io::Error> {
        let content =
            serde_yaml::to_string(self).expect("Failed to serialize object to YAML string");
        fs::write(&path, content)
    }

    fn load<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let content = fs::read_to_string(&path)?;
        let object =
            serde_yaml::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(object)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Identifier {
    pub public_key: PublicKey,
    pub network_address: SocketAddr,
    pub metrics_address: SocketAddr,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Parameters {
    identifiers: Vec<Identifier>,
    wave_length: RoundNumber,
    leader_timeout: Duration,
    rounds_in_epoch: RoundNumber,
    shutdown_grace_period: Duration,
    /// Drop transactions from network clients and instead locally generate random
    /// transactions. This is useful for testing (but should not be used for benchmarks).
    pub generate_random_transactions: bool,
    /// Maximum number of transactions in a batch. This parameter is unused if `generate_random_transactions`
    /// is set to `true`.
    pub max_batch_size: usize,
    /// Maximum delay after which a batch is sent out even if it is not full. This parameter is unused if
    /// `generate_random_transactions` is set to `true`.
    pub max_batch_delay: Duration,
}

impl Parameters {
    pub const DEFAULT_FILENAME: &'static str = "parameters.yaml";

    pub const DEFAULT_WAVE_LENGTH: RoundNumber = 3;
    pub const DEFAULT_LEADER_TIMEOUT: Duration = Duration::from_secs(2);

    pub const BENCHMARK_PORT_OFFSET: u16 = 1500;

    // Needs to be sufficiently long to run benchmarks.
    pub const DEFAULT_ROUNDS_IN_EPOCH: u64 = 3_600_000;
    pub const DEFAULT_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

    pub const DEFAULT_MAX_BATCH_SIZE: usize = 100;
    pub const DEFAULT_MAX_BATCH_DELAY: Duration = Duration::from_millis(100);

    pub fn new_for_benchmarks(ips: Vec<IpAddr>) -> Self {
        let benchmark_port_offset = ips.len() as u16;
        let mut identifiers = Vec::new();
        for (i, ip) in ips.into_iter().enumerate() {
            let public_key = dummy_public_key(); // todo - fix
            let network_port = Self::BENCHMARK_PORT_OFFSET + i as u16;
            let metrics_port = benchmark_port_offset + network_port;
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
            rounds_in_epoch: Self::DEFAULT_ROUNDS_IN_EPOCH,
            shutdown_grace_period: Self::DEFAULT_SHUTDOWN_GRACE_PERIOD,
            generate_random_transactions: false,
            max_batch_size: Self::DEFAULT_MAX_BATCH_SIZE,
            max_batch_delay: Self::DEFAULT_MAX_BATCH_DELAY,
        }
    }

    /// Return all network addresses (including our own) in the order of the authority index.
    pub fn all_network_addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.identifiers.iter().map(|id| id.network_address)
    }

    /// Return all metric addresses (including our own) in the order of the authority index.
    pub fn all_metric_addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.identifiers.iter().map(|id| id.metrics_address)
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

    pub fn shutdown_grace_period(&self) -> Duration {
        self.shutdown_grace_period
    }

    pub fn rounds_in_epoch(&self) -> RoundNumber {
        self.rounds_in_epoch
    }
}

impl Print for Parameters {}

#[derive(Serialize, Deserialize)]
pub struct PrivateConfig {
    authority_index: AuthorityIndex,
    keypair: KeyPair,
    storage_path: StorageDir,
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct StorageDir {
    path: PathBuf,
}

impl PrivateConfig {
    pub fn new_for_benchmarks(dir: &Path, authority_index: AuthorityIndex) -> Self {
        // TODO: Once we have a crypto library, generate a keypair from a fixed seed.
        tracing::warn!("Generating a predictable keypair for benchmarking");
        let path = dir.join(format!("val-{authority_index}"));
        fs::create_dir_all(&path).expect("Failed to create validator storage directory");
        Self {
            authority_index,
            keypair: 0,
            storage_path: StorageDir { path },
        }
    }

    pub fn default_filename(authority: AuthorityIndex) -> PathBuf {
        ["private", &format!("{authority}.yaml")].iter().collect()
    }

    pub fn storage(&self) -> &StorageDir {
        &self.storage_path
    }
}

impl Print for PrivateConfig {}

impl StorageDir {
    pub fn certified_transactions_log(&self) -> PathBuf {
        self.path.join("certified.txt")
    }

    pub fn committed_transactions_log(&self) -> PathBuf {
        self.path.join("committed.txt")
    }

    pub fn wal(&self) -> PathBuf {
        self.path.join("wal")
    }
}
