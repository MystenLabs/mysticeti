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

use crate::types::{AuthorityIndex, PublicKey, RoundNumber};

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
    pub identifiers: Vec<Identifier>,
    pub wave_length: RoundNumber,
    pub leader_timeout: Duration,
    pub rounds_in_epoch: RoundNumber,
    pub shutdown_grace_period: Duration,
    pub number_of_leaders: usize,
    pub enable_pipelining: bool,
    // The number of rounds to be retained when periodically cleaning up the store from in memory
    // block data and keeping only the indexes.
    pub store_retain_rounds: u64,
    pub enable_cleanup: bool,
    pub synchronizer_parameters: SynchronizerParameters,
    /// When detected connection latency is >= the `network_connection_max_latency`, then the connection breaks as best effort to fix any
    /// transient connection issues.
    pub network_connection_max_latency: Duration,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            identifiers: Vec::new(),
            wave_length: Self::DEFAULT_WAVE_LENGTH,
            leader_timeout: Self::DEFAULT_LEADER_TIMEOUT,
            rounds_in_epoch: Self::DEFAULT_ROUNDS_IN_EPOCH,
            shutdown_grace_period: Self::DEFAULT_SHUTDOWN_GRACE_PERIOD,
            number_of_leaders: Self::DEFAULT_NUMBER_OF_LEADERS,
            store_retain_rounds: Self::DEFAULT_STORE_RETAIN_ROUNDS,
            network_connection_max_latency: Self::DEFAULT_NETWORK_CONNECTION_MAX_LATENCY,
            enable_pipelining: true,
            enable_cleanup: true,
            synchronizer_parameters: SynchronizerParameters::default(),
        }
    }
}

// TODO: A central controller will eventually dynamically update these parameters.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SynchronizerParameters {
    /// The maximum number of helpers per authority.
    pub maximum_helpers_per_authority: usize,
    /// The number of blocks to send in a single batch.
    pub batch_size: usize,
    /// The sampling precision with which to re-evaluate the sync strategy.
    pub sample_precision: Duration,
    /// The interval at which to send stream blocks authored by other nodes.
    pub stream_interval: Duration,
    /// Threshold number of missing block from an authority to open a new stream.
    pub new_stream_threshold: usize,
}

impl Default for SynchronizerParameters {
    fn default() -> Self {
        Self {
            maximum_helpers_per_authority: 2,
            batch_size: 100,
            sample_precision: Duration::from_millis(250),
            stream_interval: Duration::from_secs(1),
            new_stream_threshold: 10,
        }
    }
}
impl Parameters {
    pub const DEFAULT_FILENAME: &'static str = "parameters.yaml";

    pub const DEFAULT_WAVE_LENGTH: RoundNumber = 3;
    pub const DEFAULT_LEADER_TIMEOUT: Duration = Duration::from_secs(2);

    pub const BENCHMARK_PORT_OFFSET: u16 = 1500;

    // todo - we will need to rework how we trigger reconfiguration so it works with sui
    pub const DEFAULT_ROUNDS_IN_EPOCH: u64 = u64::MAX;
    pub const DEFAULT_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

    pub const DEFAULT_NUMBER_OF_LEADERS: usize = 1;

    pub const DEFAULT_STORE_RETAIN_ROUNDS: u64 = 500;

    pub const DEFAULT_NETWORK_CONNECTION_MAX_LATENCY: Duration = Duration::from_secs(5);

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
            ..Default::default()
        }
    }

    pub fn with_port_offset(mut self, port_offset: u16) -> Self {
        for id in self.identifiers.iter_mut() {
            id.network_address
                .set_port(id.network_address.port() + port_offset);
            id.metrics_address
                .set_port(id.metrics_address.port() + port_offset);
        }
        self
    }

    pub fn with_number_of_leaders(mut self, number_of_leaders: usize) -> Self {
        self.number_of_leaders = number_of_leaders;
        self
    }

    pub fn with_pipeline(mut self, enable_pipelining: bool) -> Self {
        self.enable_pipelining = enable_pipelining;
        self
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

#[derive(Serialize, Deserialize, Clone)]
pub struct PrivateConfig {
    authority_index: AuthorityIndex,
    storage_path: StorageDir,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(transparent)]
pub struct StorageDir {
    path: PathBuf,
}

impl PrivateConfig {
    pub fn new(path: PathBuf, authority_index: AuthorityIndex) -> Self {
        fs::create_dir_all(&path).expect("Failed to create validator storage directory");
        Self {
            authority_index,
            storage_path: StorageDir { path },
        }
    }
    pub fn new_for_benchmarks(dir: &Path, authority_index: AuthorityIndex) -> Self {
        // TODO: Once we have a crypto library, generate a keypair from a fixed seed.
        tracing::warn!("Generating a predictable keypair for benchmarking");
        let path = dir.join(format!("val-{authority_index}"));
        fs::create_dir_all(&path).expect("Failed to create validator storage directory");
        Self {
            authority_index,
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
