// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    crypto::{dummy_public_key, Signer},
    types::{AuthorityIndex, PublicKey, RoundNumber},
};

pub trait ImportExport: Serialize + DeserializeOwned {
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let content = fs::read_to_string(&path)?;
        let object =
            serde_yaml::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(object)
    }

    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), io::Error> {
        let content =
            serde_yaml::to_string(self).expect("Failed to serialize object to YAML string");
        fs::write(&path, content)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeParameters {
    #[serde(default = "defaults::default_wave_length")]
    pub wave_length: RoundNumber,
    #[serde(default = "defaults::default_leader_timeout")]
    pub leader_timeout: Duration,
    #[serde(default = "defaults::default_rounds_in_epoch")]
    pub rounds_in_epoch: RoundNumber,
    #[serde(default = "defaults::default_shutdown_grace_period")]
    pub shutdown_grace_period: Duration,
    #[serde(default = "defaults::default_number_of_leaders")]
    pub number_of_leaders: usize,
    #[serde(default = "defaults::default_enable_pipelining")]
    pub enable_pipelining: bool,
    #[serde(default = "defaults::default_consensus_only")]
    pub consensus_only: bool,
}

pub mod defaults {
    pub fn default_wave_length() -> super::RoundNumber {
        3
    }

    pub fn default_leader_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(2)
    }

    pub fn default_rounds_in_epoch() -> super::RoundNumber {
        3_600_000
    }

    pub fn default_shutdown_grace_period() -> std::time::Duration {
        std::time::Duration::from_secs(2)
    }

    pub fn default_number_of_leaders() -> usize {
        2
    }

    pub fn default_enable_pipelining() -> bool {
        true
    }

    pub fn default_consensus_only() -> bool {
        true
    }
}

impl Default for NodeParameters {
    fn default() -> Self {
        Self {
            wave_length: defaults::default_wave_length(),
            leader_timeout: defaults::default_leader_timeout(),
            rounds_in_epoch: defaults::default_rounds_in_epoch(),
            shutdown_grace_period: defaults::default_shutdown_grace_period(),
            number_of_leaders: defaults::default_number_of_leaders(),
            enable_pipelining: defaults::default_enable_pipelining(),
            consensus_only: defaults::default_consensus_only(),
        }
    }
}

impl ImportExport for NodeParameters {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeIdentifier {
    pub authority_index: AuthorityIndex,
    pub machine_index: usize,
    pub instance_index: usize,
    pub public_key: PublicKey,
    pub network_address: SocketAddr,
    pub metrics_address: SocketAddr,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodePublicConfig {
    pub identifiers: Vec<NodeIdentifier>,
    pub parameters: NodeParameters,
    pub num_machines: usize,
    pub instances_per_machine: usize,
}

impl NodePublicConfig {
    pub const DEFAULT_FILENAME: &'static str = "public-config.yaml";
    pub const PORT_OFFSET_FOR_TESTS: u16 = 1500;

    pub fn new_for_tests(num_machines: usize, instances_per_machine: usize) -> Self {
        //let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
        //let benchmark_port_offset = ips.len() as u16;

        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let benchmark_port_offset = (num_machines*instances_per_machine) as u16;

        
        let mut identifiers = Vec::new();

        for machine_index in 0..num_machines {
            for instance_index in 0..instances_per_machine {
                let authority_index = (machine_index * instances_per_machine + instance_index) as AuthorityIndex;
                let public_key = dummy_public_key(); // todo - fix
                let network_port = Self::PORT_OFFSET_FOR_TESTS + (authority_index * 2) as u16;
                let metrics_port = benchmark_port_offset + network_port + 1;
                let network_address = SocketAddr::new(ip, network_port);
                let metrics_address = SocketAddr::new(ip, metrics_port);
                identifiers.push(NodeIdentifier {
                    authority_index,
                    machine_index,
                    instance_index,
                    public_key,
                    network_address,
                    metrics_address,
                });

            }
        }

        Self {
            identifiers,
            parameters: NodeParameters::default(),
            num_machines,
            instances_per_machine,
        }
    }

    pub fn new_for_benchmarks(
        ips: Vec<IpAddr>,
        node_parameters: Option<NodeParameters>,
        instances_per_machine: usize,
        num_machines: usize
    ) -> Self {
        let default_with_ips = Self::new_for_tests(num_machines, instances_per_machine).with_ips(ips);
        Self {
            identifiers: default_with_ips.identifiers,
            parameters: node_parameters.unwrap_or_default(),
            num_machines,
            instances_per_machine,
        }
    }

    pub fn with_ips(mut self, ips: Vec<IpAddr>) -> Self {
        for (id, ip) in self.identifiers.iter_mut().zip(ips) {
            id.network_address.set_ip(ip);
            id.metrics_address.set_ip(ip);
        }
        self
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

    /// Return all network addresses (including our own) in the order of the authority index.
    pub fn all_network_addresses(&self) -> impl Iterator<Item = (AuthorityIndex, SocketAddr)> + '_ {
        self.identifiers.iter().map(|id| (id.authority_index, id.network_address))

    }

    /// Return the network addresses of corresponding validator (including our own) instances
    pub fn relevant_network_addresses(&self, authority_index: AuthorityIndex) -> impl Iterator<Item = (AuthorityIndex, SocketAddr)> + '_  {
        let mut relevant_addresses = Vec::new();
        let position_on_machine = authority_index % self.instances_per_machine as u64;

        // Get corresponding validators on other machines
        let mut corresponding_validators = Vec::new();
        for i in 0..self.num_machines {
            let corresponding_validator = (i * self.instances_per_machine + position_on_machine as usize) as u64;
            corresponding_validators.push(corresponding_validator);
        }

        // Return all addresses in the corresponding validators except our own 
        for i in 0..self.identifiers.len() {
            //if corresponding_validators.contains(&(i as u64)) && i != authority_index as usize {
            if corresponding_validators.contains(&(i as u64)) {
                relevant_addresses.push((i as AuthorityIndex, self.identifiers[i].network_address));
            }
        }
        relevant_addresses.into_iter()
    }

    /// Return all metric addresses (including our own) in the order of the authority index.
    pub fn all_metric_addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.identifiers.iter().map(|id| id.metrics_address)
    }

    pub fn network_address(&self, authority: AuthorityIndex) -> Option<SocketAddr> {
        self.identifiers
            .iter()
            .find(|id| id.authority_index == authority)
            .map(|id| id.network_address)
    }

    pub fn metrics_address(&self, authority: AuthorityIndex) -> Option<SocketAddr> {
        self.identifiers
            .iter()
            .find(|id| id.authority_index == authority)
            .map(|id| id.metrics_address)
    }
}

impl ImportExport for NodePublicConfig {}

#[derive(Serialize, Deserialize)]
pub struct NodePrivateConfig {
    pub authority_index: AuthorityIndex,
    pub machine_index: usize,
    pub instance_index: usize,
    keypair: Signer,
    pub storage_path: PathBuf,
}

impl NodePrivateConfig {
    pub fn new_for_benchmarks(working_dir: &Path, num_machines: usize, instances_per_machine: usize) -> Vec<Self> {
        let total_instances = num_machines * instances_per_machine;
        Signer::new_for_test(total_instances)
            .into_iter()
            .enumerate()
            .map(|(i, keypair)| {
                let machine_index = i / instances_per_machine;
                let instance_index = i % instances_per_machine;
                let authority_index = i as AuthorityIndex;
                let path = working_dir.join(Self::default_storage_path(authority_index));
                Self {
                    authority_index,
                    machine_index,
                    instance_index,
                    keypair,
                    storage_path: path,
                }
            })
            .collect()
    }

    pub fn default_filename(authority_index: AuthorityIndex) -> PathBuf {
        format!("private-config-{authority_index}.yaml").into()
    }

    pub fn default_storage_path(authority_index: AuthorityIndex) -> PathBuf {
        format!("storage-{authority_index}").into()
    }

    pub fn certified_transactions_log(&self) -> PathBuf {
        self.storage_path.join("certified.txt")
    }

    pub fn committed_transactions_log(&self) -> PathBuf {
        self.storage_path.join("committed.txt")
    }

    pub fn wal(&self) -> PathBuf {
        self.storage_path.join("wal")
    }
}

impl ImportExport for NodePrivateConfig {}

#[derive(Serialize, Deserialize, Clone)]
pub struct ClientParameters {
    /// The number of transactions to send to the network per second.
    pub load: usize,
    /// The size of transactions to send to the network in bytes.
    pub transaction_size: usize,
    /// The initial delay before starting to send transactions.
    pub initial_delay: Duration,
}

impl Default for ClientParameters {
    fn default() -> Self {
        Self {
            load: 10,
            transaction_size: 512,
            initial_delay: Duration::from_secs(10),
        }
    }
}

impl ImportExport for ClientParameters {}
