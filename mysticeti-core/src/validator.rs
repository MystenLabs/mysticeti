// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use ::prometheus::Registry;
use eyre::{eyre, Context, Result};

use crate::{
    block_handler::{RealBlockHandler, TestCommitHandler},
    block_store::BlockStore,
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
    core::{Core, CoreOptions},
    log::TransactionLog,
    metrics::Metrics,
    net_sync::NetworkSyncer,
    network::{Network, NetworkMessage},
    prometheus,
    runtime::JoinError,
    transactions_generator::TransactionGenerator,
    types::{AuthorityIndex, BlockReference, StatementBlock},
    wal::{self, walf},
    consensus::linearizer::Linearizer,
    data::Data,
};

pub struct Validator {
    network_synchronizer: NetworkSyncer<RealBlockHandler, TestCommitHandler<TransactionLog>>,
    metrics_handle: JoinHandle<Result<(), hyper::Error>>,
}

impl Validator {
    pub async fn start(
        num_instances: usize,
        // Authority is the unique index of the validator
        authority: AuthorityIndex,
        committee: Arc<Committee>,
        public_config: &NodePublicConfig,
        private_config: NodePrivateConfig,
        client_parameters: ClientParameters,
        linearizer_sender: tokio::sync::mpsc::Sender<(BlockReference, Data<StatementBlock>)>,
        global_linearizer: Arc<tokio::sync::Mutex<Linearizer>>,
    ) -> Result<Self> {
        let network_address = public_config
            .network_address(authority)
            .ok_or(eyre!("No network address for authority {authority}"))
            .wrap_err("Unknown authority")?;
        let mut binding_network_address = network_address;
        binding_network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        let metrics_address = public_config
            .metrics_address(authority)
            .ok_or(eyre!("No metrics address for authority {authority}"))
            .wrap_err("Unknown authority")?;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        // Boot the prometheus server.
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry, Some(&committee));
        reporter.start();

        let metrics_handle =
            prometheus::start_prometheus_server(binding_metrics_address, &registry);

        // Open the block store.
        let wal_file =
            wal::open_file_for_wal(private_config.wal()).expect("Failed to open wal file");
        let (wal_writer, wal_reader) = walf(wal_file).expect("Failed to open wal");
        let recovered = BlockStore::open(
            authority,
            Arc::new(wal_reader),
            &wal_writer,
            metrics.clone(),
            &committee,
        );

        // Boot the validator node.
        let (block_handler, block_sender) = RealBlockHandler::new(
            committee.clone(),
            authority,
            &private_config.certified_transactions_log(),
            recovered.block_store.clone(),
            metrics.clone(),
        );

        TransactionGenerator::start(block_sender, authority, client_parameters);
        let committed_transaction_log =
            TransactionLog::start(private_config.committed_transactions_log())
                .expect("Failed to open committed transaction log for write");
        let commit_handler = TestCommitHandler::new_with_handler(
            committee.clone(),
            block_handler.transaction_time.clone(),
            metrics.clone(),
            committed_transaction_log,
            global_linearizer.clone(),
        );

        let network = Network::load(
            public_config,
            authority,
            binding_network_address,
            metrics.clone(),
        )
        .await;
        
        // Channel for core to send commit messages to network synchronizer
        let (local_commit_sender, local_commit_receiver) = broadcast::channel::<NetworkMessage>(100);

        let core = Core::open(
            block_handler,
            num_instances,
            authority,
            committee.clone(),
            public_config,
            metrics.clone(),
            recovered,
            wal_writer,
            CoreOptions::default(),
            linearizer_sender,
            local_commit_sender,
        ).await;
        
        let network_synchronizer = NetworkSyncer::start(
            network,
            core,
            public_config.parameters.wave_length,
            commit_handler,
            public_config.parameters.shutdown_grace_period,
            metrics,
            global_linearizer,
            local_commit_receiver,
        );

        Ok(Self {
            network_synchronizer: network_synchronizer.await,
            metrics_handle,
        })
    }

    pub async fn await_completion(
        self,
    ) -> (
        Result<(), JoinError>,
        Result<Result<(), hyper::Error>, JoinError>,
    ) {
        tokio::join!(
            self.network_synchronizer.await_completion(),
            self.metrics_handle
        )
    }

    pub async fn stop(self) {
        self.network_synchronizer.shutdown().await;
    }
}

#[cfg(test)]
mod smoke_tests {
    use std::{collections::VecDeque, fs, net::SocketAddr, time::Duration, sync::Arc};

    use tempdir::TempDir;
    use tokio::time;
    use tokio::sync::Mutex;

    use super::Validator;
    use crate::{
        committee::Committee,
        config::{self, ClientParameters, NodePrivateConfig, NodePublicConfig},
        prometheus,
        types::AuthorityIndex,
        consensus::linearizer::{Linearizer, LinearizerTask},
    };

    /// Check whether the validator specified by its metrics address has committed at least once.
    async fn check_commit(address: &SocketAddr) -> Result<bool, reqwest::Error> {
        let route = prometheus::METRICS_ROUTE;
        let res = reqwest::get(format! {"http://{address}{route}"}).await?;
        let string = res.text().await?;
        let commit = string.contains("committed_leaders_total");
        Ok(commit)
    }

    /// Await for all the validators specified by their metrics addresses to commit.
    async fn await_for_commits(addresses: Vec<SocketAddr>) {
        let mut queue = VecDeque::from(addresses);
        while let Some(address) = queue.pop_front() {
            time::sleep(Duration::from_millis(100)).await;
            match check_commit(&address).await {
                Ok(commits) if commits => (),
                _ => queue.push_back(address),
            }
        }
    }

    // TODO: Fix this test
    /// Ensure that a committee of honest validators commits.
    #[tokio::test]
    async fn validator_commit() {
        let committee_size = 4;
        let num_instances = 2;
        let num_machines = 2;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size, num_instances).with_port_offset(0);
        let client_parameters = ClientParameters::default();

        let global_linearizer = Arc::new(Mutex::new(Linearizer::new(num_instances)));
        let mut handles = Vec::new();
        let dir = TempDir::new("validator_commit").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), num_machines, num_instances);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });
        
        for (i, private_config) in private_configs.into_iter().enumerate() {
            
            let (linearizer_task_sender, linearizer_task_receiver) = tokio::sync::mpsc::channel(1024);
    
            // let linearizer_task = LinearizerTask::new(
            //     linearizer_task_receiver,
            //     Arc::clone(&global_linearizer),
            // );

            let authority = i as AuthorityIndex;
            let validator = Validator::start(
                num_instances,
                authority,
                committee.clone(),
                &public_config,
                private_config,
                client_parameters.clone(),
                linearizer_task_sender,
                Arc::clone(&global_linearizer),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = public_config
            .all_metric_addresses()
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::defaults::default_leader_timeout() * 5;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    /// Ensure validators can sync missing blocks
    #[tokio::test]
    async fn validator_sync() {
        let machine_index = 0;
        let committee_size = 4;
        let num_instances = 2;
        let num_machines = 2;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size, num_instances).with_port_offset(100);
        let client_parameters = ClientParameters::default();

        let mut handles = Vec::new();
        let dir = TempDir::new("validator_sync").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), num_machines, num_instances);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });
        let global_linearizer = Arc::new(Mutex::new(Linearizer::new(num_instances)));

        
        // Boot all validators but one.
        for (i, private_config) in private_configs.into_iter().enumerate() {
            if i == 0 {
                continue;
            }
            let (linearizer_task_sender, linearizer_task_receiver) = tokio::sync::mpsc::channel(1024);
    
            // let linearizer_task = LinearizerTask::new(
            //     linearizer_task_receiver,
            //     global_linearizer,
            // );

            let authority = i as AuthorityIndex;
            let validator = Validator::start(
                num_instances,
                authority,
                committee.clone(),
                &public_config,
                private_config,
                client_parameters.clone(),
                linearizer_task_sender,
                Arc::clone(&global_linearizer),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        // Boot the last validator after they others commit.
        let addresses = public_config
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::defaults::default_leader_timeout() * 5;
        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }

        let (linearizer_task_sender, linearizer_task_receiver) = tokio::sync::mpsc::channel(1024);
    
            let linearizer_task = LinearizerTask::new(
                linearizer_task_receiver,
                Arc::clone(&global_linearizer),
            );

        // Boot the last validator.
        let authority = 0;
        let private_config =
            NodePrivateConfig::new_for_benchmarks(dir.as_ref(), num_machines, num_instances).remove(authority as usize);
        let validator = Validator::start(
                num_instances,
                authority as AuthorityIndex,
                committee.clone(),
                &public_config,
                private_config,
                client_parameters.clone(),
                linearizer_task_sender,
                global_linearizer,
        )
        .await
        .unwrap();
        handles.push(validator.await_completion());

        // Ensure the last validator commits.
        let address = public_config
            .all_metric_addresses()
            .next()
            .map(|address| address.to_owned())
            .unwrap();
        let timeout = config::defaults::default_leader_timeout() * 5;
        tokio::select! {
            _ = await_for_commits(vec![address]) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }

    // Ensure that honest validators commit despite the presence of a crash fault.
    #[tokio::test]
    async fn validator_crash_faults() {
        let committee_size = 4;
        let num_instances = 2;
        let num_machines = 2;
        let committee = Committee::new_for_benchmarks(committee_size);
        let public_config = NodePublicConfig::new_for_tests(committee_size, num_instances).with_port_offset(200);
        let client_parameters = ClientParameters::default();

        let global_linearizer = Arc::new(Mutex::new(Linearizer::new(num_instances)));
        let mut handles = Vec::new();
        let dir = TempDir::new("validator_crash_faults").unwrap();
        let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), num_machines, num_instances);
        private_configs.iter().for_each(|private_config| {
            fs::create_dir_all(&private_config.storage_path).unwrap();
        });

        for (i, private_config) in private_configs.into_iter().enumerate() {
            if i == 0 {
                continue;
            }

            let (linearizer_task_sender, linearizer_task_receiver) = tokio::sync::mpsc::channel(1024);
    
            // let linearizer_task = LinearizerTask::new(
            //     linearizer_task_receiver,
            //     global_linearizer,
            // );

            let authority = i as AuthorityIndex;
            let validator = Validator::start(
                num_instances,
                authority,
                committee.clone(),
                &public_config,
                private_config,
                client_parameters.clone(),
                linearizer_task_sender,
                Arc::clone(&global_linearizer),
            )
            .await
            .unwrap();
            handles.push(validator.await_completion());
        }

        let addresses = public_config
            .all_metric_addresses()
            .skip(1)
            .map(|address| address.to_owned())
            .collect();
        let timeout = config::defaults::default_leader_timeout() * 15;

        tokio::select! {
            _ = await_for_commits(addresses) => (),
            _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
        }
    }
}
