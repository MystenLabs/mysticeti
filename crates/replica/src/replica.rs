// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use ::prometheus::Registry;
use consensus::committer::Committer;
use dag::{
    authority::Authority,
    block::transaction::Transaction,
    context::{Ctx, TokioCtx},
    core::{
        Core,
        block_handler::{CommitHandler, RealBlockHandler},
        syncer::Syncer,
    },
    crypto::CryptoEngine,
    metrics::Metrics,
    storage::Storage,
    sync::{net_sync::NetworkSyncer, network::Network},
};
use eyre::{Context, Result, eyre};
use tokio::sync::mpsc;

use crate::{
    builder::StorageKind,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    generator::TransactionGenerator,
};

/// A replica whose configuration is finalized and ready to run. No tokio work has happened yet;
/// all materialization (storage, metrics, network) is deferred to [`Replica::run`].
///
/// Fields are crate-private; construct via [`crate::builder::ReplicaBuilder`].
pub struct Replica {
    pub(crate) authority: Authority,
    pub(crate) public_config: PublicReplicaConfig,
    pub(crate) private_config: PrivateReplicaConfig,
    pub(crate) storage: StorageKind,
    pub(crate) crypto_disabled: bool,
    pub(crate) metrics: Option<Arc<Metrics>>,
    pub(crate) network: Option<Network>,
    pub(crate) registry: Registry,
}

impl Replica {
    /// Start the replica under the given execution context. Opens storage, materializes metrics
    /// and network defaults (tokio-only unless pre-injected via the builder), builds the block
    /// handler, committer, core, and network syncer.
    #[tracing::instrument(skip_all, fields(authority = %self.authority))]
    pub async fn run<C: Ctx>(self) -> Result<ReplicaHandle<C>> {
        let Self {
            authority,
            public_config,
            private_config,
            storage: storage_kind,
            crypto_disabled,
            metrics: metrics_override,
            network: network_override,
            registry,
        } = self;

        let committee = public_config.committee();
        let parameters = &public_config.parameters;

        // Metrics: injected or freshly built against our registry.
        let metrics =
            metrics_override.unwrap_or_else(|| Metrics::new(&registry, committee.len(), None));

        // Storage.
        let (storage, recovered) = match storage_kind {
            StorageKind::Wal(path) => Storage::open(authority, &path, metrics.clone(), &committee)
                .wrap_err("Failed to open replica storage")?,
            StorageKind::Ephemeral => {
                Storage::new_for_tests(authority, metrics.clone(), &committee)
            }
        };

        // Crypto: disabled if the caller asked for it (simulator mode) or if the chosen protocol
        // is authentication-free (e.g. a CFT variant). Otherwise use the keypair from the private
        // config.
        let protocol = parameters.consensus.to_protocol(committee.total_stake());
        let crypto = if crypto_disabled || !protocol.require_crypto {
            CryptoEngine::disabled()
        } else {
            CryptoEngine::enabled(private_config.keypair)
        };

        // Network: user-provided, or bind TCP.
        let network = match network_override {
            Some(network) => network,
            None => {
                let mut binding_address = public_config
                    .network_address(authority)
                    .ok_or_else(|| eyre!("No network address for authority {authority}"))?;
                binding_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
                let addresses: Vec<SocketAddr> = public_config.all_network_addresses().collect();
                Network::load(&addresses, authority, binding_address, metrics.clone()).await
            }
        };

        // DAG and consensus machinery.
        let (block_handler, transaction_sender) = RealBlockHandler::<C>::new(metrics.clone());
        let commit_handler =
            CommitHandler::new(block_handler.transaction_time.clone(), metrics.clone());

        let round_timeout = parameters
            .dag
            .round_timeout
            .unwrap_or_else(|| protocol.default_round_timeout());
        let enable_synchronizer = parameters.dag.enable_synchronizer;
        let fsync = parameters.dag.fsync;
        let committer = Committer::new(committee.clone(), storage.block_reader().clone(), protocol);
        let core = Core::open(
            block_handler,
            authority,
            committee,
            metrics.clone(),
            storage,
            recovered,
            fsync,
            committer,
            crypto,
        );
        let network_synchronizer = NetworkSyncer::start(
            network,
            core,
            round_timeout,
            enable_synchronizer,
            commit_handler,
            metrics.clone(),
        );

        Ok(ReplicaHandle {
            authority,
            public_config,
            network_synchronizer,
            metrics,
            transaction_sender,
        })
    }
}

/// A running replica. Generic over the execution context.
pub struct ReplicaHandle<C: Ctx> {
    authority: Authority,
    public_config: PublicReplicaConfig,
    network_synchronizer: NetworkSyncer<C, Committer>,
    metrics: Arc<Metrics>,
    /// Channel for submitting transactions to the block handler. Cloned into the built-in load
    /// generator when one is started (see [`ReplicaHandle::start_load_generator`]) and reachable
    /// externally via [`ReplicaHandle::submit`] on `TokioCtx`.
    transaction_sender: mpsc::Sender<Vec<Transaction>>,
}

impl<C: Ctx> ReplicaHandle<C> {
    pub fn authority(&self) -> Authority {
        self.authority
    }

    /// Shut down the replica and return the inner `Syncer` for inspection (commit history,
    /// metrics, etc.).
    pub async fn shutdown(self) -> Syncer<C, Committer> {
        self.network_synchronizer.shutdown().await
    }

    /// Start the built-in load generator. Transactions from the generator and from
    /// [`ReplicaHandle::submit`] (on `TokioCtx`) share the same channel and are interleaved
    /// in arrival order. Returns the task handle; drop it to detach, or keep it to abort
    /// later via `C::abort`.
    pub fn start_load_generator(&mut self, config: LoadGeneratorConfig) -> C::JoinHandle<()> {
        TransactionGenerator::start::<C>(
            self.transaction_sender.clone(),
            self.authority,
            config,
            self.public_config.parameters.dag.max_block_size,
            self.metrics.clone(),
        )
    }
}

impl ReplicaHandle<TokioCtx> {
    /// Submit transactions externally.
    pub async fn submit(&self, transactions: Vec<Transaction>) -> Result<()> {
        self.transaction_sender
            .send(transactions)
            .await
            .map_err(|_| eyre!("Transaction channel closed"))
    }

    /// Wait for the replica to finish.
    pub async fn await_completion(self) -> Result<()> {
        self.network_synchronizer
            .await_completion()
            .await
            .map_err(|error| eyre!("Replica {} crashed: {error}", self.authority))
    }
}
