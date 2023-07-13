// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, net::SocketAddr, path::PathBuf};

use tokio::sync::mpsc;

use crate::{
    client::Instance,
    error::{MonitorError, MonitorResult},
    protocol::ProtocolMetrics,
    ssh::{CommandContext, SshConnectionManager},
};

#[must_use]
pub struct NodeMonitorHandle(mpsc::Sender<()>);

impl NodeMonitorHandle {
    pub fn new() -> (Self, mpsc::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(1);
        (Self(sender), receiver)
    }
}

pub struct Monitor {
    clients: Vec<Instance>,
    nodes: Vec<Instance>,
    ssh_manager: SshConnectionManager,
    dedicated_clients: bool,
}

impl Monitor {
    /// Create a new monitor.
    pub fn new(
        clients: Vec<Instance>,
        nodes: Vec<Instance>,
        ssh_manager: SshConnectionManager,
        dedicated_clients: bool,
    ) -> Self {
        Self {
            clients,
            nodes,
            ssh_manager,
            dedicated_clients,
        }
    }

    /// Start a prometheus instance on each remote machine.
    pub async fn start_prometheus<P: ProtocolMetrics>(
        &self,
        protocol_commands: &P,
    ) -> MonitorResult<()> {
        // Select the instances to monitor.
        let instances: Vec<_> = if self.dedicated_clients {
            self.clients
                .iter()
                .cloned()
                .chain(self.nodes.iter().cloned())
                .collect()
        } else {
            self.nodes.clone()
        };

        // Configure and reload prometheus.
        let commands = Prometheus::setup_commands(instances, protocol_commands);
        self.ssh_manager
            .execute_per_instance(commands, CommandContext::default())
            .await?;

        Ok(())
    }

    /// Start grafana on the local host.
    pub fn start_grafana(&self) -> MonitorResult<()> {
        // Select the instances to monitor.
        let instances: Vec<_> = if self.dedicated_clients {
            self.clients
                .iter()
                .cloned()
                .chain(self.nodes.iter().cloned())
                .collect()
        } else {
            self.nodes.clone()
        };

        // Configure and reload grafana.
        Grafana::run(instances)?;

        Ok(())
    }
}

/// Generate the commands to setup prometheus on the given instances.
/// TODO: Modify the configuration to also get client metrics.
pub struct Prometheus;

impl Prometheus {
    /// The default prometheus configuration path.
    const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "/etc/prometheus/prometheus.yml";
    /// The default prometheus port.
    pub const DEFAULT_PORT: u16 = 9090;

    /// Generate the commands to update the prometheus configuration and restart prometheus.
    pub fn setup_commands<I, P>(instances: I, protocol: &P) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
        P: ProtocolMetrics,
    {
        protocol
            .nodes_metrics_path(instances)
            .into_iter()
            .map(|(instance, nodes_metrics_path)| {
                let config = Self::configuration(&nodes_metrics_path);
                let path = Self::DEFAULT_PROMETHEUS_CONFIG_PATH;

                let command = [
                    &format!("sudo echo \"{config}\" > {path}",),
                    "sudo service prometheus restart",
                ]
                .join(" && ");

                (instance, command)
            })
            .collect()
    }

    /// Generate the prometheus configuration from the given metrics path.
    /// NOTE: The configuration file is a yaml file so spaces are important.
    fn configuration(nodes_metrics_path: &str) -> String {
        let parts: Vec<_> = nodes_metrics_path.split("/").collect();
        let port = parts[0].parse::<SocketAddr>().unwrap().port();
        let path = parts[1];

        [
            "global:",
            "  scrape_interval: 5s",
            "  evaluation_interval: 5s",
            "scrape_configs:",
            "  - job_name: prometheus",
            &format!("    metrics_path: /{path}"),
            "    static_configs:",
            "      - targets:",
            &format!("        - localhost:{port}"),
        ]
        .join("\n")
    }
}

/// Bootstrap the grafana with datasource to connect to the given instances.
/// NOTE: Only for macOS. Grafana must be installed through homebrew (and not from source). Deeper grafana
/// configuration can be done through the grafana.ini file (/opt/homebrew/etc/grafana/grafana.ini) or the
/// plist file (~/Library/LaunchAgents/homebrew.mxcl.grafana.plist).
pub struct Grafana;

impl Grafana {
    /// The default grafana home directory (macOS, homebrew install).
    const DEFAULT_GRAFANA_HOME: &'static str = "/opt/homebrew/opt/grafana/share/grafana/";
    /// The path to the datasources directory.
    const DATASOURCES_PATH: &'static str = "conf/provisioning/datasources/";
    /// The default grafana port.
    pub const DEFAULT_PORT: u16 = 3000;

    /// Configure grafana to connect to the given instances. Only for macOS.
    pub fn run<I>(instances: I) -> MonitorResult<()>
    where
        I: IntoIterator<Item = Instance>,
    {
        let path: PathBuf = [Self::DEFAULT_GRAFANA_HOME, Self::DATASOURCES_PATH]
            .iter()
            .collect();

        // Remove the old datasources.
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir(&path).unwrap();

        // Create the new datasources.
        for (i, instance) in instances.into_iter().enumerate() {
            let mut file = path.clone();
            file.push(format!("instance-{}.yml", i));
            fs::write(&file, Self::datasource(&instance, i)).map_err(|e| {
                MonitorError::GrafanaError(format!("Failed to write grafana datasource ({e})"))
            })?;
        }

        // Restart grafana.
        std::process::Command::new("brew")
            .arg("--quiet")
            .arg("services")
            .arg("restart")
            .arg("grafana")
            .spawn()
            .map_err(|e| MonitorError::GrafanaError(e.to_string()))?;

        Ok(())
    }

    /// Generate the content of the datasource file for the given instance.
    /// NOTE: The datasource file is a yaml file so spaces are important.
    fn datasource(instance: &Instance, index: usize) -> String {
        [
            "apiVersion: 1",
            "deleteDatasources:",
            &format!("  - name: instance-{index}"),
            "    orgId: 1",
            "datasources:",
            &format!("  - name: instance-{index}"),
            "    type: prometheus",
            "    access: proxy",
            "    orgId: 1",
            &format!(
                "    url: http://{}:{}",
                instance.main_ip,
                Prometheus::DEFAULT_PORT
            ),
            "    editable: true",
            &format!("    uid: UID-{index}"),
        ]
        .join("\n")
    }
}
