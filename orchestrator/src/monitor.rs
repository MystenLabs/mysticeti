// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, net::SocketAddr, path::PathBuf};

use tokio::sync::mpsc;

use crate::{client::Instance, protocol::ProtocolMetrics};

#[must_use]
pub struct NodeMonitorHandle(mpsc::Sender<()>);

impl NodeMonitorHandle {
    pub fn new() -> (Self, mpsc::Receiver<()>) {
        let (sender, receiver) = mpsc::channel(1);
        (Self(sender), receiver)
    }
}

/// Generate the commands to setup prometheus on the given instances.
/// TODO: Modify the configuration to also get client metrics.
pub struct Prometheus;

impl Prometheus {
    /// The default prometheus configuration path.
    const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "/etc/prometheus/prometheus.yml";

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
    pub fn run<I>(instances: I)
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
            fs::write(&file, Self::datasource(&instance, i))
                .expect("Failed to write grafana datasource");
        }

        // Restart grafana.
        std::process::Command::new("brew")
            .arg("--quiet")
            .arg("services")
            .arg("restart")
            .arg("grafana")
            .spawn()
            .expect("Grafana failed to start");
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
            &format!("    url: http://{}:9090", instance.main_ip),
            "    editable: true",
            &format!("    uid: UID-{index}"),
        ]
        .join("\n")
    }
}
