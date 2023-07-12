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

pub struct PrometheusConfigs {
    /// The prometheus configuration for each instance.
    configs: Vec<(Instance, String)>,
}

impl PrometheusConfigs {
    const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "/etc/prometheus/prometheus.yml";

    /// Create a new prometheus configuration for the given instances.
    pub fn new<I, P>(instances: I, protocol: &P) -> Self
    where
        I: IntoIterator<Item = Instance>,
        P: ProtocolMetrics,
    {
        // TODO: Add a scrape config to also get client metrics.
        Self {
            configs: protocol
                .nodes_metrics_path(instances)
                .into_iter()
                .map(|(instance, url)| {
                    let parts: Vec<_> = url.split("/").collect();
                    let port = parts[0].parse::<SocketAddr>().unwrap().port();
                    let path = parts[1];

                    let config = [
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
                    .join("\n");

                    (instance, config)
                })
                .collect(),
        }
    }

    /// Generate the commands to update the prometheus configuration and restart prometheus.
    pub fn print_commands(&self) -> Vec<(Instance, String)> {
        self.configs
            .iter()
            .map(|(instance, config)| {
                let command = [
                    &format!(
                        "sudo echo \"{config}\" > {}",
                        Self::DEFAULT_PROMETHEUS_CONFIG_PATH
                    ),
                    "sudo service prometheus restart",
                ]
                .join(" && ");

                (instance.clone(), command)
            })
            .collect()
    }
}

// brew install grafana
// configuration in: /opt/homebrew/etc/grafana/grafana.ini
// home:  /opt/homebrew/opt/grafana/share/grafana
// plist: ~/Library/LaunchAgents/homebrew.mxcl.grafana.plist
pub struct GrafanaConfigs {
    /// The grafana configuration with a data source for each instance.
    configs: Vec<(Instance, String)>,
}

impl GrafanaConfigs {
    // const DEFAULT_GRAFANA_HOME: &'static str = "/opt/homebrew/opt/grafana/share/grafana";

    pub fn new<I>(instances: I) -> Self
    where
        I: IntoIterator<Item = Instance>,
    {
        Self {
            configs: instances
                .into_iter()
                .enumerate()
                .map(|(i, instance)| {
                    let config = [
                        "apiVersion: 1",
                        "deleteDatasources:",
                        &format!("  - name: instance-{i}"),
                        "    orgId: 1",
                        "datasources:",
                        &format!("  - name: instance-{i}"),
                        "    type: prometheus",
                        "    access: proxy",
                        "    orgId: 1",
                        &format!("    url: http://{}:9090", instance.main_ip),
                        "    editable: true",
                    ]
                    .join("\n");

                    (instance, config)
                })
                .collect(),
        }
    }

    pub fn execute(&self) {
        let path =
            PathBuf::from("/opt/homebrew/opt/grafana/share/grafana/conf/provisioning/datasources/");
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir(&path).unwrap();

        for (i, (_instance, config)) in self.configs.iter().enumerate() {
            let mut file = path.clone();
            file.push(format!("instance-{i}.yml"));
            fs::write(&file, config.as_bytes()).expect("Failed to configure grafana datasource");
        }

        std::process::Command::new("brew")
            .arg("services")
            .arg("restart")
            .arg("grafana")
            .arg("--quiet")
            .spawn()
            .expect("Grafana failed to start");
    }
}
