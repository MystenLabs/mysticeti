// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::format, net::SocketAddr};

use reqwest::Url;
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
    configs: Vec<(Instance, String)>,
}

impl PrometheusConfigs {
    const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "/etc/prometheus/prometheus.yml";
    // const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "./prometheus.yml";

    pub fn new<I, P>(instances: I, protocol: &P) -> Self
    where
        I: IntoIterator<Item = Instance>,
        P: ProtocolMetrics,
    {
        Self {
            configs: protocol
                .nodes_metrics_path(instances)
                .into_iter()
                .map(|(instance, url)| {
                    //
                    let parts: Vec<_> = url.split("/").collect();
                    let port = parts[0].parse::<SocketAddr>().unwrap().port();
                    let path = parts[1];

                    // let config = format!(
                    //     r"
                    //     global:
                    //         scrape_interval: 5s
                    //         scrape_timeout: 5s
                    //     scrape_configs:
                    //         - job_name: 'prometheus'
                    //             static_configs:
                    //                 - targets: ['localhost:{port}']
                    //     "
                    // );

                    let config = [
                        "global:",
                        "\tscrape_interval: 5s",
                        "\tscrape_timeout: 5s",
                        "scrape_configs:",
                        "scrape_configs:",
                        "\t- job_name: 'prometheus'",
                        "\t\tstatic_configs:",
                        &format!("\t\t\t- targets: ['localhost:{port}']"),
                    ]
                    .join("\n");

                    let config = Self::config_string(port);

                    (instance, config)
                    // serde_yaml::from_str(&config).unwrap();
                })
                .collect(),
        }
    }

    fn config_string(port: u16) -> String {
        let mut scrape = BTreeMap::new();
        scrape.insert("scrape_interval".to_string(), "5s".to_string());
        scrape.insert("scrape_timeout".to_string(), "5s".to_string());

        let mut global = BTreeMap::new();
        global.insert("global".to_string(), scrape);

        let mut config = Vec::new();
        config.push(global);

        serde_yaml::to_string(&config).unwrap()
    }

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
