// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

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

                    let config = format!(
                        "
                        global:
                            scrape_interval: 5s
                            scrape_timeout: 5s
                        scrape_configs:
                            - job_name: 'prometheus'
                                static_configs:
                                    - targets: ['localhost:{port}']
                        "
                    );

                    (instance, config)
                    // serde_yaml::from_str(&config).unwrap();
                })
                .collect(),
        }
    }

    pub fn print_commands(&self) -> Vec<(Instance, String)> {
        self.configs
            .iter()
            .map(|(instance, config)| {
                let command = [
                    "sudo service prometheus stop",
                    &format!(
                        "sudo echo \"{config}\" > {}",
                        Self::DEFAULT_PROMETHEUS_CONFIG_PATH
                    ),
                    "sudo service prometheus start",
                ]
                .join(" && ");

                (instance.clone(), command)
            })
            .collect()
    }
}
