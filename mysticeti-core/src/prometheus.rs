// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{http::StatusCode, routing::get, Extension, Router};
use prometheus::{Registry, TextEncoder};
use std::net::SocketAddr;
use tokio::sync::oneshot::{channel, Sender};

use crate::runtime::{Handle, JoinHandle};

pub const METRICS_ROUTE: &str = "/metrics";

pub struct PrometheusServerHandle {
    pub handle: JoinHandle<Result<(), std::io::Error>>,
    stop: Sender<()>,
}

impl PrometheusServerHandle {
    pub fn noop() -> PrometheusServerHandle {
        let (stop, _rx_stop) = channel();
        let handle = Handle::current().spawn(async { Ok(()) });

        Self { stop, handle }
    }
    pub async fn shutdown(self) {
        self.stop.send(()).ok();
        self.handle.await.ok();
    }
}

pub fn start_prometheus_server(address: SocketAddr, registry: &Registry) -> PrometheusServerHandle {
    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .layer(Extension(registry.clone()));

    let std_listener = std::net::TcpListener::bind(address).unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    let (stop, rx_stop) = channel();

    tracing::info!("Prometheus server booted on {address}");
    let handle = Handle::current().spawn(async move {
        let result = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx_stop.await.ok();
            })
            .await;
        if result.is_ok() {
            Ok(())
        } else {
            Err(result.err().unwrap())
        }
    });

    PrometheusServerHandle { handle, stop }
}

async fn metrics(registry: Extension<Registry>) -> (StatusCode, String) {
    let metrics_families = registry.gather();
    match TextEncoder.encode_to_string(&metrics_families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unable to encode metrics: {error}"),
        ),
    }
}
