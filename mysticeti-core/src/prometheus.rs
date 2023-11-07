// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{http::StatusCode, routing::get, Extension, Router, Server};
use prometheus::{Registry, TextEncoder};
use std::net::SocketAddr;
use tokio::sync::oneshot::{channel, Sender};

use crate::runtime::{Handle, JoinHandle};

pub const METRICS_ROUTE: &str = "/metrics";

pub struct PrometheusServerHandle {
    pub handle: JoinHandle<Result<(), hyper::Error>>,
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

    let (stop, rx_stop) = channel();

    tracing::info!("Prometheus server booted on {address}");
    let handle = Handle::current().spawn(async move {
        Server::bind(&address)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async {
                rx_stop.await.ok();
            })
            .await
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
