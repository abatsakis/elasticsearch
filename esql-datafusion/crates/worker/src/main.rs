// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use dashmap::DashMap;
use esql_df_ir::{HealthResponse, SubmitJobRequest};
use tokio::sync::watch;
use tracing::{error, info, warn};

mod execute;
mod flight;

use flight::WorkerFlight;

#[derive(Parser, Debug)]
#[command(name = "esql-df-worker", about = "ES|QL DataFusion Flight worker")]
struct Args {
    /// Flight bind address
    #[arg(long, env = "DF_FLIGHT_BIND", default_value = "0.0.0.0:47470")]
    bind: SocketAddr,
    /// Address Elasticsearch should dial (host:port)
    #[arg(long, env = "DF_ADVERTISE")]
    advertise: Option<String>,
    /// Worker id recorded at the rendezvous
    #[arg(long, env = "DF_WORKER_ID")]
    worker_id: Option<String>,
    /// Rendezvous base URL, e.g. http://127.0.0.1:8090
    #[arg(long, env = "DF_RENDEZVOUS")]
    rendezvous: Option<String>,
    /// Heartbeat interval
    #[arg(long, default_value_t = 2000)]
    heartbeat_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let advertise = args.advertise.clone().unwrap_or_else(|| {
        if args.bind.ip().is_unspecified() {
            format!("127.0.0.1:{}", args.bind.port())
        } else {
            args.bind.to_string()
        }
    });
    let worker_id = args
        .worker_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let jobs: Arc<DashMap<String, Arc<SubmitJobRequest>>> = Arc::new(DashMap::new());
    let service = WorkerFlight { jobs: jobs.clone() };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    if let Some(url) = args.rendezvous.clone() {
        let id = worker_id.clone();
        let addr = advertise.clone();
        let interval = Duration::from_millis(args.heartbeat_ms);
        let mut rx = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = heartbeat_loop(&url, &id, &addr, interval, &mut rx).await {
                error!("heartbeat loop exited: {e:#}");
            }
        });
    } else {
        warn!(
            "no --rendezvous; worker will not register (ES must be given --advertise {advertise})"
        );
    }

    info!(
        "flight worker {worker_id} listening on {} (advertise {advertise})",
        args.bind
    );
    let incoming = tonic::transport::Server::builder()
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(service))
        .serve_with_shutdown(args.bind, async move {
            tokio::signal::ctrl_c().await.ok();
            info!("shutdown signal");
            let _ = shutdown_tx.send(true);
        });

    incoming.await.context("flight server")?;
    if let Some(url) = args.rendezvous {
        let _ = deregister(&url, &worker_id).await;
    }
    Ok(())
}

async fn heartbeat_loop(
    base: &str,
    id: &str,
    flight_addr: &str,
    interval: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/workers/{}", base.trim_end_matches('/'), id);
    let body = serde_json::json!({ "flight_addr": flight_addr });
    loop {
        match client.put(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => warn!("heartbeat HTTP {}", resp.status()),
            Err(e) => warn!("heartbeat failed: {e}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn deregister(base: &str, id: &str) -> anyhow::Result<()> {
    let url = format!("{}/v1/workers/{}", base.trim_end_matches('/'), id);
    let _ = reqwest::Client::new().delete(&url).send().await;
    Ok(())
}

pub fn health(jobs: &DashMap<String, Arc<SubmitJobRequest>>) -> HealthResponse {
    HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        inflight_jobs: jobs.len(),
    }
}
