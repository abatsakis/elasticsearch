// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};
use clap::Parser;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "esql-df-rendezvous", about = "DataFusion worker membership")]
struct Args {
    /// Bind address, e.g. 0.0.0.0:8090
    #[arg(long, env = "RENDEZVOUS_BIND", default_value = "0.0.0.0:8090")]
    bind: String,
    /// Evict workers whose last heartbeat is older than this (milliseconds)
    #[arg(long, env = "RENDEZVOUS_TTL_MS", default_value_t = 5000)]
    ttl_ms: u64,
}

#[derive(Clone)]
struct App {
    inner: Arc<Mutex<Store>>,
    ttl: Duration,
}

struct Store {
    workers: HashMap<String, WorkerEntry>,
}

#[derive(Clone, Debug)]
struct WorkerEntry {
    info: WorkerInfo,
    last_seen: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: String,
    pub flight_addr: String,
    #[serde(default)]
    pub capacity: Option<u32>,
    pub last_seen_ms: i64,
}

#[derive(Debug, Deserialize)]
struct HeartbeatBody {
    flight_addr: String,
    #[serde(default)]
    capacity: Option<u32>,
}

#[derive(Serialize, Deserialize)]
struct WorkerList {
    workers: Vec<WorkerInfo>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let app_state = App {
        inner: Arc::new(Mutex::new(Store {
            workers: HashMap::new(),
        })),
        ttl: Duration::from_millis(args.ttl_ms),
    };

    let janitor = app_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            janitor.evict_stale();
        }
    });

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    info!("rendezvous listening on {}", args.bind);
    axum::serve(listener, router(app_state)).await?;
    Ok(())
}

fn router(app_state: App) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/workers", get(list_workers))
        .route("/v1/workers/:id", put(heartbeat).delete(deregister))
        .with_state(app_state)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn list_workers(State(app): State<App>) -> Json<WorkerList> {
    Json(WorkerList {
        workers: app.live_workers(),
    })
}

async fn heartbeat(
    State(app): State<App>,
    Path(id): Path<String>,
    Json(body): Json<HeartbeatBody>,
) -> (StatusCode, Json<WorkerInfo>) {
    let info = app.upsert(id, body.flight_addr, body.capacity);
    (StatusCode::OK, Json(info))
}

async fn deregister(State(app): State<App>, Path(id): Path<String>) -> StatusCode {
    app.remove(&id);
    StatusCode::NO_CONTENT
}

impl App {
    fn upsert(&self, id: String, flight_addr: String, capacity: Option<u32>) -> WorkerInfo {
        let info = WorkerInfo {
            id: id.clone(),
            flight_addr,
            capacity,
            last_seen_ms: chrono::Utc::now().timestamp_millis(),
        };
        self.inner.lock().workers.insert(
            id,
            WorkerEntry {
                info: info.clone(),
                last_seen: Instant::now(),
            },
        );
        info
    }

    fn remove(&self, id: &str) {
        self.inner.lock().workers.remove(id);
    }

    fn live_workers(&self) -> Vec<WorkerInfo> {
        let mut guard = self.inner.lock();
        guard.workers.retain(|id, e| {
            let live = e.last_seen.elapsed() <= self.ttl;
            if !live {
                warn!("evicting stale worker {id}");
            }
            live
        });
        guard.workers.values().map(|e| e.info.clone()).collect()
    }

    fn evict_stale(&self) {
        let _ = self.live_workers();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_app(ttl_ms: u64) -> App {
        App {
            inner: Arc::new(Mutex::new(Store {
                workers: HashMap::new(),
            })),
            ttl: Duration::from_millis(ttl_ms),
        }
    }

    #[tokio::test]
    async fn heartbeat_list_and_delete() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router(test_app(5_000)))
                .await
                .unwrap();
        });
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();

        let health: serde_json::Value = client
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(health["status"], "ok");

        client
            .put(format!("{base}/v1/workers/w1"))
            .json(&serde_json::json!({ "flight_addr": "127.0.0.1:47470", "capacity": 4 }))
            .send()
            .await
            .unwrap();

        let list: WorkerList = client
            .get(format!("{base}/v1/workers"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(list.workers.len(), 1);
        assert_eq!(list.workers[0].id, "w1");
        assert_eq!(list.workers[0].flight_addr, "127.0.0.1:47470");

        let status = client
            .delete(format!("{base}/v1/workers/w1"))
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status.as_u16(), 204);

        let list: WorkerList = client
            .get(format!("{base}/v1/workers"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(list.workers.is_empty());
    }

    #[tokio::test]
    async fn ttl_evicts_stale_workers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router(test_app(50))).await.unwrap();
        });
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();
        client
            .put(format!("{base}/v1/workers/stale"))
            .json(&serde_json::json!({ "flight_addr": "127.0.0.1:1" }))
            .send()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let list: WorkerList = client
            .get(format!("{base}/v1/workers"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(list.workers.is_empty());
    }
}
