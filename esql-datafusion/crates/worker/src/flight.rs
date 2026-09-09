// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use async_stream::try_stream;
use bytes::Bytes;
use dashmap::DashMap;
use esql_df_ir::{ErrorBody, SubmitJobRequest, SubmitJobResponse};
use futures::{Stream, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};
use uuid::Uuid;

use crate::execute::execute_job;
use crate::health;

pub struct WorkerFlight {
    pub jobs: Arc<DashMap<String, Arc<SubmitJobRequest>>>,
}

type BoxedStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for WorkerFlight {
    type HandshakeStream = BoxedStream<HandshakeResponse>;
    type ListFlightsStream = BoxedStream<FlightInfo>;
    type DoGetStream = BoxedStream<FlightData>;
    type DoPutStream = BoxedStream<PutResult>;
    type DoActionStream = BoxedStream<arrow_flight::Result>;
    type ListActionsStream = BoxedStream<ActionType>;
    type DoExchangeStream = BoxedStream<FlightData>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let s = try_stream! {
            yield HandshakeResponse { protocol_version: 0, payload: Bytes::new() };
        };
        Ok(Response::new(Box::pin(s)))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info: ES submits jobs via do_action(submit_job)",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "get_schema: schema is on the job payload",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let job_id = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket must be utf-8 job_id"))?;
        let job = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| not_found("UNKNOWN_JOB", format!("unknown job_id {job_id}")))?
            .clone();

        info!(%job_id, paths = job.paths.len(), "do_get");
        let df = execute_job(job.as_ref())
            .await
            .map_err(|e| map_exec_error(&e))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = stream.schema();
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|e| FlightError::from_external_error(Box::new(e))));
        let out = encoded.map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(out)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let Action { r#type, body } = request.into_inner();
        let payload = match r#type.as_str() {
            "health" => serde_json::to_vec(&health(&self.jobs)).map_err(internal_json)?,
            "submit_job" => {
                let mut req = SubmitJobRequest::from_json(&body).map_err(|e| {
                    Status::invalid_argument(
                        serde_json::to_string(&ErrorBody {
                            code: "INVALID_ARGUMENT".into(),
                            message: e.to_string(),
                            path: None,
                        })
                        .unwrap_or_else(|_| e.to_string()),
                    )
                })?;
                let id = req
                    .job_id
                    .clone()
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                req.job_id = Some(id.clone());
                info!(job_id = %id, paths = req.paths.len(), "submit_job");
                self.jobs.insert(id.clone(), Arc::new(req));
                serde_json::to_vec(&SubmitJobResponse { job_id: id }).map_err(internal_json)?
            }
            "cancel_job" => {
                let v: serde_json::Value =
                    serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                let id = v.get("job_id").and_then(|x| x.as_str()).unwrap_or("");
                self.jobs.remove(id);
                serde_json::to_vec(&serde_json::json!({ "cancelled": true, "job_id": id }))
                    .map_err(internal_json)?
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown action [{other}]"
                )));
            }
        };
        let s = try_stream! {
            yield arrow_flight::Result { body: payload.into() };
        };
        Ok(Response::new(Box::pin(s)))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let s = try_stream! {
            yield ActionType { r#type: "health".into(), description: "worker liveness".into() };
            yield ActionType { r#type: "submit_job".into(), description: "register a plan+files job".into() };
            yield ActionType { r#type: "cancel_job".into(), description: "drop a registered job".into() };
        };
        Ok(Response::new(Box::pin(s)))
    }
}

fn internal_json(e: serde_json::Error) -> Status {
    Status::internal(e.to_string())
}

fn not_found(code: &str, message: String) -> Status {
    let body = serde_json::to_string(&ErrorBody {
        code: code.into(),
        message,
        path: None,
    })
    .unwrap_or_default();
    Status::not_found(body)
}

fn map_exec_error(e: &anyhow::Error) -> Status {
    let msg = format!("{e:#}");
    warn!(error = %msg, "job execution failed");
    if msg.contains("read parquet") || msg.contains("ObjectStore") || msg.contains("not found") {
        let body = serde_json::to_string(&ErrorBody {
            code: "MISSING_OBJECT".into(),
            message: msg,
            path: None,
        })
        .unwrap_or_default();
        return Status::not_found(body);
    }
    if msg.contains("unsupported") || msg.contains("invalid IR") {
        let body = serde_json::to_string(&ErrorBody {
            code: "INVALID_ARGUMENT".into(),
            message: msg,
            path: None,
        })
        .unwrap_or_default();
        return Status::invalid_argument(body);
    }
    Status::internal(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use esql_df_ir::HealthResponse;
    use futures::StreamExt;

    fn svc() -> WorkerFlight {
        WorkerFlight {
            jobs: Arc::new(DashMap::new()),
        }
    }

    #[tokio::test]
    async fn health_action() {
        let s = svc();
        let resp = s
            .do_action(Request::new(Action {
                r#type: "health".into(),
                body: Bytes::new(),
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();
        let body = stream.next().await.unwrap().unwrap().body;
        let health: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(health.status, "ok");
        assert_eq!(health.inflight_jobs, 0);
    }

    #[tokio::test]
    async fn submit_cancel_and_unknown_ticket() {
        let s = svc();
        let json = br#"{
            "api_version": 1,
            "output_schema": [{"name": "x", "type": "long"}],
            "paths": [{"path": "/tmp/missing.parquet"}],
            "plan": {"op": "scan", "format": "parquet"}
        }"#;
        let resp = s
            .do_action(Request::new(Action {
                r#type: "submit_job".into(),
                body: Bytes::from_static(json),
            }))
            .await
            .unwrap();
        let mut stream = resp.into_inner();
        let body = stream.next().await.unwrap().unwrap().body;
        let submitted: SubmitJobResponse = serde_json::from_slice(&body).unwrap();
        assert!(!submitted.job_id.is_empty());
        assert_eq!(s.jobs.len(), 1);

        let missing = s
            .do_get(Request::new(Ticket {
                ticket: Bytes::from("no-such-job"),
            }))
            .await;
        match missing {
            Err(status) => assert_eq!(status.code(), tonic::Code::NotFound),
            Ok(_) => panic!("expected unknown ticket to fail"),
        }

        s.do_action(Request::new(Action {
            r#type: "cancel_job".into(),
            body: Bytes::from(format!(r#"{{"job_id":"{}"}}"#, submitted.job_id)),
        }))
        .await
        .unwrap();
        assert!(s.jobs.is_empty());
    }
}
