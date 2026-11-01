// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

//! Versioned submit-job payload and physical-ish plan IR.
//!
//! Elasticsearch produces this after the branch cutter. The DataFusion worker
//! physicalizes it; it never parses ES|QL or SQL.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const API_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum IrError {
    #[error("unsupported api_version {0} (supported: {API_VERSION})")]
    UnsupportedVersion(u32),
    #[error("invalid IR: {0}")]
    Invalid(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobRequest {
    pub api_version: u32,
    #[serde(default)]
    pub job_id: Option<String>,
    #[serde(default)]
    pub dataset_name: Option<String>,
    #[serde(default)]
    pub agg_mode: AggMode,
    pub output_schema: Vec<Field>,
    #[serde(default)]
    pub index_value: Option<String>,
    #[serde(default)]
    pub store: Store,
    #[serde(default)]
    pub paths: Vec<ObjectPath>,
    pub plan: PlanNode,
    #[serde(default)]
    pub budget: Budget,
}

impl SubmitJobRequest {
    pub fn from_json(bytes: &[u8]) -> Result<Self, IrError> {
        let req: Self = serde_json::from_slice(bytes)?;
        req.validate()?;
        Ok(req)
    }

    pub fn validate(&self) -> Result<(), IrError> {
        if self.api_version != API_VERSION {
            return Err(IrError::UnsupportedVersion(self.api_version));
        }
        if self.output_schema.is_empty() {
            return Err(IrError::Invalid("output_schema must not be empty".into()));
        }
        if self.paths.is_empty() {
            return Err(IrError::Invalid("paths must not be empty".into()));
        }
        self.plan.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum AggMode {
    #[default]
    Partial,
    Final,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Store {
    #[serde(default = "default_scheme")]
    pub scheme: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
}

fn default_scheme() -> String {
    "file".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPath {
    /// Object key (S3) or filesystem path (file scheme).
    #[serde(alias = "key")]
    pub path: String,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Budget {
    #[serde(default)]
    pub max_memory_bytes: Option<u64>,
    #[serde(default)]
    pub max_rows: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PlanNode {
    Scan {
        format: String,
        #[serde(default)]
        projection: Option<Vec<String>>,
    },
    Filter {
        expr: Expr,
        input: Box<PlanNode>,
    },
    Project {
        cols: Vec<String>,
        input: Box<PlanNode>,
    },
    Eval {
        #[serde(default)]
        exprs: Vec<EvalExpr>,
        input: Box<PlanNode>,
    },
    Aggregate {
        #[serde(default)]
        mode: AggMode,
        #[serde(default)]
        groups: Vec<String>,
        aggs: Vec<Agg>,
        input: Box<PlanNode>,
    },
    Limit {
        n: u32,
        input: Box<PlanNode>,
    },
}

impl PlanNode {
    pub fn validate(&self) -> Result<(), IrError> {
        match self {
            PlanNode::Scan { format, .. } => {
                let f = format.to_ascii_lowercase();
                if !matches!(f.as_str(), "parquet" | "csv" | "ndjson" | "json") {
                    return Err(IrError::Invalid(format!(
                        "unsupported scan format [{format}]"
                    )));
                }
                Ok(())
            }
            PlanNode::Filter { expr, input } => {
                expr.validate()?;
                input.validate()
            }
            PlanNode::Project { cols, input } => {
                if cols.is_empty() {
                    return Err(IrError::Invalid("project.cols must not be empty".into()));
                }
                input.validate()
            }
            PlanNode::Eval { input, .. } => input.validate(),
            PlanNode::Aggregate { aggs, input, .. } => {
                if aggs.is_empty() {
                    return Err(IrError::Invalid("aggregate.aggs must not be empty".into()));
                }
                for a in aggs {
                    a.validate()?;
                }
                input.validate()
            }
            PlanNode::Limit { n, input } => {
                if *n == 0 {
                    return Err(IrError::Invalid("limit.n must be > 0".into()));
                }
                input.validate()
            }
        }
    }

    pub fn scan_format(&self) -> Option<&str> {
        match self {
            PlanNode::Scan { format, .. } => Some(format.as_str()),
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Eval { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Limit { input, .. } => input.scan_format(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalExpr {
    pub alias: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agg {
    pub alias: String,
    #[serde(rename = "fn")]
    pub fn_name: String,
    #[serde(default)]
    pub input: Option<String>,
}

impl Agg {
    fn validate(&self) -> Result<(), IrError> {
        let f = self.fn_name.to_ascii_lowercase();
        match f.as_str() {
            "count" => Ok(()),
            "sum" | "min" | "max" => {
                if self.input.is_none() {
                    return Err(IrError::Invalid(format!(
                        "{} requires input column",
                        self.fn_name
                    )));
                }
                Ok(())
            }
            other => Err(IrError::Invalid(format!("unsupported aggregate [{other}]"))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Expr {
    Col {
        col: String,
    },
    Lit {
        lit: serde_json::Value,
        #[serde(default, rename = "type")]
        data_type: Option<String>,
    },
    Call {
        op: String,
        #[serde(default)]
        left: Option<Box<Expr>>,
        #[serde(default)]
        right: Option<Box<Expr>>,
        #[serde(default)]
        args: Vec<Expr>,
    },
}

impl Expr {
    fn validate(&self) -> Result<(), IrError> {
        match self {
            Expr::Col { col } if col.is_empty() => {
                Err(IrError::Invalid("empty column name".into()))
            }
            Expr::Col { .. } | Expr::Lit { .. } => Ok(()),
            Expr::Call {
                op,
                left,
                right,
                args,
            } => {
                let o = op.to_ascii_lowercase();
                match o.as_str() {
                    "and" | "or" | "eq" | "neq" | "lt" | "lte" | "gt" | "gte" | "in" => {
                        if left.is_none() || right.is_none() {
                            return Err(IrError::Invalid(format!("{op} requires left and right")));
                        }
                    }
                    "not" | "is_null" | "is_not_null" => {
                        if left.is_none() && args.is_empty() {
                            return Err(IrError::Invalid(format!("{op} requires an operand")));
                        }
                    }
                    other => {
                        return Err(IrError::Invalid(format!("unsupported expr op [{other}]")))
                    }
                }
                if let Some(l) = left {
                    l.validate()?;
                }
                if let Some(r) = right {
                    r.validate()?;
                }
                for a in args {
                    a.validate()?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub inflight_jobs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scan_filter_agg_job() {
        let json = r#"{
            "api_version": 1,
            "agg_mode": "PARTIAL",
            "output_schema": [
                {"name": "$$partial$$bytes", "type": "long"},
                {"name": "host", "type": "keyword"}
            ],
            "paths": [{"path": "/tmp/a.parquet"}],
            "plan": {
                "op": "aggregate",
                "mode": "PARTIAL",
                "groups": ["host"],
                "aggs": [{"alias": "$$partial$$bytes", "fn": "sum", "input": "bytes"}],
                "input": {
                    "op": "filter",
                    "expr": {
                        "op": "gte",
                        "left": {"col": "status"},
                        "right": {"lit": 500, "type": "integer"}
                    },
                    "input": {
                        "op": "project",
                        "cols": ["bytes", "host", "status"],
                        "input": {"op": "scan", "format": "parquet", "projection": ["bytes", "host", "status"]}
                    }
                }
            }
        }"#;
        let req = SubmitJobRequest::from_json(json.as_bytes()).unwrap();
        assert_eq!(req.paths.len(), 1);
        assert_eq!(req.plan.scan_format(), Some("parquet"));
    }

    #[test]
    fn rejects_unknown_agg() {
        let json = r#"{
            "api_version": 1,
            "output_schema": [{"name": "x", "type": "long"}],
            "paths": [{"path": "a.parquet"}],
            "plan": {
                "op": "aggregate",
                "aggs": [{"alias": "x", "fn": "percentile", "input": "v"}],
                "input": {"op": "scan", "format": "parquet"}
            }
        }"#;
        let err = SubmitJobRequest::from_json(json.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("percentile"));
    }
}
