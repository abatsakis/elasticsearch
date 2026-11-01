// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use esql_df_ir::{Agg, Expr, ObjectPath, PlanNode, Store, SubmitJobRequest};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use url::Url;

pub async fn execute_job(job: &SubmitJobRequest) -> Result<datafusion::dataframe::DataFrame> {
    // DataFusion has no global row cap; Limit in the plan is authoritative.
    let _ = job.budget.max_rows;
    let config = SessionConfig::new().with_information_schema(true);
    let runtime = runtime_for(job)?;
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    register_store(&ctx, &job.store).await?;
    apply_plan(&ctx, &job.plan, &job.paths, &job.store).await
}

fn runtime_for(job: &SubmitJobRequest) -> Result<Arc<RuntimeEnv>> {
    let mut b = RuntimeEnvBuilder::new();
    if let Some(bytes) = job.budget.max_memory_bytes {
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(bytes as usize));
        b = b.with_memory_pool(pool);
    }
    Ok(Arc::new(b.build()?))
}

async fn register_store(ctx: &SessionContext, store: &Store) -> Result<()> {
    match store.scheme.to_ascii_lowercase().as_str() {
        "file" | "" => {
            ctx.register_object_store(&Url::parse("file://")?, Arc::new(LocalFileSystem::new()));
            Ok(())
        }
        "s3" => {
            let bucket = store
                .bucket
                .as_deref()
                .context("store.bucket is required for s3")?;
            let mut b = AmazonS3Builder::from_env().with_bucket_name(bucket);
            if let Some(region) = &store.region {
                b = b.with_region(region);
            }
            if let Some(endpoint) = &store.endpoint {
                b = b.with_endpoint(endpoint).with_allow_http(true);
            }
            let s3 = b.build().context("s3 store")?;
            let url = Url::parse(&format!("s3://{bucket}"))?;
            ctx.register_object_store(&url, Arc::new(s3));
            Ok(())
        }
        other => bail!("unsupported store scheme [{other}]"),
    }
}

async fn apply_plan(
    ctx: &SessionContext,
    node: &PlanNode,
    paths: &[ObjectPath],
    store: &Store,
) -> Result<DataFrame> {
    match node {
        PlanNode::Scan { format, projection } => {
            scan(ctx, format, projection.as_deref(), paths, store).await
        }
        PlanNode::Filter { expr, input } => {
            let df = Box::pin(apply_plan(ctx, input, paths, store)).await?;
            Ok(df.filter(to_df_expr(expr)?)?)
        }
        PlanNode::Project { cols, input } => {
            let df = Box::pin(apply_plan(ctx, input, paths, store)).await?;
            let exprs: Vec<_> = cols.iter().map(col).collect();
            Ok(df.select(exprs)?)
        }
        PlanNode::Eval { exprs, input } => {
            let mut df = Box::pin(apply_plan(ctx, input, paths, store)).await?;
            for e in exprs {
                df = df.with_column(&e.alias, to_df_expr(&e.expr)?)?;
            }
            Ok(df)
        }
        PlanNode::Aggregate {
            groups,
            aggs,
            input,
            mode: _,
        } => {
            // Per-worker aggregation over this file bucket is the PARTIAL for ES.
            let df = Box::pin(apply_plan(ctx, input, paths, store)).await?;
            let g: Vec<_> = groups.iter().map(col).collect();
            let a = aggs.iter().map(to_agg).collect::<Result<Vec<_>>>()?;
            Ok(df.aggregate(g, a)?)
        }
        PlanNode::Limit { n, input } => {
            let df = Box::pin(apply_plan(ctx, input, paths, store)).await?;
            Ok(df.limit(0, Some(*n as usize))?)
        }
    }
}

async fn scan(
    ctx: &SessionContext,
    format: &str,
    projection: Option<&[String]>,
    paths: &[ObjectPath],
    store: &Store,
) -> Result<DataFrame> {
    if !format.eq_ignore_ascii_case("parquet") {
        bail!("v1 worker only reads parquet (got {format})");
    }
    if paths.is_empty() {
        bail!("no paths");
    }
    let mut acc: Option<DataFrame> = None;
    for p in paths {
        let url = object_url(store, &p.path)?;
        let mut df = ctx
            .read_parquet(&url, ParquetReadOptions::default())
            .await
            .with_context(|| format!("read parquet {url}"))?;
        if let Some(cols) = projection {
            let exprs: Vec<_> = cols.iter().map(col).collect();
            df = df.select(exprs)?;
        }
        acc = Some(match acc {
            None => df,
            Some(prev) => prev.union(df)?,
        });
    }
    Ok(acc.expect("paths non-empty"))
}

fn object_url(store: &Store, path: &str) -> Result<String> {
    match store.scheme.to_ascii_lowercase().as_str() {
        "s3" => {
            let bucket = store.bucket.as_deref().context("store.bucket")?;
            let key = path.trim_start_matches('/');
            Ok(format!("s3://{bucket}/{key}"))
        }
        _ => Ok(path.to_string()),
    }
}

fn to_agg(a: &Agg) -> Result<datafusion::logical_expr::Expr> {
    use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
    let alias = a.alias.as_str();
    let expr = match a.fn_name.to_ascii_lowercase().as_str() {
        "count" => match &a.input {
            Some(c) => count(col(c)),
            None => count(lit(1)),
        },
        "sum" => sum(col(a.input.as_deref().context("sum input")?)),
        "min" => min(col(a.input.as_deref().context("min input")?)),
        "max" => max(col(a.input.as_deref().context("max input")?)),
        other => bail!("unsupported aggregate [{other}]"),
    };
    Ok(expr.alias(alias))
}

fn to_df_expr(expr: &Expr) -> Result<datafusion::logical_expr::Expr> {
    use datafusion::logical_expr::{and, or};
    match expr {
        Expr::Col { col: name } => Ok(col(name)),
        Expr::Lit { lit, data_type } => Ok(datafusion::logical_expr::lit(json_lit(
            lit,
            data_type.as_deref(),
        )?)),
        Expr::Call {
            op,
            left,
            right,
            args,
        } => {
            let o = op.to_ascii_lowercase();
            match o.as_str() {
                "and" => Ok(and(
                    to_df_expr(left.as_deref().context("and.left")?)?,
                    to_df_expr(right.as_deref().context("and.right")?)?,
                )),
                "or" => Ok(or(
                    to_df_expr(left.as_deref().context("or.left")?)?,
                    to_df_expr(right.as_deref().context("or.right")?)?,
                )),
                "not" => {
                    let inner = left.as_deref().or(args.first()).context("not operand")?;
                    Ok(!to_df_expr(inner)?)
                }
                "eq" => Ok(to_df_expr(left.as_deref().context("eq.left")?)?
                    .eq(to_df_expr(right.as_deref().context("eq.right")?)?)),
                "neq" => Ok(to_df_expr(left.as_deref().context("neq.left")?)?
                    .not_eq(to_df_expr(right.as_deref().context("neq.right")?)?)),
                "lt" => Ok(to_df_expr(left.as_deref().context("lt.left")?)?
                    .lt(to_df_expr(right.as_deref().context("lt.right")?)?)),
                "lte" => Ok(to_df_expr(left.as_deref().context("lte.left")?)?
                    .lt_eq(to_df_expr(right.as_deref().context("lte.right")?)?)),
                "gt" => Ok(to_df_expr(left.as_deref().context("gt.left")?)?
                    .gt(to_df_expr(right.as_deref().context("gt.right")?)?)),
                "gte" => Ok(to_df_expr(left.as_deref().context("gte.left")?)?
                    .gt_eq(to_df_expr(right.as_deref().context("gte.right")?)?)),
                "is_null" => {
                    let inner = left
                        .as_deref()
                        .or(args.first())
                        .context("is_null operand")?;
                    Ok(to_df_expr(inner)?.is_null())
                }
                "is_not_null" => {
                    let inner = left
                        .as_deref()
                        .or(args.first())
                        .context("is_not_null operand")?;
                    Ok(to_df_expr(inner)?.is_not_null())
                }
                "in" => {
                    let l = to_df_expr(left.as_deref().context("in.left")?)?;
                    let r = right.as_deref().context("in.right")?;
                    match r {
                        Expr::Lit {
                            lit: serde_json::Value::Array(items),
                            ..
                        } => {
                            let list = items
                                .iter()
                                .map(|v| json_lit(v, None))
                                .collect::<Result<Vec<_>>>()?;
                            Ok(l.in_list(
                                list.into_iter()
                                    .map(datafusion::logical_expr::lit)
                                    .collect(),
                                false,
                            ))
                        }
                        _ => bail!("in.right must be an array literal"),
                    }
                }
                other => bail!("unsupported expr op [{other}]"),
            }
        }
    }
}

fn json_lit(v: &serde_json::Value, ty: Option<&str>) -> Result<ScalarValue> {
    if let Some(t) = ty {
        match t.to_ascii_lowercase().as_str() {
            "integer" | "int" => {
                let n = v.as_i64().context("integer lit")?;
                return Ok(ScalarValue::Int32(Some(n as i32)));
            }
            "long" => return Ok(ScalarValue::Int64(Some(v.as_i64().context("long lit")?))),
            "double" | "float" => {
                let n = v
                    .as_f64()
                    .or_else(|| v.as_i64().map(|i| i as f64))
                    .context("double lit")?;
                return Ok(ScalarValue::Float64(Some(n)));
            }
            "boolean" | "bool" => {
                return Ok(ScalarValue::Boolean(Some(v.as_bool().context("bool lit")?)))
            }
            "keyword" | "text" | "string" => {
                let s = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                return Ok(ScalarValue::Utf8(Some(s)));
            }
            _ => {}
        }
    }
    Ok(match v {
        serde_json::Value::Null => ScalarValue::Null,
        serde_json::Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
        serde_json::Value::Number(n) if n.is_i64() => ScalarValue::Int64(Some(n.as_i64().unwrap())),
        serde_json::Value::Number(n) => ScalarValue::Float64(Some(n.as_f64().unwrap())),
        serde_json::Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
        other => bail!("unsupported literal {other}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::dataframe::DataFrameWriteOptions;
    use esql_df_ir::SubmitJobRequest;

    use super::*;

    async fn write_sample_parquet(path: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("bytes", DataType::Int64, false),
            Field::new("status", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![10i64, 20, 5])),
                Arc::new(Int32Array::from(vec![500, 200, 500])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        ctx.table("t")
            .await
            .unwrap()
            .write_parquet(path, DataFrameWriteOptions::new(), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn scan_filter_project_agg() {
        let dir = std::env::temp_dir().join(format!("esql-df-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("sample.parquet");
        write_sample_parquet(path.to_str().unwrap()).await;

        let json = format!(
            r#"{{
            "api_version": 1,
            "output_schema": [
                {{"name": "$$partial$$bytes", "type": "long"}},
                {{"name": "host", "type": "keyword"}}
            ],
            "store": {{"scheme": "file"}},
            "paths": [{{"path": "{}"}}],
            "plan": {{
                "op": "aggregate",
                "mode": "PARTIAL",
                "groups": ["host"],
                "aggs": [{{"alias": "$$partial$$bytes", "fn": "sum", "input": "bytes"}}],
                "input": {{
                    "op": "filter",
                    "expr": {{
                        "op": "gte",
                        "left": {{"col": "status"}},
                        "right": {{"lit": 500, "type": "integer"}}
                    }},
                    "input": {{
                        "op": "project",
                        "cols": ["bytes", "host", "status"],
                        "input": {{"op": "scan", "format": "parquet", "projection": ["bytes", "host", "status"]}}
                    }}
                }}
            }}
        }}"#,
            path.display()
        );
        let job = SubmitJobRequest::from_json(json.as_bytes()).unwrap();
        let df = execute_job(&job).await.unwrap();
        let batches = df.collect().await.unwrap();
        let mut got = std::collections::HashMap::new();
        for batch in &batches {
            let schema = batch.schema();
            let host_idx = schema.index_of("host").unwrap();
            let bytes_idx = schema.index_of("$$partial$$bytes").unwrap();
            let host_arr = cast(batch.column(host_idx), &DataType::Utf8).unwrap();
            let hosts = host_arr.as_any().downcast_ref::<StringArray>().unwrap();
            let bytes_arr = cast(batch.column(bytes_idx), &DataType::Int64).unwrap();
            let bytes = bytes_arr.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..batch.num_rows() {
                got.insert(hosts.value(i).to_string(), bytes.value(i));
            }
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("a"), Some(&10));
        assert_eq!(got.get("b"), Some(&5));
        let _ = std::fs::remove_dir_all(dir);
    }
}
