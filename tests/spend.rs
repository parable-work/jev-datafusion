use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use axum::{routing::post, Json, Router};
use datafusion::{common::Result, prelude::SessionContext};
use jev_datafusion::{register_jev, sql, JevProvider};
use serde_json::json;

#[tokio::test]
async fn cheap_predicate_runs_before_paid_judgment() -> Result<()> {
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let app = Router::new().route("/v1/systemone", post(move || {
        let count = counted.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Json(json!({"model":"jev-1.13.0","answers":{"q0":{"type":"noul","noul":0.9}},"usage":{"input_tokens":10,"output_tokens":2}}))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("test-only".into()), &endpoint)?),
    )?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![41, 42, 43])),
        Arc::new(StringArray::from(vec![
            "excluded before",
            "included",
            "excluded after",
        ])),
    ];
    context.register_batch("episodes", RecordBatch::try_new(schema, arrays)?)?;
    let batches = sql(
        &context,
        "SELECT body FROM episodes WHERE tenant_id = 42 AND noul(body, 'Is this recurring?') > 0.8",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "paid calls must equal rows surviving the cheap predicate"
    );
    server.abort();
    Ok(())
}

mod support;
use support::{rows, Mock};

#[tokio::test]
async fn select_and_where_share_one_judgment_per_surviving_row() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![
        (0, "excluded".into()),
        (42, "included".into()),
        (42, "reject".into()),
    ]]])?;
    let result = sql(&mock.context, "SELECT body, noul(body, 'Recurring?') AS p FROM episodes WHERE tenant_id=42 AND noul(body, 'Recurring?')>.8").await?.collect().await?;
    assert_eq!(rows(&result), 1);
    assert_eq!(mock.count(), 2);
    Ok(())
}

#[tokio::test]
async fn repeated_projection_and_predicate_calls_are_deduplicated() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![(42, "one".into()), (42, "two".into())]]])?;
    let df = sql(&mock.context, "SELECT noul(body, 'Q?') AS a, noul(body, 'Q?') AS b FROM episodes WHERE noul(body, 'Q?')>.8 AND noul(body, 'Q?')<1").await?;
    let result = df.collect().await?;
    assert_eq!(rows(&result), 2);
    assert_eq!(mock.count(), 2);
    Ok(())
}

#[tokio::test]
async fn canonical_inputs_deduplicate_inside_batch_but_not_between_queries() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![
        (42, r#"{"b":2,"a":1}"#.into()),
        (42, r#"{ "a": 1, "b": 2 }"#.into()),
    ]]])?;
    for expected in [1, 2] {
        let result = sql(&mock.context, "SELECT noul(body, 'Q?') FROM episodes")
            .await?
            .collect()
            .await?;
        assert_eq!(rows(&result), 2);
        assert_eq!(mock.count(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn dedupe_includes_questions_and_model() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![(42, "same".into()), (42, "same".into())]]])?;
    let result = sql(&mock.context, "SELECT noul(body, 'Q1?'), noul(body, 'Q2?'), noul(body, 'Q1?', model=>'different') FROM episodes").await?.collect().await?;
    assert_eq!(rows(&result), 2);
    assert_eq!(mock.count(), 3);
    Ok(())
}

#[tokio::test]
async fn limit_stops_new_spend_across_batches_and_partitions() -> Result<()> {
    for partitions in [1, 4] {
        let mock = Mock::new(partitions, 16).await?;
        mock.table(
            (0..partitions)
                .map(|partition| {
                    (0..3)
                        .map(|batch| {
                            (0..16)
                                .map(|row| (42, format!("{partition}-{batch}-{row}")))
                                .collect()
                        })
                        .collect()
                })
                .collect(),
        )?;
        let df = sql(
            &mock.context,
            "SELECT body, noul(body, 'Q?') FROM episodes WHERE noul(body, 'Q?')>.8 LIMIT 3",
        )
        .await?;
        let result = df.collect().await?;
        assert_eq!(rows(&result), 3);
        assert_eq!(mock.count(), 3, "partition count {partitions}");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert_eq!(mock.count(), 3, "no detached calls after completion");
    }
    Ok(())
}

#[tokio::test]
async fn limit_with_rejections_offset_and_empty_partitions() -> Result<()> {
    let mock = Mock::new(3, 16).await?;
    mock.table(vec![
        vec![],
        vec![
            vec![
                (42, "reject-0".into()),
                (42, "skip".into()),
                (42, "reject-1".into()),
            ],
            vec![(42, "take".into()), (42, "untouched".into())],
        ],
        vec![vec![(42, "untouched-partition".into())]],
    ])?;
    let result = sql(
        &mock.context,
        "SELECT body FROM episodes WHERE noul(body, 'Q?')>.8 LIMIT 1 OFFSET 1",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(rows(&result), 1);
    assert_eq!(mock.count(), 4);
    Ok(())
}

#[tokio::test]
async fn limit_zero_makes_no_requests() -> Result<()> {
    let mock = Mock::new(4, 32).await?;
    mock.table(vec![vec![vec![(42, "one".into())]]])?;
    let result = sql(
        &mock.context,
        "SELECT noul(body, 'Q?') FROM episodes WHERE noul(body, 'Q?')>.8 LIMIT 0",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(rows(&result), 0);
    assert_eq!(mock.count(), 0);
    Ok(())
}

#[tokio::test]
async fn explain_analyze_exposes_operator_metrics() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![(42, "same".into()), (42, "same".into())]]])?;
    let result = sql(
        &mock.context,
        "EXPLAIN ANALYZE SELECT noul(body, 'Q?') FROM episodes",
    )
    .await?
    .collect()
    .await?;
    let text = arrow::util::pretty::pretty_format_batches(&result)?.to_string();
    for field in [
        "JevExec",
        "requests=1",
        "cache_hits=1",
        "retries=0",
        "failures=0",
        "input_tokens=",
        "output_tokens=",
        "estimated_cost_nano_usd=",
        "unpriced_requests=",
    ] {
        assert!(text.contains(field), "missing {field}: {text}");
    }
    Ok(())
}

#[tokio::test]
async fn dropping_polled_stream_stops_requests() -> Result<()> {
    let mock = Mock::with_response(
        1,
        32,
        |_| {
            (
                axum::http::StatusCode::OK,
                r#"{"answers":{"q0":{"noul":0.9}}}"#.into(),
            )
        },
        std::time::Duration::ZERO,
    )
    .await?;
    mock.table(vec![vec![(0..16).map(|i| (42, i.to_string())).collect()]])?;
    mock.pause_responses();
    let mut stream = sql(&mock.context, "SELECT noul(body, 'Q?') FROM episodes")
        .await?
        .execute_stream()
        .await?;
    mock.wait_for_request(&mut stream).await;
    assert_eq!(mock.count(), 1);
    drop(stream);
    mock.resume_responses();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(mock.count(), 1);
    Ok(())
}

#[tokio::test]
async fn cheap_filters_precede_cse_projections_and_paid_or_predicates() -> Result<()> {
    for predicate in [
        "tenant_id=42 AND noul(body,'Q?')>.8 AND noul(body,'Q?')<1",
        "noul(body,'Q?')>.8 AND tenant_id=42 AND noul(body,'Q?')<1",
        "tenant_id=42 AND (noul(body,'Q?')>.8 OR noul(body,'Q?')<.2)",
    ] {
        let mock = Mock::new(1, 32).await?;
        mock.table(vec![vec![vec![
            (41, "exclude-1".into()),
            (42, "include".into()),
            (43, "exclude-2".into()),
        ]]])?;
        let result = sql(
            &mock.context,
            &format!("SELECT body FROM episodes WHERE {predicate}"),
        )
        .await?
        .collect()
        .await?;
        assert_eq!(rows(&result), 1);
        assert_eq!(mock.count(), 1, "{predicate}");
    }
    Ok(())
}

#[tokio::test]
async fn ask_reused_by_registered_json_extractors_makes_one_request() -> Result<()> {
    let mut mock = Mock::new(1, 32).await?;
    datafusion_functions_json::register_all(&mut mock.context)?;
    mock.table(vec![vec![vec![(42, "one".into()), (42, "two".into())]]])?;
    let query = r#"SELECT json_get_str(ask(body,'{"q0":{"type":"noul","instructions":"Q?"}}'),'model') AS model,
      json_get_float(ask(body,'{"q0":{"type":"noul","instructions":"Q?"}}'),'answers','q0','noul') AS score
      FROM episodes WHERE json_get_float(ask(body,'{"q0":{"type":"noul","instructions":"Q?"}}'),'answers','q0','noul')>.8"#;
    let result = sql(&mock.context, query).await?.collect().await?;
    assert_eq!(rows(&result), 2);
    assert_eq!(mock.count(), 2);
    Ok(())
}

#[tokio::test]
async fn same_physical_plan_executions_never_share_cached_responses() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    let plan = sql(&mock.context, "SELECT noul('same','Q?')")
        .await?
        .create_physical_plan()
        .await?;
    for expected in [1, 2] {
        datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
        assert_eq!(mock.count(), expected);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "mocked scaling benchmark; run explicitly with --ignored --nocapture"]
async fn benchmark_mocked_rows() -> Result<()> {
    for size in [1_000, 10_000, 400_000] {
        let mock = Mock::new(1, 8192).await?;
        let batches = (0..size)
            .collect::<Vec<_>>()
            .chunks(8192)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|i| (42, format!("state-{}", i % 100)))
                    .collect()
            })
            .collect();
        mock.table(vec![batches])?;
        let start = std::time::Instant::now();
        let result = sql(
            &mock.context,
            "SELECT noul(body,'Q?') AS p FROM episodes WHERE noul(body,'Q?')>.8",
        )
        .await?
        .collect()
        .await?;
        let elapsed = start.elapsed();
        assert_eq!(rows(&result), size);
        let expected = (size / 8192) * 100 + (size % 8192).min(100);
        assert_eq!(mock.count(), expected);
        eprintln!(
            "MOCK_BENCH rows={size} requests={} elapsed_ms={} rows_per_second={:.0}",
            mock.count(),
            elapsed.as_millis(),
            size as f64 / elapsed.as_secs_f64()
        );
    }
    Ok(())
}

#[tokio::test]
async fn cache_does_not_cross_input_batches() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![
        vec![(42, "same".into()), (42, "same".into())],
        vec![(42, "same".into()), (42, "same".into())],
    ]])?;
    let result = sql(&mock.context, "SELECT noul(body,'Q?') FROM episodes")
        .await?
        .collect()
        .await?;
    assert_eq!(rows(&result), 4);
    assert_eq!(mock.count(), 2);
    Ok(())
}

#[tokio::test]
async fn projection_only_limit_does_not_prefetch_paid_partitions() -> Result<()> {
    let mock = Mock::new(4, 32).await?;
    mock.table(
        (0..4)
            .map(|partition| {
                vec![(0..32)
                    .map(|row| (42, format!("{partition}-{row}")))
                    .collect()]
            })
            .collect(),
    )?;
    let result = sql(
        &mock.context,
        "SELECT noul(body,'Q?') FROM episodes LIMIT 3",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(rows(&result), 3);
    assert_eq!(mock.count(), 3);
    Ok(())
}

#[tokio::test]
async fn sort_limit_evaluates_required_rows_and_retains_correct_result() -> Result<()> {
    let mock = Mock::new(2, 32).await?;
    mock.table(vec![
        vec![vec![(42, "reject-first".into())]],
        vec![vec![(42, "accept".into()), (42, "reject-last".into())]],
    ])?;
    let result = sql(
        &mock.context,
        "SELECT body, noul(body,'Q?') AS p FROM episodes ORDER BY p DESC LIMIT 1",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(rows(&result), 1);
    assert_eq!(mock.count(), 3);
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "accept"
    );
    Ok(())
}

#[tokio::test]
async fn counters_are_attached_to_the_operator_that_sent_requests() -> Result<()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mock = Mock::new(1, 32).await?;
    mock.table(vec![vec![vec![
        (42, "one".into()),
        (42, "two".into()),
        (42, "reject".into()),
    ]]])?;
    let plan = sql(
        &mock.context,
        "SELECT noul(body,'selected?') FROM episodes WHERE noul(body,'included?')>.8",
    )
    .await?
    .create_physical_plan()
    .await?;
    datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
    let mut counts = Vec::new();
    plan.apply(|node| {
        if node.name() == "JevExec" {
            let metrics = node.metrics().unwrap();
            let requests: usize = metrics
                .iter()
                .filter(|metric| metric.value().name() == "requests")
                .map(|metric| metric.value().as_usize())
                .sum();
            counts.push(requests);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    counts.sort();
    assert_eq!(counts, vec![2, 3]);
    assert_eq!(mock.count(), 5);
    Ok(())
}

#[tokio::test]
async fn large_criteria_and_response_batch_never_repays_duplicate_keys() -> Result<()> {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use serde_json::json;
    // 128 distinct 64 KiB rubrics plus 96 KiB raw replies exceed the old
    // 16 MiB bypass limit. The input columns plus cached replies fit a
    // configured 32 MiB DataFusion pool when request keys are fingerprints.
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024));
    let response =
        json!({"answers":{"q0":{"noul":0.9}},"future_detail":"r".repeat(96*1024)}).to_string();
    let mock = Mock::with_response_and_pool(
        1,
        512,
        move |_| (axum::http::StatusCode::OK, response.clone()),
        std::time::Duration::ZERO,
        pool.clone(),
    )
    .await?;
    let criteria: Vec<_> = (0..256)
        .map(|row| json!({"true":format!("rubric {} {}",row%128,"c".repeat(64*1024))}).to_string())
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, false),
        Field::new("criteria", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["same"; 256])),
        Arc::new(StringArray::from(criteria)),
    ];
    mock.context
        .register_batch("large_rubrics", RecordBatch::try_new(schema, columns)?)?;
    let plan = sql(
        &mock.context,
        "SELECT noul(body,'Q?',criteria) FROM large_rubrics",
    )
    .await?
    .create_physical_plan()
    .await?;
    let result = datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
    assert_eq!(rows(&result), 256);
    assert_eq!(
        mock.count(),
        128,
        "every canonical request is paid once in the batch"
    );
    assert_eq!(support::metric(&plan, "cache_hits")?, 128);
    assert_eq!(
        pool.reserved(),
        0,
        "cache reservation must be released after the query"
    );
    Ok(())
}

#[tokio::test]
async fn cache_budget_exhaustion_is_fatal_even_in_null_mode() -> Result<()> {
    use datafusion::{
        common::DataFusionError,
        execution::memory_pool::{GreedyMemoryPool, MemoryPool},
    };
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
    let response =
        serde_json::json!({"answers":{"q0":{"noul":0.9}},"future_detail":"r".repeat(8192)})
            .to_string();
    let mock = Mock::with_response_and_pool(
        1,
        32,
        move |_| (axum::http::StatusCode::OK, response.clone()),
        std::time::Duration::ZERO,
        pool.clone(),
    )
    .await?;
    mock.table(vec![vec![vec![(42, "same".into()), (42, "same".into())]]])?;
    sql(&mock.context, "SET jev.on_error='null'")
        .await?
        .collect()
        .await?;
    let result = sql(&mock.context, "SELECT noul(body,'Q?') FROM episodes")
        .await?
        .collect()
        .await;
    let error = result.unwrap_err();
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert_eq!(
        mock.count(),
        1,
        "query aborts instead of issuing a duplicate after cache refusal"
    );
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn memory_reservations_are_released_before_the_next_input_batch() -> Result<()> {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(12 * 1024));
    let response =
        serde_json::json!({"answers":{"q0":{"noul":0.9}},"future_detail":"r".repeat(8192)})
            .to_string();
    let mock = Mock::with_response_and_pool(
        1,
        32,
        move |_| (axum::http::StatusCode::OK, response.clone()),
        std::time::Duration::ZERO,
        pool.clone(),
    )
    .await?;
    mock.table(vec![vec![
        vec![(42, "same".into()), (42, "same".into())],
        vec![(42, "same".into()), (42, "same".into())],
    ]])?;
    let result = sql(&mock.context, "SELECT noul(body,'Q?') FROM episodes")
        .await?
        .collect()
        .await?;
    assert_eq!(rows(&result), 4);
    assert_eq!(mock.count(), 2);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn zero_cache_budget_refuses_before_spending() -> Result<()> {
    use datafusion::{
        common::DataFusionError,
        execution::memory_pool::{GreedyMemoryPool, MemoryPool},
    };
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(0));
    let mock = Mock::with_response_and_pool(
        1,
        32,
        |_| {
            (
                axum::http::StatusCode::OK,
                r#"{"answers":{"q0":{"noul":0.9}}}"#.into(),
            )
        },
        std::time::Duration::ZERO,
        pool.clone(),
    )
    .await?;
    let error = sql(&mock.context, "SELECT noul('same','Q?')")
        .await?
        .collect()
        .await
        .unwrap_err();
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert_eq!(mock.count(), 0);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn more_than_8192_distinct_keys_deduplicate_across_execution_chunks() -> Result<()> {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let mock = Mock::with_response_and_pool(
        1,
        32768,
        |_| {
            (
                axum::http::StatusCode::OK,
                r#"{"answers":{"q0":{"noul":0.9}}}"#.into(),
            )
        },
        std::time::Duration::ZERO,
        pool.clone(),
    )
    .await?;
    // DataSourceExec splits at the configured source batch size. Keep all
    // 16,600 rows in one input batch, while LIMIT makes Jev evaluate one-row
    // demand chunks, so this exercises both the old entry cap and lifetime.
    mock.table(vec![vec![(0..16600)
        .map(|row| (42, format!("state-{}", row % 8300)))
        .collect()]])?;
    let result = sql(
        &mock.context,
        "SELECT noul(body,'Q?') FROM episodes WHERE noul(body,'Q?')>.8 LIMIT 16600",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(rows(&result), 16600);
    assert_eq!(mock.count(), 8300);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn cancelled_request_releases_batch_memory() -> Result<()> {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
    let mock = Mock::with_response_and_pool(
        1,
        32,
        |_| {
            (
                axum::http::StatusCode::OK,
                r#"{"answers":{"q0":{"noul":0.9}}}"#.into(),
            )
        },
        std::time::Duration::ZERO,
        pool.clone(),
    )
    .await?;
    mock.table(vec![vec![(0..16)
        .map(|row| (42, row.to_string()))
        .collect()]])?;
    mock.pause_responses();
    let mut stream = sql(&mock.context, "SELECT noul(body,'Q?') FROM episodes")
        .await?
        .execute_stream()
        .await?;
    mock.wait_for_request(&mut stream).await;
    assert!(pool.reserved() > 0);
    drop(stream);
    mock.resume_responses();
    assert_eq!(pool.reserved(), 0);
    let count = mock.count();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(mock.count(), count);
    Ok(())
}

#[tokio::test]
#[ignore = "mocked workload benchmark; run explicitly with --ignored --nocapture"]
async fn benchmark_duplicate_selective_and_limit_workloads() -> Result<()> {
    for (name, size, partitions) in [
        ("distinct", 1_000, 1),
        ("duplicates", 10_000, 1),
        ("cheap_selectivity", 10_000, 1),
        ("partitioned_limit", 400_000, 4),
    ] {
        let mock = Mock::new(partitions, 8192).await?;
        let mut source = vec![Vec::new(); partitions];
        let all: Vec<_> = (0..size)
            .map(|row| {
                let tenant = if name == "cheap_selectivity" && row % 20 != 0 {
                    41
                } else {
                    42
                };
                let state = if name == "duplicates" { row % 100 } else { row };
                (tenant, format!("state-{state}"))
            })
            .collect();
        for (index, chunk) in all.chunks(8192).enumerate() {
            source[index % partitions].push(chunk.to_vec());
        }
        mock.table(source)?;
        let limit = if name == "partitioned_limit" {
            " LIMIT 10"
        } else {
            ""
        };
        let start = std::time::Instant::now();
        let result = sql(&mock.context,&format!("SELECT noul(body,'Q?') AS p FROM episodes WHERE tenant_id=42 AND noul(body,'Q?')>.8{limit}")).await?.collect().await?;
        let elapsed = start.elapsed();
        let (expected_rows, expected_requests) = match name {
            "duplicates" => (10_000, 200),
            "cheap_selectivity" => (500, 500),
            "partitioned_limit" => (10, 10),
            _ => (1_000, 1_000),
        };
        assert_eq!(rows(&result), expected_rows);
        assert_eq!(mock.count(), expected_requests);
        eprintln!(
            "MOCK_WORKLOAD case={name} source_rows={size} output_rows={} requests={} elapsed_ms={}",
            rows(&result),
            mock.count(),
            elapsed.as_millis()
        );
    }
    Ok(())
}

#[tokio::test]
async fn large_choice_taxonomies_fit_a_configured_budget_without_duplicate_spend() -> Result<()> {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use serde_json::{json, Map, Value};
    // A wide classification catalog with 1,024 meaningful option identifiers,
    // rubrics, and a full probability distribution produces large requests
    // and replies without synthetic padding or unsupported response fields.
    let labels: Vec<_> = (0..1024)
        .map(|index| {
            format!("business_unit_{index:04}_regional_operating_department_classification")
        })
        .collect();
    let criteria: Map<String, Value> = labels
        .iter()
        .map(|label| {
            (
                label.clone(),
                json!("Operational evidence belonging to this business unit and region"),
            )
        })
        .collect();
    let probabilities: Map<String, Value> = labels
        .iter()
        .map(|label| (label.clone(), json!(1.0 / 1024.0)))
        .collect();
    let rubric = serde_json::to_string(&criteria).unwrap();
    let response = json!({"answers":{"q0":{"type":"choice","choice":labels[0],"confidence":1.0/1024.0,"probabilities":probabilities}}}).to_string();
    assert!(128 * (rubric.len() + response.len()) > 16 * 1024 * 1024);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
    let mock = Mock::with_response_and_pool(
        1,
        512,
        move |_| (axum::http::StatusCode::OK, response.clone()),
        std::time::Duration::ZERO,
        pool.clone(),
    )
    .await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, false),
        Field::new("criteria", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            (0..256)
                .map(|row| format!("record-{}", row % 128))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec![rubric; 256])),
    ];
    mock.context
        .register_batch("wide_catalog", RecordBatch::try_new(schema, columns)?)?;
    let result = sql(
        &mock.context,
        "SELECT choice(body,'Select the matching business unit',criteria) FROM wide_catalog",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(rows(&result), 256);
    assert_eq!(mock.count(), 128);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
