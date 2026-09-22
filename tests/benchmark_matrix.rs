//! Controlled Jev benchmarks. The only provider endpoint is a local test server.
use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use axum::{routing::post, Json, Router};
use datafusion::{
    common::{
        tree_node::{TreeNode, TreeNodeRecursion},
        Result,
    },
    datasource::MemTable,
    prelude::{SessionConfig, SessionContext},
};
use jev_datafusion::{register_jev, sql, JevProvider};
use jev_datafusion::{JevQuestionType, JevRequest};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::{WriterProperties, WriterVersion},
};
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeSet,
    fs::File,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

#[derive(Default)]
struct Counters {
    requests: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    states: Mutex<BTreeSet<String>>,
}
struct ActiveRequest(Arc<Counters>);
impl ActiveRequest {
    fn enter(counters: Arc<Counters>) -> Self {
        let active = counters.active.fetch_add(1, Ordering::SeqCst) + 1;
        counters.peak.fetch_max(active, Ordering::SeqCst);
        Self(counters)
    }
}
impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct ControlledServer {
    endpoint: String,
    counters: Arc<Counters>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for ControlledServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl ControlledServer {
    async fn start(delay: Duration, capture_states: bool) -> Result<Self> {
        let counters = Arc::new(Counters::default());
        let received = counters.clone();
        let app = Router::new().route(
            "/v1/systemone",
            post(move |Json(request): Json<JevRequest>| {
                let received = received.clone();
                async move {
                    received.requests.fetch_add(1, Ordering::SeqCst);
                    let _active = ActiveRequest::enter(received.clone());
                    if capture_states {
                        received
                            .states
                            .lock()
                            .unwrap()
                            .insert(request.state.to_string());
                    }
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    Json(controlled_response(request))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(Self {
            endpoint,
            counters,
            task,
        })
    }
    fn context(&self, partitions: usize) -> Result<SessionContext> {
        let context = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(partitions)
                .with_batch_size(2048),
        );
        register_jev(
            &context,
            Arc::new(JevProvider::new(
                Some("controlled-benchmark-only".into()),
                &self.endpoint,
            )?),
        )?;
        Ok(context)
    }
}

fn controlled_response(request: JevRequest) -> Value {
    let answer = match request.questions["q0"].r#type {
        JevQuestionType::Noul => json!({"type":"noul","noul":0.9}),
        JevQuestionType::Choice => {
            let row = request
                .state
                .as_str()
                .and_then(|state| state.strip_prefix("record-"))
                .and_then(|index| index.parse::<u64>().ok())
                .unwrap_or(0);
            let weights: Vec<u64> = (0..75)
                .map(|option| 1 + ((row + 1) * (option + 17) * 73) % 9973)
                .collect();
            let total = weights.iter().sum::<u64>() as f64;
            let winner = (0..75).fold(0, |winner, index| {
                if weights[index] > weights[winner] {
                    index
                } else {
                    winner
                }
            });
            let probabilities: Map<String, Value> = weights
                .iter()
                .enumerate()
                .map(|(option, weight)| {
                    (format!("option_{option:02}"), json!(*weight as f64 / total))
                })
                .collect();
            json!({"type":"choice","choice":format!("option_{winner:02}"),"confidence":weights[winner] as f64/total,"probabilities":probabilities})
        }
        _ => panic!("unsupported benchmark question type"),
    };
    json!({"model":request.model,"answers":{"q0":answer}})
}

#[derive(Clone, Copy)]
enum StateEncoding {
    Text,
    JsonText,
    Struct,
}
impl StateEncoding {
    fn name(self) -> &'static str {
        match self {
            Self::Text => "plain_text",
            Self::JsonText => "json_text",
            Self::Struct => "struct",
        }
    }
}
#[derive(Clone, Copy)]
enum Criteria {
    Omitted,
    Literal,
    PerRow,
}
impl Criteria {
    fn name(self) -> &'static str {
        match self {
            Self::Omitted => "omitted",
            Self::Literal => "literal",
            Self::PerRow => "row_varying_unique",
        }
    }
}
struct Case {
    axis: &'static str,
    label: String,
    rows: usize,
    unique: usize,
    partitions: usize,
    selectivity: usize,
    limit: Option<usize>,
    encoding: StateEncoding,
    criteria: Criteria,
    delay_ms: u64,
}
impl Case {
    fn baseline(axis: &'static str, label: impl Into<String>) -> Self {
        Self {
            axis,
            label: label.into(),
            rows: 1000,
            unique: 1000,
            partitions: 1,
            selectivity: 100,
            limit: None,
            encoding: StateEncoding::Text,
            criteria: Criteria::Omitted,
            delay_ms: 0,
        }
    }
    fn query(&self) -> String {
        let criteria = match self.criteria {
            Criteria::Omitted => "",
            Criteria::Literal => ", '{\"true\":\"Controlled condition\"}'",
            Criteria::PerRow => ", criteria",
        };
        let call = format!("noul(body, 'Is this a controlled benchmark row?'{criteria})");
        let limit = self
            .limit
            .map_or(String::new(), |limit| format!(" LIMIT {limit}"));
        format!("SELECT row_id, {call} AS judgment FROM inputs WHERE tenant_id=42 AND {call}>0.8{limit}")
    }
}

fn source(context: &SessionContext, case: &Case) -> Result<()> {
    let mut schema = None;
    let mut partitions = Vec::new();
    for partition in 0..case.partitions {
        let indices: Vec<_> = (0..case.rows)
            .filter(|row| row % case.partitions == partition)
            .collect();
        let keys: Vec<_> = indices.iter().map(|row| row % case.unique).collect();
        let body: ArrayRef = match case.encoding {
            StateEncoding::Text => Arc::new(StringArray::from_iter_values(
                keys.iter().map(|key| format!("record-{key}")),
            )),
            StateEncoding::JsonText => {
                Arc::new(StringArray::from_iter_values(keys.iter().map(|key| {
                    json!({"ordinal":key,"text":format!("record-{key}")}).to_string()
                })))
            }
            StateEncoding::Struct => Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("ordinal", DataType::Int64, false)),
                    Arc::new(Int64Array::from_iter_values(
                        keys.iter().map(|key| *key as i64),
                    )) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("text", DataType::Utf8, false)),
                    Arc::new(StringArray::from_iter_values(
                        keys.iter().map(|key| format!("record-{key}")),
                    )) as ArrayRef,
                ),
            ])),
        };
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, false),
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("body", body.data_type().clone(), false),
            Field::new("criteria", DataType::Utf8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(
                indices.iter().map(|row| *row as i64),
            )),
            Arc::new(Int64Array::from_iter_values(indices.iter().map(|row| {
                if row % 100 < case.selectivity {
                    42
                } else {
                    41
                }
            }))),
            body,
            Arc::new(StringArray::from_iter_values(indices.iter().map(|row| {
                json!({"true":format!("Controlled condition {row}")}).to_string()
            }))),
        ];
        partitions.push(vec![RecordBatch::try_new(batch_schema.clone(), columns)?]);
        schema = Some(batch_schema);
    }
    context.register_table(
        "inputs",
        Arc::new(MemTable::try_new(schema.unwrap(), partitions)?),
    )?;
    Ok(())
}

fn metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> Result<usize> {
    let mut total = 0;
    plan.apply(|node| {
        if let Some(metrics) = node.metrics() {
            total += metrics
                .iter()
                .filter(|metric| metric.value().name() == name)
                .map(|metric| metric.value().as_usize())
                .sum::<usize>();
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(total)
}
struct Measurement {
    report: Value,
    states: BTreeSet<String>,
}
async fn measure(case: &Case) -> Result<Measurement> {
    let capture = matches!(
        case.encoding,
        StateEncoding::JsonText | StateEncoding::Struct
    );
    let server = ControlledServer::start(Duration::from_millis(case.delay_ms), capture).await?;
    let context = server.context(case.partitions)?;
    source(&context, case)?;
    let query = case.query();
    let start = Instant::now();
    let plan = sql(&context, &query).await?.create_physical_plan().await?;
    let batches = datafusion::physical_plan::collect(plan.clone(), context.task_ctx()).await?;
    let elapsed = start.elapsed();
    let output_partitions: BTreeSet<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .map(|row| *row as usize % case.partitions)
        })
        .collect();
    let requests = server.counters.requests.load(Ordering::SeqCst);
    let peak = server.counters.peak.load(Ordering::SeqCst);
    assert_eq!(
        metric(&plan, "requests")?,
        requests,
        "client and server request counts must agree"
    );
    assert_eq!(metric(&plan, "retries")?, 0);
    assert_eq!(metric(&plan, "failures")?, 0);
    assert_eq!(server.counters.active.load(Ordering::SeqCst), 0);
    assert!(
        peak > 0 && peak <= 8,
        "invalid observed peak HTTP concurrency: {peak}"
    );
    let states = server.counters.states.lock().unwrap().clone();
    Ok(Measurement {
        report: json!({
            "axis":case.axis,"case":case.label,"input_rows":case.rows,"unique_input_states":case.unique,
            "partitions":case.partitions,"source_batch_size":2048,"cheap_predicate_selectivity_percent":case.selectivity,
            "output_source_partitions":output_partitions,
            "limit":case.limit,"state_encoding":case.encoding.name(),"criteria":case.criteria.name(),"mock_handler_delay_ms":case.delay_ms,
            "output_rows":batches.iter().map(RecordBatch::num_rows).sum::<usize>(),"requests":requests,"cache_hits":metric(&plan,"cache_hits")?,
            "retries":0,"failures":0,"peak_http_concurrency":peak,"elapsed_ns":elapsed.as_nanos() as u64,
            "query":query,
        }),
        states,
    })
}

#[tokio::test]
async fn records_actual_server_requests_for_duplicate_rows() -> Result<()> {
    let mut case = Case::baseline("tracer", "duplicates");
    case.rows = 10;
    case.unique = 5;
    let measured = measure(&case).await?.report;
    assert_eq!(measured["output_rows"], 10);
    assert_eq!(
        measured["requests"], 5,
        "measurement must observe actual provider requests"
    );
    assert_eq!(measured["cache_hits"], 5);
    Ok(())
}
#[tokio::test]
async fn observes_overlapping_http_requests_without_exceeding_global_cap() -> Result<()> {
    let mut case = Case::baseline("tracer", "concurrency");
    case.rows = 16;
    case.unique = 16;
    case.partitions = 4;
    case.delay_ms = 5;
    let measured = measure(&case).await?.report;
    assert_eq!(measured["requests"], 16);
    let peak = measured["peak_http_concurrency"].as_u64().unwrap();
    assert!(
        peak > 1,
        "the delayed server must observe overlapping requests, got {peak}"
    );
    assert!(peak <= 8);
    Ok(())
}

async fn choice_outputs(rows: usize) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>, usize)> {
    let server = ControlledServer::start(Duration::ZERO, false).await?;
    let context = server.context(4)?;
    let mut case = Case::baseline("storage", "75_options");
    case.rows = rows;
    case.unique = rows;
    case.partitions = 4;
    source(&context, &case)?;
    let criteria: Map<String, Value> = (0..75)
        .map(|option| {
            (
                format!("option_{option:02}"),
                json!(format!("Controlled option {option}")),
            )
        })
        .collect();
    let query = format!(
        "SELECT row_id, choice(body, 'Choose a controlled option', '{}') AS judgment FROM inputs",
        Value::Object(criteria)
    );
    let batches = sql(&context, &query).await?.collect().await?;
    assert_eq!(server.counters.requests.load(Ordering::SeqCst), rows);
    context.register_table(
        "outputs",
        Arc::new(MemTable::try_new(batches[0].schema(), vec![batches])?),
    )?;
    // Both storage layouts come from the same actual UDF outputs, sorted in
    // the same deterministic order; projecting cannot cause new inference.
    let full = context
        .sql("SELECT row_id, judgment FROM outputs ORDER BY row_id")
        .await?
        .collect()
        .await?;
    let projected=context.sql("SELECT row_id, judgment.label AS label, judgment.confidence AS confidence FROM outputs ORDER BY row_id").await?.collect().await?;
    assert_eq!(server.counters.requests.load(Ordering::SeqCst), rows);
    Ok((full, projected, rows))
}

fn write_parquet(path: &Path, batches: &[RecordBatch]) -> Result<Value> {
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_dictionary_enabled(true)
        .set_max_row_group_row_count(Some(1000))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path)?, batches[0].schema(), Some(properties))?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let rows = reader.metadata().file_metadata().num_rows() as usize;
    let columns: Vec<_> = reader
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|group| group.columns().iter())
        .map(|column| {
            assert!(matches!(column.compression(), Compression::ZSTD(_)));
            // Parquet stores the codec, not its level. The reader supplies a
            // default level in the Rust enum; it is not the writer's setting.
            json!({
                "path":column.column_path().string(),"physical_type":format!("{:?}",column.column_type()),
                "compression":"ZSTD","encodings":column.encodings().map(|encoding|format!("{encoding:?}")).collect::<Vec<_>>(),
                "compressed_bytes":column.compressed_size(),"uncompressed_bytes":column.uncompressed_size(),
            })
        })
        .collect();
    let row_groups = reader.metadata().num_row_groups();
    let schema = format!("{:?}", reader.schema());
    let mut decoded_rows = 0;
    for batch in reader.build()? {
        decoded_rows += batch?.num_rows();
    }
    assert_eq!(decoded_rows, rows, "verify the physical file round trip");
    assert_eq!(
        rows,
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    let bytes = std::fs::metadata(path)?.len();
    Ok(
        json!({"rows":rows,"file_bytes":bytes,"bytes_per_row":bytes as f64/rows as f64,"row_groups":row_groups,
        "format":"parquet","writer_version":"PARQUET_2_0","compression":"ZSTD","configured_zstd_level":3,"compression_level_stored_in_footer":false,"dictionary_enabled":true,
        "max_row_group_rows":1000,"column_chunks":columns,"arrow_schema":schema,"read_back_rows":decoded_rows}),
    )
}

#[tokio::test]
async fn measures_choice_storage_from_actual_udf_outputs() -> Result<()> {
    let (full, projected, requests) = choice_outputs(8).await?;
    assert_eq!(requests, 8);
    let directory = tempfile::tempdir()?;
    let full = write_parquet(&directory.path().join("full.parquet"), &full)?;
    let projected = write_parquet(&directory.path().join("projected.parquet"), &projected)?;
    assert_eq!(
        full["rows"], 8,
        "Parquet measurement must verify written rows"
    );
    assert_eq!(projected["rows"], 8);
    assert!(full["file_bytes"].as_u64().unwrap() > 0);
    assert!(projected["file_bytes"].as_u64().unwrap() > 0);
    Ok(())
}

fn require_counts(report: &Value, rows: usize, requests: usize, hits: usize) {
    assert_eq!(report["output_rows"], rows, "{}", report["case"]);
    assert_eq!(report["requests"], requests, "{}", report["case"]);
    assert_eq!(report["cache_hits"], hits, "{}", report["case"]);
}

fn command_text(program: &str, args: &[&str], root: &Path) -> String {
    let output = std::process::Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .expect("benchmark provenance command must run");
    assert!(output.status.success(), "{program} provenance failed");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

#[tokio::test]
#[ignore = "bounded controlled-server matrix; opt in and set JEV_MOCK_MATRIX_WRITE_REPORT=1 to refresh the JSON artifact"]
async fn bounded_mock_matrix() -> Result<()> {
    let mut cases = Vec::new();
    for percent in [0, 50, 90] {
        let mut case = Case::baseline("duplicate_requests", format!("duplicate_{percent}_percent"));
        case.unique = 1000 * (100 - percent) / 100;
        let requests = case.unique;
        cases.push((case, 1000, requests, 1000 - requests));
    }
    for percent in [1, 10, 100] {
        let mut case = Case::baseline(
            "cheap_selectivity",
            format!("selectivity_{percent}_percent"),
        );
        case.selectivity = percent;
        cases.push((case, 10 * percent, 10 * percent, 0));
    }
    for limit in [1, 10, 100] {
        let mut case = Case::baseline("streaming_limit", format!("limit_{limit}"));
        case.rows = 256;
        case.unique = 256;
        case.partitions = 16;
        case.limit = Some(limit);
        // Sixteen rows per partition makes LIMIT 100 cross partition boundaries.
        cases.push((case, limit, limit, 0));
    }
    for partitions in [1, 4, 16] {
        let mut case = Case::baseline("partitions", format!("partitions_{partitions}"));
        case.partitions = partitions;
        case.delay_ms = 5;
        cases.push((case, 1000, 1000, 0));
    }
    for encoding in [StateEncoding::JsonText, StateEncoding::Struct] {
        let mut case = Case::baseline("state_encoding", encoding.name());
        case.encoding = encoding;
        cases.push((case, 1000, 1000, 0));
    }
    for criteria in [Criteria::Literal, Criteria::PerRow] {
        let mut case = Case::baseline("criteria", criteria.name());
        case.unique = 1;
        case.criteria = criteria;
        let requests = if matches!(criteria, Criteria::Literal) {
            1
        } else {
            1000
        };
        cases.push((case, 1000, requests, 1000 - requests));
    }
    let mut observations = Vec::new();
    let mut equivalent_states = None;
    for (case, rows, requests, hits) in cases {
        let mut measured = measure(&case).await?;
        require_counts(&measured.report, rows, requests, hits);
        if case.axis == "partitions" && case.partitions > 1 {
            assert!(measured.report["peak_http_concurrency"].as_u64().unwrap() > 1);
        }
        if case.axis == "streaming_limit" && case.limit == Some(100) {
            assert!(
                measured.report["output_source_partitions"]
                    .as_array()
                    .unwrap()
                    .len()
                    > 1
            );
        }
        if case.axis == "state_encoding" {
            assert_eq!(measured.states.len(), 1000);
            if let Some(expected) = &equivalent_states {
                assert_eq!(expected, &measured.states);
            } else {
                equivalent_states = Some(measured.states);
            }
        }
        measured.report["expected_requests"] = json!(requests);
        measured.report["expected_cache_hits"] = json!(hits);
        measured.report["assertions_passed"] = json!(true);
        eprintln!(
            "MATRIX case={} rows={} requests={} cache_hits={} peak={} elapsed_ms={:.3}",
            case.label,
            rows,
            requests,
            hits,
            measured.report["peak_http_concurrency"],
            measured.report["elapsed_ns"].as_u64().unwrap() as f64 / 1_000_000.0
        );
        observations.push(measured.report);
    }
    let storage_rows = 1000;
    let start = Instant::now();
    let (full, projected, requests) = choice_outputs(storage_rows).await?;
    for batch in &full {
        let judgments = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let probabilities = judgments
            .column_by_name("probabilities")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(probabilities.value(row).len(), 75);
        }
    }
    let directory = tempfile::tempdir()?;
    let full = write_parquet(&directory.path().join("full-choice.parquet"), &full)?;
    let projected = write_parquet(
        &directory.path().join("label-confidence.parquet"),
        &projected,
    )?;
    assert_eq!(full["rows"], storage_rows);
    assert_eq!(projected["rows"], storage_rows);
    let full_bytes = full["file_bytes"].as_u64().unwrap();
    let projected_bytes = projected["file_bytes"].as_u64().unwrap();
    assert!(
        full_bytes > projected_bytes,
        "the 75-option probability maps must be present in the full output"
    );
    let storage = json!({
        "rows":storage_rows,"options_per_distribution":75,"provider_requests":requests,
        "source":"actual choice UDF output from deterministic local HTTP distributions",
        "same_udf_outputs_for_both_layouts":true,"row_order":"row_id ASC","common_identifier_column":"row_id",
        "probability_map_entries_reordered":false,
        "weight_formula":"1 + (((row_id + 1) * (option_index + 17) * 73) % 9973)",
        "normalization":"divide each integer weight by the sum of 75 weights",
        "label_rule":"first option attaining maximum weight","confidence_rule":"maximum normalized weight",
        "full_choice":full,"label_confidence":projected,
        "additional_file_bytes":full_bytes-projected_bytes,"full_to_projected_ratio":full_bytes as f64/projected_bytes as f64,
        "elapsed_ns":start.elapsed().as_nanos() as u64,"delta_log_included":false,
    });
    eprintln!("STORAGE rows={storage_rows} full_bytes={full_bytes} projected_bytes={projected_bytes} ratio={:.3}",full_bytes as f64/projected_bytes as f64);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let report = json!({
        "artifact":"jev_controlled_mock_matrix",
        "run_unix_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64,
        "source_commit":command_text("git",&["rev-parse","HEAD"],root),
        "source_parent_commits":command_text("git",&["show","-s","--format=%P","HEAD"],root).split_whitespace().map(str::to_owned).collect::<Vec<_>>(),
        "benchmark_source_git_blob":command_text("git",&["hash-object","tests/benchmark_matrix.rs"],root),
        "environment":{"os":std::env::consts::OS,"architecture":std::env::consts::ARCH,"rustc":command_text("rustc",&["--version"],root),"datafusion":datafusion::DATAFUSION_VERSION,"debug_assertions":cfg!(debug_assertions)},
        "method":{"axes":"independent, not Cartesian","measured_runs_per_case":1,"warmup_runs":0,"source_batch_size":2048,
            "query_timer":"SQL planning through collection; server/client/table creation excluded",
            "http_endpoint":"new loopback-only controlled server per case","real_provider_calls":0,"mock_noul":0.9,
            "http_concurrency_metric":"peak simultaneously active server handlers, after JSON request acceptance through response construction",
            "global_transport_concurrency_limit":8,"state_encoding_equivalent":true,
            "command":"cargo test --test benchmark_matrix bounded_mock_matrix -- --ignored --nocapture --test-threads=1"},
        "cases":observations,"storage":storage,
        "limitations":["Synthetic controlled outputs are not live semantic judgments.","Wall times include cold loopback connections and local runtime scheduling; one run per case is not a latency distribution or production throughput claim.","Concurrency cases deliberately add 5 ms of handler delay; other axes do not.","Storage results apply to these deterministic 75-option distributions, Parquet writer settings and sorted row order; Delta logs, object-store overhead and real model entropy are not measured.","Probability values are deterministic, but the actual UDF uses unordered maps; map entry order and exact compressed byte counts can vary between processes.","Existing 10k/400k scaling evidence was not rerun for this matrix."]
    });
    if std::env::var("JEV_MOCK_MATRIX_WRITE_REPORT").as_deref() == Ok("1") {
        let output = root.join("target/mock-matrix.json");
        std::fs::create_dir_all(output.parent().unwrap())?;
        let mut bytes = serde_json::to_vec_pretty(&report).unwrap();
        bytes.push(b'\n');
        std::fs::write(&output, bytes)?;
        eprintln!("REPORT {}", output.display());
    }
    Ok(())
}
