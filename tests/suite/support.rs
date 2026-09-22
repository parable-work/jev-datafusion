#![allow(dead_code)]
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use axum::{http::StatusCode, routing::post, Json, Router};
use datafusion::{
    common::Result,
    datasource::MemTable,
    execution::{
        memory_pool::{MemoryPool, UnboundedMemoryPool},
        runtime_env::RuntimeEnvBuilder,
    },
    prelude::{SessionConfig, SessionContext},
};
use jev_datafusion::{register_jev, JevProvider};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

pub struct Mock {
    pub context: SessionContext,
    pub requests: Arc<AtomicUsize>,
    pub bodies: Arc<Mutex<Vec<Value>>>,
    response_gate: Arc<tokio::sync::Semaphore>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Mock {
    pub async fn new(partitions: usize, batch_size: usize) -> Result<Self> {
        Self::with_response(partitions, batch_size, |body| {
            let state = body["state"].as_str().unwrap_or("");
            (StatusCode::OK, json!({"model":"jev-1.13.0", "answers":{"q0":{"type":"noul","noul": if state.starts_with("reject") {0.1} else {0.9}}},"usage":{"input_tokens":10,"output_tokens":2}}).to_string())
        }, std::time::Duration::ZERO).await
    }
    pub async fn with_response(
        partitions: usize,
        batch_size: usize,
        response: impl Fn(&Value) -> (StatusCode, String) + Send + Sync + 'static,
        delay: std::time::Duration,
    ) -> Result<Self> {
        Self::with_response_and_pool(
            partitions,
            batch_size,
            response,
            delay,
            Arc::new(UnboundedMemoryPool::default()),
        )
        .await
    }
    pub async fn with_response_and_pool(
        partitions: usize,
        batch_size: usize,
        response: impl Fn(&Value) -> (StatusCode, String) + Send + Sync + 'static,
        delay: std::time::Duration,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let requests = Arc::new(AtomicUsize::new(0));
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let counted = requests.clone();
        let recorded = bodies.clone();
        let response = Arc::new(response);
        let response_gate = Arc::new(tokio::sync::Semaphore::new(
            tokio::sync::Semaphore::MAX_PERMITS,
        ));
        let gate = response_gate.clone();
        let app = Router::new().route(
            "/v1/systemone",
            post(move |Json(body): Json<Value>| {
                let counted = counted.clone();
                let recorded = recorded.clone();
                let response = response.clone();
                let gate = gate.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    recorded.lock().unwrap().push(body.clone());
                    let _permit = gate.acquire().await.unwrap();
                    tokio::time::sleep(delay).await;
                    response(&body)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(partitions)
                .with_batch_size(batch_size),
            Arc::new(RuntimeEnvBuilder::new().with_memory_pool(pool).build()?),
        );
        register_jev(
            &context,
            Arc::new(JevProvider::new(Some("test-only".into()), &endpoint)?),
        )?;
        Ok(Self {
            context,
            requests,
            bodies,
            response_gate,
            server,
        })
    }
    pub fn pause_responses(&self) {
        self.response_gate
            .forget_permits(tokio::sync::Semaphore::MAX_PERMITS);
    }
    pub fn resume_responses(&self) {
        self.response_gate.add_permits(1);
    }
    pub async fn wait_for_request(
        &self,
        stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
    ) {
        use futures::StreamExt;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                response = stream.next() => panic!("query completed while mock response was gated: {response:?}"),
                _ = async {
                    while self.count() == 0 { tokio::time::sleep(std::time::Duration::from_millis(1)).await; }
                } => {}
            }
        }).await.expect("the polled query must reach the mock server");
    }
    pub fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
    pub fn table(&self, partitions: Vec<Vec<Vec<(i64, String)>>>) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let partitions = partitions
            .into_iter()
            .map(|batches| {
                batches
                    .into_iter()
                    .map(|rows| {
                        let columns: Vec<ArrayRef> = vec![
                            Arc::new(Int64Array::from_iter_values(
                                rows.iter().map(|(tenant, _)| *tenant),
                            )),
                            Arc::new(StringArray::from_iter_values(
                                rows.iter().map(|(_, body)| body.as_str()),
                            )),
                        ];
                        Ok(RecordBatch::try_new(schema.clone(), columns)?)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        self.context
            .register_table("episodes", Arc::new(MemTable::try_new(schema, partitions)?))?;
        Ok(())
    }
}
pub fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

pub fn metric(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    name: &str,
) -> Result<usize> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
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
