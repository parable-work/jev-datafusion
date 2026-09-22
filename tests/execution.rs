mod support;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        Result, ScalarValue,
    },
    execution::TaskContext,
    logical_expr::{
        async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    },
    parquet::arrow::ArrowWriter,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
    prelude::ParquetReadOptions,
};
use futures::StreamExt;
use jev_datafusion::sql;
use std::{
    any::Any,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use support::Mock;

fn elapsed(plan: &Arc<dyn ExecutionPlan>) -> Result<Duration> {
    let mut nanos = 0;
    plan.apply(|node| {
        if node.name() == "JevExec" {
            nanos += node
                .metrics()
                .unwrap()
                .iter()
                .filter(|metric| metric.value().name() == "elapsed_compute")
                .map(|metric| metric.value().as_usize())
                .sum::<usize>();
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(Duration::from_nanos(nanos as u64))
}

#[derive(Debug)]
struct DelayedInput {
    input: Arc<dyn ExecutionPlan>,
    delay: Duration,
}
impl DisplayAs for DelayedInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DelayedInput")
    }
}
impl ExecutionPlan for DelayedInput {
    fn name(&self) -> &str {
        "DelayedInput"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            delay: self.delay,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let delay = self.delay;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            input.then(move |batch| async move {
                tokio::time::sleep(delay).await;
                batch
            }),
        )))
    }
}

#[tokio::test]
async fn jev_elapsed_compute_includes_async_evaluation_but_not_upstream_wait() -> Result<()> {
    let request_delay = Duration::from_millis(60);
    let upstream_delay = Duration::from_millis(300);
    for status in [
        axum::http::StatusCode::OK,
        axum::http::StatusCode::BAD_REQUEST,
    ] {
        let mock = Mock::with_response(
            1,
            32,
            move |_| (status, r#"{"answers":{"q0":{"noul":0.9}}}"#.into()),
            request_delay,
        )
        .await?;
        mock.table(vec![vec![vec![
            (42, "one".into()),
            (42, "two".into()),
            (42, "three".into()),
        ]]])?;
        let plan = sql(&mock.context, "SELECT noul(body,'Q?') FROM episodes")
            .await?
            .create_physical_plan()
            .await?;
        let plan = plan
            .transform_up(|node| {
                if node.name() == "JevExec" {
                    let input = Arc::new(DelayedInput {
                        input: node.children()[0].clone(),
                        delay: upstream_delay,
                    });
                    return Ok(Transformed::yes(node.with_new_children(vec![input])?));
                }
                Ok(Transformed::no(node))
            })?
            .data;
        let start = Instant::now();
        let output =
            datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await;
        let wall = start.elapsed();
        let measured = elapsed(&plan)?;
        eprintln!(
            "JEV_TIMING status={} wall_ms={} operator_ms={} requests={}",
            status.as_u16(),
            wall.as_millis(),
            measured.as_millis(),
            mock.count()
        );
        let requests = if status.is_success() {
            assert_eq!(support::rows(&output?), 3);
            3
        } else {
            assert!(output.is_err());
            1
        };
        assert_eq!(mock.count(), requests as usize);
        assert!(
            measured >= requests * request_delay,
            "missing async duration: {measured:?}"
        );
        assert!(
            wall.saturating_sub(measured) >= upstream_delay,
            "upstream wait leaked into operator duration: wall={wall:?} operator={measured:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn dropping_pending_async_evaluation_records_elapsed_time() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.pause_responses();
    let plan = sql(&mock.context, "SELECT noul('one','Q?')")
        .await?
        .create_physical_plan()
        .await?;
    let mut stream =
        datafusion::physical_plan::execute_stream(plan.clone(), mock.context.task_ctx())?;
    mock.wait_for_request(&mut stream).await;
    let hold = Duration::from_millis(50);
    tokio::time::sleep(hold).await;
    drop(stream);
    assert!(elapsed(&plan)? >= hold);
    assert_eq!(mock.count(), 1);
    mock.resume_responses();
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FieldMetadataProbe {
    signature: Signature,
}
impl ScalarUDFImpl for FieldMetadataProbe {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "field_metadata_probe"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::common::not_impl_err!("async probe")
    }
}
#[async_trait]
impl AsyncScalarUDFImpl for FieldMetadataProbe {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            args.arg_fields[0]
                .metadata()
                .get("jev:root-metadata")
                .cloned()
                .unwrap_or_else(|| "missing".into()),
        ))))
    }
}

#[tokio::test]
async fn parquet_root_field_metadata_reaches_native_async_arguments() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    mock.context.register_udf(
        AsyncScalarUDF::new(Arc::new(FieldMetadataProbe {
            signature: Signature::any(1, Volatility::Stable),
        }))
        .into_scalar_udf(),
    );
    let field =
        Field::new("body", DataType::Utf8, false).with_metadata(std::collections::HashMap::from([
            ("jev:root-metadata".into(), "retained".into()),
        ]));
    let schema = Arc::new(Schema::new(vec![field]));
    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["one"]))];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let file = tempfile::Builder::new().suffix(".parquet").tempfile()?;
    let mut writer = ArrowWriter::try_new(file.reopen()?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    // DataFusion 53 clears root field metadata during schema inference by
    // default. Sources carrying semantic extension metadata must opt out of
    // that clearing, or provide an authoritative metadata-bearing schema.
    for (table, options, expected) in [
        ("default_source", ParquetReadOptions::default(), "missing"),
        (
            "typed_source",
            ParquetReadOptions::default().skip_metadata(false),
            "retained",
        ),
    ] {
        mock.context
            .register_parquet(table, file.path().to_str().unwrap(), options)
            .await?;
        let frame = sql(
            &mock.context,
            &format!("SELECT noul(body,'Q?'), field_metadata_probe(body) FROM {table}"),
        )
        .await?;
        let output = frame.collect().await?;
        let metadata = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(metadata.value(0), expected, "{table}");
    }
    Ok(())
}
