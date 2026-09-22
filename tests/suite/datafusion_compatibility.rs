//! Executable engine compatibility probes before the production UDF is wired.
use std::{any::Any, sync::Arc};

use arrow::datatypes::DataType;
use async_trait::async_trait;
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::{
        async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

#[derive(Debug, PartialEq, Eq, Hash)]
struct Probe {
    signature: Signature,
}

impl ScalarUDFImpl for Probe {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "noul"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::common::not_impl_err!("noul requires asynchronous execution")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for Probe {
    async fn invoke_async_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.9))))
    }
}

fn context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let signature = Signature::any(4, Volatility::Stable).with_parameter_names(vec![
        "state",
        "instructions",
        "criteria",
        "model",
    ])?;
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(Probe { signature })).into_scalar_udf());
    Ok(ctx)
}

#[tokio::test]
async fn stable_literal_must_use_async_path() -> Result<()> {
    let batches = context()?
        .sql("SELECT noul('work', 'recurring?', NULL, 'jev-1.13.0')")
        .await?
        .collect()
        .await?;
    assert_eq!(batches[0].num_rows(), 1);
    Ok(())
}

#[tokio::test]
async fn named_model_must_allow_optional_criteria_gap() -> Result<()> {
    let sql = jev_datafusion::normalize_jev_sql(
        "SELECT noul('work', instructions => 'recurring?', model => 'jev-1.13.0')",
    )?;
    let _ = context()?.sql(&sql).await?;
    Ok(())
}
