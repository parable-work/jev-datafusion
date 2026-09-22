mod support;

use arrow::datatypes::{DataType, Field};
use datafusion::{
    common::{config::ConfigOptions, Result, ScalarValue},
    logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Volatility},
    prelude::SessionContext,
};
use jev_datafusion::{register_jev, sql, JevProvider, JevUdf};
use std::sync::Arc;
use support::Mock;

#[tokio::test]
async fn stable_literals_use_async_execution() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    for name in ["noul", "choice", "score", "ask"] {
        assert_eq!(
            mock.context.state().scalar_functions()[name]
                .signature()
                .volatility,
            Volatility::Stable
        );
    }
    sql(&mock.context, "SELECT noul('all literal', 'Q?')")
        .await?
        .collect()
        .await?;
    assert_eq!(mock.count(), 1);
    Ok(())
}

#[tokio::test]
async fn missing_key_fails_before_execution() -> Result<()> {
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(None, "http://127.0.0.1:1")?),
    )?;
    let error = sql(&context, "SELECT noul('state', 'Q?')")
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("TYPESAFE_API_KEY"), "{error}");
    Ok(())
}

#[tokio::test]
async fn datafusion_validates_names_and_optional_gaps() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    for query in [
        "SELECT noul('s', instructions=>'Q?', model=>'m')",
        "SELECT noul(model=>'m', instructions=>'Q?', state=>'s')",
        "SELECT noul('s', 'Q?', NULL, 'm')",
    ] {
        sql(&mock.context, query).await?.collect().await?;
    }
    assert_eq!(mock.count(), 3);
    for query in [
        "SELECT noul('s', instructions=>'Q?', unknown=>'m')",
        "SELECT noul('s', instructions=>'Q?', instructions=>'again')",
        "SELECT noul(instructions=>'Q?')",
        "SELECT noul('s', model=>'m')",
        "SELECT noul('s','Q?',NULL,'m','extra')",
    ] {
        assert!(sql(&mock.context, query).await.is_err(), "{query}");
    }
    assert_eq!(mock.count(), 3);
    Ok(())
}

#[tokio::test]
async fn column_criteria_and_model_resolve_per_row() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    sql(&mock.context, r#"SELECT noul(body, 'Q?', criteria, model) FROM (VALUES ('same','{"true":"A"}','model-a'),('same','{"true":"B"}','model-a'),('same','{"true":"A"}','model-b')) AS t(body, criteria, model)"#).await?.collect().await?;
    assert_eq!(mock.count(), 3);
    let bodies = mock.bodies.lock().unwrap();
    assert_eq!(bodies[0]["questions"]["q0"]["criteria"]["true"], "A");
    assert_eq!(bodies[1]["questions"]["q0"]["criteria"]["true"], "B");
    assert_eq!(bodies[2]["model"], "model-b");
    Ok(())
}

#[test]
fn synchronous_entry_point_returns_error_without_panic() -> Result<()> {
    let udf = JevUdf::new(
        "noul",
        Arc::new(JevProvider::new(
            Some("test-only".into()),
            "http://127.0.0.1:1",
        )?),
    )?;
    let error = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ScalarValue::Utf8(Some("s".into())).into()],
            arg_fields: vec![Arc::new(Field::new("state", DataType::Utf8, false))],
            number_rows: 1,
            return_field: Arc::new(Field::new("noul", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::new()),
        })
        .unwrap_err()
        .to_string();
    assert!(error.contains("asynchronous execution"));
    Ok(())
}
