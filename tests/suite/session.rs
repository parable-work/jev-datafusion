use crate::support::Mock;
use arrow::array::{Array, Float64Array};
use axum::http::StatusCode;
use datafusion::common::Result;
use jev_datafusion::sql;
use std::time::Duration;

#[tokio::test]
async fn settings_are_query_snapshots_and_explicit_model_wins() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    let old = sql(&mock.context, "SELECT noul('old', 'Q?')").await?;
    sql(&mock.context, "SET jev.model = 'new-model'")
        .await?
        .collect()
        .await?;
    old.collect().await?;
    sql(&mock.context, "SELECT noul('new', 'Q?')")
        .await?
        .collect()
        .await?;
    sql(
        &mock.context,
        "SELECT noul('explicit', 'Q?', model=>'explicit-model')",
    )
    .await?
    .collect()
    .await?;
    let bodies = mock.bodies.lock().unwrap();
    assert_eq!(bodies[0]["model"], "jev-1.13.0");
    assert_eq!(bodies[1]["model"], "new-model");
    assert_eq!(bodies[2]["model"], "explicit-model");
    Ok(())
}

#[tokio::test]
async fn fail_is_default_and_null_recovers_only_eligible_errors() -> Result<()> {
    let mock = Mock::with_response(
        1,
        32,
        |_| (StatusCode::BAD_GATEWAY, "upstream failed".into()),
        Duration::ZERO,
    )
    .await?;
    assert!(sql(&mock.context, "SELECT noul('default', 'Q?')")
        .await?
        .collect()
        .await
        .is_err());
    sql(&mock.context, "SET jev.on_error = 'null'")
        .await?
        .collect()
        .await?;
    let null_query = sql(&mock.context, "SELECT noul('snapshot', 'Q?')").await?;
    sql(&mock.context, "SET jev.on_error = 'fail'")
        .await?
        .collect()
        .await?;
    let result = null_query.collect().await?;
    assert!(result[0].column(0).is_null(0));
    assert!(sql(&mock.context, "SELECT noul('reset', 'Q?')")
        .await?
        .collect()
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn authentication_and_validation_errors_stay_fatal_under_null() -> Result<()> {
    for code in [StatusCode::UNAUTHORIZED, StatusCode::UNPROCESSABLE_ENTITY] {
        let mock = Mock::with_response(
            1,
            32,
            move |_| (code, "private provider diagnostic".into()),
            Duration::ZERO,
        )
        .await?;
        sql(&mock.context, "SET jev.on_error = 'null'")
            .await?
            .collect()
            .await?;
        let error = sql(&mock.context, "SELECT noul('secret input', 'Q?')")
            .await?
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(&code.as_u16().to_string()), "{error}");
        assert!(!error.contains("secret input"));
        assert!(!error.contains("private provider diagnostic"));
    }
    Ok(())
}

#[tokio::test]
async fn null_decode_failures_are_batch_local_and_count_failed_rows() -> Result<()> {
    let mock = Mock::with_response(
        1,
        32,
        |_| (StatusCode::OK, r#"{"answers":{"q0":{"noul":2}}}"#.into()),
        Duration::ZERO,
    )
    .await?;
    mock.table(vec![vec![vec![(42, "same".into()), (42, "same".into())]]])?;
    sql(&mock.context, "SET jev.on_error = 'null'")
        .await?
        .collect()
        .await?;
    let result = sql(
        &mock.context,
        "EXPLAIN ANALYZE SELECT noul(body, 'Q?') FROM episodes",
    )
    .await?
    .collect()
    .await?;
    let text = arrow::util::pretty::pretty_format_batches(&result)?.to_string();
    assert!(text.contains("failures=2"), "{text}");
    assert!(text.contains("cache_hits=1"), "{text}");
    assert_eq!(mock.count(), 1);
    let result = sql(&mock.context, "SELECT noul(body, 'Q?') FROM episodes")
        .await?
        .collect()
        .await?;
    let values = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.null_count(), 2);
    assert_eq!(mock.count(), 2);
    Ok(())
}

#[tokio::test]
async fn invalid_settings_do_not_change_previous_value() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    for command in [
        "SET jev.on_error='ignore'",
        "SET jev.model=''",
        "SET jev.unknown='null'",
    ] {
        assert!(sql(&mock.context, command).await.is_err(), "{command}");
    }
    sql(&mock.context, "SELECT noul('default', 'Q?')")
        .await?
        .collect()
        .await?;
    assert_eq!(mock.bodies.lock().unwrap()[0]["model"], "jev-1.13.0");
    Ok(())
}

#[tokio::test]
async fn invalid_typed_answer_does_not_poison_raw_ask_cache() -> Result<()> {
    const RAW: &str = "{\n\"answers\":{\"q0\":{\"noul\":2}},\"future\":true\n}";
    let mock = Mock::with_response(1, 32, |_| (StatusCode::OK, RAW.into()), Duration::ZERO).await?;
    sql(&mock.context, "SET jev.on_error='null'")
        .await?
        .collect()
        .await?;
    let result = sql(&mock.context, r#"SELECT noul('same','Q?') AS typed, ask('same','{"q0":{"type":"noul","instructions":"Q?"}}') AS raw"#).await?.collect().await?;
    assert!(result[0].column(0).is_null(0));
    assert_eq!(
        result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        RAW
    );
    assert_eq!(mock.count(), 1);
    Ok(())
}

#[tokio::test]
async fn malformed_choice_answers_return_errors_without_panicking() -> Result<()> {
    let mock = Mock::with_response(
        1,
        32,
        |_| (StatusCode::OK, r#"{"answers":{"q0":7}}"#.into()),
        Duration::ZERO,
    )
    .await?;
    assert!(
        sql(&mock.context, r#"SELECT choice('s','Q?','{"a":null}')"#)
            .await?
            .collect()
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn malformed_column_criteria_nulls_only_the_bad_row_before_sending() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    sql(&mock.context, "SET jev.on_error='null'")
        .await?
        .collect()
        .await?;
    let query = "SELECT noul(body, 'Q?', criteria) AS p FROM (VALUES ('good','{}'),('bad','{broken'),('good-again','{}')) AS t(body,criteria)";
    let frame = sql(&mock.context, query).await?;
    let plan = frame.create_physical_plan().await?;
    let result = datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
    let values = result
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some(0.9), None, Some(0.9)]);
    assert_eq!(mock.count(), 2);
    assert_eq!(crate::support::metric(&plan, "failures")?, 1);
    assert!(mock
        .bodies
        .lock()
        .unwrap()
        .iter()
        .all(|body| body["state"] != "bad"));
    Ok(())
}

#[tokio::test]
async fn cached_transport_errors_count_every_failed_row() -> Result<()> {
    let mock = Mock::with_response(
        1,
        32,
        |_| (StatusCode::BAD_GATEWAY, "unavailable".into()),
        Duration::ZERO,
    )
    .await?;
    mock.table(vec![vec![vec![
        (42, "same".into()),
        (42, "same".into()),
        (42, "same".into()),
    ]]])?;
    sql(&mock.context, "SET jev.on_error='null'")
        .await?
        .collect()
        .await?;
    let plan = sql(&mock.context, "SELECT noul(body,'Q?') FROM episodes")
        .await?
        .create_physical_plan()
        .await?;
    let result = datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
    assert_eq!(result[0].column(0).null_count(), 3);
    assert_eq!(mock.count(), 1);
    assert_eq!(crate::support::metric(&plan, "failures")?, 3);
    assert_eq!(crate::support::metric(&plan, "cache_hits")?, 2);
    Ok(())
}

#[tokio::test]
async fn null_policy_applies_to_each_kind_of_row_validation() -> Result<()> {
    for (argument, query) in [
        ("state", "SELECT noul(named_struct('x',x),'Q?') FROM (VALUES (1.0::DOUBLE),('NaN'::DOUBLE)) AS t(x)"),
        ("instructions", "SELECT noul(body,instructions) FROM (VALUES ('good','Q?'),('bad',NULL)) AS t(body,instructions)"),
        ("model", "SELECT noul(body,'Q?',model=>model) FROM (VALUES ('good','jev-1.13.0'),('bad','  ')) AS t(body,model)"),
        ("questions", r#"SELECT ask(body,questions) FROM (VALUES ('good','{"q0":{"type":"noul","instructions":"Q?"}}'),('bad','{broken')) AS t(body,questions)"#),
    ] {
        let mock = Mock::new(1,32).await?;
        sql(&mock.context,"SET jev.on_error='null'").await?.collect().await?;
        let plan = sql(&mock.context,query).await?.create_physical_plan().await?;
        let result = datafusion::physical_plan::collect(plan.clone(),mock.context.task_ctx()).await?;
        let nulls = result.iter().flat_map(|batch| (0..batch.num_rows()).map(|row| batch.column(0).is_null(row))).collect::<Vec<_>>();
        assert_eq!(nulls, vec![false,true], "{argument}");
        assert_eq!(mock.count(),1,"invalid {argument} must fail before sending");
        assert_eq!(crate::support::metric(&plan,"failures")?,1,"{argument}");
    }
    Ok(())
}

#[tokio::test]
async fn fail_policy_aborts_row_validation_and_literal_errors_stay_fatal() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    let frame = sql(&mock.context,"SELECT noul(body,'Q?',criteria) FROM (VALUES ('bad','{broken'),('good','{}')) AS t(body,criteria)").await?;
    assert!(frame.collect().await.is_err());
    assert_eq!(mock.count(), 0);
    sql(&mock.context, "SET jev.on_error='null'")
        .await?
        .collect()
        .await?;
    for query in [
        "SELECT noul('s','Q?','{broken')",
        "SELECT noul('s',NULL)",
        "SELECT noul('s','Q?',model=>'')",
        "SELECT noul(42,'Q?')",
        "SELECT ask('s','{broken')",
    ] {
        let error = sql(&mock.context, query)
            .await
            .expect_err("literal validation must fail while planning");
        assert!(
            matches!(
                error.find_root(),
                datafusion::common::DataFusionError::Plan(_)
            ),
            "{query}: {error}"
        );
    }
    assert_eq!(mock.count(), 0);
    Ok(())
}

#[tokio::test]
async fn sql_null_state_is_not_a_failed_row() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    sql(&mock.context, "SET jev.on_error='null'")
        .await?
        .collect()
        .await?;
    let plan = sql(
        &mock.context,
        "SELECT noul(body,'Q?') FROM (VALUES ('good'),(NULL)) AS t(body)",
    )
    .await?
    .create_physical_plan()
    .await?;
    let result = datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
    assert_eq!(result[0].column(0).null_count(), 1);
    assert_eq!(mock.count(), 1);
    assert_eq!(crate::support::metric(&plan, "failures")?, 0);
    Ok(())
}

#[tokio::test]
async fn generic_json_fields_preserve_serde_private_marker_keys() -> Result<()> {
    use axum::{routing::post, Router};
    use datafusion::prelude::SessionContext;
    use jev_datafusion::{register_jev, JevProvider};
    use std::sync::{Arc, Mutex};
    let body = Arc::new(Mutex::new(String::new()));
    let recorded = body.clone();
    // Inspect outbound bytes: reparsing with serde_json::Value would itself
    // interpret marker-first maps when raw_value is enabled.
    let app = Router::new().route(
        "/v1/systemone",
        post(move |raw: String| {
            *recorded.lock().unwrap() = raw;
            async { r#"{"answers":{"q0":{"noul":0.9}}}"# }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("test-only".into()), &endpoint)?),
    )?;
    // The base state parser can read these objects before canonical sorting;
    // deserializing the parsed Values again would reinterpret their markers.
    sql(&context,r#"SELECT noul(named_struct('$serde_json::private::RawValue','42'), '{"z":true,"$serde_json::private::RawValue":"42"}', '{"true":{"z":true,"$serde_json::private::RawValue":"99"}}')"#).await?.collect().await?;
    server.abort();
    let body = body.lock().unwrap();
    for expected in [
        r#""state":{"$serde_json::private::RawValue":"42"}"#,
        r#""instructions":{"$serde_json::private::RawValue":"42","z":true}"#,
        r#""criteria":{"true":{"$serde_json::private::RawValue":"99","z":true}}"#,
    ] {
        assert!(body.contains(expected), "missing {expected}: {body}");
    }
    Ok(())
}

#[test]
fn generated_options_have_a_validated_public_builder() -> Result<()> {
    use jev_datafusion::JevConfig;
    use jev_datafusion::{JevOnError, JevSessionOptions};
    let options = JevSessionOptions {
        model: "custom-model".into(),
        on_error: JevOnError::Null,
    };
    assert_eq!(JevConfig::try_from_options(options.clone())?.0, options);
    assert_eq!(
        JevConfig::try_from_options(JevSessionOptions::default())?.0,
        JevSessionOptions::default()
    );
    for model in ["", " ", "\t\n"] {
        let error = JevConfig::try_from_options(JevSessionOptions {
            model: model.into(),
            ..JevSessionOptions::default()
        })
        .unwrap_err();
        assert!(error.to_string().contains("model cannot be empty"));
    }
    Ok(())
}
