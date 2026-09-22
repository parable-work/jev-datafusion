use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arrow::array::{Array, Float64Array};
use axum::{routing::post, Json, Router};
use datafusion::{common::Result, prelude::SessionContext};
use jev_datafusion::{register_jev, sql, JevProvider};
use serde_json::{json, Value};

// All fixed answers below are controlled HTTP fixtures, not expectations of a
// live nondeterministic model. Live proofs assert contracts and report drift.

#[tokio::test]
async fn equivalent_map_struct_and_json_text_have_identical_request_bytes() -> Result<()> {
    let bodies = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let recorded = bodies.clone();
    let app = Router::new().route("/v1/systemone", post(move |body: String| {
        let bodies = recorded.clone();
        async move {
            bodies.lock().unwrap().push(body);
            Json(json!({"model":"jev-1.13.0","answers":{"q0":{"type":"noul","noul":0.6}},"usage":{"input_tokens":30,"output_tokens":5}}))
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
    for state in [
        "MAP(['b','a'], ['B','A'])",
        "named_struct('a','A','b','B')",
        r#"'{"b":"B","a":"A"}'"#,
    ] {
        sql(
            &context,
            &format!("SELECT noul({state}, 'Does it contain A?')"),
        )
        .await?
        .collect()
        .await?;
    }
    let values = bodies.lock().unwrap();
    assert_eq!(
        values.len(),
        3,
        "distinct queries do not share response caches"
    );
    assert_eq!(values[0], values[1]);
    assert_eq!(values[1], values[2]);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn struct_state_reaches_provider_as_an_object() -> Result<()> {
    let app = Router::new().route("/v1/systemone", post(|Json(body): Json<Value>| async move {
        assert_eq!(body["state"], json!({"team":"Finance","minutes":95}));
        Json(json!({"model":"jev-1.13.0","answers":{"q0":{"type":"noul","noul":0.7}},"usage":{"input_tokens":30,"output_tokens":5}}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("test-only".into()), &endpoint)?),
    )?;
    let batches = sql(
        &context,
        "SELECT noul(named_struct('team', 'Finance', 'minutes', 95), 'Is time recorded?')",
    )
    .await?
    .collect()
    .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.7
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn invalid_literal_criteria_fails_during_planning() -> Result<()> {
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(
            Some("test-only".into()),
            "http://127.0.0.1:1",
        )?),
    )?;
    for query in [
        "SELECT choice('work', 'classify', '{}')",
        "SELECT choice('work', 'classify', '[]')",
        "SELECT choice('work', 'classify', '{bad')",
        "SELECT score('work', 'rate', '[\"one\"]')",
        "SELECT score('work', 'rate', '{}')",
        "SELECT noul('work', 'recurring?', '[]')",
    ] {
        let result = sql(&context, query).await;
        assert!(result.is_err(), "broken criteria planned: {query}");
        assert!(result.err().unwrap().to_string().contains("criteria"));
    }
    Ok(())
}

#[tokio::test]
async fn ask_keeps_original_bytes_and_future_answer_fields() -> Result<()> {
    const RAW: &str = "{\n  \"model\": \"jev-1.13.0\", \"answers\": {\"a\": {\"type\":\"future_kind\", \"new_field\":true}}, \"usage\":{\"input_tokens\":12,\"output_tokens\":3}, \"future_top_level\": [2,1]\n}\n";
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let app = Router::new().route(
        "/v1/systemone",
        post(move |Json(body): Json<Value>| {
            let count = counted.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                assert_eq!(body["questions"].as_object().unwrap().len(), 2);
                RAW
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("test-only".into()), &endpoint)?),
    )?;
    let result = sql(&context, r#"SELECT ask('Weekly reconciliation', questions => '{"a":{"type":"noul","instructions":"Recurring?"},"b":{"type":"choice","instructions":"Which?","criteria":{"ops":null,"other":null}}}') AS response"#)
        .await?.collect().await?;
    let response = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(response.value(0).as_bytes(), RAW.as_bytes());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn score_preserves_fractional_position_and_distribution() -> Result<()> {
    let app = Router::new().route("/v1/systemone", post(|Json(body): Json<Value>| async move {
        assert_eq!(body["questions"]["q0"]["type"], "score");
        assert_eq!(body["questions"]["q0"]["criteria"], json!(["manual", "assisted", "automated"]));
        Json(json!({"model":"jev-1.13.0","answers":{"q0":{"type":"score","score":1.6,"confidence":0.73,"probabilities":{"0":0.1,"1":0.2,"2":0.7},"legend":{"0":"manual","1":"assisted","2":"automated"}}},"usage":{"input_tokens":50,"output_tokens":8}}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("test-only".into()), &endpoint)?),
    )?;
    let result = sql(&context, r#"SELECT score('Weekly reconciliation', 'Rate feasibility', '["manual","assisted","automated"]') AS judgment"#)
        .await?.collect().await?;
    let value = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    assert_eq!(
        value
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["score", "confidence", "probabilities"]
    );
    assert_eq!(
        value
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.6
    );
    assert_eq!(
        value
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap()
            .value(0)
            .len(),
        3
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn choice_returns_schema_owned_struct_and_full_distribution() -> Result<()> {
    let app = Router::new().route("/v1/systemone", post(|Json(body): Json<Value>| async move {
        assert_eq!(body["questions"]["q0"]["type"], "choice");
        Json(json!({"model":"jev-1.13.0","answers":{"q0":{"type":"choice","choice":"reporting","confidence":0.84,"probabilities":{"reporting":0.9,"other":0.1}}},"usage":{"input_tokens":50,"output_tokens":8}}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let context = SessionContext::new();
    register_jev(
        &context,
        Arc::new(JevProvider::new(Some("test-only".into()), &endpoint)?),
    )?;
    let result = sql(&context, r#"SELECT choice('Weekly account update', criteria => '{"reporting":"Recurring status","other":null}', instructions => 'Which work pattern?') AS judgment"#)
        .await?.collect().await?;
    let value = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    assert_eq!(
        value
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["label", "confidence", "probabilities"]
    );
    assert_eq!(
        value
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "reporting"
    );
    assert_eq!(
        value
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.84
    );
    let distribution = value
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    assert_eq!(distribution.value(0).len(), 2);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn table_scan_returns_noul_and_null_without_request() -> Result<()> {
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let app = Router::new().route("/v1/systemone", post(move |Json(body): Json<Value>| {
        let count = counted.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(body["state"], "Every Monday I reconcile invoices.");
            assert_eq!(body["model"], "jev-1.13.0");
            assert_eq!(body["questions"]["q0"]["type"], "noul");
            Json(json!({"model":"jev-1.13.0","answers":{"q0":{"type":"noul","noul":0.98}},"usage":{"input_tokens":40,"output_tokens":5}}))
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
    let result = sql(&context, "SELECT id, noul(body, instructions => 'Does this describe recurring work?') AS recurring FROM (VALUES (1, 'Every Monday I reconcile invoices.'), (2, NULL)) AS episodes(id, body) ORDER BY id")
        .await?.collect().await?;
    let scores = result[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(scores.iter().collect::<Vec<_>>(), vec![Some(0.98), None]);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    server.abort();
    Ok(())
}
