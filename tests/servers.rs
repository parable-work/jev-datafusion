//! The same SQL runs against more than one server. TypeSafe is one of them.
use std::sync::Arc;

use arrow::array::Array;
use axum::{routing::post, Json, Router};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use jev_datafusion::{sql, Compatible, JevMetrics, JevRequest, OpenRouter, TypeSafe};
use serde_json::{json, Value};

async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (endpoint, server)
}

fn noul_answer(model: &str) -> Value {
    json!({
        "model": model,
        "answers": {"q0": {"type": "noul", "noul": 0.8}},
        "usage": {"input_tokens": 10, "output_tokens": 2}
    })
}

#[tokio::test]
async fn typesafe_and_openrouter_accept_the_same_sql() {
    let typesafe_seen = Arc::new(std::sync::Mutex::new(Value::Null));
    let seen = typesafe_seen.clone();
    let typesafe_app = Router::new().route(
        "/v1/systemone",
        post(move |Json(body): Json<Value>| {
            let seen = seen.clone();
            async move {
                *seen.lock().unwrap() = body.clone();
                Json(noul_answer("jev-1.13.0"))
            }
        }),
    );
    let (typesafe_url, typesafe_server) = serve(typesafe_app).await;
    let context = datafusion::prelude::SessionContext::new();
    jev_datafusion::register(
        &context,
        Arc::new(TypeSafe::new(Some("test-only".into()), &typesafe_url).unwrap()),
    )
    .unwrap();
    let batches = sql(
        &context,
        "SELECT noul('refund the charge', 'Is a refund requested?')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let probability = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(probability, 0.8);
    assert_eq!(typesafe_seen.lock().unwrap()["model"], "jev-1.13.0");
    typesafe_server.abort();

    let openrouter_seen = Arc::new(std::sync::Mutex::new(Value::Null));
    let seen = openrouter_seen.clone();
    let openrouter_app = Router::new().route(
        "/api/alpha/decisions",
        post(move |Json(body): Json<Value>| {
            let seen = seen.clone();
            async move {
                *seen.lock().unwrap() = body;
                Json(json!({
                    "model": "typesafe/jev-1.13-20260917",
                    "answers": {"q0": {"type": "noul", "noul": 0.4}},
                    "usage": {"input_tokens": 476, "output_tokens": 12, "cost": 0.000019992}
                }))
            }
        }),
    );
    let (openrouter_url, openrouter_server) = serve(openrouter_app).await;
    let context = datafusion::prelude::SessionContext::new();
    jev_datafusion::register(
        &context,
        Arc::new(OpenRouter::new(Some("test-only".into()), &openrouter_url).unwrap()),
    )
    .unwrap();
    let batches = sql(
        &context,
        "SELECT noul(state => 'login times out', instructions => 'Is the customer angry?')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .value(0),
        0.4
    );
    assert_eq!(
        openrouter_seen.lock().unwrap()["model"],
        "typesafe/jev-1.13"
    );
    openrouter_server.abort();
}

#[tokio::test]
async fn openrouter_prices_reported_cost_and_names_its_own_key() {
    let app = Router::new().route(
        "/api/alpha/decisions",
        post(|| async {
            r#"{"model":"typesafe/jev-1.13","answers":{},"usage":{"input_tokens":476,"output_tokens":1,"cost":0.000019992}}"#
        }),
    );
    let (endpoint, server) = serve(app).await;
    let provider = OpenRouter::new(Some("test-only".into()), &endpoint).unwrap();
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let request = serde_json::from_value::<JevRequest>(json!({
        "state": "synthetic",
        "model": "typesafe/jev-1.13",
        "questions": {"q0": {"type": "noul", "instructions": "Is it recurring?"}}
    }))
    .unwrap();
    provider.send(&request, "ask", &metrics).await.unwrap();
    assert_eq!(metrics.estimated_cost_nano_usd.value(), 19992);
    assert_eq!(metrics.unpriced_requests.value(), 0);
    server.abort();

    let context = datafusion::prelude::SessionContext::new();
    jev_datafusion::register(
        &context,
        Arc::new(OpenRouter::new(None, "http://127.0.0.1:1").unwrap()),
    )
    .unwrap();
    let error = sql(&context, "SELECT noul('state', 'Q?')")
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("OPENROUTER_API_KEY"), "{error}");
    assert!(!error.contains("TYPESAFE_API_KEY"), "{error}");
}

#[tokio::test]
async fn compatible_server_keeps_the_callers_model_id() {
    let seen = Arc::new(std::sync::Mutex::new(Value::Null));
    let captured = seen.clone();
    let app = Router::new().route(
        "/typesafe/v1/systemone",
        post(move |Json(body): Json<Value>| {
            let captured = captured.clone();
            async move {
                *captured.lock().unwrap() = body;
                Json(noul_answer("typesafe-ai/jev"))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let context = datafusion::prelude::SessionContext::new();
    jev_datafusion::register(
        &context,
        Arc::new(
            Compatible::new(
                Some("gateway".into()),
                &format!("{origin}/typesafe/v1/systemone"),
                "typesafe-ai/jev",
            )
            .unwrap(),
        ),
    )
    .unwrap();
    sql(&context, "SELECT noul('state', 'Q?')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(seen.lock().unwrap()["model"], "typesafe-ai/jev");
    server.abort();
}
