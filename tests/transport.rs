use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{http::StatusCode, response::IntoResponse, routing::post, Router};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use jev_datafusion::JevRequest;
use jev_datafusion::{JevMetrics, JevProvider};
use serde_json::json;

#[tokio::test]
async fn response_body_without_content_length_is_preserved() {
    use axum::{body::Body, http::Response};
    use futures::stream;
    use std::convert::Infallible;

    let raw = "{\"answers\":{\"q0\":{\"new\":true}},\"future\":\"é\"}\n";
    let chunks = raw
        .as_bytes()
        .chunks(3)
        .map(|chunk| Ok::<_, Infallible>(chunk.to_vec()))
        .collect::<Vec<_>>();
    let app = Router::new().route(
        "/v1/systemone",
        post(move || {
            let chunks = chunks.clone();
            async move {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from_stream(stream::iter(chunks)))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    assert_eq!(
        provider.send(&request(), "ask", &metrics).await.unwrap(),
        raw
    );
    server.abort();
}

#[tokio::test]
async fn oversized_response_body_is_rejected_without_content_length() {
    use axum::body::Body;
    use futures::stream;
    use std::convert::Infallible;

    let chunks = vec![
        Ok::<_, Infallible>(vec![b'x'; 1_048_576]),
        Ok::<_, Infallible>(vec![b'y']),
    ];
    let app = Router::new().route(
        "/v1/systemone",
        post(move || {
            let chunks = chunks.clone();
            async move { (StatusCode::OK, Body::from_stream(stream::iter(chunks))) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let error = provider
        .send(&request(), "ask", &metrics)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("1048576-byte JSON limit"));
    server.abort();
}

fn request() -> JevRequest {
    serde_json::from_value(
        json!({"state":"synthetic recurring work","model":"jev-1.13.0",
        "questions":{"q0":{"type":"noul","instructions":"Is recurrence evidenced?"}}}),
    )
    .unwrap()
}

#[tokio::test]
async fn missing_usage_or_unknown_model_is_explicitly_unpriced() {
    for raw in [
        r#"{"model":"future-model","usage":{"input_tokens":100,"output_tokens":2}}"#,
        r#"{"model":"jev-1.13.0","answers":{}}"#,
        "future opaque body",
    ] {
        let app = Router::new().route("/v1/systemone", post(move || async move { raw }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
        let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        assert_eq!(
            provider.send(&request(), "ask", &metrics).await.unwrap(),
            raw
        );
        assert_eq!(metrics.unpriced_requests.value(), 1);
        assert_eq!(metrics.estimated_cost_nano_usd.value(), 0);
        server.abort();
    }
}

#[tokio::test]
async fn retry_after_beyond_deadline_fails_without_overflow_or_early_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let app = Router::new().route(
        "/v1/systemone",
        post(move || {
            let calls = counted.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "18446744073709551615")],
                    "busy",
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        provider.send(&request(), "ask", &metrics),
    )
    .await
    .expect("an impossible retry deadline must fail promptly");
    assert!(result.unwrap_err().to_string().contains("deadline"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.retries.value(), 0);
    server.abort();
}

#[tokio::test]
async fn overload_honors_retry_after_and_exhaustion_is_bounded() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let app = Router::new().route(
        "/v1/systemone",
        post(move || {
            let calls = counted.clone();
            async move {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                (
                    StatusCode::from_u16(529).unwrap(),
                    [("retry-after", if attempt == 0 { "1" } else { "0" })],
                    "overloaded",
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let start = std::time::Instant::now();
    let error = provider
        .send(&request(), "ask", &metrics)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("529"));
    assert!(start.elapsed() >= std::time::Duration::from_secs(1));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(metrics.retries.value(), 3);
    server.abort();
}

#[tokio::test]
async fn dropping_request_closes_inflight_socket_within_one_second() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for response_started in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            while !request.windows(4).any(|value| value == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
            }
            if response_started {
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n{")
                    .await
                    .unwrap();
            }
            started_tx.send(()).unwrap();
            loop {
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => {
                        let _ = closed_tx.send(());
                        break;
                    }
                    Ok(_) => {}
                }
            }
        });
        // Keep the provider/client alive after cancellation, as a real session does.
        let provider = Arc::new(JevProvider::new(Some("test-only".into()), &endpoint).unwrap());
        let running = provider.clone();
        let request_task = tokio::spawn(async move {
            let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
            running.send(&request(), "ask", &metrics).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), started_rx)
            .await
            .unwrap()
            .unwrap();
        request_task.abort();
        assert!(request_task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(1), closed_rx)
            .await
            .expect("server must observe socket cancellation within one second")
            .unwrap();
        server.await.unwrap();
        drop(provider);
    }
}

#[tokio::test]
async fn authentication_and_validation_fail_without_retry() {
    for status in [401, 422] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let app = Router::new().route(
            "/v1/systemone",
            post(move || {
                let calls = counted.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    (
                        StatusCode::from_u16(status).unwrap(),
                        "private provider detail must not be echoed",
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
        let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let error = provider
            .send(&request(), "noul", &metrics)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(&status.to_string()));
        assert!(!error.to_string().contains("private provider detail"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.retries.value(), 0);
        server.abort();
    }
}

#[tokio::test]
async fn usage_and_cost_are_metered_without_reserializing_the_body() {
    const RAW: &str = "{ \"model\":\"jev-1.13.0\", \"answers\":{}, \"usage\":{\"input_tokens\":100,\"output_tokens\":25}, \"new\":true }\n";
    let app = Router::new().route("/v1/systemone", post(|| async { RAW }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let raw = provider.send(&request(), "ask", &metrics).await.unwrap();
    assert_eq!(raw.as_bytes(), RAW.as_bytes());
    assert_eq!(metrics.input_tokens.value(), 100);
    assert_eq!(metrics.output_tokens.value(), 25);
    assert_eq!(metrics.estimated_cost_nano_usd.value(), 4200);
    server.abort();
}

#[tokio::test]
async fn process_global_limit_is_shared_across_independent_clients() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let observed_active = active.clone();
    let observed_peak = peak.clone();
    let app = Router::new().route(
        "/v1/systemone",
        post(move || {
            let active = observed_active.clone();
            let peak = observed_peak.clone();
            async move {
                let in_flight = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(in_flight, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                "{}"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let providers = [
        JevProvider::new(Some("test-only".into()), &endpoint).unwrap(),
        JevProvider::new(Some("test-only".into()), &endpoint).unwrap(),
    ];
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let request = request();
    let results = futures::future::join_all(
        (0..24).map(|index| providers[index % 2].send(&request, "ask", &metrics)),
    )
    .await;
    assert!(results.iter().all(Result::is_ok));
    assert!(
        peak.load(Ordering::SeqCst) <= 8,
        "cap applies to all clients together"
    );
    assert!(
        peak.load(Ordering::SeqCst) > 1,
        "requests are still concurrent"
    );
    assert_eq!(metrics.requests.value(), 24);
    server.abort();
}

#[tokio::test]
async fn retries_rate_limit_without_changing_raw_body() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let app = Router::new().route("/v1/systemone", post(move || {
        let calls = counted.clone();
        async move {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "0")], "busy").into_response()
            } else {
                (StatusCode::OK, "{ \"model\":\"jev-1.13.0\", \"answers\":{}, \"usage\":{\"input_tokens\":10,\"output_tokens\":2} }\n").into_response()
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = JevProvider::new(Some("test-only".into()), &endpoint).unwrap();
    let metrics = JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let response = provider.send(&request(), "ask", &metrics).await.unwrap();
    assert!(response.starts_with("{ ") && response.ends_with(" }\n"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(metrics.requests.value(), 2);
    assert_eq!(metrics.retries.value(), 1);
    server.abort();
}
