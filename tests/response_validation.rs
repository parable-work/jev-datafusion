mod support;

use std::time::Duration;

use arrow::array::{Array, StringArray};
use axum::http::StatusCode;
use datafusion::common::Result;
use jev_datafusion::sql;
use serde_json::json;
use support::Mock;

// Exact answers are controlled fixtures from a local HTTP server, never live
// model expectations.
#[tokio::test]
async fn choice_and_score_probabilities_follow_fail_and_null_policy() -> Result<()> {
    for kind in ["choice", "score"] {
        for (field, invalid) in [
            ("confidence", -0.01),
            ("confidence", 1.01),
            ("probabilities", -0.01),
            ("probabilities", 1.01),
        ] {
            let mut answer = if kind == "choice" {
                json!({"choice":"a","confidence":0.5,"probabilities":{"a":0.5,"b":0.5}})
            } else {
                json!({"score":0.5,"confidence":0.5,"probabilities":{"0":0.5,"1":0.5}})
            };
            if field == "confidence" {
                answer[field] = json!(invalid);
            } else {
                answer[field][if kind == "choice" { "b" } else { "1" }] = json!(invalid);
            }
            let response = json!({"answers":{"q0":answer}}).to_string();
            let mock = Mock::with_response(
                1,
                32,
                move |_| (StatusCode::OK, response.clone()),
                Duration::ZERO,
            )
            .await?;
            let criteria = if kind == "choice" {
                r#"{"a":null,"b":null}"#
            } else {
                r#"["low","high"]"#
            };
            let query = format!("SELECT {kind}('state','Q?','{criteria}')");
            let result = sql(&mock.context, &query).await?.collect().await;
            assert!(result.is_err(), "{kind} accepted invalid {field}={invalid}");
            let error = result.unwrap_err().to_string();
            assert!(error.contains(field), "{error}");
            sql(&mock.context, "SET jev.on_error='null'")
                .await?
                .collect()
                .await?;
            let output = sql(&mock.context, &query).await?.collect().await?;
            assert!(output[0].column(0).is_null(0), "{kind} {field}");
            assert_eq!(mock.count(), 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn object_markers_cannot_masquerade_as_numeric_probabilities() -> Result<()> {
    for kind in ["choice", "score"] {
        for field in ["confidence", "probabilities"] {
            for marker in [
                "$serde_json::private::Number",
                "$serde_json::private::RawValue",
            ] {
                let bucket = if kind == "choice" { "a" } else { "0" };
                let probabilities = if kind == "choice" {
                    json!({"a":0.5,"b":0.5})
                } else {
                    json!({"0":0.5,"1":0.5})
                };
                let mut answer = json!({"choice":"a","score":0.5,"confidence":0.5,"probabilities":probabilities});
                let value = json!({marker:"0.5"});
                if field == "confidence" {
                    answer[field] = value;
                } else {
                    answer[field][bucket] = value;
                }
                let response = json!({"answers":{"q0":answer}}).to_string();
                let mock = Mock::with_response(
                    1,
                    32,
                    move |_| (StatusCode::OK, response.clone()),
                    Duration::ZERO,
                )
                .await?;
                let criteria = if kind == "choice" {
                    r#"{"a":null,"b":null}"#
                } else {
                    r#"["low","high"]"#
                };
                let query = format!("SELECT {kind}('state','Q?','{criteria}')");
                let error = sql(&mock.context, &query)
                    .await?
                    .collect()
                    .await
                    .expect_err(&format!("{kind} accepted object-valued {field}"))
                    .to_string();
                assert!(error.contains(field), "{kind}: {error}");
                sql(&mock.context, "SET jev.on_error='null'")
                    .await?
                    .collect()
                    .await?;
                let output = sql(&mock.context, &query).await?.collect().await?;
                assert!(output[0].column(0).is_null(0));
            }
        }
    }
    Ok(())
}

fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn invalid_questions() -> Vec<String> {
    vec![
        r#"{"q":{"type":"noul","instructions":"first"},"q":{"type":"noul","instructions":"second"}}"#.into(),
        r#"{"q":{"type":"noul","instructions":{"x":1,"\u0078":2}}}"#.into(),
        r#"{"q":{"type":"choice","instructions":"Q?","criteria":{"a":"first","a":"second"}}}"#.into(),
        format!(r#"{{"q":{{"type":"noul","instructions":{}null{}}}}}"#, "[".repeat(65), "]".repeat(65)),
        format!(r#"{{"q":{{"type":"noul","instructions":"{}"}}}}"#, "x".repeat(1_048_576)),
    ]
}

#[tokio::test]
async fn literal_questions_use_strict_json_validation_before_execution() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    for (index, questions) in invalid_questions().into_iter().enumerate() {
        let query = format!("SELECT ask('state', {})", quoted(&questions));
        let result = sql(&mock.context, &query).await;
        assert!(result.is_err(), "invalid question case {index} planned");
        let error = result.err().unwrap().to_string();
        assert!(error.contains("questions"), "{error}");
    }
    assert_eq!(mock.count(), 0);
    Ok(())
}

#[tokio::test]
async fn column_question_errors_null_only_invalid_rows_without_spending() -> Result<()> {
    let mock = Mock::new(1, 32).await?;
    let mut questions = vec![r#"{"q0":{"type":"noul","instructions":"Q?"}}"#.to_string()];
    questions.extend(invalid_questions());
    mock.table(vec![vec![questions
        .into_iter()
        .map(|question| (42, question))
        .collect()]])?;
    let query = "SELECT ask('state', body) FROM episodes";
    assert!(sql(&mock.context, query).await?.collect().await.is_err());
    assert_eq!(mock.count(), 1);
    sql(&mock.context, "SET jev.on_error='null'")
        .await?
        .collect()
        .await?;
    let plan = sql(&mock.context, query)
        .await?
        .create_physical_plan()
        .await?;
    let output = datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
    assert_eq!(
        output.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        6
    );
    assert_eq!(
        output
            .iter()
            .map(|batch| batch.column(0).null_count())
            .sum::<usize>(),
        5
    );
    assert_eq!(support::metric(&plan, "failures")?, 5);
    assert_eq!(mock.count(), 2, "each query sends only its valid row");
    Ok(())
}

#[tokio::test]
async fn typed_probability_failures_leave_raw_ask_cache_bytes_available() -> Result<()> {
    for kind in ["choice", "score"] {
        let probabilities = if kind == "choice" {
            json!({"a":0.2,"b":0.8})
        } else {
            json!({"0":0.5,"1":0.5})
        };
        let raw = format!(
            " \n{}\n",
            json!({"answers":{"q0":{"choice":"b","score":0.5,"confidence":1.5,"probabilities":probabilities}},"future":[true,1]})
        );
        let response = raw.clone();
        let mock = Mock::with_response(
            1,
            32,
            move |_| (StatusCode::OK, response.clone()),
            Duration::ZERO,
        )
        .await?;
        sql(&mock.context, "SET jev.on_error='null'")
            .await?
            .collect()
            .await?;
        let criteria = if kind == "choice" {
            json!({"a":null,"b":null})
        } else {
            json!(["low", "high"])
        };
        let questions = json!({"q0":{"type":kind,"instructions":"Q?","criteria":criteria}});
        let query = format!(
            "SELECT {kind}('same','Q?',{}) AS typed, ask('same',{}) AS raw",
            quoted(&criteria.to_string()),
            quoted(&questions.to_string())
        );
        let plan = sql(&mock.context, &query)
            .await?
            .create_physical_plan()
            .await?;
        let output =
            datafusion::physical_plan::collect(plan.clone(), mock.context.task_ctx()).await?;
        assert!(output[0].column(0).is_null(0));
        assert_eq!(
            output[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            raw
        );
        assert_eq!(mock.count(), 1);
        assert_eq!(support::metric(&plan, "failures")?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn ask_returns_malformed_and_future_response_bytes_unchanged() -> Result<()> {
    for raw in [
        "not JSON {\nraw response\0\n",
        "{\n\"answers\":{\"q0\":{\"type\":\"future\",\"confidence\":42,\"new\":true}}}\n",
    ] {
        let mock =
            Mock::with_response(1, 32, move |_| (StatusCode::OK, raw.into()), Duration::ZERO)
                .await?;
        for policy in ["fail", "null"] {
            sql(&mock.context, &format!("SET jev.on_error='{policy}'"))
                .await?
                .collect()
                .await?;
            let output = sql(
                &mock.context,
                r#"SELECT ask('state','{"q0":{"type":"noul","instructions":"Q?"}}')"#,
            )
            .await?
            .collect()
            .await?;
            assert_eq!(
                output[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .as_bytes(),
                raw.as_bytes()
            );
        }
        assert_eq!(mock.count(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn probability_endpoints_are_valid_and_score_is_not_a_probability() -> Result<()> {
    for kind in ["choice", "score"] {
        for confidence in [0.0, 1.0] {
            let probabilities = if kind == "choice" {
                json!({"a":0.0,"b":1.0})
            } else {
                json!({"0":0.0,"1":0.0,"2":0.75,"3":0.25})
            };
            let response = json!({"answers":{"q0":{"choice":"b","score":2.25,"confidence":confidence,"probabilities":probabilities}}}).to_string();
            let mock = Mock::with_response(
                1,
                32,
                move |_| (StatusCode::OK, response.clone()),
                Duration::ZERO,
            )
            .await?;
            let criteria = if kind == "choice" {
                r#"{"a":null,"b":null}"#
            } else {
                r#"["0","1","2","3"]"#
            };
            let output = sql(
                &mock.context,
                &format!("SELECT {kind}('state','Q?','{criteria}')"),
            )
            .await?
            .collect()
            .await?;
            assert!(!output[0].column(0).is_null(0));
        }
    }
    Ok(())
}

#[tokio::test]
async fn strict_questions_preserve_marker_objects_and_exact_numbers() -> Result<()> {
    use axum::{routing::post, Router};
    use datafusion::prelude::SessionContext;
    use jev_datafusion::JevRequest;
    use jev_datafusion::{register_jev, JevProvider};
    use std::sync::{Arc, Mutex};
    const QUESTIONS: &str = r#"{"q0":{"type":"choice","instructions":{"marker":{"$serde_json::private::Number":"1"},"amount":12345678901234567890.12345678901234567890123456789},"criteria":{"a":{"$serde_json::private::RawValue":"true"},"b":null}}}"#;
    let received = Arc::new(Mutex::new(Vec::<JevRequest>::new()));
    let captured = received.clone();
    let app = Router::new().route(
        "/v1/systemone",
        post(move |body: String| {
            captured
                .lock()
                .unwrap()
                .push(serde_json::from_str(&body).unwrap());
            async { "future response bytes\n" }
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
    let output = sql(
        &context,
        &format!("SELECT ask('state',{})", quoted(QUESTIONS)),
    )
    .await?
    .collect()
    .await?;
    assert_eq!(
        output[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "future response bytes\n"
    );
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    let question = &received[0].questions["q0"];
    assert_eq!(
        question.instructions["marker"],
        json!({"$serde_json::private::Number":"1"})
    );
    assert_eq!(
        question.instructions["amount"].to_string(),
        "12345678901234567890.12345678901234567890123456789"
    );
    assert_eq!(
        question.criteria.as_ref().unwrap()["a"],
        json!({"$serde_json::private::RawValue":"true"})
    );
    server.abort();
    Ok(())
}
