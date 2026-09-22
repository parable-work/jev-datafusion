//! One real request to each hosted server. Ignored unless CI or a person opts in.
use std::sync::Arc;

use arrow::array::{Array, Float64Array};
use datafusion::prelude::SessionContext;
use jev_datafusion::{register, sql, JevProvider};

const QUESTION: &str = "SELECT noul('I was charged twice. Please refund one charge.', instructions => 'Is the customer asking for money back?')";

fn required_key(name: &str) -> String {
    let value = std::env::var(name).unwrap_or_default();
    assert!(
        !value.trim().is_empty(),
        "{name} must be set for the live server test"
    );
    value
}

async fn assert_probability(server_name: &str, provider: JevProvider) {
    let context = SessionContext::new();
    register(&context, Arc::new(provider)).unwrap();
    let batches = sql(&context, QUESTION)
        .await
        .unwrap_or_else(|error| panic!("{server_name} query failed: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{server_name} execution failed: {error}"));
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{server_name} did not return a float"));
    assert!(!column.is_null(0), "{server_name} returned NULL");
    let probability = column.value(0);
    assert!(
        probability.is_finite() && (0.0..=1.0).contains(&probability),
        "{server_name} returned a probability outside 0..=1"
    );
}

#[tokio::test]
#[ignore = "calls OpenRouter; requires OPENROUTER_API_KEY"]
async fn live_openrouter_answers_one_noul() {
    let key = required_key("OPENROUTER_API_KEY");
    let provider =
        jev_datafusion::OpenRouter::new(Some(key), jev_datafusion::OpenRouter::DEFAULT_BASE_URL)
            .unwrap();
    assert_probability("OpenRouter", provider).await;
}

#[tokio::test]
#[ignore = "calls TypeSafe; requires TYPESAFE_API_KEY"]
async fn live_typesafe_answers_one_noul() {
    let key = required_key("TYPESAFE_API_KEY");
    let provider =
        jev_datafusion::TypeSafe::new(Some(key), jev_datafusion::TypeSafe::DEFAULT_BASE_URL)
            .unwrap();
    assert_probability("TypeSafe", provider).await;
}
