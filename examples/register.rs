//! Register the judgment functions against one server.
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use jev_datafusion::{register, Compatible, OpenRouter, TypeSafe};

fn main() -> datafusion::common::Result<()> {
    let context = SessionContext::new();
    // TypeSafe::from_env() reads TYPESAFE_API_KEY.
    // OpenRouter::from_env() reads OPENROUTER_API_KEY.
    // Compatible takes any System One URL and model id.
    let server = if std::env::var_os("OPENROUTER_API_KEY").is_some() {
        OpenRouter::from_env()?
    } else if std::env::var_os("TYPESAFE_API_KEY").is_some() {
        TypeSafe::from_env()?
    } else {
        Compatible::new(
            Some("local-test".into()),
            "http://127.0.0.1:9/v1/systemone",
            "example-model",
        )?
    };
    register(&context, Arc::new(server))?;
    println!("registered noul, choice, score, and ask");
    Ok(())
}
