//! Run the four judgment queries against TypeSafe or OpenRouter.
//!
//! ```text
//! TYPESAFE_API_KEY=... cargo run --example live
//! OPENROUTER_API_KEY=... cargo run --example live
//! ```
//!
//! This is not part of CI. It spends a few real requests.
use std::sync::Arc;

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    util::pretty::print_batches,
};
use datafusion::prelude::SessionContext;
use jev_datafusion::{register, sql, OpenRouter, TypeSafe};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (server, server_name) = if std::env::var_os("OPENROUTER_API_KEY").is_some() {
        (OpenRouter::from_env()?, "OpenRouter")
    } else if std::env::var_os("TYPESAFE_API_KEY").is_some() {
        (TypeSafe::from_env()?, "TypeSafe")
    } else {
        eprintln!("Set TYPESAFE_API_KEY or OPENROUTER_API_KEY, then rerun this example.");
        std::process::exit(1);
    };

    let context = SessionContext::new();
    register(&context, Arc::new(server))?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            "I was charged twice for my annual plan this morning. Please refund one of the charges today.",
        ]))],
    )?;
    context.register_batch("tickets", batch)?;

    println!("server: {server_name}");
    for (name, query) in [
        (
            "noul",
            "SELECT noul(message, instructions => 'Is the customer asking for money back?') AS p_refund FROM tickets",
        ),
        (
            "choice",
            "SELECT choice(message, instructions => 'Which queue should handle this?', criteria => '{\"billing\":\"Charges, invoices, refunds\",\"technical\":\"Bugs and outages\",\"other\":\"Anything else\"}') AS queue FROM tickets",
        ),
        (
            "score",
            "SELECT score(message, instructions => 'How soon does this need a reply?', criteria => '[\"Can wait a week\",\"This week\",\"Today\"]') AS urgency FROM tickets",
        ),
        (
            "ask",
            "SELECT ask(message, questions => '{\"refund\":{\"type\":\"noul\",\"instructions\":\"Is the customer asking for money back?\"},\"queue\":{\"type\":\"choice\",\"instructions\":\"Which queue should handle this?\",\"criteria\":{\"billing\":\"Charges and refunds\",\"other\":\"Anything else\"}}}') AS raw FROM tickets",
        ),
    ] {
        println!("\n-- {name}");
        let batches = sql(&context, query).await?.collect().await?;
        print_batches(&batches)?;
    }
    Ok(())
}
