//! DataFusion functions for typed judgments.
//!
//! `noul`, `choice`, `score`, and `ask` speak the System One JSON contract.
//! TypeSafe is one server. OpenRouter is another. [`Compatible`] is any
//! endpoint that accepts the same request body, including models that are not Jev.

mod arrow_json;
mod contract;
mod execution;
mod joins;
mod json_value;
mod metrics;
mod planner;
mod provider;
mod reuse;
mod session;
mod sql;
mod state;
mod udf;

pub use contract::{
    JevArgumentName, JevChoiceResult, JevOnError, JevQuestion, JevQuestionType, JevRequest,
    JevScoreResult, JevSessionOptions,
};
pub use metrics::JevMetrics;
pub use provider::{Compatible, JevHttpError, JevProvider, OpenRouter, TypeSafe};
pub use session::JevConfig;
pub use sql::{normalize_jev_sql, parameter_names};
pub use udf::{register_jev, JevUdf};

pub use register_jev as register;

pub async fn sql(
    context: &datafusion::prelude::SessionContext,
    query: &str,
) -> datafusion::common::Result<datafusion::dataframe::DataFrame> {
    context.sql(normalize_jev_sql(query)?.as_ref()).await
}
