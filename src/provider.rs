use std::{
    fmt,
    time::{Duration, SystemTime},
};

use datafusion::common::{DataFusionError, Result};
use futures::StreamExt;

use crate::{
    contract::{JevRequest, OPENROUTER_DEFAULT_MODEL, TYPESAFE_DEFAULT_MODEL},
    metrics::JevMetrics,
    state::{canonical_bytes, MAX_BYTES},
};

pub const PROCESS_MAX_CONCURRENCY: usize = 8;
const MAX_ATTEMPTS: usize = 4;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
static HTTP_PERMITS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(PROCESS_MAX_CONCURRENCY);

const TYPESAFE_KEY: &str = "TYPESAFE_API_KEY";
const OPENROUTER_KEY: &str = "OPENROUTER_API_KEY";
const COMPATIBLE_KEY: &str = "JEV_API_KEY";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerKind {
    TypeSafe,
    OpenRouter,
    Compatible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pricing {
    /// TypeSafe publishes $0.042 / 1M input tokens for this exact model id.
    /// Integer nanodollars retain sub-cent costs without rounding them to zero.
    ModelRate {
        model: &'static str,
        nano_per_token: u64,
    },
    /// Prefer `usage.cost` in dollars when the server reports it.
    ReportedCost,
}

pub struct JevProvider {
    pub(crate) api_key: Option<String>,
    pub(crate) endpoint: String,
    pub(crate) default_model: String,
    pub(crate) key_name: &'static str,
    kind: ServerKind,
    pricing: Pricing,
    pub(crate) client: reqwest::Client,
}

impl fmt::Debug for JevProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JevProvider")
            .field("server", &self.kind)
            .field("configured", &self.api_key.is_some())
            .field("default_model", &self.default_model)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct JevHttpError {
    pub status: u16,
    pub function: String,
    pub fatal: bool,
}

impl fmt::Display for JevHttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Jev HTTP {} for {}", self.status, self.function)
    }
}
impl std::error::Error for JevHttpError {}

/// TypeSafe's System One API. One server, not the only one.
pub struct TypeSafe;

impl TypeSafe {
    pub const DEFAULT_BASE_URL: &'static str = "https://api.typesafe.ai";
    pub const DEFAULT_MODEL: &'static str = TYPESAFE_DEFAULT_MODEL;

    pub fn from_env() -> Result<JevProvider> {
        Self::new(env_key(TYPESAFE_KEY), Self::DEFAULT_BASE_URL)
    }

    pub fn new(api_key: Option<String>, base_url: &str) -> Result<JevProvider> {
        JevProvider::build(
            api_key,
            &join_url(base_url, "/v1/systemone"),
            Self::DEFAULT_MODEL,
            TYPESAFE_KEY,
            ServerKind::TypeSafe,
            Pricing::ModelRate {
                model: Self::DEFAULT_MODEL,
                nano_per_token: 42,
            },
        )
    }
}

/// OpenRouter's Decisions API. Same question types, different path and model id.
pub struct OpenRouter;

impl OpenRouter {
    pub const DEFAULT_BASE_URL: &'static str = "https://openrouter.ai";
    pub const DEFAULT_MODEL: &'static str = OPENROUTER_DEFAULT_MODEL;

    pub fn from_env() -> Result<JevProvider> {
        Self::new(env_key(OPENROUTER_KEY), Self::DEFAULT_BASE_URL)
    }

    pub fn new(api_key: Option<String>, base_url: &str) -> Result<JevProvider> {
        JevProvider::build(
            api_key,
            &join_url(base_url, "/api/alpha/decisions"),
            Self::DEFAULT_MODEL,
            OPENROUTER_KEY,
            ServerKind::OpenRouter,
            Pricing::ReportedCost,
        )
    }
}

/// Any HTTP endpoint that accepts the System One JSON body.
///
/// Use this for Vercel's TypeSafe-compatible base URL and for models that are
/// not Jev. The caller supplies the full request URL and the default model id.
pub struct Compatible;

impl Compatible {
    pub fn new(
        api_key: Option<String>,
        endpoint: &str,
        default_model: impl Into<String>,
    ) -> Result<JevProvider> {
        JevProvider::build(
            api_key,
            endpoint,
            &default_model.into(),
            COMPATIBLE_KEY,
            ServerKind::Compatible,
            Pricing::ReportedCost,
        )
    }
}

fn env_key(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn join_url(base_url: &str, path: &str) -> String {
    format!("{}{path}", base_url.trim_end_matches('/'))
}

impl JevProvider {
    /// TypeSafe client. `base_url` is the origin; requests go to `{base_url}/v1/systemone`.
    pub fn new(api_key: Option<String>, base_url: &str) -> Result<Self> {
        TypeSafe::new(api_key, base_url)
    }

    fn build(
        api_key: Option<String>,
        endpoint: &str,
        default_model: &str,
        key_name: &'static str,
        kind: ServerKind,
        pricing: Pricing,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|_| {
                DataFusionError::Execution("could not initialize Jev HTTP client".into())
            })?;
        Ok(Self {
            api_key: api_key.filter(|key| !key.trim().is_empty()),
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            default_model: default_model.to_owned(),
            key_name,
            kind,
            pricing,
            client,
        })
    }

    fn retries(&self, status: u16) -> bool {
        match self.kind {
            ServerKind::OpenRouter => status == 429,
            ServerKind::TypeSafe | ServerKind::Compatible => matches!(status, 429 | 529),
        }
    }

    fn fatal_status(&self, status: u16) -> bool {
        match self.kind {
            ServerKind::OpenRouter => matches!(status, 401 | 402 | 422),
            ServerKind::TypeSafe | ServerKind::Compatible => matches!(status, 401 | 422),
        }
    }

    pub async fn send(
        &self,
        request: &JevRequest,
        function: &str,
        metrics: &JevMetrics,
    ) -> Result<String> {
        tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.send_attempts(request, function, metrics),
        )
        .await
        .map_err(|_| {
            DataFusionError::Execution(format!(
                "Jev {function} exceeded its 60 second request deadline"
            ))
        })?
    }

    async fn send_attempts(
        &self,
        request: &JevRequest,
        function: &str,
        metrics: &JevMetrics,
    ) -> Result<String> {
        let key = self.api_key.as_ref().ok_or_else(|| {
            DataFusionError::Execution(format!("{function} requires {}", self.key_name))
        })?;
        let value = serde_json::to_value(request)
            .map_err(|_| DataFusionError::Execution("invalid Jev request".into()))?;
        let body = canonical_bytes(&value)?;
        for attempt in 0..MAX_ATTEMPTS {
            let permit = HTTP_PERMITS
                .acquire()
                .await
                .map_err(|_| DataFusionError::Execution("Jev HTTP admission is closed".into()))?;
            if attempt > 0 {
                metrics.retries.add(1);
            }
            metrics.requests.add(1);
            let response = self
                .client
                .post(&self.endpoint)
                .bearer_auth(key)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await
                .map_err(|_| {
                    DataFusionError::Execution("Jev request failed or timed out".into())
                })?;
            let status = response.status().as_u16();
            if self.retries(status) && attempt + 1 < MAX_ATTEMPTS {
                let after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(retry_after);
                let base_ms = 100_u64 << attempt;
                let backoff = Duration::from_millis(base_ms + fastrand::u64(0..=base_ms));
                let delay = after.map_or(backoff, |after| after.max(backoff));
                drop(response);
                drop(permit);
                if delay >= REQUEST_TIMEOUT {
                    return datafusion::common::exec_err!("Jev {function} HTTP {status} Retry-After exceeds its 60 second request deadline");
                }
                tokio::time::sleep(delay).await;
                continue;
            }
            if !response.status().is_success() {
                return Err(DataFusionError::External(Box::new(JevHttpError {
                    status,
                    function: function.into(),
                    fatal: self.fatal_status(status),
                })));
            }
            let mut body = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| {
                    DataFusionError::Execution("could not read Jev response".into())
                })?;
                if chunk.len() > MAX_BYTES.saturating_sub(body.len()) {
                    return Err(DataFusionError::Execution(format!(
                        "Jev response exceeds the local {MAX_BYTES}-byte JSON limit"
                    )));
                }
                body.extend_from_slice(&chunk);
            }
            drop(permit);
            let raw = String::from_utf8(body).map_err(|_| {
                DataFusionError::Execution("Jev response is not valid UTF-8".into())
            })?;
            record_usage(&raw, metrics, self.pricing);
            return Ok(raw);
        }
        datafusion::common::exec_err!("Jev retry budget exhausted")
    }
}

fn record_usage(raw: &str, metrics: &JevMetrics, pricing: Pricing) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        metrics.unpriced_requests.add(1);
        return;
    };
    let input = value["usage"]["input_tokens"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok());
    let output = value["usage"]["output_tokens"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    metrics.input_tokens.add(input.unwrap_or(0));
    metrics.output_tokens.add(output);
    match pricing {
        Pricing::ModelRate {
            model,
            nano_per_token,
        } => match (
            value["model"].as_str(),
            input.and_then(|tokens| tokens.checked_mul(nano_per_token as usize)),
        ) {
            (Some(seen), Some(cost)) if seen == model => {
                metrics.estimated_cost_nano_usd.add(cost);
            }
            _ => metrics.unpriced_requests.add(1),
        },
        Pricing::ReportedCost => match reported_cost_nano(&value) {
            Some(cost) => metrics.estimated_cost_nano_usd.add(cost),
            None => metrics.unpriced_requests.add(1),
        },
    }
}

fn reported_cost_nano(value: &serde_json::Value) -> Option<usize> {
    let cost = value["usage"]["cost"].as_f64()?;
    if !cost.is_finite() || cost < 0.0 {
        return None;
    }
    let nano = (cost * 1_000_000_000.0).round();
    if !nano.is_finite() || nano < 0.0 {
        return None;
    }
    usize::try_from(nano as u64).ok()
}

fn retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_supports_seconds_dates_and_malformed_headers() {
        assert_eq!(retry_after("3"), Some(Duration::from_secs(3)));
        assert_eq!(retry_after("invalid"), None);
        assert_eq!(retry_after("-1"), None);
        assert_eq!(
            retry_after("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(Duration::ZERO)
        );
        let future = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(3));
        let wait = retry_after(&future).unwrap();
        assert!(wait > Duration::from_secs(1) && wait <= Duration::from_secs(3));
    }

    #[test]
    fn servers_choose_their_own_endpoint_and_model() {
        let typesafe = TypeSafe::new(None, "https://api.typesafe.ai/").unwrap();
        assert_eq!(typesafe.endpoint, "https://api.typesafe.ai/v1/systemone");
        assert_eq!(typesafe.default_model, "jev-1.13.0");
        assert_eq!(typesafe.key_name, "TYPESAFE_API_KEY");

        let openrouter = OpenRouter::new(None, "https://openrouter.ai").unwrap();
        assert_eq!(
            openrouter.endpoint,
            "https://openrouter.ai/api/alpha/decisions"
        );
        assert_eq!(openrouter.default_model, "typesafe/jev-1.13");
        assert_eq!(openrouter.key_name, "OPENROUTER_API_KEY");

        let compatible = Compatible::new(
            None,
            "https://ai-gateway.vercel.sh/typesafe/v1/systemone",
            "typesafe-ai/jev",
        )
        .unwrap();
        assert_eq!(
            compatible.endpoint,
            "https://ai-gateway.vercel.sh/typesafe/v1/systemone"
        );
        assert_eq!(compatible.default_model, "typesafe-ai/jev");
    }
}
