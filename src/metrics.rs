use std::sync::Arc;

use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

#[derive(Debug, Clone)]
pub struct JevMetrics {
    pub requests: Count,
    pub retries: Count,
    pub cache_hits: Count,
    pub failures: Count,
    pub input_tokens: Count,
    pub output_tokens: Count,
    pub estimated_cost_nano_usd: Count,
    pub unpriced_requests: Count,
}

impl JevMetrics {
    pub fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let counter = |name| MetricBuilder::new(set).counter(name, partition);
        Self {
            requests: counter("requests"),
            retries: counter("retries"),
            cache_hits: counter("cache_hits"),
            failures: counter("failures"),
            input_tokens: counter("input_tokens"),
            output_tokens: counter("output_tokens"),
            estimated_cost_nano_usd: counter("estimated_cost_nano_usd"),
            unpriced_requests: counter("unpriced_requests"),
        }
    }
}

tokio::task_local! {
    pub(crate) static INVOCATION_METRICS: Arc<JevMetrics>;
}

pub(crate) fn current_metrics() -> Arc<JevMetrics> {
    INVOCATION_METRICS
        .try_with(Arc::clone)
        .unwrap_or_else(|_| Arc::new(JevMetrics::new(&ExecutionPlanMetricsSet::new(), 0)))
}
