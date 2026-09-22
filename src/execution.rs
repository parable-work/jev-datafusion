//! Paid evaluation is owned by the stream that consumes it. There are no
//! detached request tasks or caches on a UDF, provider, session, or plan.
use std::{
    any::Any,
    fmt,
    sync::{Arc, Mutex},
};

use arrow::{
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{config::ConfigOptions, DataFusionError, HashMap, Result, ScalarValue},
    execution::{
        memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool},
        TaskContext,
    },
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        async_func::AsyncFuncExec,
        coalesce_partitions::CoalescePartitionsExec,
        filter::{FilterExec, FilterExecBuilder},
        limit::{GlobalLimitExec, LocalLimitExec},
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput},
        projection::ProjectionExec,
        repartition::RepartitionExec,
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
    },
};
use futures::{stream, StreamExt, TryStreamExt};

use crate::metrics::{JevMetrics, INVOCATION_METRICS};

#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;

type RequestKey = [u8; 32];
type CachedRaw = Option<Arc<str>>;

/// Fixed-width cryptographic fingerprint of the complete canonical request.
/// Use the already linked DataFusion SHA-256 implementation, with no new
/// provider dependency or second canonicalization implementation.
pub(crate) fn fingerprint(bytes: Vec<u8>, config: Arc<ConfigOptions>) -> Result<RequestKey> {
    let field = Arc::new(Field::new("canonical_request", DataType::Binary, false));
    let result = datafusion::functions::crypto::sha256().invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes)))],
        arg_fields: vec![field.clone()],
        number_rows: 1,
        return_field: field,
        config_options: config,
    })?;
    match result {
        ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => bytes.try_into().map_err(|_| {
            DataFusionError::Internal("invalid Jev SHA-256 fingerprint length".into())
        }),
        _ => datafusion::common::internal_err!("invalid Jev SHA-256 fingerprint type"),
    }
}

/// Exact one-batch deduplication: no eviction and no capacity bypass.
/// Retained input buffers, the hash table's actual allocation, and raw replies
/// are charged to the query's DataFusion memory pool. If retention cannot fit,
/// abort with ResourcesExhausted rather than paying for a duplicate later.
#[derive(Debug)]
pub(crate) struct BatchCache {
    values: HashMap<RequestKey, CachedRaw>,
    reservation: MemoryReservation,
}

impl BatchCache {
    pub fn new(pool: &Arc<dyn MemoryPool>, input_bytes: usize, partition: usize) -> Result<Self> {
        let reservation =
            MemoryConsumer::new(format!("JevExec[{partition}] batch cache")).register(pool);
        reservation.try_grow(input_bytes)?;
        Ok(Self {
            values: HashMap::default(),
            reservation,
        })
    }

    pub fn get(&self, key: &RequestKey) -> Option<CachedRaw> {
        self.values.get(key).cloned()
    }

    /// Reserve index space before sending a new paid request. Hashbrown grows
    /// a full table by doubling; reserve the full new allocation as well as
    /// the old allocation until rehashing finishes, then reconcile actual bytes.
    pub fn prepare(&mut self) -> Result<()> {
        if self.values.len() < self.values.capacity() {
            return Ok(());
        }
        let old = self.values.allocation_size();
        let additional = if old == 0 {
            4 * std::mem::size_of::<(RequestKey, CachedRaw)>() + 64
        } else {
            old.checked_mul(2).ok_or_else(|| {
                DataFusionError::ResourcesExhausted("Jev cache index size overflow".into())
            })?
        };
        self.reservation.try_grow(additional)?;
        if self.values.try_reserve(1).is_err() {
            self.reservation.shrink(additional);
            return datafusion::common::resources_err!("could not allocate Jev batch cache index");
        }
        let unused = (old + additional)
            .checked_sub(self.values.allocation_size())
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "Jev cache index exceeded its reserved allocation".into(),
                )
            })?;
        self.reservation.shrink(unused);
        Ok(())
    }

    pub fn insert(&mut self, key: RequestKey, value: Option<String>) -> Result<CachedRaw> {
        if let Some(cached) = self.get(&key) {
            return Ok(cached);
        }
        self.prepare()?;
        if let Some(value) = &value {
            // Arc's two counters and alignment, in addition to the exact UTF-8 bytes.
            let bytes = value
                .len()
                .checked_add(3 * std::mem::size_of::<usize>())
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted("Jev cached response size overflow".into())
                })?;
            self.reservation.try_grow(bytes)?;
        }
        let value = value.map(Arc::<str>::from);
        self.values.insert(key, value.clone());
        Ok(value)
    }
}

tokio::task_local! {
    pub(crate) static INVOCATION_CACHE: Arc<Mutex<BatchCache>>;
}

pub(crate) fn batch_cache() -> Result<Arc<Mutex<BatchCache>>> {
    match INVOCATION_CACHE.try_with(Arc::clone) {
        Ok(cache) => Ok(cache),
        // Direct async UDF invocation has no TaskContext. Its cache is still
        // invocation-local; execution plans always use the actual query pool.
        Err(_) => {
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            Ok(Arc::new(Mutex::new(BatchCache::new(&pool, 0, 0)?)))
        }
    }
}

#[derive(Debug)]
pub(crate) struct JevPhysicalOptimizer {
    pub functions: Vec<Arc<ScalarUDF>>,
}

impl JevPhysicalOptimizer {
    #[allow(deprecated)]
    fn rewrite(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        demand: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let starts_limit = plan
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .is_some_and(|limit| limit.fetch().is_some())
            || plan.as_any().is::<LocalLimitExec>()
            || plan
                .as_any()
                .downcast_ref::<FilterExec>()
                .is_some_and(|filter| filter.fetch().is_some())
            || plan
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_some_and(|coalesce| coalesce.fetch().is_some());
        let passes_demand = plan.as_any().is::<ProjectionExec>()
            || plan.as_any().is::<FilterExec>()
            || plan.as_any().is::<AsyncFuncExec>()
            || plan.as_any().is::<GlobalLimitExec>()
            || plan.as_any().is::<LocalLimitExec>()
            || plan.as_any().is::<CoalescePartitionsExec>()
            || plan.as_any().is::<CoalesceBatchesExec>()
            || plan.as_any().is::<RepartitionExec>()
            || plan.name() == "CooperativeExec";
        // Blocking operators (sorts, joins, aggregates) may need every input
        // row; a LIMIT above them cannot safely reduce judgment work.
        let demand = (demand || starts_limit)
            && passes_demand
            && crate::planner::has_jev(&plan, &self.functions);
        let children = plan
            .children()
            .iter()
            .map(|child| self.rewrite((*child).clone(), demand))
            .collect::<Result<Vec<_>>>()?;
        if demand
            && (plan.as_any().is::<CoalesceBatchesExec>() || plan.as_any().is::<RepartitionExec>())
        {
            return Ok(children[0].clone());
        }
        let plan = if children.is_empty() {
            plan
        } else {
            plan.with_new_children(children)?
        };
        if let Some(exec) = plan.as_any().downcast_ref::<AsyncFuncExec>() {
            if exec
                .async_exprs()
                .iter()
                .any(|expr| crate::planner::is_jev(expr, &self.functions))
            {
                return Ok(Arc::new(JevExec::new(exec.clone(), demand)));
            }
        }
        if demand {
            if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
                return Ok(Arc::new(
                    FilterExecBuilder::from(filter).with_batch_size(1).build()?,
                ));
            }
            if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
                let serial: Arc<dyn ExecutionPlan> = Arc::new(JevSerialPartitionsExec {
                    input: coalesce.input().clone(),
                    properties: coalesce.properties().clone(),
                });
                return Ok(match coalesce.fetch() {
                    Some(fetch) => Arc::new(GlobalLimitExec::new(serial, 0, Some(fetch))),
                    None => serial,
                });
            }
        }
        Ok(plan)
    }
}

impl PhysicalOptimizerRule for JevPhysicalOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = crate::planner::push_cheap_filters(plan, &self.functions)?;
        self.rewrite(crate::reuse::reuse_computed(plan, &self.functions)?, false)
    }
    fn name(&self) -> &str {
        "jev_spend"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct JevExec {
    inner: AsyncFuncExec,
    demand: bool,
    metrics: ExecutionPlanMetricsSet,
}

impl JevExec {
    fn new(inner: AsyncFuncExec, demand: bool) -> Self {
        Self {
            inner,
            demand,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for JevExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "JevExec: expressions={}, demand_driven={}",
            self.inner.async_exprs().len(),
            self.demand
        )
    }
}

impl ExecutionPlan for JevExec {
    fn name(&self) -> &str {
        "JevExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.inner.input()]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        datafusion::common::assert_eq_or_internal_err!(
            children.len(),
            1,
            "JevExec requires one input"
        );
        Ok(Arc::new(Self::new(
            AsyncFuncExec::try_new(self.inner.async_exprs().to_vec(), children[0].clone())?,
            self.demand,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.inner.input().execute(partition, context.clone())?;
        let expressions = Arc::new(self.inner.async_exprs().to_vec());
        let schema = self.schema();
        let output_schema = schema.clone();
        let config = context.session_config().options().clone();
        let metrics = Arc::new(JevMetrics::new(&self.metrics, partition));
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let chunk_size = if self.demand {
            1
        } else {
            config.execution.batch_size.max(1)
        };
        let pool = context.memory_pool().clone();
        let stream = stream::try_unfold(
            (
                input,
                None::<RecordBatch>,
                0,
                None::<Arc<Mutex<BatchCache>>>,
            ),
            move |(mut input, mut pending, mut offset, mut cache)| {
                let expressions = expressions.clone();
                let schema = schema.clone();
                let config = config.clone();
                let metrics = metrics.clone();
                let baseline = baseline.clone();
                let pool = pool.clone();
                async move {
                    loop {
                        if pending
                            .as_ref()
                            .is_some_and(|batch| offset < batch.num_rows())
                        {
                            break;
                        }
                        // Release the old batch before polling an upstream
                        // operator that may need the same memory budget.
                        drop(cache.take());
                        drop(pending.take());
                        pending = input.try_next().await?;
                        if pending.is_none() {
                            return Ok(None);
                        }
                        offset = 0;
                        cache = Some(Arc::new(Mutex::new(BatchCache::new(
                            &pool,
                            pending
                                .as_ref()
                                .map_or(0, RecordBatch::get_array_memory_size),
                            partition,
                        )?)));
                    }
                    // This async operator's elapsed_compute includes time
                    // awaiting its judgments (including provider/retry waits),
                    // but begins only after upstream input has arrived. The
                    // guard also records work ended by an error or cancellation.
                    let _timer = baseline.elapsed_compute().timer();
                    let batch = pending.as_ref().expect("pending input batch");
                    let current_cache = cache.as_ref().expect("pending input cache").clone();
                    let size = chunk_size.min(batch.num_rows() - offset);
                    let batch = batch.slice(offset, size);
                    offset += size;
                    let mut arrays = batch.columns().to_vec();
                    for expression in expressions.iter() {
                        let value = INVOCATION_METRICS
                            .scope(
                                metrics.clone(),
                                INVOCATION_CACHE.scope(
                                    current_cache.clone(),
                                    expression.invoke_with_args(&batch, config.clone()),
                                ),
                            )
                            .await?;
                        arrays.push(value.to_array(size)?);
                    }
                    let output = RecordBatch::try_new(schema, arrays)?.record_output(&baseline);
                    Ok(Some((output, (input, pending, offset, cache))))
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// DataFusion's regular partition coalescer eagerly spawns one producer per
/// partition. Beneath a LIMIT, poll partitions in sequence so an unconsumed
/// partition cannot start paid work.
#[derive(Debug)]
struct JevSerialPartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for JevSerialPartitionsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JevSerialPartitionsExec")
    }
}

impl ExecutionPlan for JevSerialPartitionsExec {
    fn name(&self) -> &str {
        "JevSerialPartitionsExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        datafusion::common::assert_eq_or_internal_err!(
            children.len(),
            1,
            "JevSerialPartitionsExec requires one input"
        );
        let properties = CoalescePartitionsExec::new(children[0].clone())
            .properties()
            .clone();
        Ok(Arc::new(Self {
            input: children[0].clone(),
            properties,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        datafusion::common::assert_eq_or_internal_err!(
            partition,
            0,
            "JevSerialPartitionsExec has one output partition"
        );
        let input = self.input.clone();
        let count = input.output_partitioning().partition_count();
        let stream = stream::iter(0..count)
            .map(move |partition| input.execute(partition, context.clone()))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_reservations_release_and_never_bypass_capacity() -> Result<()> {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024));
        let mut cache = BatchCache::new(&pool, 1024, 0)?;
        for index in 0_u64..9000 {
            let mut key = [0; 32];
            key[..8].copy_from_slice(&index.to_le_bytes());
            cache.prepare()?;
            cache.insert(key, Some("response".into()))?;
        }
        assert_eq!(cache.values.len(), 9000);
        assert!(pool.reserved() > 1024);
        let mut key = [0; 32];
        key[..8].copy_from_slice(&8999_u64.to_le_bytes());
        assert_eq!(cache.get(&key).unwrap().unwrap().as_ref(), "response");
        let reserved = pool.reserved();
        let error = cache
            .insert([255; 32], Some("x".repeat(2 * 1024 * 1024)))
            .unwrap_err();
        assert!(matches!(
            error.find_root(),
            DataFusionError::ResourcesExhausted(_)
        ));
        assert_eq!(pool.reserved(), reserved);
        assert_eq!(cache.values.len(), 9000);
        drop(cache);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }
}
