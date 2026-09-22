//! Carry an already computed judgment through row-preserving physical nodes.
//! DataFusion may put CSE projections or a pushed-down LIMIT between SELECT
//! and WHERE; matching only adjacent logical Projection/Filter nodes misses it.
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode},
        Result,
    },
    logical_expr::ScalarUDF,
    physical_expr::{expressions::Column, PhysicalExpr, ScalarFunctionExpr},
    physical_plan::{
        async_func::AsyncFuncExec,
        coalesce_partitions::CoalescePartitionsExec,
        filter::{FilterExec, FilterExecBuilder},
        limit::{GlobalLimitExec, LocalLimitExec},
        projection::{ProjectionExec, ProjectionExpr},
        repartition::RepartitionExec,
        ExecutionPlan,
    },
};
use std::sync::Arc;

type Plan = Arc<dyn ExecutionPlan>;
type Expression = Arc<dyn PhysicalExpr>;

fn column(plan: &Plan, index: usize) -> Expression {
    Arc::new(Column::new(plan.schema().field(index).name(), index))
}

fn map_columns(
    expression: Expression,
    mut map: impl FnMut(usize) -> Expression,
) -> Result<Expression> {
    Ok(expression
        .transform_up(|expression| {
            Ok(match expression.as_any().downcast_ref::<Column>() {
                Some(column) => Transformed::yes(map(column.index())),
                None => Transformed::no(expression),
            })
        })?
        .data)
}

fn find_computed(plan: Plan, expression: Expression) -> Result<Option<(Plan, usize)>> {
    if let Some(exec) = plan.as_any().downcast_ref::<AsyncFuncExec>() {
        return Ok(exec
            .async_exprs()
            .iter()
            .position(|candidate| candidate.func.eq(&expression))
            .map(|index| (plan.clone(), exec.input().schema().fields().len() + index)));
    }
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let mapped = map_columns(expression, |index| projection.expr()[index].expr.clone())?;
        if let Some((input, index)) = find_computed(projection.input().clone(), mapped)? {
            let computed = column(&input, index);
            let mut expressions = projection.expr().to_vec();
            let position = match expressions.iter().position(|expr| expr.expr.eq(&computed)) {
                Some(position) => position,
                None => {
                    let position = expressions.len();
                    expressions.push(ProjectionExpr::new(
                        computed,
                        format!("__jev_reuse_{position}"),
                    ));
                    position
                }
            };
            return Ok(Some((
                Arc::new(ProjectionExec::try_new(expressions, input)?),
                position,
            )));
        }
        return Ok(None);
    }
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        let mapped = map_columns(expression, |index| {
            column(
                filter.input(),
                filter
                    .projection()
                    .as_ref()
                    .map_or(index, |projection| projection[index]),
            )
        })?;
        if let Some((input, index)) = find_computed(filter.input().clone(), mapped)? {
            let mut projection = filter
                .projection()
                .as_ref()
                .map(|projection| projection.to_vec())
                .unwrap_or_else(|| (0..filter.schema().fields().len()).collect());
            let position = match projection.iter().position(|candidate| *candidate == index) {
                Some(position) => position,
                None => {
                    projection.push(index);
                    projection.len() - 1
                }
            };
            let filter = FilterExecBuilder::new(filter.predicate().clone(), input)
                .with_default_selectivity(filter.default_selectivity())
                .with_batch_size(filter.batch_size())
                .with_fetch(filter.fetch())
                .apply_projection(Some(projection))?
                .build()?;
            return Ok(Some((Arc::new(filter), position)));
        }
        return Ok(None);
    }
    if plan.as_any().is::<GlobalLimitExec>()
        || plan.as_any().is::<LocalLimitExec>()
        || plan.as_any().is::<CoalescePartitionsExec>()
        || plan.as_any().is::<RepartitionExec>()
        || plan.name() == "CooperativeExec"
    {
        if let Some((input, index)) = find_computed(plan.children()[0].clone(), expression)? {
            return Ok(Some((plan.with_new_children(vec![input])?, index)));
        }
    }
    Ok(None)
}

pub(crate) fn reuse_computed(plan: Plan, functions: &[Arc<ScalarUDF>]) -> Result<Plan> {
    let children = plan
        .children()
        .iter()
        .map(|child| reuse_computed((*child).clone(), functions))
        .collect::<Result<Vec<_>>>()?;
    let plan = if children.is_empty() {
        plan
    } else {
        plan.with_new_children(children)?
    };
    let Some(exec) = plan.as_any().downcast_ref::<AsyncFuncExec>() else {
        return Ok(plan);
    };
    let mut input = exec.input().clone();
    let mut positions = Vec::new();
    let mut remaining = Vec::new();
    for expression in exec.async_exprs() {
        let owned = expression
            .func
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|call| {
                functions
                    .iter()
                    .any(|function| function.as_ref() == call.fun())
            });
        if owned {
            if let Some((rewritten, index)) = find_computed(input.clone(), expression.func.clone())?
            {
                input = rewritten;
                positions.push(Ok(index));
                continue;
            }
        }
        positions.push(Err(remaining.len()));
        remaining.push(expression.clone());
    }
    if remaining.len() == exec.async_exprs().len() {
        return Ok(plan);
    }
    let input_columns = input.schema().fields().len();
    let computed: Plan = if remaining.is_empty() {
        input
    } else {
        Arc::new(AsyncFuncExec::try_new(remaining, input)?)
    };
    let indices = (0..exec.input().schema().fields().len()).chain(
        positions
            .into_iter()
            .map(|position| position.unwrap_or_else(|offset| input_columns + offset)),
    );
    let expressions = indices
        .zip(plan.schema().fields())
        .map(|(index, field)| ProjectionExpr::new(column(&computed, index), field.name()))
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(expressions, computed)?))
}
