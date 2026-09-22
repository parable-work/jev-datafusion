//! Filter placement after DataFusion's CSE and projection optimizations.
//! Move only conjuncts independent of paid results; never split an OR or
//! commute a predicate across a LIMIT, join, sort, or aggregate.
use std::sync::Arc;

use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        Result,
    },
    logical_expr::ScalarUDF,
    physical_expr::{
        async_scalar_function::AsyncFuncExpr, conjunction, expressions::Column, split_conjunction,
        utils::collect_columns, PhysicalExpr, ScalarFunctionExpr,
    },
    physical_plan::{
        async_func::AsyncFuncExec,
        coalesce_partitions::CoalescePartitionsExec,
        filter::{FilterExec, FilterExecBuilder},
        projection::ProjectionExec,
        repartition::RepartitionExec,
        ExecutionPlan,
    },
};

type Plan = Arc<dyn ExecutionPlan>;
type Expression = Arc<dyn PhysicalExpr>;

pub(crate) fn is_jev(expression: &AsyncFuncExpr, functions: &[Arc<ScalarUDF>]) -> bool {
    expression
        .func
        .as_any()
        .downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|call| {
            functions
                .iter()
                .any(|function| function.as_ref() == call.fun())
        })
}

pub(crate) fn has_jev(plan: &Plan, functions: &[Arc<ScalarUDF>]) -> bool {
    plan.as_any()
        .downcast_ref::<AsyncFuncExec>()
        .is_some_and(|exec| {
            exec.async_exprs()
                .iter()
                .any(|expr| is_jev(expr, functions))
        })
        || plan
            .children()
            .iter()
            .any(|child| has_jev(child, functions))
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

fn is_volatile(expression: &Expression) -> Result<bool> {
    let mut volatile = false;
    expression.apply(|expression| {
        if expression.is_volatile_node() {
            volatile = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(volatile)
}

fn move_below_paid(
    plan: Plan,
    predicate: Expression,
    functions: &[Arc<ScalarUDF>],
) -> Result<Option<Plan>> {
    if is_volatile(&predicate)? {
        return Ok(None);
    }
    if let Some(exec) = plan.as_any().downcast_ref::<AsyncFuncExec>() {
        if !exec
            .async_exprs()
            .iter()
            .any(|expression| is_jev(expression, functions))
        {
            return Ok(None);
        }
        if collect_columns(&predicate)
            .iter()
            .any(|column| column.index() >= exec.input().schema().fields().len())
        {
            return Ok(None);
        }
        let input = match move_below_paid(exec.input().clone(), predicate.clone(), functions)? {
            Some(input) => input,
            None => Arc::new(FilterExec::try_new(predicate, exec.input().clone())?),
        };
        return Ok(Some(Arc::new(AsyncFuncExec::try_new(
            exec.async_exprs().to_vec(),
            input,
        )?)));
    }
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let predicate = map_columns(predicate, |index| projection.expr()[index].expr.clone())?;
        return move_below_paid(projection.input().clone(), predicate, functions)?
            .map(|input| plan.with_new_children(vec![input]))
            .transpose();
    }
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        if filter.fetch().is_some() {
            return Ok(None);
        }
        let predicate = map_columns(predicate, |index| {
            let index = filter
                .projection()
                .as_ref()
                .map_or(index, |projection| projection[index]);
            Arc::new(Column::new(
                filter.input().schema().field(index).name(),
                index,
            ))
        })?;
        return move_below_paid(filter.input().clone(), predicate, functions)?
            .map(|input| plan.with_new_children(vec![input]))
            .transpose();
    }
    if plan.as_any().is::<CoalescePartitionsExec>() && plan.fetch().is_none()
        || plan.as_any().is::<RepartitionExec>()
        || plan.name() == "CooperativeExec"
    {
        return move_below_paid(plan.children()[0].clone(), predicate, functions)?
            .map(|input| plan.with_new_children(vec![input]))
            .transpose();
    }
    Ok(None)
}

pub(crate) fn push_cheap_filters(plan: Plan, functions: &[Arc<ScalarUDF>]) -> Result<Plan> {
    let children = plan
        .children()
        .iter()
        .map(|child| push_cheap_filters((*child).clone(), functions))
        .collect::<Result<Vec<_>>>()?;
    let plan = if children.is_empty() {
        plan
    } else {
        plan.with_new_children(children)?
    };
    let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() else {
        return Ok(plan);
    };
    if !has_jev(filter.input(), functions) {
        return Ok(plan);
    }
    let mut input = filter.input().clone();
    let mut remaining = Vec::new();
    let mut changed = false;
    for predicate in split_conjunction(filter.predicate()) {
        if let Some(pushed) = move_below_paid(input.clone(), predicate.clone(), functions)? {
            input = pushed;
            changed = true;
        } else {
            remaining.push(predicate.clone());
        }
    }
    if !changed {
        return Ok(plan);
    }
    Ok(Arc::new(
        FilterExecBuilder::from(filter)
            .with_input(input)
            .with_predicate(conjunction(remaining))
            .build()?,
    ))
}
