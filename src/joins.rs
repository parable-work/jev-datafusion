//! Keep paid inner-join predicates in the native async Filter execution path.
//! DataFusion 53 can push a WHERE predicate into Join.filter, whose physical
//! expression evaluator is synchronous. Only inner joins are equivalent to a
//! post-join filter; outer/semi/anti join matching semantics are not rewritten.
use std::sync::Arc;

use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        JoinType, Result,
    },
    logical_expr::{
        utils::{conjunction, split_conjunction},
        Expr, Filter, LogicalPlan, ScalarUDF,
    },
    optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
};

#[derive(Debug)]
pub(crate) struct JevInnerJoinFilters {
    pub functions: Vec<Arc<ScalarUDF>>,
}

impl JevInnerJoinFilters {
    fn contains_jev(&self, expression: &Expr) -> Result<bool> {
        let mut found = false;
        expression.apply(|expression| {
            if let Expr::ScalarFunction(call) = expression {
                if self
                    .functions
                    .iter()
                    .any(|function| function.as_ref() == call.func.as_ref())
                {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(found)
    }
}

impl OptimizerRule for JevInnerJoinFilters {
    fn name(&self) -> &str {
        "jev_inner_join_filters"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(mut join) = plan else {
            return Ok(Transformed::no(plan));
        };
        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        let Some(predicate) = join.filter.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        let mut cheap = Vec::new();
        let mut paid = Vec::new();
        for conjunct in split_conjunction(predicate) {
            if self.contains_jev(conjunct)? {
                paid.push(conjunct.clone());
            } else {
                cheap.push(conjunct.clone());
            }
        }
        let Some(predicate) = conjunction(paid) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        join.filter = conjunction(cheap);
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            predicate,
            Arc::new(LogicalPlan::Join(join)),
        )?)))
    }
}
