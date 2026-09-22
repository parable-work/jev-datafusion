use std::{borrow::Cow, ops::ControlFlow};

use crate::contract::{JevArgumentName as Arg, JevQuestionType};
use datafusion::{
    common::{DataFusionError, Result},
    sql::sqlparser::{
        ast::{
            Expr, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments, Ident,
            Value, VisitMut, VisitorMut,
        },
        dialect::GenericDialect,
        parser::Parser,
    },
};

pub fn parameter_names(name: &str) -> Option<Vec<&'static str>> {
    let arguments = if name == "ask" {
        vec![Arg::State, Arg::Questions, Arg::Model]
    } else if JevQuestionType::ALL
        .iter()
        .any(|kind| kind.as_str() == name)
    {
        vec![Arg::State, Arg::Instructions, Arg::Criteria, Arg::Model]
    } else {
        return None;
    };
    Some(arguments.iter().map(Arg::as_str).collect())
}

/// Fill only omitted optional arguments. DataFusion still resolves, validates,
/// and reorders named arguments through Signature::with_parameter_names.
pub fn normalize_jev_sql(sql: &str) -> Result<Cow<'_, str>> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| DataFusionError::Plan(format!("invalid SQL: {error}")))?;
    let mut normalizer = Defaults { changed: false };
    let _: ControlFlow<()> = statements.visit(&mut normalizer);
    if normalizer.changed {
        Ok(Cow::Owned(
            statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        ))
    } else {
        Ok(Cow::Borrowed(sql))
    }
}

struct Defaults {
    changed: bool,
}

impl VisitorMut for Defaults {
    type Break = ();

    fn post_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<()> {
        let Expr::Function(function) = expression else {
            return ControlFlow::Continue(());
        };
        // Only the unqualified registered SQL surface belongs to this normalizer.
        if function.name.0.len() != 1 {
            return ControlFlow::Continue(());
        }
        let Some(identifier) = function.name.0[0].as_ident() else {
            return ControlFlow::Continue(());
        };
        let name = if identifier.quote_style.is_some() {
            identifier.value.clone()
        } else {
            identifier.value.to_ascii_lowercase()
        };
        let Some(names) = parameter_names(&name) else {
            return ControlFlow::Continue(());
        };
        let FunctionArguments::List(list) = &mut function.args else {
            return ControlFlow::Continue(());
        };
        if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
            return ControlFlow::Continue(());
        }
        let has_names = list
            .args
            .iter()
            .any(|argument| !matches!(argument, FunctionArg::Unnamed(_)));
        let first_optional = if name == JevQuestionType::Noul.as_str() {
            2
        } else {
            names.len() - 1
        };
        if !has_names && list.args.len() < first_optional {
            return ControlFlow::Continue(());
        }
        for (index, optional_name) in names.iter().enumerate().skip(first_optional) {
            let present = list
                .args
                .iter()
                .enumerate()
                .any(|(position, argument)| match argument {
                    FunctionArg::Unnamed(_) => position == index,
                    FunctionArg::Named { name, .. } => {
                        if name.quote_style.is_some() {
                            name.value == *optional_name
                        } else {
                            name.value.eq_ignore_ascii_case(optional_name)
                        }
                    }
                    _ => false,
                });
            if present {
                continue;
            }
            let arg = FunctionArgExpr::Expr(Expr::Value(Value::Null.into()));
            list.args.push(if has_names {
                FunctionArg::Named {
                    name: Ident::new(*optional_name),
                    arg,
                    operator: FunctionArgOperator::RightArrow,
                }
            } else {
                FunctionArg::Unnamed(arg)
            });
            self.changed = true;
        }
        ControlFlow::Continue(())
    }
}
