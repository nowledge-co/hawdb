// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `RETURN` after an atomic mutation clause.
//!
//! The mutation keeps its existing storage command. Each summary row it
//! reports binds the returned variables, and the projection is evaluated over
//! those bindings in the same statement, before anything is published.

use super::*;
use hawdb_cypher::{AggregateExpression, ProjectionClause, ReturnExpressionKind};

pub(super) fn bind(
    mutation: LogicalPlan,
    bindings: Vec<MutationReturnBinding>,
    projection: &ProjectionClause,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if projection.distinct
        || projection.predicate.is_some()
        || !projection.order_by.is_empty()
        || projection.offset.is_some()
        || projection.limit.is_some()
    {
        return Err(unsupported(
            "mutation RETURN does not support result modifiers",
        ));
    }
    let scope = bindings
        .iter()
        .map(|binding| binding.variable.clone())
        .collect::<BTreeSet<_>>();
    let mode = match projection.items.as_slice() {
        [item] => match &item.expression.kind {
            ReturnExpressionKind::Aggregate(AggregateExpression::CountAll) => {
                Some(SetNodePropertiesReturnMode::Count {
                    name: item.alias.clone().unwrap_or_else(|| "count(*)".to_string()),
                })
            }
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                variable,
                distinct: false,
            }) => {
                if !scope.contains(variable) {
                    return Err(HawDBError::Semantic(format!(
                        "mutation RETURN count variable '{variable}' is not bound by the mutation"
                    )));
                }
                Some(SetNodePropertiesReturnMode::Count {
                    name: item
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("count({variable})")),
                })
            }
            _ => None,
        },
        _ => None,
    };
    let mode = match mode {
        Some(mode) => mode,
        None => {
            let PlannedReturns::Projections(mut items) = plan_return_items_with_columns(
                &scope,
                &BTreeSet::new(),
                &projection.items,
                parameters,
            )?
            else {
                return Err(unsupported(
                    "mutation RETURN supports count(*) or count(variable) as its only aggregate",
                ));
            };
            let names =
                unique_projection_names(items.iter().map(|item| item.name.clone()).collect());
            for (item, name) in items.iter_mut().zip(names) {
                item.name = name;
            }
            SetNodePropertiesReturnMode::Project(items)
        }
    };
    Ok(LogicalPlan::MutationReturn {
        mutation: Box::new(mutation),
        returns: MutationReturn { bindings, mode },
    })
}
