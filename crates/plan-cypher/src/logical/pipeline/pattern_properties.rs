// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Lower row-dependent node properties into predicates inside their MATCH clause.

use super::*;

pub(super) fn needs_lowering(query: &QueryPipeline) -> bool {
    query.clauses.iter().any(|clause| match &clause.kind {
        ClauseKind::Match { patterns, .. } => patterns.iter().any(|pattern| {
            std::iter::once(&pattern.first)
                .chain(pattern.steps.iter().map(|step| &step.target))
                .any(|node| {
                    node.properties
                        .values()
                        .any(|value| value.reads_row_binding())
                })
        }),
        _ => false,
    })
}

pub(super) fn lower(query: &QueryPipeline) -> Result<QueryPipeline> {
    let mut query = query.clone();
    for clause in &mut query.clauses {
        let ClauseKind::Match {
            patterns,
            predicate,
            ..
        } = &mut clause.kind
        else {
            continue;
        };
        let mut conjuncts = Vec::new();
        for pattern in patterns {
            let pattern = &mut pattern.kind;
            for node in std::iter::once(&mut pattern.first)
                .chain(pattern.steps.iter_mut().map(|step| &mut step.kind.target))
            {
                let dynamic = node
                    .properties
                    .iter()
                    .filter(|(_, value)| value.reads_row_binding())
                    .map(|(property, value)| (property.clone(), value.clone()))
                    .collect::<Vec<_>>();
                for (property, value) in dynamic {
                    node.properties.remove(&property);
                    conjuncts.push(PropertyPredicate::ExpressionEq {
                        expression: AstNode::synthetic(ScalarExpressionKind::Property {
                            variable: node.variable.clone(),
                            property,
                        }),
                        value: scalar(value)?,
                    });
                }
            }
        }
        if conjuncts.is_empty() {
            continue;
        }
        // A filter above OPTIONAL MATCH would discard the required unmatched row.
        conjuncts.extend(predicate.take().map(|predicate| predicate.kind));
        let combined = if conjuncts.len() == 1 {
            conjuncts.pop().expect("one pattern predicate")
        } else {
            PropertyPredicate::And(conjuncts)
        };
        *predicate = Some(AstNode::synthetic(combined));
    }
    Ok(query)
}

fn scalar(value: ValueExpression) -> Result<ScalarExpression> {
    let span = value.span;
    let kind = match value.kind {
        ValueExpressionKind::Binding(variable) => ScalarExpressionKind::Variable(variable),
        ValueExpressionKind::BindingProperty { variable, property } => {
            ScalarExpressionKind::Property { variable, property }
        }
        ValueExpressionKind::List(values) => {
            ScalarExpressionKind::List(values.into_iter().map(scalar).collect::<Result<_>>()?)
        }
        ValueExpressionKind::Timestamp(ref inner) if inner.reads_row_binding() => {
            return Err(HawDBError::Semantic(
                "row-bound timestamp conversion in pattern properties is not supported".into(),
            ));
        }
        _ => ScalarExpressionKind::Value(value),
    };
    Ok(AstNode { kind, span })
}
