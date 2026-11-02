// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::{Catalog, LabelId};
use hawdb_plan_cypher::{NodeProjectionAccess, ProjectionExpression};

pub(super) struct IndexedNodeAccess {
    pub label: LabelId,
    pub access: NodeProjectionAccess,
    _allocation: Box<dyn GraphReadAllocation>,
}

impl MatchRuntime<'_> {
    pub(super) fn indexed_node_access(
        &self,
        node: &GraphMatchNode,
        label: Option<LabelId>,
        row: &Binding,
    ) -> Result<Option<IndexedNodeAccess>> {
        // Keep traversal, imported bindings and union-label reads on their
        // original paths. The complete single-node MATCH retains its predicate.
        if self.program.steps.len() != 1
            || !self.program.imports.is_empty()
            || self.program.introduced.as_slice() != [node.variable.as_str()]
            || node.label.contains(':')
        {
            return Ok(None);
        }
        let Some(label) = label else { return Ok(None) };
        let Some((property, value)) = self.program.predicate.as_ref().and_then(|predicate| {
            lookup_key(predicate, &node.variable, row, self.context.catalog, label)
        }) else {
            return Ok(None);
        };
        let allocation = crate::scan::owned::admit_access_copy(
            self.account,
            self.context.task_context,
            std::iter::once(property),
            std::iter::once(value),
        )?;
        Ok(Some(IndexedNodeAccess {
            label,
            access: NodeProjectionAccess::PropertyValues {
                property: property.into(),
                values: vec![value.clone()],
            },
            _allocation: allocation,
        }))
    }
}

fn lookup_key<'p, 'r>(
    predicate: &'p Predicate,
    target: &str,
    row: &'r Binding,
    catalog: &Catalog,
    label: LabelId,
) -> Option<(&'p str, &'r Value)> {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                if let Some(key) = lookup_key(child, target, row, catalog, label) {
                    return Some(key);
                }
                // An index must not hide an error in an earlier conjunct on
                // a node whose key it would reject.
                if !infallible_prefix(child) {
                    break;
                }
            }
            None
        }
        Predicate::ExpressionEq { expression, value } => {
            lookup_sides(expression, value, target, row, catalog, label)
                .or_else(|| lookup_sides(value, expression, target, row, catalog, label))
        }
        _ => None,
    }
}

fn infallible_prefix(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::ConstantBool(_)
        | Predicate::PropertyEq { .. }
        | Predicate::PropertyNotEq { .. }
        | Predicate::PropertyCompare { .. }
        | Predicate::PropertyIsNull { .. }
        | Predicate::PropertyIsNotNull { .. } => true,
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().all(infallible_prefix)
        }
        Predicate::Not(child) => infallible_prefix(child),
        _ => false,
    }
}

fn lookup_sides<'p, 'r>(
    expression: &'p ProjectionExpression,
    key: &ProjectionExpression,
    target: &str,
    row: &'r Binding,
    catalog: &Catalog,
    label: LabelId,
) -> Option<(&'p str, &'r Value)> {
    let ProjectionExpression::Property { variable, property } = expression else {
        return None;
    };
    if variable != target || !catalog.has_scalar_property_index(label, property) {
        return None;
    }
    // Borrow only already-bound values. Fallible/computed keys, including a
    // non-map ColumnProperty, retain ordinary predicate evaluation. NULL keys
    // do too, preserving demand-sensitive errors in other WHERE conjuncts.
    let value = match key {
        ProjectionExpression::Column(column) if column != target => row.values.get(column)?,
        ProjectionExpression::ColumnProperty { column, property } if column != target => {
            let Value::Map(values) = row.values.get(column)? else {
                return None;
            };
            values.get(property)?
        }
        ProjectionExpression::Property { variable, property } if variable != target => {
            crate::expression::binding_property(row, variable, property)?
        }
        _ => return None,
    };
    (value != &Value::Null).then_some((property, value))
}
