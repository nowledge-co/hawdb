// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Requested-heap bounds for retained binding templates. These depend on the
//! pinned Rust BTreeMap/Arc layouts and must be requalified on toolchain changes.
//! Allocator rounding and request-owned/in-flight plans are outside this bound.

use super::*;
use std::mem::size_of;

fn sum(values: impl IntoIterator<Item = Option<usize>>) -> Option<usize> {
    values
        .into_iter()
        .try_fold(0usize, |total, value| total.checked_add(value?))
}

fn vector<T>(values: &Vec<T>, heap: impl Fn(&T) -> Option<usize>) -> Option<usize> {
    values.iter().try_fold(
        values.capacity().checked_mul(size_of::<T>())?,
        |total, value| total.checked_add(heap(value)?),
    )
}

fn map<K, V>(values: &BTreeMap<K, V>, heap: impl Fn(&K, &V) -> Option<usize>) -> Option<usize> {
    // 11 slots, 12 child pointers, >=5 entries/non-root node. Include a
    // possibly allocated empty root and bound every node as internal. Same
    // pinned-std bound as executor owned-result accounting.
    let nodes = 1usize.checked_add(values.len().saturating_sub(1) / 5)?;
    let node = 11usize
        .checked_mul(size_of::<(K, V)>())?
        .checked_add(14usize.checked_mul(size_of::<usize>())?)?;
    values
        .iter()
        .try_fold(nodes.checked_mul(node)?, |total, (key, value)| {
            total.checked_add(heap(key, value)?)
        })
}

fn boxed<T>(value: &T, heap: impl Fn(&T) -> Option<usize>) -> Option<usize> {
    size_of::<T>().checked_add(heap(value)?)
}

fn arc<T>(value: &T, heap: impl Fn(&T) -> Option<usize>) -> Option<usize> {
    // Both Arc counters plus worst-case alignment padding before/after T.
    sum([
        Some(2 * size_of::<usize>()),
        Some(2 * std::mem::align_of::<T>()),
        boxed(value, heap),
    ])
}

fn value_bytes(value: &Value) -> Option<usize> {
    match value {
        Value::String(value) => Some(value.capacity()),
        Value::Binary(value) => Some(value.capacity()),
        Value::Vector(value) => Some(value.retained_capacity()),
        Value::List(values) => vector(values, value_bytes),
        Value::Map(values) => map(values, |key, value| {
            key.capacity().checked_add(value_bytes(value)?)
        }),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Uuid(_) => Some(0),
    }
}

fn shape_bytes(shape: &ParameterValueShape) -> Option<usize> {
    match shape {
        ParameterValueShape::List(values) => vector(values, shape_bytes),
        ParameterValueShape::Map(values) => map(values, |key, value| {
            key.capacity().checked_add(shape_bytes(value)?)
        }),
        ParameterValueShape::Null
        | ParameterValueShape::Bool
        | ParameterValueShape::Int
        | ParameterValueShape::Float
        | ParameterValueShape::String
        | ParameterValueShape::Binary
        | ParameterValueShape::Vector(_)
        | ParameterValueShape::Uuid => Some(0),
    }
}

pub(super) fn template_bytes(template: &ParameterBindingTemplate) -> Option<usize> {
    sum([
        arc(template, |_| Some(0)),
        arc(template.plan.logical.as_ref(), logical_bytes),
        arc(template.plan.cache_key.as_ref(), |key| {
            map(&key.values, |name, value| {
                name.capacity().checked_add(match value {
                    ParameterCacheValue::Slot(shape) | ParameterCacheValue::CaseSlot(shape) => {
                        shape_bytes(shape)
                    }
                    ParameterCacheValue::Exact(value) => value_bytes(value),
                }?)
            })
        }),
    ])
}

fn expression_bytes(expression: &ProjectionExpression) -> Option<usize> {
    use ProjectionExpression as E;
    match expression {
        E::Variable { variable }
        | E::Id { variable }
        | E::RelationshipType { variable }
        | E::Column(variable) => Some(variable.capacity()),
        E::Property { variable, property }
        | E::DatePart {
            part: _,
            variable,
            property,
        }
        | E::ColumnProperty {
            column: variable,
            property,
        } => variable.capacity().checked_add(property.capacity()),
        E::Literal(value) => value_bytes(value),
        E::Coalesce(values) => vector(values, expression_bytes),
        E::Lower(value)
        | E::Not(value)
        | E::IsNull {
            expression: value,
            negated: _,
        }
        | E::Left {
            expression: value,
            length: _,
        } => boxed(value.as_ref(), expression_bytes),
        E::Binary { left, op: _, right }
        | E::VectorDistance {
            metric: _,
            left,
            right,
        } => sum([
            boxed(left.as_ref(), expression_bytes),
            boxed(right.as_ref(), expression_bytes),
        ]),
        E::JsonValue { expression, path } => {
            boxed(expression.as_ref(), expression_bytes)?.checked_add(path.capacity())
        }
        E::Case {
            operand,
            branches,
            otherwise,
        } => sum([
            operand
                .as_ref()
                .map_or(Some(0), |value| boxed(value.as_ref(), expression_bytes)),
            vector(branches, |(key, result)| {
                sum([expression_bytes(key), expression_bytes(result)])
            }),
            otherwise
                .as_ref()
                .map_or(Some(0), |value| boxed(value.as_ref(), expression_bytes)),
        ]),
        E::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        }
        | E::ColumnDefaultIfNullOrEq {
            column: variable,
            property,
            empty,
            default,
        } => sum([
            Some(variable.capacity()),
            Some(property.capacity()),
            value_bytes(empty),
            value_bytes(default),
        ]),
        E::DefaultIfNull {
            variable,
            property,
            default,
        }
        | E::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => sum([
            Some(variable.capacity()),
            Some(property.capacity()),
            value_bytes(default),
        ]),
        E::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => sum([
            Some(variable.capacity()),
            Some(property.capacity()),
            value_bytes(empty),
            value_bytes(non_empty),
            value_bytes(null_or_empty),
        ]),
        E::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => sum([
            Some(variable.capacity()),
            Some(property.capacity()),
            value_bytes(default),
            vector(branches, |(key, result)| {
                sum([value_bytes(key), value_bytes(result)])
            }),
        ]),
        E::CaseCoalesceDifferenceFloorZero { variable, terms } => Some(variable.capacity())
            .and_then(|base| {
                base.checked_add(vector(terms, |term| {
                    let planner::CoalesceDifferenceProjectionTerm { property, default } = term;
                    property.capacity().checked_add(value_bytes(default)?)
                })?)
            }),
        E::ColumnValueDefaultIfNull { column, default } => {
            column.capacity().checked_add(value_bytes(default)?)
        }
        E::ColumnValueCasePropertyNotNullOrEq {
            column,
            empty,
            non_empty,
            null_or_empty,
        } => sum([
            Some(column.capacity()),
            value_bytes(empty),
            value_bytes(non_empty),
            value_bytes(null_or_empty),
        ]),
        // Opaque/specialized expressions are deliberately not admitted yet.
        E::CaseEntitySearchRank(_) | E::CaseColumnSearchRank(_) => None,
    }
}

fn predicate_bytes(predicate: &Predicate) -> Option<usize> {
    use Predicate as P;
    match predicate {
        P::And(values) | P::Or(values) => vector(values, predicate_bytes),
        P::Not(value) => boxed(value.as_ref(), predicate_bytes),
        P::ConstantBool(_) => Some(0),
        P::IdEq { variable, value }
        | P::IdNotEq { variable, value }
        | P::IdCompare {
            variable,
            op: _,
            value,
        } => variable.capacity().checked_add(value_bytes(value)?),
        P::IdIn { variable, values } => variable
            .capacity()
            .checked_add(vector(values, value_bytes)?),
        P::PropertyEq {
            variable,
            property,
            value,
        }
        | P::PropertyNotEq {
            variable,
            property,
            value,
        }
        | P::PropertyCompare {
            variable,
            property,
            op: _,
            value,
        }
        | P::PropertyListContains {
            variable,
            property,
            value,
        } => sum([
            Some(variable.capacity()),
            Some(property.capacity()),
            value_bytes(value),
        ]),
        P::PropertyIn {
            variable,
            property,
            values,
        } => sum([
            Some(variable.capacity()),
            Some(property.capacity()),
            vector(values, value_bytes),
        ]),
        P::PropertyIsNull { variable, property } | P::PropertyIsNotNull { variable, property } => {
            variable.capacity().checked_add(property.capacity())
        }
        P::PropertyListContainsLower {
            variable,
            property,
            value,
        }
        | P::PropertyContains {
            variable,
            property,
            value,
        }
        | P::PropertyStartsWith {
            variable,
            property,
            value,
        }
        | P::PropertyEndsWith {
            variable,
            property,
            value,
        } => sum([
            Some(variable.capacity()),
            Some(property.capacity()),
            Some(value.capacity()),
        ]),
        P::ExpressionEq { expression, value }
        | P::ExpressionNotEq { expression, value }
        | P::ExpressionCompare {
            expression,
            op: _,
            value,
        }
        | P::ExpressionContains { expression, value } => {
            sum([expression_bytes(expression), expression_bytes(value)])
        }
        P::ExpressionIn { expression, values } => {
            sum([expression_bytes(expression), vector(values, value_bytes)])
        }
        P::RelationshipExists {
            variable,
            rel_type,
            direction: _,
            target_label,
        } => sum([
            Some(variable.capacity()),
            Some(rel_type.capacity()),
            Some(target_label.capacity()),
        ]),
        P::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction: _,
            target_variable,
        } => sum([
            Some(source_variable.capacity()),
            Some(rel_type.capacity()),
            Some(target_variable.capacity()),
        ]),
        P::PropertyRegexMatch {
            variable: _,
            property: _,
            pattern: _,
        } => None,
    }
}

fn projections(items: &Vec<Projection>) -> Option<usize> {
    vector(items, |item| {
        let Projection { expression, name } = item;
        name.capacity().checked_add(expression_bytes(expression)?)
    })
}

fn logical_bytes(plan: &LogicalPlan) -> Option<usize> {
    use LogicalPlan as L;
    match plan {
        L::NodeScan { variable, label } => variable.capacity().checked_add(label.capacity()),
        L::NodeCartesianProduct { left, right } => sum([
            boxed(left.as_ref(), logical_bytes),
            boxed(right.as_ref(), logical_bytes),
        ]),
        L::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional: _,
            node_visibility_predicate,
            input,
        } => sum([
            Some(variable.capacity()),
            Some(label.capacity()),
            Some(property.capacity()),
            Some(column.capacity()),
            node_visibility_predicate
                .as_ref()
                .map_or(Some(0), predicate_bytes),
            boxed(input.as_ref(), logical_bytes),
        ]),
        L::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction: _,
            target_variable,
            target_label,
            min_hops: _,
            max_hops: _,
            optional: _,
            input,
        } => sum([
            Some(source_variable.capacity()),
            Some(source_label.capacity()),
            Some(rel_variable.as_ref().map_or(0, String::capacity)),
            Some(rel_type.capacity()),
            Some(target_variable.capacity()),
            Some(target_label.capacity()),
            map(rel_properties, |key, value| {
                key.capacity().checked_add(value_bytes(value)?)
            }),
            boxed(input.as_ref(), logical_bytes),
        ]),
        L::Filter { predicate, input } => sum([
            predicate_bytes(predicate),
            boxed(input.as_ref(), logical_bytes),
        ]),
        L::Project { items, input } => {
            sum([projections(items), boxed(input.as_ref(), logical_bytes)])
        }
        L::Distinct { input }
        | L::Limit {
            offset: _,
            limit: _,
            input,
        } => boxed(input.as_ref(), logical_bytes),
        L::Sort { items, input } => sum([
            vector(items, |item| {
                let SortItem { key, direction: _ } = item;
                match key {
                    SortKey::Property { variable, property } => {
                        variable.capacity().checked_add(property.capacity())
                    }
                    SortKey::Id { variable } | SortKey::Column(variable) => {
                        Some(variable.capacity())
                    }
                    SortKey::Expression(expression) => expression_bytes(expression),
                }
            }),
            boxed(input.as_ref(), logical_bytes),
        ]),
        // Unsupported nodes never enter the retained template cache. The normal
        // planner/executor still handles them and produces identical results.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_and_shape_bounds_include_spare_capacity() {
        let mut text = String::with_capacity(16384);
        text.push('x');
        let mut bytes = Vec::with_capacity(8192);
        bytes.push(1);
        let mut values = Vec::with_capacity(512);
        values.push(Value::String(text));
        values.push(Value::Binary(bytes));
        assert!(
            value_bytes(&Value::List(values)).unwrap() >= 512 * size_of::<Value>() + 16384 + 8192
        );
        let mut shapes = Vec::with_capacity(512);
        shapes.push(ParameterValueShape::Null);
        assert!(
            shape_bytes(&ParameterValueShape::List(shapes)).unwrap()
                >= 512 * size_of::<ParameterValueShape>()
        );
    }

    #[test]
    fn unknown_plan_shapes_are_not_admitted() {
        assert_eq!(
            logical_bytes(&LogicalPlan::CreateNodeLabel {
                label: "Item".into()
            }),
            None
        );
    }
}
