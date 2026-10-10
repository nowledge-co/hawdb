// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Borrow existing values until projection row admission. Computed values own
//! an independently admitted working-memory lease until the output takes them.

use super::*;
use crate::binding::map_memory_bytes;
use crate::{QueryMemoryAccount, QueryMemoryLease};
use std::borrow::Cow;

pub(crate) struct ProjectedValue<'a> {
    value: Cow<'a, Value>,
    _allocation: Option<QueryMemoryLease>,
}

impl<'a> ProjectedValue<'a> {
    pub(crate) fn borrowed(value: &'a Value) -> Self {
        Self {
            value: Cow::Borrowed(value),
            _allocation: None,
        }
    }

    pub(crate) fn scalar(value: Value) -> Self {
        Self {
            value: Cow::Owned(value),
            _allocation: None,
        }
    }

    /// The caller must admit the complete owned output row before this copy.
    pub(crate) fn into_owned(self) -> Value {
        self.value.into_owned()
    }
}

impl AsRef<Value> for ProjectedValue<'_> {
    fn as_ref(&self) -> &Value {
        &self.value
    }
}

fn allocated<'a>(
    bytes: usize,
    account: Option<&QueryMemoryAccount>,
    create: impl FnOnce() -> Value,
) -> Result<ProjectedValue<'a>> {
    let allocation = account.map(|account| account.reserve(bytes)).transpose()?;
    Ok(ProjectedValue {
        value: Cow::Owned(create()),
        _allocation: allocation,
    })
}

fn lowered<'a>(value: &str, account: Option<&QueryMemoryAccount>) -> Result<ProjectedValue<'a>> {
    // Unicode case conversion may expand UTF-8. Count the output before its
    // allocation; contextual final sigma has the same byte width as sigma.
    let bytes = value
        .chars()
        .flat_map(char::to_lowercase)
        .fold(std::mem::size_of::<Value>(), |total, value| {
            total.saturating_add(value.len_utf8())
        });
    allocated(bytes, account, || Value::String(value.to_lowercase()))
}

struct ValueDescription<'a> {
    value: &'a Value,
    bounded: bool,
}

impl std::fmt::Debug for ValueDescription<'_> {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.bounded {
            match self.value {
                Value::String(_) => return output.write_str("String(..)"),
                Value::Binary(_) => return output.write_str("Binary(..)"),
                Value::List(_) => return output.write_str("List(..)"),
                Value::Map(_) => return output.write_str("Map(..)"),
                _ => {}
            }
        }
        std::fmt::Debug::fmt(self.value, output)
    }
}

fn description<'a>(value: &'a Value, account: Option<&QueryMemoryAccount>) -> ValueDescription<'a> {
    ValueDescription {
        value,
        bounded: account.is_some(),
    }
}

fn scoped_truth(value: &Value, account: Option<&QueryMemoryAccount>) -> Result<PredicateTruth> {
    if matches!(value, Value::Bool(_) | Value::Null) {
        scalar_truth(value)
    } else {
        Err(HawDBError::Execution(format!(
            "CASE condition requires a boolean value, got {:?}",
            description(value, account)
        )))
    }
}

pub(crate) fn evaluate_projection_borrowed<'a>(
    expression: &'a ProjectionExpression,
    catalog: &'a Catalog,
    binding: &'a Binding,
    account: Option<&QueryMemoryAccount>,
) -> Result<ProjectedValue<'a>> {
    let evaluate = |expression| evaluate_projection_borrowed(expression, catalog, binding, account);
    match expression {
        ProjectionExpression::Case {
            operand,
            branches,
            otherwise,
        } => {
            let operand = operand.as_deref().map(evaluate).transpose()?;
            for (condition, result) in branches {
                let condition = evaluate(condition)?;
                let matches = if let Some(operand) = &operand {
                    predicate_comparison_truth(
                        Some(operand.as_ref()),
                        condition.as_ref(),
                        |left, right| left == right,
                    )
                    .is_true()
                } else {
                    scoped_truth(condition.as_ref(), account)?.is_true()
                };
                if matches {
                    return evaluate(result);
                }
            }
            otherwise
                .as_deref()
                .map(evaluate)
                .transpose()
                .map(|value| value.unwrap_or_else(|| ProjectedValue::scalar(Value::Null)))
        }
        ProjectionExpression::Binary { left, op, right } => {
            binary(left, *op, right, catalog, binding, account)
        }
        ProjectionExpression::Not(child) => Ok(ProjectedValue::scalar(truth_value(
            scoped_truth(evaluate(child)?.as_ref(), account)?.not(),
        ))),
        ProjectionExpression::IsNull {
            expression,
            negated,
        } => Ok(ProjectedValue::scalar(Value::Bool(
            (evaluate(expression)?.as_ref() == &Value::Null) != *negated,
        ))),
        ProjectionExpression::Variable { variable } => {
            let entry_bytes = std::mem::size_of::<(String, Value)>() * 3;
            if let Some(node) = binding.nodes.get(variable) {
                let bytes = map_memory_bytes(&node.properties)
                    .saturating_add(entry_bytes * 2)
                    .saturating_add("_id".len() + "labels".len() + std::mem::size_of::<i64>())
                    .saturating_add(node.labels.iter().fold(0usize, |total, label| {
                        total.saturating_add(
                            catalog
                                .label_name(*label)
                                .map(|name| std::mem::size_of::<Value>() + name.len())
                                .unwrap_or(0),
                        )
                    }));
                return allocated(bytes, account, || node_value(node, catalog));
            }
            if let Some(relationship) = binding.relationships.get(variable) {
                let bytes = map_memory_bytes(&relationship.properties)
                    .saturating_add(entry_bytes * 4)
                    .saturating_add(
                        "_id".len() + "source_id".len() + "target_id".len() + "type".len(),
                    )
                    .saturating_add(std::mem::size_of::<i64>() * 3)
                    .saturating_add(
                        catalog
                            .rel_type_name(relationship.rel_type)
                            .map(str::len)
                            .unwrap_or(0),
                    );
                return allocated(bytes, account, || relationship_value(relationship, catalog));
            }
            if binding.values.get(variable) == Some(&Value::Null) {
                return Ok(ProjectedValue::scalar(Value::Null));
            }
            Err(HawDBError::Execution(format!(
                "missing variable '{variable}' during projection"
            )))
        }
        ProjectionExpression::Property { variable, property } => {
            require_variable(binding, variable)?;
            Ok(binding_property(binding, variable, property)
                .map(ProjectedValue::borrowed)
                .unwrap_or_else(|| ProjectedValue::scalar(Value::Null)))
        }
        ProjectionExpression::Id { variable }
            if binding.values.get(variable) == Some(&Value::Null)
                && !binding_has_variable(binding, variable) =>
        {
            Ok(ProjectedValue::scalar(Value::Null))
        }
        ProjectionExpression::Id { variable } => binding_id(binding, variable)
            .map(ProjectedValue::scalar)
            .ok_or_else(|| {
                HawDBError::Execution(format!("missing variable '{variable}' during projection"))
            }),
        ProjectionExpression::RelationshipType { variable }
            if binding.values.get(variable) == Some(&Value::Null)
                && !binding_has_variable(binding, variable) =>
        {
            Ok(ProjectedValue::scalar(Value::Null))
        }
        ProjectionExpression::RelationshipType { variable } => {
            let relationship = binding.relationships.get(variable).ok_or_else(|| {
                HawDBError::Execution(format!("missing variable '{variable}' during projection"))
            })?;
            match catalog.rel_type_name(relationship.rel_type) {
                Some(name) => allocated(std::mem::size_of::<Value>() + name.len(), account, || {
                    Value::String(name.into())
                }),
                None => Ok(ProjectedValue::scalar(Value::Null)),
            }
        }
        ProjectionExpression::Literal(value) => Ok(ProjectedValue::borrowed(value)),
        ProjectionExpression::Coalesce(expressions) => {
            for expression in expressions {
                let value = evaluate(expression)?;
                if value.as_ref() != &Value::Null {
                    return Ok(value);
                }
            }
            Ok(ProjectedValue::scalar(Value::Null))
        }
        ProjectionExpression::Left { expression, length } => {
            let value = evaluate(expression)?;
            match value.as_ref() {
                Value::Null => Ok(ProjectedValue::scalar(Value::Null)),
                Value::String(value) => {
                    let bytes = value
                        .chars()
                        .take(*length)
                        .fold(std::mem::size_of::<Value>(), |total, character| {
                            total.saturating_add(character.len_utf8())
                        });
                    allocated(bytes, account, || {
                        let mut output =
                            String::with_capacity(bytes - std::mem::size_of::<Value>());
                        output.extend(value.chars().take(*length));
                        Value::String(output)
                    })
                }
                value => Err(HawDBError::Execution(format!(
                    "LEFT expression requires a string value, got {:?}",
                    description(value, account)
                ))),
            }
        }
        ProjectionExpression::Lower(expression) => {
            let value = evaluate(expression)?;
            match value.as_ref() {
                Value::Null => Ok(ProjectedValue::scalar(Value::Null)),
                Value::String(value) => lowered(value, account),
                value => Err(HawDBError::Execution(format!(
                    "LOWER expression requires a string value, got {:?}",
                    description(value, account)
                ))),
            }
        }
        ProjectionExpression::DatePart {
            part,
            variable,
            property,
        } => {
            require_variable(binding, variable)?;
            match binding_property(binding, variable, property) {
                Some(Value::Int(nanos)) => Ok(ProjectedValue::scalar(Value::Int(
                    timestamp_date_part(*part, *nanos),
                ))),
                Some(Value::Null) | None => Ok(ProjectedValue::scalar(Value::Null)),
                Some(value) => Err(HawDBError::Execution(format!(
                    "date_part requires an integer timestamp value, got {:?}",
                    description(value, account)
                ))),
            }
        }
        ProjectionExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            require_variable(binding, variable)?;
            let value = binding_property(binding, variable, property);
            Ok(ProjectedValue::borrowed(match value {
                None | Some(Value::Null) => default,
                Some(value) if value.as_ref() == empty => default,
                Some(value) => value,
            }))
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            require_variable(binding, variable)?;
            Ok(ProjectedValue::borrowed(
                match binding_property(binding, variable, property) {
                    None | Some(Value::Null) => default,
                    Some(value) => value,
                },
            ))
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            require_variable(binding, variable)?;
            let value = binding_property(binding, variable, property)
                .map(Value::as_ref)
                .unwrap_or(ValueRef::Null);
            Ok(ProjectedValue::borrowed(
                if !value.is_null() && value != empty {
                    non_empty
                } else {
                    null_or_empty
                },
            ))
        }
        ProjectionExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            require_variable(binding, variable)?;
            let value = binding_property(binding, variable, property)
                .map(Value::as_ref)
                .unwrap_or(ValueRef::Null);
            Ok(ProjectedValue::borrowed(
                branches
                    .iter()
                    .find(|(candidate, _)| value == candidate)
                    .map(|(_, rank)| rank)
                    .unwrap_or(default),
            ))
        }
        ProjectionExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            require_variable(binding, variable)?;
            match binding_property(binding, variable, property) {
                Some(Value::String(value)) => lowered(value, account),
                Some(Value::Null) | None => Ok(ProjectedValue::borrowed(default)),
                Some(value) => Err(HawDBError::Execution(format!(
                    "CASE lower-default requires a string value, got {:?}",
                    description(value, account)
                ))),
            }
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            require_variable(binding, variable)?;
            Ok(ProjectedValue::scalar(Value::Int(
                coalesce_difference(binding, variable, terms)?.max(0),
            )))
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            if !binding_has_variable(binding, &expression.variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{}' during projection",
                    expression.variable
                )));
            }
            let name_matches =
                match binding_property(binding, &expression.variable, &expression.name_property) {
                    Some(Value::String(name)) => {
                        let lowered = lowered(name, account)?;
                        lowered.as_ref() == &expression.raw_query
                            || lowered.as_ref() == &expression.normalized_query
                    }
                    None | Some(Value::Null) => false,
                    Some(value) => {
                        return Err(HawDBError::Execution(format!(
                            "LOWER expression requires a string value, got {:?}",
                            description(value, account)
                        )))
                    }
                };
            if name_matches {
                return Ok(ProjectedValue::borrowed(&expression.exact_rank));
            }
            let alias_matches =
                match binding_property(binding, &expression.variable, &expression.aliases_property)
                {
                    Some(Value::List(values)) => {
                        values.iter().any(|alias| alias == &expression.raw_input)
                    }
                    _ => false,
                };
            Ok(ProjectedValue::borrowed(if alias_matches {
                &expression.alias_rank
            } else {
                &expression.fallback_rank
            }))
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            let column = column(binding, &expression.column)?;
            let matches = |query: &Value| {
                predicate_comparison_truth(Some(column), query, |left, right| left == right)
                    .is_true()
            };
            if matches(&expression.raw_query) || matches(&expression.normalized_query) {
                return Ok(ProjectedValue::borrowed(&expression.exact_rank));
            }
            let contains = matches!((column, &expression.raw_query), (Value::String(value), Value::String(query)) if value.contains(query))
                || matches!((column, &expression.normalized_query), (Value::String(value), Value::String(query)) if value.contains(query));
            Ok(ProjectedValue::borrowed(if contains {
                &expression.contains_rank
            } else {
                &expression.fallback_rank
            }))
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column: name,
            property,
            empty,
            default,
        } => {
            let value = match column(binding, name)? {
                Value::Map(values) => values.get(property),
                Value::Null => None,
                value => {
                    return Err(HawDBError::Execution(format!(
                        "column default expression requires a map value, got {:?}",
                        description(value, account)
                    )))
                }
            };
            Ok(ProjectedValue::borrowed(match value {
                None | Some(Value::Null) => default,
                Some(value) if value.as_ref() == empty => default,
                Some(value) => value,
            }))
        }
        ProjectionExpression::ColumnValueDefaultIfNull {
            column: name,
            default,
        } => {
            let value = column(binding, name)?;
            Ok(ProjectedValue::borrowed(if value == &Value::Null {
                default
            } else {
                value
            }))
        }
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column: name,
            empty,
            non_empty,
            null_or_empty,
        } => {
            let value = column(binding, name)?;
            Ok(ProjectedValue::borrowed(
                if value == &Value::Null || value == empty {
                    null_or_empty
                } else {
                    non_empty
                },
            ))
        }
        ProjectionExpression::Column(name) => column(binding, name).map(ProjectedValue::borrowed),
        ProjectionExpression::ColumnProperty {
            column: name,
            property,
        } => match column(binding, name)? {
            Value::Map(values) => Ok(values
                .get(property)
                .map(ProjectedValue::borrowed)
                .unwrap_or_else(|| ProjectedValue::scalar(Value::Null))),
            Value::Null => Ok(ProjectedValue::scalar(Value::Null)),
            value => Err(HawDBError::Execution(format!(
                "column property projection requires a map value, got {:?}",
                description(value, account)
            ))),
        },
    }
}

fn require_variable(binding: &Binding, variable: &str) -> Result<()> {
    if binding_declares_variable(binding, variable) {
        Ok(())
    } else {
        Err(HawDBError::Execution(format!(
            "missing variable '{variable}' during projection"
        )))
    }
}

fn column<'a>(binding: &'a Binding, name: &str) -> Result<&'a Value> {
    binding
        .values
        .get(name)
        .ok_or_else(|| HawDBError::Execution(format!("missing column '{name}' during projection")))
}

fn binary<'a>(
    left: &'a ProjectionExpression,
    op: ScalarBinaryOp,
    right: &'a ProjectionExpression,
    catalog: &'a Catalog,
    binding: &'a Binding,
    account: Option<&QueryMemoryAccount>,
) -> Result<ProjectedValue<'a>> {
    let left = evaluate_projection_borrowed(left, catalog, binding, account)?;
    if matches!(op, ScalarBinaryOp::And | ScalarBinaryOp::Or) {
        let left = scoped_truth(left.as_ref(), account)?;
        if (op == ScalarBinaryOp::And && left == PredicateTruth::False)
            || (op == ScalarBinaryOp::Or && left == PredicateTruth::True)
        {
            return Ok(ProjectedValue::scalar(truth_value(left)));
        }
        let right = evaluate_projection_borrowed(right, catalog, binding, account)?;
        let right = scoped_truth(right.as_ref(), account)?;
        return Ok(ProjectedValue::scalar(truth_value(
            if op == ScalarBinaryOp::And {
                left.and(right)
            } else {
                left.or(right)
            },
        )));
    }
    let right = evaluate_projection_borrowed(right, catalog, binding, account)?;
    if matches!(
        op,
        ScalarBinaryOp::Add
            | ScalarBinaryOp::Subtract
            | ScalarBinaryOp::Multiply
            | ScalarBinaryOp::Divide
            | ScalarBinaryOp::Remainder
    ) {
        return evaluate_arithmetic(left.as_ref(), op, right.as_ref()).map(ProjectedValue::scalar);
    }
    if op == ScalarBinaryOp::ListContains {
        return Ok(ProjectedValue::scalar(match left.as_ref() {
            Value::Null => Value::Null,
            Value::List(values) => Value::Bool(values.contains(right.as_ref())),
            _ => Value::Bool(false),
        }));
    }
    Ok(ProjectedValue::scalar(truth_value(
        predicate_comparison_truth(
            Some(left.as_ref()),
            right.as_ref(),
            |left, right| match op {
                ScalarBinaryOp::Eq => left == right,
                ScalarBinaryOp::NotEq => left != right,
                ScalarBinaryOp::Lt => compare_property_values(left, ComparisonOp::Lt, right),
                ScalarBinaryOp::Lte => compare_property_values(left, ComparisonOp::Lte, right),
                ScalarBinaryOp::Gt => compare_property_values(left, ComparisonOp::Gt, right),
                ScalarBinaryOp::Gte => compare_property_values(left, ComparisonOp::Gte, right),
                ScalarBinaryOp::Contains => {
                    matches!((left, right), (Value::String(left), Value::String(right)) if left.contains(right))
                }
                _ => unreachable!("handled before comparison"),
            },
        ),
    )))
}
