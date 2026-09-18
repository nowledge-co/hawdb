// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::predicate::{predicate_comparison_truth, PredicateTruth};
use super::*;
use hawdb_plan::ScalarBinaryOp;

pub fn insert_projected_value(values: &mut BTreeMap<String, Value>, name: &str, value: Value) {
    let mut candidate = name.to_string();
    let mut suffix = 2;
    loop {
        match values.entry(candidate) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
                return;
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                candidate = format!("{name}#{suffix}");
                suffix += 1;
            }
        }
    }
}

pub fn project_value(item: &Projection, catalog: &Catalog, binding: &Binding) -> Result<Value> {
    evaluate_projection_expression(&item.expression, catalog, binding)
}

pub fn evaluate_projection_expression(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    match expression {
        ProjectionExpression::Case {
            operand,
            branches,
            otherwise,
        } => {
            let operand = operand
                .as_deref()
                .map(|value| project_expression_value(value, catalog, binding))
                .transpose()?;
            for (condition, result) in branches {
                let condition = project_expression_value(condition, catalog, binding)?;
                let matches = if let Some(operand) = &operand {
                    predicate_comparison_truth(Some(operand), &condition, |left, right| {
                        left == right
                    })
                    .is_true()
                } else {
                    scalar_truth(condition)?.is_true()
                };
                if matches {
                    return project_expression_value(result, catalog, binding);
                }
            }
            otherwise
                .as_deref()
                .map(|value| project_expression_value(value, catalog, binding))
                .transpose()
                .map(|value| value.unwrap_or(Value::Null))
        }
        ProjectionExpression::Binary { left, op, right } => {
            evaluate_scalar_binary(left, *op, right, catalog, binding)
        }
        ProjectionExpression::Not(child) => Ok(truth_value(
            scalar_truth(project_expression_value(child, catalog, binding)?)?.not(),
        )),
        ProjectionExpression::IsNull {
            expression,
            negated,
        } => Ok(Value::Bool(
            (project_expression_value(expression, catalog, binding)? == Value::Null) != *negated,
        )),
        ProjectionExpression::Variable { variable } => binding_value(binding, catalog, variable)
            .ok_or_else(|| {
                HawDBError::Execution(format!("missing variable '{variable}' during projection"))
            }),
        ProjectionExpression::Property { variable, property } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null))
        }
        ProjectionExpression::Id { variable } => binding_id(binding, variable).ok_or_else(|| {
            HawDBError::Execution(format!("missing variable '{variable}' during projection"))
        }),
        ProjectionExpression::RelationshipType { variable } => {
            let relationship = binding.relationships.get(variable).ok_or_else(|| {
                HawDBError::Execution(format!("missing variable '{variable}' during projection"))
            })?;
            Ok(catalog
                .rel_type_name(relationship.rel_type)
                .map(|rel_type| Value::String(rel_type.to_string()))
                .unwrap_or(Value::Null))
        }
        ProjectionExpression::Literal(value) => Ok(value.clone()),
        ProjectionExpression::Coalesce(expressions) => {
            for expression in expressions {
                let value = project_expression_value(expression, catalog, binding)?;
                if value != Value::Null {
                    return Ok(value);
                }
            }
            Ok(Value::Null)
        }
        ProjectionExpression::Left { expression, length } => {
            match project_expression_value(expression, catalog, binding)? {
                Value::Null => Ok(Value::Null),
                Value::String(value) => Ok(Value::String(value.chars().take(*length).collect())),
                value => Err(HawDBError::Execution(format!(
                    "LEFT expression requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::Lower(expression) => {
            match project_expression_value(expression, catalog, binding)? {
                Value::Null => Ok(Value::Null),
                Value::String(value) => Ok(Value::String(value.to_lowercase())),
                value => Err(HawDBError::Execution(format!(
                    "LOWER expression requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::DatePart {
            part,
            variable,
            property,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::Int(nanos)) => Ok(Value::Int(timestamp_date_part(*part, *nanos))),
                Some(Value::Null) | None => Ok(Value::Null),
                Some(value) => Err(HawDBError::Execution(format!(
                    "date_part requires an integer timestamp value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .map(Value::as_ref)
                .unwrap_or(ValueRef::Null);
            if value.is_null() || value == empty {
                Ok(default.clone())
            } else {
                Ok(value.to_owned_value())
            }
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .map(Value::as_ref)
                .unwrap_or(ValueRef::Null);
            if value.is_null() {
                Ok(default.clone())
            } else {
                Ok(value.to_owned_value())
            }
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .map(Value::as_ref)
                .unwrap_or(ValueRef::Null);
            if !value.is_null() && value != empty {
                Ok(non_empty.clone())
            } else {
                Ok(null_or_empty.clone())
            }
        }
        ProjectionExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .map(Value::as_ref)
                .unwrap_or(ValueRef::Null);
            for (candidate, rank) in branches {
                if value == candidate {
                    return Ok(rank.clone());
                }
            }
            Ok(default.clone())
        }
        ProjectionExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::String(value)) => Ok(Value::String(value.to_lowercase())),
                Some(Value::Null) | None => Ok(default.clone()),
                Some(value) => Err(HawDBError::Execution(format!(
                    "CASE lower-default requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !binding_has_variable(binding, variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(Value::Int(
                coalesce_difference(binding, variable, terms)?.max(0),
            ))
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            if !binding_has_variable(binding, &expression.variable) {
                return Err(HawDBError::Execution(format!(
                    "missing variable '{}' during projection",
                    expression.variable
                )));
            }
            let name_matches = match binding_property(
                binding,
                &expression.variable,
                &expression.name_property,
            ) {
                Some(Value::String(name)) => {
                    let lowered = name.to_lowercase();
                    matches!(&expression.raw_query, Value::String(query) if lowered == *query)
                        || matches!(&expression.normalized_query, Value::String(query) if lowered == *query)
                }
                None | Some(Value::Null) => false,
                Some(value) => {
                    return Err(HawDBError::Execution(format!(
                        "LOWER expression requires a string value, got {value:?}"
                    )))
                }
            };
            if name_matches {
                return Ok(expression.exact_rank.clone());
            }
            let alias_matches =
                match binding_property(binding, &expression.variable, &expression.aliases_property)
                {
                    Some(Value::List(values)) => {
                        values.iter().any(|alias| alias == &expression.raw_input)
                    }
                    _ => false,
                };
            if alias_matches {
                Ok(expression.alias_rank.clone())
            } else {
                Ok(expression.fallback_rank.clone())
            }
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            let column = binding.values.get(&expression.column).ok_or_else(|| {
                HawDBError::Execution(format!(
                    "missing column '{}' during projection",
                    expression.column
                ))
            })?;
            let matches = |query: &Value| {
                predicate_comparison_truth(Some(column), query, |left, right| left == right)
                    .is_true()
            };
            if matches(&expression.raw_query) || matches(&expression.normalized_query) {
                return Ok(expression.exact_rank.clone());
            }
            let Value::String(value) = column else {
                return Ok(expression.fallback_rank.clone());
            };
            if matches!(&expression.raw_query, Value::String(query) if value.contains(query))
                || matches!(&expression.normalized_query, Value::String(query) if value.contains(query))
            {
                Ok(expression.contains_rank.clone())
            } else {
                Ok(expression.fallback_rank.clone())
            }
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column,
            property,
            empty,
            default,
        } => {
            let value = binding.values.get(column).ok_or_else(|| {
                HawDBError::Execution(format!("missing column '{column}' during projection"))
            })?;
            let value = match value {
                Value::Map(values) => values
                    .get(property)
                    .map(Value::as_ref)
                    .unwrap_or(ValueRef::Null),
                Value::Null => ValueRef::Null,
                value => {
                    return Err(HawDBError::Execution(format!(
                        "column default expression requires a map value, got {value:?}"
                    )));
                }
            };
            if value.is_null() || value == empty {
                Ok(default.clone())
            } else {
                Ok(value.to_owned_value())
            }
        }
        ProjectionExpression::ColumnValueDefaultIfNull { column, default } => {
            let value = binding.values.get(column).ok_or_else(|| {
                HawDBError::Execution(format!("missing column '{column}' during projection"))
            })?;
            if value == &Value::Null {
                Ok(default.clone())
            } else {
                Ok(value.clone())
            }
        }
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column,
            empty,
            non_empty,
            null_or_empty,
        } => {
            let value = binding.values.get(column).ok_or_else(|| {
                HawDBError::Execution(format!("missing column '{column}' during projection"))
            })?;
            if value == &Value::Null || value == empty {
                Ok(null_or_empty.clone())
            } else {
                Ok(non_empty.clone())
            }
        }
        ProjectionExpression::Column(name) => binding.values.get(name).cloned().ok_or_else(|| {
            HawDBError::Execution(format!("missing column '{name}' during projection"))
        }),
        ProjectionExpression::ColumnProperty { column, property } => {
            let value = binding.values.get(column).ok_or_else(|| {
                HawDBError::Execution(format!("missing column '{column}' during projection"))
            })?;
            match value {
                Value::Map(values) => Ok(values.get(property).cloned().unwrap_or(Value::Null)),
                Value::Null => Ok(Value::Null),
                value => Err(HawDBError::Execution(format!(
                    "column property projection requires a map value, got {value:?}"
                ))),
            }
        }
    }
}

pub(super) fn project_expression_value(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    evaluate_projection_expression(expression, catalog, binding)
}

fn coalesce_difference(
    binding: &Binding,
    variable: &str,
    terms: &[CoalesceDifferenceProjectionTerm],
) -> Result<i64> {
    let Some((first, rest)) = terms.split_first() else {
        return Err(HawDBError::Execution(
            "coalesce difference requires at least one term".to_string(),
        ));
    };
    let mut value = coalesce_integer_term(binding, variable, first)?;
    for term in rest {
        value -= coalesce_integer_term(binding, variable, term)?;
    }
    Ok(value)
}

fn coalesce_integer_term(
    binding: &Binding,
    variable: &str,
    term: &CoalesceDifferenceProjectionTerm,
) -> Result<i64> {
    match binding_property(binding, variable, &term.property) {
        Some(Value::Int(value)) => Ok(*value),
        Some(Value::Null) | None => integer_value(&term.default, "COALESCE default"),
        Some(value) => Err(HawDBError::Execution(format!(
            "COALESCE difference requires integer property '{}.{}', got {value:?}",
            variable, term.property
        ))),
    }
}

fn integer_value(value: &Value, context: &str) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        value => Err(HawDBError::Execution(format!(
            "{context} requires an integer value, got {value:?}"
        ))),
    }
}

pub fn group_key_value(item: &Projection, catalog: &Catalog, binding: &Binding) -> Value {
    project_value(item, catalog, binding).unwrap_or(Value::Null)
}

fn timestamp_date_part(part: DatePart, nanos: i64) -> i64 {
    let days = div_floor(nanos, 86_400_000_000_000);
    let (year, month, _) = civil_from_days(days);
    match part {
        DatePart::Year => year as i64,
        DatePart::Month => month as i64,
    }
}

fn div_floor(value: i64, divisor: i64) -> i64 {
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && ((remainder < 0) != (divisor < 0)) {
        quotient - 1
    } else {
        quotient
    }
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

pub fn binding_has_variable(binding: &Binding, variable: &str) -> bool {
    binding.nodes.contains_key(variable) || binding.relationships.contains_key(variable)
}

pub fn binding_property<'a>(
    binding: &'a Binding,
    variable: &str,
    property: &str,
) -> Option<&'a Value> {
    binding
        .nodes
        .get(variable)
        .and_then(|node| node.properties.get(property))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .and_then(|relationship| relationship.properties.get(property))
        })
}

pub fn binding_value(binding: &Binding, catalog: &Catalog, variable: &str) -> Option<Value> {
    binding
        .nodes
        .get(variable)
        .map(|node| node_value(node, catalog))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| relationship_value(relationship, catalog))
        })
}

fn node_value(node: &NodeRecord, catalog: &Catalog) -> Value {
    let mut values = node.properties.clone();
    values.insert("_id".to_string(), Value::Int(node.id.0 as i64));
    values.insert(
        "labels".to_string(),
        Value::List(
            node.labels
                .iter()
                .filter_map(|label_id| catalog.label_name(*label_id))
                .map(|label| Value::String(label.to_string()))
                .collect(),
        ),
    );
    Value::Map(values)
}

fn relationship_value(relationship: &RelRecord, catalog: &Catalog) -> Value {
    let mut values = relationship.properties.clone();
    values.insert("_id".to_string(), Value::Int(relationship.id.0 as i64));
    values.insert(
        "source_id".to_string(),
        Value::Int(relationship.source.0 as i64),
    );
    values.insert(
        "target_id".to_string(),
        Value::Int(relationship.target.0 as i64),
    );
    values.insert(
        "type".to_string(),
        catalog
            .rel_type_name(relationship.rel_type)
            .map(|rel_type| Value::String(rel_type.to_string()))
            .unwrap_or(Value::Null),
    );
    Value::Map(values)
}

pub(super) fn binding_id(binding: &Binding, variable: &str) -> Option<Value> {
    binding
        .nodes
        .get(variable)
        .map(|node| Value::Int(node.id.0 as i64))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| Value::Int(relationship.id.0 as i64))
        })
}

pub fn binding_identity_key(binding: &Binding, variable: &str) -> Option<(u8, u64)> {
    binding
        .nodes
        .get(variable)
        .map(|node| (0, node.id.0))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| (1, relationship.id.0))
        })
}

fn scalar_truth(value: Value) -> Result<PredicateTruth> {
    match value {
        Value::Bool(value) => Ok(PredicateTruth::from_bool(value)),
        Value::Null => Ok(PredicateTruth::Unknown),
        value => Err(HawDBError::Execution(format!(
            "CASE condition requires a boolean value, got {value:?}"
        ))),
    }
}

fn truth_value(truth: PredicateTruth) -> Value {
    match truth {
        PredicateTruth::True => Value::Bool(true),
        PredicateTruth::False => Value::Bool(false),
        PredicateTruth::Unknown => Value::Null,
    }
}

fn evaluate_scalar_binary(
    left: &ProjectionExpression,
    op: ScalarBinaryOp,
    right: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    let left = project_expression_value(left, catalog, binding)?;
    if matches!(op, ScalarBinaryOp::And | ScalarBinaryOp::Or) {
        let left = scalar_truth(left)?;
        if (op == ScalarBinaryOp::And && left == PredicateTruth::False)
            || (op == ScalarBinaryOp::Or && left == PredicateTruth::True)
        {
            return Ok(truth_value(left));
        }
        let right = scalar_truth(project_expression_value(right, catalog, binding)?)?;
        return Ok(truth_value(if op == ScalarBinaryOp::And {
            left.and(right)
        } else {
            left.or(right)
        }));
    }
    let right = project_expression_value(right, catalog, binding)?;
    if op == ScalarBinaryOp::ListContains {
        return Ok(match left {
            Value::Null => Value::Null,
            Value::List(values) => Value::Bool(values.contains(&right)),
            _ => Value::Bool(false),
        });
    }
    Ok(truth_value(predicate_comparison_truth(
        Some(&left),
        &right,
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
            ScalarBinaryOp::ListContains | ScalarBinaryOp::And | ScalarBinaryOp::Or => {
                unreachable!("handled before comparison")
            }
        },
    )))
}

#[cfg(test)]
mod case_tests {
    use super::*;

    fn literal(value: Value) -> ProjectionExpression {
        ProjectionExpression::Literal(value)
    }
    fn evaluate(expression: &ProjectionExpression) -> Result<Value> {
        evaluate_projection_expression(
            expression,
            &Catalog::default(),
            &Binding::values(BTreeMap::new()),
        )
    }
    fn invalid() -> ProjectionExpression {
        ProjectionExpression::Lower(Box::new(literal(Value::Int(7))))
    }

    #[test]
    fn derived_column_rank_matches_generic_case_across_types_and_missing_values() {
        let values = [
            Value::Null,
            Value::String("first".into()),
            Value::String("first second".into()),
            Value::Int(1),
            Value::Bool(true),
            Value::Float(1.5),
            Value::List(vec![Value::Int(1)]),
        ];
        for raw in &values {
            for normalized in &values {
                let binary = |op, value: &Value| ProjectionExpression::Binary {
                    left: Box::new(ProjectionExpression::Column("value".into())),
                    op,
                    right: Box::new(literal(value.clone())),
                };
                let generic = ProjectionExpression::Case {
                    operand: None,
                    branches: vec![
                        (binary(ScalarBinaryOp::Eq, raw), literal(Value::Int(3))),
                        (
                            binary(ScalarBinaryOp::Eq, normalized),
                            literal(Value::Int(3)),
                        ),
                        (
                            binary(ScalarBinaryOp::Contains, raw),
                            literal(Value::Int(2)),
                        ),
                        (
                            binary(ScalarBinaryOp::Contains, normalized),
                            literal(Value::Int(2)),
                        ),
                    ],
                    otherwise: Some(Box::new(literal(Value::Int(1)))),
                };
                let specialized = ProjectionExpression::CaseColumnSearchRank(Box::new(
                    hawdb_plan::CaseColumnSearchRankProjection {
                        column: "value".into(),
                        raw_query: raw.clone(),
                        normalized_query: normalized.clone(),
                        exact_rank: Value::Int(3),
                        contains_rank: Value::Int(2),
                        fallback_rank: Value::Int(1),
                    },
                ));
                for column in values.iter().map(Some).chain([None]) {
                    let binding = Binding::values(
                        column
                            .map(|value| BTreeMap::from([("value".into(), value.clone())]))
                            .unwrap_or_default(),
                    );
                    let evaluate = |expression| {
                        evaluate_projection_expression(expression, &Catalog::default(), &binding)
                            .map_err(|error| error.to_string())
                    };
                    assert_eq!(
                        evaluate(&specialized),
                        evaluate(&generic),
                        "raw={raw:?} normalized={normalized:?} column={column:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn derived_entity_rank_matches_generic_case_for_values_nulls_and_errors() {
        let property = |property: &str| ProjectionExpression::Property {
            variable: "n".into(),
            property: property.into(),
        };
        let comparison = |query: Value| ProjectionExpression::Binary {
            left: Box::new(ProjectionExpression::Lower(Box::new(property("name")))),
            op: ScalarBinaryOp::Eq,
            right: Box::new(literal(query)),
        };
        let raw = Value::String("first".into());
        let normalized = Value::String("second".into());
        let alias = Value::String("alias".into());
        let generic = ProjectionExpression::Case {
            operand: None,
            branches: vec![
                (comparison(raw.clone()), literal(Value::Int(0))),
                (comparison(normalized.clone()), literal(Value::Int(0))),
                (
                    ProjectionExpression::Binary {
                        left: Box::new(property("aliases")),
                        op: ScalarBinaryOp::ListContains,
                        right: Box::new(literal(alias.clone())),
                    },
                    literal(Value::Int(1)),
                ),
            ],
            otherwise: Some(Box::new(literal(Value::Int(2)))),
        };
        let specialized = ProjectionExpression::CaseEntitySearchRank(Box::new(
            hawdb_plan::CaseEntitySearchRankProjection {
                variable: "n".into(),
                name_property: "name".into(),
                aliases_property: "aliases".into(),
                raw_query: raw,
                normalized_query: normalized,
                raw_input: alias.clone(),
                exact_rank: Value::Int(0),
                alias_rank: Value::Int(1),
                fallback_rank: Value::Int(2),
            },
        ));
        for name in [
            None,
            Some(Value::Null),
            Some(Value::String("FIRST".into())),
            Some(Value::String("SECOND".into())),
            Some(Value::String("other".into())),
            Some(Value::Int(5)),
            Some(Value::Bool(false)),
        ] {
            for aliases in [
                Value::Null,
                Value::List(vec![alias.clone()]),
                Value::List(vec![]),
                Value::Int(7),
            ] {
                let mut properties = BTreeMap::from([("aliases".into(), aliases)]);
                if let Some(name) = &name {
                    properties.insert("name".into(), name.clone());
                }
                let mut binding = Binding::values(BTreeMap::new());
                binding.nodes.insert(
                    "n".into(),
                    NodeRecord {
                        id: hawdb_storage::NodeId(1),
                        labels: Default::default(),
                        properties,
                    },
                );
                let evaluate = |expression| {
                    evaluate_projection_expression(expression, &Catalog::default(), &binding)
                        .map_err(|error| error.to_string())
                };
                assert_eq!(evaluate(&specialized), evaluate(&generic));
            }
        }
    }

    #[test]
    fn case_is_lazy_and_null_does_not_select_a_branch() {
        let expression = ProjectionExpression::Case {
            operand: None,
            branches: vec![
                (literal(Value::Null), invalid()),
                (literal(Value::Bool(true)), literal(Value::Int(9))),
                (invalid(), invalid()),
            ],
            otherwise: Some(Box::new(invalid())),
        };
        assert_eq!(evaluate(&expression).unwrap(), Value::Int(9));
        let expression = ProjectionExpression::Case {
            operand: None,
            branches: vec![(literal(Value::Bool(false)), invalid())],
            otherwise: None,
        };
        assert_eq!(evaluate(&expression).unwrap(), Value::Null);
    }

    #[test]
    fn simple_case_null_does_not_equal_null_and_selected_errors_propagate() {
        let expression = ProjectionExpression::Case {
            operand: Some(Box::new(literal(Value::Null))),
            branches: vec![(literal(Value::Null), invalid())],
            otherwise: Some(Box::new(literal(Value::Int(4)))),
        };
        assert_eq!(evaluate(&expression).unwrap(), Value::Int(4));
        for condition in [literal(Value::Int(1)), literal(Value::Bool(true))] {
            let expression = ProjectionExpression::Case {
                operand: None,
                branches: vec![(condition, invalid())],
                otherwise: None,
            };
            assert!(evaluate(&expression).is_err());
        }
    }

    #[test]
    fn conditional_boolean_operators_preserve_three_valued_truth_and_short_circuit() {
        let values = [Value::Bool(false), Value::Bool(true), Value::Null];
        let and = [
            [Some(false), Some(false), Some(false)],
            [Some(false), Some(true), None],
            [Some(false), None, None],
        ];
        let or = [
            [Some(false), Some(true), None],
            [Some(true), Some(true), Some(true)],
            [None, Some(true), None],
        ];
        for (op, truth) in [(ScalarBinaryOp::And, and), (ScalarBinaryOp::Or, or)] {
            for (i, left) in values.iter().enumerate() {
                for (j, right) in values.iter().enumerate() {
                    let expression = ProjectionExpression::Binary {
                        left: Box::new(literal(left.clone())),
                        op,
                        right: Box::new(literal(right.clone())),
                    };
                    assert_eq!(
                        evaluate(&expression).unwrap(),
                        truth[i][j].map(Value::Bool).unwrap_or(Value::Null)
                    );
                }
            }
        }
        for (op, left) in [(ScalarBinaryOp::And, false), (ScalarBinaryOp::Or, true)] {
            let expression = ProjectionExpression::Binary {
                left: Box::new(literal(Value::Bool(left))),
                op,
                right: Box::new(invalid()),
            };
            assert_eq!(evaluate(&expression).unwrap(), Value::Bool(left));
        }
        assert_eq!(
            evaluate(&ProjectionExpression::Not(Box::new(literal(Value::Null)))).unwrap(),
            Value::Null
        );
    }
}
