use super::value::{binding_id, project_expression_value};
use super::*;

pub fn evaluate_predicate(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    binding: &Binding,
    observer: &dyn ExecutionObserver,
) -> Result<bool> {
    evaluate_predicate_with_memory(
        predicate,
        catalog,
        store,
        binding,
        observer,
        AdjacencyReadMemory {
            budget_bytes: DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES,
            account: None,
        },
    )
}

pub fn evaluate_predicate_with_memory(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    binding: &Binding,
    observer: &dyn ExecutionObserver,
    adjacency_memory: AdjacencyReadMemory<'_>,
) -> Result<bool> {
    let context = PredicateEvaluationContext {
        catalog,
        store,
        observer,
        adjacency_memory,
    };
    Ok(evaluate_predicate_truth(predicate, binding, &context)?.is_true())
}

struct PredicateEvaluationContext<'a> {
    catalog: &'a Catalog,
    store: &'a dyn GraphExecutionRead,
    observer: &'a dyn ExecutionObserver,
    adjacency_memory: AdjacencyReadMemory<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PredicateTruth {
    True,
    False,
    Unknown,
}

impl PredicateTruth {
    pub(super) const fn from_bool(value: bool) -> Self {
        if value {
            Self::True
        } else {
            Self::False
        }
    }

    pub(super) const fn is_true(self) -> bool {
        matches!(self, Self::True)
    }

    pub(super) const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    pub(super) const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::True, Self::True) => Self::True,
        }
    }

    pub(super) const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::False, Self::False) => Self::False,
        }
    }
}

pub(super) fn predicate_comparison_truth(
    actual: Option<&Value>,
    expected: &Value,
    compare: impl FnOnce(&Value, &Value) -> bool,
) -> PredicateTruth {
    match actual {
        Some(actual) if actual != &Value::Null && expected != &Value::Null => {
            PredicateTruth::from_bool(compare(actual, expected))
        }
        _ => PredicateTruth::Unknown,
    }
}

fn predicate_in_truth(actual: Option<&Value>, values: &[Value]) -> PredicateTruth {
    let Some(actual) = actual.filter(|actual| *actual != &Value::Null) else {
        return PredicateTruth::Unknown;
    };
    if values
        .iter()
        .any(|value| value != &Value::Null && value == actual)
    {
        PredicateTruth::True
    } else if values.iter().any(|value| value == &Value::Null) {
        PredicateTruth::Unknown
    } else {
        PredicateTruth::False
    }
}

fn evaluate_predicate_truth(
    predicate: &Predicate,
    binding: &Binding,
    context: &PredicateEvaluationContext<'_>,
) -> Result<PredicateTruth> {
    let catalog = context.catalog;
    Ok(match predicate {
        Predicate::And(predicates) => {
            let mut truth = PredicateTruth::True;
            for predicate in predicates {
                truth = truth.and(evaluate_predicate_truth(predicate, binding, context)?);
                if truth == PredicateTruth::False {
                    break;
                }
            }
            truth
        }
        Predicate::Or(predicates) => {
            let mut truth = PredicateTruth::False;
            for predicate in predicates {
                truth = truth.or(evaluate_predicate_truth(predicate, binding, context)?);
                if truth == PredicateTruth::True {
                    break;
                }
            }
            truth
        }
        Predicate::Not(predicate) => evaluate_predicate_truth(predicate, binding, context)?.not(),
        Predicate::ConstantBool(value) => PredicateTruth::from_bool(*value),
        Predicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => PredicateTruth::from_bool(relationship_exists(
            context,
            binding,
            variable,
            rel_type,
            *direction,
            target_label,
        )?),
        Predicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => PredicateTruth::from_bool(bound_relationship_exists(
            context,
            binding,
            source_variable,
            rel_type,
            *direction,
            target_variable,
        )?),
        Predicate::IdEq { variable, value } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                actual == expected
            })
        }
        Predicate::IdNotEq { variable, value } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                actual != expected
            })
        }
        Predicate::IdCompare {
            variable,
            op,
            value,
        } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                compare_property_values(actual, *op, expected)
            })
        }
        Predicate::IdIn { variable, values } => {
            let actual = binding_id(binding, variable);
            predicate_in_truth(actual.as_ref(), values)
        }
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| actual == expected,
        ),
        Predicate::PropertyNotEq {
            variable,
            property,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| actual != expected,
        ),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| compare_property_values(actual, *op, expected),
        ),
        Predicate::ExpressionEq { expression, value } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| actual == expected,
            )
        }
        Predicate::ExpressionNotEq { expression, value } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| actual != expected,
            )
        }
        Predicate::ExpressionCompare {
            expression,
            op,
            value,
        } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| compare_property_values(actual, *op, expected),
            )
        }
        Predicate::ExpressionContains { expression, value } => {
            match (
                predicate_expression_value(expression, catalog, binding),
                predicate_expression_value(value, catalog, binding),
            ) {
                (Some(Value::Null) | None, _) | (_, Some(Value::Null) | None) => {
                    PredicateTruth::Unknown
                }
                (Some(Value::String(actual)), Some(Value::String(expected))) => {
                    PredicateTruth::from_bool(actual.contains(&expected))
                }
                _ => PredicateTruth::False,
            }
        }
        Predicate::PropertyListContains {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::List(values)) => {
                PredicateTruth::from_bool(values.iter().any(|actual| actual == value))
            }
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyListContainsLower {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::List(values)) => {
                PredicateTruth::from_bool(values.iter().any(|actual| match actual {
                    Value::String(actual) => actual.to_lowercase().contains(value),
                    _ => false,
                }))
            }
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyContains {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.contains(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyStartsWith {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.starts_with(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyEndsWith {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.ends_with(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyRegexMatch {
            variable,
            property,
            pattern,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(pattern.is_match(actual)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyIsNull { variable, property } => PredicateTruth::from_bool(
            binding_property(binding, variable, property)
                .map(|actual| actual == &Value::Null)
                .unwrap_or(true),
        ),
        Predicate::PropertyIsNotNull { variable, property } => PredicateTruth::from_bool(
            binding_property(binding, variable, property)
                .map(|actual| actual != &Value::Null)
                .unwrap_or(false),
        ),
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => predicate_in_truth(binding_property(binding, variable, property), values),
    })
}

fn relationship_exists(
    context: &PredicateEvaluationContext<'_>,
    binding: &Binding,
    variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_label: &str,
) -> Result<bool> {
    let catalog = context.catalog;
    let store = context.store;
    let Some(source) = binding.nodes.get(variable) else {
        return Ok(false);
    };
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            return Ok(false);
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    let mut found = false;
    visit_one_hop_relationships_with_budget(
        store,
        OneHopRelationshipSpec {
            source: source.id,
            rel_type_id,
            target_label_ids: target_label_ids.as_deref(),
            rel_properties: &BTreeMap::new(),
            relationship_scan_filter: None,
            direction,
        },
        context.adjacency_memory,
        context.observer,
        &mut |_, _| {
            found = true;
            Ok(ScanControl::Stop)
        },
    )?;
    Ok(found)
}

fn bound_relationship_exists(
    context: &PredicateEvaluationContext<'_>,
    binding: &Binding,
    source_variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_variable: &str,
) -> Result<bool> {
    let catalog = context.catalog;
    let store = context.store;
    let (Some(source), Some(target)) = (
        binding.nodes.get(source_variable),
        binding.nodes.get(target_variable),
    ) else {
        return Ok(false);
    };
    let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
        return Ok(false);
    };
    crate::scan::adjacency_exists(store, source.id, target.id, rel_type_id, direction, None)
}

fn predicate_expression_value(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Option<Value> {
    project_expression_value(expression, catalog, binding).ok()
}

pub fn compare_bindings(
    catalog: &Catalog,
    left: &Binding,
    right: &Binding,
    items: &[SortItem],
) -> std::cmp::Ordering {
    for item in items {
        let ordering =
            sort_value(catalog, left, &item.key).cmp(&sort_value(catalog, right, &item.key));
        let ordering = match item.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

pub fn sort_value(catalog: &Catalog, binding: &Binding, key: &SortKey) -> Value {
    match key {
        SortKey::Property { variable, property } => binding_property(binding, variable, property)
            .cloned()
            .unwrap_or(Value::Null),
        SortKey::Id { variable } => binding_id(binding, variable).unwrap_or(Value::Null),
        SortKey::Expression(expression) => {
            project_expression_value(expression, catalog, binding).unwrap_or(Value::Null)
        }
        SortKey::Column(name) => binding.values.get(name).cloned().unwrap_or(Value::Null),
    }
}

pub fn property_filter_from_predicate(predicate: &Predicate) -> Result<PropertyFilter> {
    match predicate {
        Predicate::And(predicates) => predicates
            .iter()
            .map(property_filter_from_predicate)
            .collect::<Result<Vec<_>>>()
            .map(PropertyFilter::And),
        Predicate::Or(predicates) => predicates
            .iter()
            .map(property_filter_from_predicate)
            .collect::<Result<Vec<_>>>()
            .map(PropertyFilter::Or),
        Predicate::Not(predicate) => property_filter_from_predicate(predicate)
            .map(Box::new)
            .map(PropertyFilter::Not),
        Predicate::IdEq { value, .. } => Ok(PropertyFilter::IdEq {
            value: value.clone(),
        }),
        Predicate::IdNotEq { value, .. } => Ok(PropertyFilter::IdNotEq {
            value: value.clone(),
        }),
        Predicate::IdCompare { op, value, .. } => {
            let (lower, upper) = range_bounds_from_comparison(*op, value.clone());
            Ok(PropertyFilter::IdRange { lower, upper })
        }
        Predicate::IdIn { values, .. } => Ok(PropertyFilter::IdIn {
            values: values.clone(),
        }),
        Predicate::PropertyEq {
            property, value, ..
        } => Ok(PropertyFilter::Eq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyNotEq {
            property, value, ..
        } => Ok(PropertyFilter::NotEq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyCompare {
            property,
            op,
            value,
            ..
        } => {
            let (lower, upper) = range_bounds_from_comparison(*op, value.clone());
            Ok(PropertyFilter::Range {
                property: property.clone(),
                lower,
                upper,
            })
        }
        Predicate::ExpressionEq { expression, value } => {
            property_filter_from_default_expression(expression, value, false)
        }
        Predicate::ExpressionNotEq { expression, value } => {
            property_filter_from_default_expression(expression, value, true)
        }
        Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. }
        | Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. } => Err(SkeinError::Execution(
            "expression predicates are not supported in property filters".to_string(),
        )),
        Predicate::PropertyListContains {
            property, value, ..
        } => Ok(PropertyFilter::ListContains {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyListContainsLower {
            property, value, ..
        } => Ok(PropertyFilter::ListContainsLower {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyContains {
            property, value, ..
        } => Ok(PropertyFilter::Contains {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyStartsWith {
            property, value, ..
        } => Ok(PropertyFilter::StartsWith {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyEndsWith {
            property, value, ..
        } => Ok(PropertyFilter::EndsWith {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyRegexMatch {
            property, pattern, ..
        } => Ok(PropertyFilter::RegexMatch {
            property: property.clone(),
            pattern: pattern.clone(),
        }),
        Predicate::PropertyIsNull { property, .. } => Ok(PropertyFilter::IsNull {
            property: property.clone(),
        }),
        Predicate::PropertyIsNotNull { property, .. } => Ok(PropertyFilter::IsNotNull {
            property: property.clone(),
        }),
        Predicate::PropertyIn {
            property, values, ..
        } => Ok(PropertyFilter::In {
            property: property.clone(),
            values: values.clone(),
        }),
    }
}

pub fn predicate_references_only_variable(predicate: &Predicate, variable: &str) -> bool {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .all(|predicate| predicate_references_only_variable(predicate, variable)),
        Predicate::Not(predicate) => predicate_references_only_variable(predicate, variable),
        Predicate::IdEq {
            variable: current, ..
        }
        | Predicate::IdNotEq {
            variable: current, ..
        }
        | Predicate::IdCompare {
            variable: current, ..
        }
        | Predicate::IdIn {
            variable: current, ..
        }
        | Predicate::PropertyEq {
            variable: current, ..
        }
        | Predicate::PropertyNotEq {
            variable: current, ..
        }
        | Predicate::PropertyCompare {
            variable: current, ..
        }
        | Predicate::PropertyListContains {
            variable: current, ..
        }
        | Predicate::PropertyListContainsLower {
            variable: current, ..
        }
        | Predicate::PropertyContains {
            variable: current, ..
        }
        | Predicate::PropertyStartsWith {
            variable: current, ..
        }
        | Predicate::PropertyEndsWith {
            variable: current, ..
        }
        | Predicate::PropertyRegexMatch {
            variable: current, ..
        }
        | Predicate::PropertyIsNull {
            variable: current, ..
        }
        | Predicate::PropertyIsNotNull {
            variable: current, ..
        }
        | Predicate::PropertyIn {
            variable: current, ..
        } => current == variable,
        Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. }
        | Predicate::ExpressionEq { .. }
        | Predicate::ExpressionNotEq { .. }
        | Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. } => false,
    }
}

pub fn node_scan_filter_from_predicate(
    predicate: &Predicate,
    variable: &str,
) -> Option<PropertyFilter> {
    if let Predicate::And(predicates) = predicate {
        let mut filters = predicates
            .iter()
            .filter_map(|predicate| node_scan_filter_from_predicate(predicate, variable))
            .collect::<Vec<_>>();
        return match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(PropertyFilter::And(filters)),
        };
    }
    predicate_references_only_variable(predicate, variable)
        .then(|| property_filter_from_predicate(predicate).ok())
        .flatten()
}

pub fn exact_relationship_scan_filter_from_predicate(
    predicate: &Predicate,
    variable: &str,
) -> Option<PropertyFilter> {
    if let Predicate::And(predicates) = predicate {
        let mut filters = predicates
            .iter()
            .filter_map(|predicate| {
                exact_relationship_scan_filter_from_predicate(predicate, variable)
            })
            .collect::<Vec<_>>();
        return match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(PropertyFilter::And(filters)),
        };
    }
    if predicate_references_only_variable(predicate, variable) {
        return property_filter_from_predicate(predicate)
            .ok()
            .filter(exact_relationship_scan_filter_is_safe);
    }
    None
}

fn exact_relationship_scan_filter_is_safe(filter: &PropertyFilter) -> bool {
    match filter {
        PropertyFilter::And(filters) | PropertyFilter::Or(filters) => {
            filters.iter().all(exact_relationship_scan_filter_is_safe)
        }
        PropertyFilter::Eq { .. }
        | PropertyFilter::IdEq { .. }
        | PropertyFilter::IdRange { .. }
        | PropertyFilter::IdIn { .. }
        | PropertyFilter::IsNull { .. }
        | PropertyFilter::IsNotNull { .. }
        | PropertyFilter::In { .. }
        | PropertyFilter::Range { .. } => true,
        PropertyFilter::DefaultIfNullOrEq { negated, .. } => !negated,
        PropertyFilter::Not(_)
        | PropertyFilter::IdNotEq { .. }
        | PropertyFilter::NotEq { .. }
        | PropertyFilter::ListContains { .. }
        | PropertyFilter::ListContainsLower { .. }
        | PropertyFilter::Contains { .. }
        | PropertyFilter::StartsWith { .. }
        | PropertyFilter::EndsWith { .. }
        | PropertyFilter::RegexMatch { .. } => false,
    }
}

fn property_filter_from_default_expression(
    expression: &ProjectionExpression,
    value: &ProjectionExpression,
    negated: bool,
) -> Result<PropertyFilter> {
    match (expression, value) {
        (
            ProjectionExpression::DefaultIfNullOrEq {
                property,
                empty,
                default,
                ..
            },
            ProjectionExpression::Literal(value),
        ) => Ok(PropertyFilter::DefaultIfNullOrEq {
            property: property.clone(),
            empty: empty.clone(),
            default: default.clone(),
            value: value.clone(),
            negated,
        }),
        (
            ProjectionExpression::Literal(value),
            ProjectionExpression::DefaultIfNullOrEq {
                property,
                empty,
                default,
                ..
            },
        ) => Ok(PropertyFilter::DefaultIfNullOrEq {
            property: property.clone(),
            empty: empty.clone(),
            default: default.clone(),
            value: value.clone(),
            negated,
        }),
        _ => Err(SkeinError::Execution(
            "expression predicates are not supported in property filters".to_string(),
        )),
    }
}

pub fn relationship_filter_from_properties_and_predicate(
    properties: &BTreeMap<String, Value>,
    predicate: Option<&Predicate>,
) -> Result<Option<PropertyFilter>> {
    Ok(combine_property_filters(
        property_filter_from_properties(properties),
        predicate.map(property_filter_from_predicate).transpose()?,
    ))
}

fn range_bounds_from_comparison(op: ComparisonOp, value: Value) -> ValueRangeBounds {
    match op {
        ComparisonOp::Lt => (None, Some((value, false))),
        ComparisonOp::Lte => (None, Some((value, true))),
        ComparisonOp::Gt => (Some((value, false)), None),
        ComparisonOp::Gte => (Some((value, true)), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluates_nested_projection_expression_without_rebuilding_projection() {
        let expression = ProjectionExpression::Coalesce(vec![
            ProjectionExpression::Literal(Value::Null),
            ProjectionExpression::Lower(Box::new(ProjectionExpression::Left {
                expression: Box::new(ProjectionExpression::Literal(Value::String(
                    "SKEIN".to_string(),
                ))),
                length: 3,
            })),
        ]);
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };

        let value = evaluate_projection_expression(&expression, &Catalog::default(), &binding)
            .expect("nested projection expression should evaluate");

        assert_eq!(value, Value::String("ske".to_string()));
    }

    #[test]
    fn lowers_conjunctive_predicates_into_storage_filters() {
        let predicate = Predicate::And(vec![
            Predicate::PropertyEq {
                variable: "memory".to_string(),
                property: "space".to_string(),
                value: Value::String("default".to_string()),
            },
            Predicate::PropertyCompare {
                variable: "memory".to_string(),
                property: "importance".to_string(),
                op: ComparisonOp::Gte,
                value: Value::Float(0.5),
            },
        ]);

        let filter = property_filter_from_predicate(&predicate).expect("lowered filter");

        assert!(matches!(
            filter,
            PropertyFilter::And(filters)
                if matches!(filters.as_slice(), [
                    PropertyFilter::Eq { property, .. },
                    PropertyFilter::Range {
                        property: range_property,
                        lower: Some((Value::Float(value), true)),
                        upper: None,
                    },
                ] if property == "space" && range_property == "importance" && *value == 0.5)
        ));
    }
}
