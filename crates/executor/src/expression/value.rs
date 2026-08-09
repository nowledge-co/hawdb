use super::*;

pub fn execute_aggregate(
    catalog: &Catalog,
    group_keys: &[Projection],
    items: &[Aggregation],
    input: &[Binding],
) -> Vec<Binding> {
    if group_keys.is_empty() {
        let mut values = BTreeMap::new();
        for item in items {
            insert_projected_value(
                &mut values,
                &item.name,
                aggregate_value(catalog, item, input),
            );
        }
        return vec![Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }];
    }

    let mut groups = BTreeMap::<Vec<Value>, Vec<&Binding>>::new();
    for binding in input {
        let key = group_keys
            .iter()
            .map(|item| group_key_value(item, catalog, binding))
            .collect::<Vec<_>>();
        groups.entry(key).or_default().push(binding);
    }

    groups
        .into_iter()
        .map(|(key, bindings)| {
            let mut values = BTreeMap::new();
            for (item, value) in group_keys.iter().zip(key) {
                insert_projected_value(&mut values, &item.name, value);
            }
            let group = bindings.into_iter().cloned().collect::<Vec<_>>();
            for item in items {
                insert_projected_value(
                    &mut values,
                    &item.name,
                    aggregate_value(catalog, item, &group),
                );
            }
            Binding {
                values,
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }
        })
        .collect()
}

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

pub fn distinct_bindings(input: Vec<Binding>) -> Vec<Binding> {
    let mut seen = std::collections::BTreeSet::new();
    let mut output = Vec::new();
    for binding in input {
        let key = binding
            .values
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        if seen.insert(key) {
            output.push(binding);
        }
    }
    output
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
        ProjectionExpression::Variable { variable } => binding_value(binding, catalog, variable)
            .ok_or_else(|| {
                SkeinError::Execution(format!("missing variable '{variable}' during projection"))
            }),
        ProjectionExpression::Property { variable, property } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null))
        }
        ProjectionExpression::Id { variable } => binding_id(binding, variable).ok_or_else(|| {
            SkeinError::Execution(format!("missing variable '{variable}' during projection"))
        }),
        ProjectionExpression::RelationshipType { variable } => {
            let relationship = binding.relationships.get(variable).ok_or_else(|| {
                SkeinError::Execution(format!("missing variable '{variable}' during projection"))
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
                value => Err(SkeinError::Execution(format!(
                    "LEFT expression requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::Lower(expression) => {
            match project_expression_value(expression, catalog, binding)? {
                Value::Null => Ok(Value::Null),
                Value::String(value) => Ok(Value::String(value.to_lowercase())),
                value => Err(SkeinError::Execution(format!(
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
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::Int(nanos)) => Ok(Value::Int(timestamp_date_part(*part, *nanos))),
                Some(Value::Null) | None => Ok(Value::Null),
                Some(value) => Err(SkeinError::Execution(format!(
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
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value == Value::Null || value == *empty {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value == Value::Null {
                Ok(default.clone())
            } else {
                Ok(value)
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
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value != Value::Null && value != *empty {
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
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            for (candidate, rank) in branches {
                if value == *candidate {
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
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::String(value)) => Ok(Value::String(value.to_lowercase())),
                Some(Value::Null) | None => Ok(default.clone()),
                Some(value) => Err(SkeinError::Execution(format!(
                    "CASE lower-default requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(Value::Int(
                coalesce_difference(binding, variable, terms)?.max(0),
            ))
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            if !binding_has_variable(binding, &expression.variable) {
                return Err(SkeinError::Execution(format!(
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
                _ => false,
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
                SkeinError::Execution(format!(
                    "missing column '{}' during projection",
                    expression.column
                ))
            })?;
            let Value::String(value) = column else {
                return Ok(expression.fallback_rank.clone());
            };
            if matches!(&expression.raw_query, Value::String(query) if value == query)
                || matches!(&expression.normalized_query, Value::String(query) if value == query)
            {
                return Ok(expression.exact_rank.clone());
            }
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
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            let value = match value {
                Value::Map(values) => values.get(property).cloned().unwrap_or(Value::Null),
                Value::Null => Value::Null,
                value => {
                    return Err(SkeinError::Execution(format!(
                        "column default expression requires a map value, got {value:?}"
                    )));
                }
            };
            if value == Value::Null || value == *empty {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::ColumnValueDefaultIfNull { column, default } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
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
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            if value == &Value::Null || value == empty {
                Ok(null_or_empty.clone())
            } else {
                Ok(non_empty.clone())
            }
        }
        ProjectionExpression::Column(name) => binding.values.get(name).cloned().ok_or_else(|| {
            SkeinError::Execution(format!("missing column '{name}' during projection"))
        }),
        ProjectionExpression::ColumnProperty { column, property } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            match value {
                Value::Map(values) => Ok(values.get(property).cloned().unwrap_or(Value::Null)),
                Value::Null => Ok(Value::Null),
                value => Err(SkeinError::Execution(format!(
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
        return Err(SkeinError::Execution(
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
        Some(value) => Err(SkeinError::Execution(format!(
            "COALESCE difference requires integer property '{}.{}', got {value:?}",
            variable, term.property
        ))),
    }
}

fn integer_value(value: &Value, context: &str) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        value => Err(SkeinError::Execution(format!(
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

pub fn binding_has_countable_variable(binding: &Binding, variable: &str) -> bool {
    binding
        .nodes
        .get(variable)
        .is_some_and(|node| !is_null_lookup_node(node))
        || binding.relationships.contains_key(variable)
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
        .map(|node| {
            if is_null_lookup_node(node) {
                Value::Null
            } else {
                node_value(node, catalog)
            }
        })
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
        .map(|node| {
            if is_null_lookup_node(node) {
                Value::Null
            } else {
                Value::Int(node.id.0 as i64)
            }
        })
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| Value::Int(relationship.id.0 as i64))
        })
}

fn aggregate_value(catalog: &Catalog, item: &Aggregation, input: &[Binding]) -> Value {
    match item.function {
        AggregateFunction::Count => {
            Value::Int(count_aggregate(&item.target, item.distinct, input) as i64)
        }
        AggregateFunction::Min => min_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Max => max_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Avg => avg_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Collect => {
            collect_aggregate(catalog, &item.target, item.distinct, input)
        }
    }
}

fn count_aggregate(target: &AggregateTarget, distinct: bool, input: &[Binding]) -> usize {
    if distinct {
        return count_distinct_aggregate(target, input);
    }
    match target {
        AggregateTarget::All => input.len(),
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter(|binding| binding_has_countable_variable(binding, variable))
            .count(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter(|binding| {
                binding_property(binding, variable, property)
                    .map(|value| value != &Value::Null)
                    .unwrap_or(false)
            })
            .count(),
    }
}

fn count_distinct_aggregate(target: &AggregateTarget, input: &[Binding]) -> usize {
    match target {
        AggregateTarget::All => input.len(),
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter_map(|binding| binding_identity_key(binding, variable))
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
    }
}

fn min_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    match target {
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .min(),
        AggregateTarget::All | AggregateTarget::Variable(_) => None,
    }
}

fn max_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    match target {
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .max(),
        AggregateTarget::All | AggregateTarget::Variable(_) => None,
    }
}

fn avg_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    let AggregateTarget::Property { variable, property } = target else {
        return None;
    };
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in input
        .iter()
        .filter_map(|binding| binding_property(binding, variable, property))
    {
        match value {
            Value::Int(value) => {
                sum += *value as f64;
                count += 1;
            }
            Value::Float(value) if value.is_finite() => {
                sum += *value;
                count += 1;
            }
            _ => {}
        }
    }
    (count > 0).then_some(Value::Float(sum / count as f64))
}

fn collect_aggregate(
    catalog: &Catalog,
    target: &AggregateTarget,
    distinct: bool,
    input: &[Binding],
) -> Value {
    let values: Vec<Value> = match target {
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter_map(|binding| binding_value(binding, catalog, variable))
            .filter(|value| *value != Value::Null)
            .collect(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .collect(),
        AggregateTarget::All => Vec::new(),
    };
    if distinct {
        Value::List(
            values
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
        )
    } else {
        Value::List(values)
    }
}

pub fn binding_identity_key(binding: &Binding, variable: &str) -> Option<(u8, u64)> {
    binding
        .nodes
        .get(variable)
        .filter(|node| !is_null_lookup_node(node))
        .map(|node| (0, node.id.0))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| (1, relationship.id.0))
        })
}
