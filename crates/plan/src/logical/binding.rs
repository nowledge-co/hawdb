use super::*;

pub(super) fn plan_graph_algorithm_kind(kind: CypherGraphAlgorithmKind) -> GraphAlgorithmKind {
    match kind {
        CypherGraphAlgorithmKind::PageRank => GraphAlgorithmKind::PageRank,
        CypherGraphAlgorithmKind::Louvain => GraphAlgorithmKind::Louvain,
    }
}

pub(super) fn bind_graph_algorithm_options(
    options: &CypherGraphAlgorithmOptions,
    parameters: &BTreeMap<String, Value>,
) -> Result<GraphAlgorithmOptions> {
    Ok(GraphAlgorithmOptions {
        damping: options
            .damping
            .as_ref()
            .map(|value| bind_optional_f64(value, parameters, "damping"))
            .transpose()?,
        max_iterations: options
            .max_iterations
            .as_ref()
            .map(|value| bind_non_negative_usize(value, parameters, "maxIterations"))
            .transpose()?,
        max_levels: options
            .max_levels
            .as_ref()
            .map(|value| bind_non_negative_usize(value, parameters, "maxLevels"))
            .transpose()?,
    })
}

pub(super) fn bind_vector_embedding(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<(String, usize)> {
    let AstNode {
        kind: ValueExpressionKind::Parameter(name),
        ..
    } = expression
    else {
        return Err(SkeinError::Semantic(
            "vector search embedding must be a parameter".to_string(),
        ));
    };
    let Some(Value::List(values)) = parameters.get(name) else {
        return Err(SkeinError::Semantic(format!(
            "vector search parameter '${name}' must be a numeric list"
        )));
    };
    if values.is_empty() {
        return Err(SkeinError::Semantic(
            "vector search embedding must not be empty".to_string(),
        ));
    }
    if values.iter().any(|value| match value {
        Value::Float(value) => !value.is_finite(),
        Value::Int(_) => false,
        _ => true,
    }) {
        return Err(SkeinError::Semantic(format!(
            "vector search parameter '${name}' must contain finite numbers"
        )));
    }
    Ok((name.clone(), values.len()))
}

pub(super) fn bind_vector_seed(
    search: &CypherVectorSearch,
    parameters: &BTreeMap<String, Value>,
    output_external_id: bool,
) -> Result<LogicalPlan> {
    let (embedding_parameter, embedding_dimension) =
        bind_vector_embedding(&search.embedding, parameters)?;
    let top_k = search
        .top_k
        .as_ref()
        .map(|value| bind_non_negative_usize(value, parameters, "topK"))
        .transpose()?
        .unwrap_or(10);
    if top_k == 0 {
        return Err(SkeinError::Semantic(
            "vector search topK must be greater than zero".to_string(),
        ));
    }
    Ok(LogicalPlan::VectorSeed {
        embedding_parameter,
        embedding_dimension,
        top_k,
        output_external_id,
    })
}

pub(super) fn bind_properties(
    properties: &BTreeMap<String, ValueExpression>,
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    properties
        .iter()
        .map(|(name, value)| Ok((name.clone(), bind_value(value, parameters)?)))
        .collect()
}

pub(super) fn bind_relationship_count_legs(
    legs: &[skein_cypher::OptionalRelationshipCountLeg],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<RelationshipCountLeg>> {
    legs.iter()
        .map(|leg| {
            Ok(RelationshipCountLeg {
                rel_type: leg.rel_type.clone(),
                direction: leg.direction,
                distinct: leg.distinct,
                filter: leg
                    .filter
                    .as_ref()
                    .map(|filter| bind_relationship_count_filter(filter, parameters))
                    .transpose()?,
            })
        })
        .collect()
}

pub(super) fn bind_relationship_count_filter(
    filter: &skein_cypher::OptionalRelationshipCountFilter,
    parameters: &BTreeMap<String, Value>,
) -> Result<RelationshipCountFilter> {
    match filter {
        skein_cypher::OptionalRelationshipCountFilter::PropertyNotEqOrEmpty { property, value } => {
            Ok(RelationshipCountFilter::PropertyNotEqOrEmpty {
                property: property.clone(),
                value: bind_value(value, parameters)?,
            })
        }
    }
}

pub(super) fn bind_on_create_set_properties(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    if sets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "MERGE ON CREATE SET requires a bound node variable".to_string(),
        ));
    };
    let mut properties = BTreeMap::new();
    for set in sets {
        if set.variable != variable {
            return Err(SkeinError::Semantic(format!(
                "MERGE ON CREATE SET variable '{}' does not match bound variable '{variable}'",
                set.variable
            )));
        }
        let SetValueExpression::Value(value) = &set.value else {
            return Err(SkeinError::Semantic(
                "MERGE ON CREATE SET supports only value assignments".to_string(),
            ));
        };
        properties.insert(set.property.clone(), bind_value(value, parameters)?);
    }
    Ok(properties)
}

pub(super) fn bind_on_match_set_assignments(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<SetAssignment>> {
    if sets.is_empty() {
        return Ok(Vec::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "MERGE ON MATCH SET requires a bound node variable".to_string(),
        ));
    };
    sets.iter()
        .map(|set| {
            if set.variable != variable {
                return Err(SkeinError::Semantic(format!(
                    "MERGE ON MATCH SET variable '{}' does not match bound variable '{variable}'",
                    set.variable
                )));
            }
            Ok(SetAssignment {
                property: set.property.clone(),
                value: plan_set_value(set, parameters)?,
            })
        })
        .collect()
}

pub(super) fn bind_post_merge_set_assignments(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<SetAssignment>> {
    if sets.is_empty() {
        return Ok(Vec::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "MERGE SET requires a bound node variable".to_string(),
        ));
    };
    sets.iter()
        .map(|set| {
            if set.variable != variable {
                return Err(SkeinError::Semantic(format!(
                    "MERGE SET variable '{}' does not match bound variable '{variable}'",
                    set.variable
                )));
            }
            Ok(SetAssignment {
                property: set.property.clone(),
                value: plan_set_value(set, parameters)?,
            })
        })
        .collect()
}

pub(super) fn bind_relationship_on_create_set_properties(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    if sets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "relationship MERGE ON CREATE SET requires a bound relationship variable".to_string(),
        ));
    };
    let mut properties = BTreeMap::new();
    for set in sets {
        if set.variable != variable {
            return Err(SkeinError::Semantic(format!(
                "relationship MERGE ON CREATE SET variable '{}' does not match bound relationship variable '{variable}'",
                set.variable
            )));
        }
        let SetValueExpression::Value(value) = &set.value else {
            return Err(SkeinError::Semantic(
                "relationship MERGE ON CREATE SET supports only value assignments".to_string(),
            ));
        };
        properties.insert(set.property.clone(), bind_value(value, parameters)?);
    }
    Ok(properties)
}

pub(super) fn bind_relationship_copy_on_create_set_properties(
    new_variable: Option<&str>,
    old_variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, RelationshipOnCreateValue>> {
    if sets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(new_variable) = new_variable else {
        return Err(SkeinError::Semantic(
            "relationship-copy MERGE ON CREATE SET requires a bound new relationship variable"
                .to_string(),
        ));
    };
    let Some(old_variable) = old_variable else {
        return Err(SkeinError::Semantic(
            "relationship-copy MERGE ON CREATE SET requires a bound matched relationship variable"
                .to_string(),
        ));
    };
    let mut properties = BTreeMap::new();
    for set in sets {
        if set.variable != new_variable {
            return Err(SkeinError::Semantic(format!(
                "relationship-copy MERGE ON CREATE SET variable '{}' does not match bound relationship variable '{new_variable}'",
                set.variable
            )));
        }
        let value = match &set.value {
            SetValueExpression::Value(value) => {
                RelationshipOnCreateValue::Value(bind_value(value, parameters)?)
            }
            SetValueExpression::Property { variable, property } if variable == old_variable => {
                RelationshipOnCreateValue::MatchedRelationshipProperty {
                    property: property.clone(),
                }
            }
            SetValueExpression::Property { variable, .. } => {
                return Err(SkeinError::Semantic(format!(
                    "relationship-copy MERGE ON CREATE SET cannot read property from variable '{variable}'"
                )));
            }
            SetValueExpression::PropertyAdd { .. }
            | SetValueExpression::CoalesceProperty { .. }
            | SetValueExpression::DecrementFloorZero { .. }
            | SetValueExpression::PreserveNewerExisting { .. }
            | SetValueExpression::CoalescePropertyAdd { .. } => {
                return Err(SkeinError::Semantic(
                    "relationship-copy MERGE ON CREATE SET supports only values and matched relationship properties"
                        .to_string(),
                ));
            }
        };
        properties.insert(set.property.clone(), value);
    }
    Ok(properties)
}

pub(super) fn plan_match_pattern_predicate(
    source_variable: &str,
    source_properties: &BTreeMap<String, ValueExpression>,
    expand: Option<&CypherRelationshipExpand>,
    post_expand: Option<&skein_cypher::PostMatchRelationshipExpand>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let mut predicates =
        plan_node_pattern_predicates(source_variable, source_properties, parameters)?;
    if let Some(expand) = expand {
        predicates.extend(plan_node_pattern_predicates(
            &expand.target_variable,
            &expand.target_properties,
            parameters,
        )?);
    }
    if let Some(post_expand) = post_expand {
        predicates.extend(plan_node_pattern_predicates(
            &post_expand.source_variable,
            &post_expand.source_properties,
            parameters,
        )?);
        predicates.extend(plan_node_pattern_predicates(
            &post_expand.expand.target_variable,
            &post_expand.expand.target_properties,
            parameters,
        )?);
    }
    Ok(combine_predicates(predicates))
}

pub(super) fn plan_node_pattern_predicates(
    variable: &str,
    properties: &BTreeMap<String, ValueExpression>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<Predicate>> {
    bind_properties(properties, parameters).map(|properties| {
        properties
            .into_iter()
            .map(|(property, value)| Predicate::PropertyEq {
                variable: variable.to_string(),
                property,
                value,
            })
            .collect()
    })
}

pub(super) fn bind_two_node_relationship_create_filters(
    source_variable: &str,
    source_properties: &BTreeMap<String, ValueExpression>,
    target_variable: &str,
    target_properties: &BTreeMap<String, ValueExpression>,
    predicate: Option<&PropertyPredicate>,
    parameters: &BTreeMap<String, Value>,
) -> Result<(BTreeMap<String, Value>, BTreeMap<String, Value>)> {
    let mut source = bind_properties(source_properties, parameters)?;
    let mut target = bind_properties(target_properties, parameters)?;
    if let Some(predicate) = predicate {
        bind_endpoint_equality_predicate(
            predicate,
            source_variable,
            &mut source,
            target_variable,
            &mut target,
            parameters,
        )?;
    }
    Ok((source, target))
}

pub(super) fn bind_endpoint_equality_predicate(
    predicate: &PropertyPredicate,
    source_variable: &str,
    source: &mut BTreeMap<String, Value>,
    target_variable: &str,
    target: &mut BTreeMap<String, Value>,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    match predicate {
        PropertyPredicate::And(predicates) => {
            for predicate in predicates {
                bind_endpoint_equality_predicate(
                    predicate,
                    source_variable,
                    source,
                    target_variable,
                    target,
                    parameters,
                )?;
            }
            Ok(())
        }
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if variable == source_variable => {
            insert_endpoint_property(source, property, bind_value(value, parameters)?)
        }
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if variable == target_variable => {
            insert_endpoint_property(target, property, bind_value(value, parameters)?)
        }
        _ => Err(SkeinError::Semantic(
            "two-node relationship CREATE supports only AND-connected equality predicates on matched node properties".to_string(),
        )),
    }
}

pub(super) fn insert_endpoint_property(
    properties: &mut BTreeMap<String, Value>,
    property: &str,
    value: Value,
) -> Result<()> {
    if let Some(existing) = properties.get(property) {
        if existing != &value {
            return Err(SkeinError::Semantic(format!(
                "conflicting equality predicates for matched node property '{property}'"
            )));
        }
        return Ok(());
    }
    properties.insert(property.to_string(), value);
    Ok(())
}

pub(super) fn combine_pattern_and_optional_cypher_predicate(
    variable: &str,
    properties: &BTreeMap<String, ValueExpression>,
    predicate: Option<&PropertyPredicate>,
    scope: &BTreeSet<String>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let pattern_predicate =
        plan_match_pattern_predicate(variable, properties, None, None, parameters)?;
    combine_pattern_and_optional_cypher_predicate_parts(
        pattern_predicate,
        predicate,
        scope,
        parameters,
    )
}

pub(super) fn combine_pattern_and_optional_cypher_predicate_parts(
    pattern_predicate: Option<Predicate>,
    predicate: Option<&PropertyPredicate>,
    scope: &BTreeSet<String>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let planned_predicate = predicate
        .map(|predicate| plan_predicate(predicate, scope, parameters))
        .transpose()?;
    Ok(combine_optional_predicates(
        pattern_predicate,
        planned_predicate,
    ))
}

pub(super) fn combine_pattern_and_optional_predicate(
    variable: &str,
    properties: &BTreeMap<String, ValueExpression>,
    predicate: Option<Predicate>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let pattern_predicate =
        plan_match_pattern_predicate(variable, properties, None, None, parameters)?;
    Ok(combine_optional_predicates(pattern_predicate, predicate))
}

pub(super) fn combine_optional_predicates(
    left: Option<Predicate>,
    right: Option<Predicate>,
) -> Option<Predicate> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Predicate::And(vec![left, right])),
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (None, None) => None,
    }
}

pub(super) fn pushdown_relationship_property_eq_predicates(
    input: &mut LogicalPlan,
    predicate: Predicate,
) -> Option<Predicate> {
    let predicates = match predicate {
        Predicate::And(predicates) => predicates,
        predicate => vec![predicate],
    };
    let mut residual = Vec::new();
    for predicate in predicates {
        if !try_pushdown_relationship_property_eq(input, &predicate) {
            residual.push(predicate);
        }
    }
    combine_predicates(residual)
}

pub(super) fn try_pushdown_relationship_property_eq(
    input: &mut LogicalPlan,
    predicate: &Predicate,
) -> bool {
    let Predicate::PropertyEq {
        variable,
        property,
        value,
    } = predicate
    else {
        return false;
    };
    match input {
        LogicalPlan::Expand {
            rel_variable: Some(rel_variable),
            rel_properties,
            min_hops,
            max_hops,
            ..
        } if rel_variable == variable && *min_hops == 1 && *max_hops == 1 => {
            if let Some(existing) = rel_properties.get(property) {
                existing == value
            } else {
                rel_properties.insert(property.clone(), value.clone());
                true
            }
        }
        LogicalPlan::Expand { input, .. } => {
            try_pushdown_relationship_property_eq(input, predicate)
        }
        _ => false,
    }
}

pub(super) fn bind_value(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<Value> {
    match &expression.kind {
        ValueExpressionKind::Literal(value) => Ok(value.clone()),
        ValueExpressionKind::Parameter(name) => parameters
            .get(name)
            .cloned()
            .ok_or_else(|| SkeinError::Semantic(format!("missing parameter '${name}'"))),
        ValueExpressionKind::List(values) => values
            .iter()
            .map(|value| bind_value(value, parameters))
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        ValueExpressionKind::CurrentTimestamp => Ok(current_timestamp_value()),
        ValueExpressionKind::Timestamp(value) => {
            bind_value(value, parameters).and_then(timestamp_value)
        }
    }
}

pub(super) fn current_timestamp_value() -> Value {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0);
    Value::Int(nanos)
}

pub(super) fn timestamp_value(value: Value) -> Result<Value> {
    match value {
        Value::Int(value) => Ok(Value::Int(scale_epoch_integer_to_nanos(value))),
        Value::Float(value) if value.is_finite() => {
            let nanos = (value * 1_000_000_000.0).round();
            if nanos < i64::MIN as f64 || nanos > i64::MAX as f64 {
                return Err(SkeinError::Semantic(format!(
                    "timestamp() value is out of range: {value}"
                )));
            }
            Ok(Value::Int(nanos as i64))
        }
        Value::String(value) => parse_timestamp_string(&value).map(Value::Int),
        value => Err(SkeinError::Semantic(format!(
            "timestamp() expects an ISO string or numeric epoch, got {value:?}"
        ))),
    }
}

pub(super) fn scale_epoch_integer_to_nanos(value: i64) -> i64 {
    let abs = value.unsigned_abs();
    let multiplier = if abs < 100_000_000_000 {
        1_000_000_000
    } else if abs < 100_000_000_000_000 {
        1_000_000
    } else if abs < 100_000_000_000_000_000 {
        1_000
    } else {
        1
    };
    value.saturating_mul(multiplier)
}

pub(super) fn parse_timestamp_string(input: &str) -> Result<i64> {
    let input = input.trim().trim_end_matches('Z');
    let (date, time) = input
        .split_once('T')
        .or_else(|| input.split_once(' '))
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "timestamp() expects YYYY-MM-DDTHH:MM:SS, got '{input}'"
            ))
        })?;
    let (year, month, day) = parse_timestamp_date(date)?;
    let (hour, minute, second, nanos) = parse_timestamp_time(time)?;
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add((hour as i64) * 3_600))
        .and_then(|value| value.checked_add((minute as i64) * 60))
        .and_then(|value| value.checked_add(second as i64))
        .ok_or_else(|| {
            SkeinError::Semantic(format!("timestamp() value is out of range: '{input}'"))
        })?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanos as i64))
        .ok_or_else(|| {
            SkeinError::Semantic(format!("timestamp() value is out of range: '{input}'"))
        })
}

pub(super) fn parse_timestamp_date(input: &str) -> Result<(i32, u32, u32)> {
    let mut parts = input.split('-');
    let year = parse_timestamp_part::<i32>(parts.next(), "year", input)?;
    let month = parse_timestamp_part::<u32>(parts.next(), "month", input)?;
    let day = parse_timestamp_part::<u32>(parts.next(), "day", input)?;
    if parts.next().is_some() || !(1..=12).contains(&month) {
        return Err(invalid_timestamp_date(input));
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(invalid_timestamp_date(input));
    }
    Ok((year, month, day))
}

pub(super) fn parse_timestamp_time(input: &str) -> Result<(u32, u32, u32, u32)> {
    let mut parts = input.split(':');
    let hour = parse_timestamp_part::<u32>(parts.next(), "hour", input)?;
    let minute = parse_timestamp_part::<u32>(parts.next(), "minute", input)?;
    let second_part = parts
        .next()
        .ok_or_else(|| SkeinError::Semantic(format!("invalid timestamp time: '{input}'")))?;
    if parts.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "invalid timestamp time: '{input}'"
        )));
    }
    let (second_text, fraction) = second_part
        .split_once('.')
        .map(|(second, fraction)| (second, Some(fraction)))
        .unwrap_or((second_part, None));
    let second = second_text
        .parse::<u32>()
        .map_err(|_| SkeinError::Semantic(format!("invalid timestamp time: '{input}'")))?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(SkeinError::Semantic(format!(
            "invalid timestamp time: '{input}'"
        )));
    }
    let nanos = fraction
        .map(parse_fractional_nanos)
        .transpose()?
        .unwrap_or(0);
    Ok((hour, minute, second, nanos))
}

pub(super) fn parse_fractional_nanos(input: &str) -> Result<u32> {
    if input.is_empty() || input.len() > 9 || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SkeinError::Semantic(format!(
            "invalid timestamp fractional seconds: '{input}'"
        )));
    }
    let mut nanos = input.parse::<u32>().map_err(|_| {
        SkeinError::Semantic(format!("invalid timestamp fractional seconds: '{input}'"))
    })?;
    for _ in input.len()..9 {
        nanos *= 10;
    }
    Ok(nanos)
}

pub(super) fn parse_timestamp_part<T>(part: Option<&str>, name: &str, full: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    part.ok_or_else(|| SkeinError::Semantic(format!("invalid timestamp {name}: '{full}'")))?
        .parse::<T>()
        .map_err(|_| SkeinError::Semantic(format!("invalid timestamp {name}: '{full}'")))
}

pub(super) fn invalid_timestamp_date(input: &str) -> SkeinError {
    SkeinError::Semantic(format!("invalid timestamp date: '{input}'"))
}

pub(super) fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

pub(super) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(super) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub(super) fn bind_id_value(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<Value> {
    match bind_value(expression, parameters)? {
        Value::Int(value) if value >= 0 => Ok(Value::Int(value)),
        value => Err(SkeinError::Semantic(format!(
            "id() predicate requires a non-negative integer value, got {value:?}"
        ))),
    }
}

pub(super) fn validate_predicate(
    scope: &BTreeSet<String>,
    predicate: &PropertyPredicate,
) -> Result<()> {
    for variable in predicate_variables(predicate) {
        if !scope.contains(&variable) {
            return Err(SkeinError::Semantic(format!(
                "unknown variable '{variable}' in predicate"
            )));
        }
    }
    Ok(())
}

pub(super) fn plan_predicate(
    predicate: &PropertyPredicate,
    scope: &BTreeSet<String>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Predicate> {
    match predicate {
        PropertyPredicate::And(predicates) => predicates
            .iter()
            .map(|predicate| plan_predicate(predicate, scope, parameters))
            .collect::<Result<Vec<_>>>()
            .map(Predicate::And),
        PropertyPredicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| plan_predicate(predicate, scope, parameters))
            .collect::<Result<Vec<_>>>()
            .map(Predicate::Or),
        PropertyPredicate::Not(predicate) => Ok(Predicate::Not(Box::new(plan_predicate(
            predicate, scope, parameters,
        )?))),
        PropertyPredicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => Ok(Predicate::RelationshipExists {
            variable: variable.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_label: target_label.clone(),
        }),
        PropertyPredicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => Ok(Predicate::BoundRelationshipExists {
            source_variable: source_variable.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_variable: target_variable.clone(),
        }),
        PropertyPredicate::IdEq { variable, value } => Ok(Predicate::IdEq {
            variable: variable.clone(),
            value: bind_id_value(value, parameters)?,
        }),
        PropertyPredicate::IdNotEq { variable, value } => Ok(Predicate::IdNotEq {
            variable: variable.clone(),
            value: bind_id_value(value, parameters)?,
        }),
        PropertyPredicate::IdCompare {
            variable,
            op,
            value,
        } => Ok(Predicate::IdCompare {
            variable: variable.clone(),
            op: plan_comparison_op(*op),
            value: bind_id_value(value, parameters)?,
        }),
        PropertyPredicate::IdIn { variable, values } => match bind_value(values, parameters)? {
            Value::List(values) => Ok(Predicate::IdIn {
                variable: variable.clone(),
                values,
            }),
            value => Err(SkeinError::Semantic(format!(
                "IN predicate requires a list value, got {value:?}"
            ))),
        },
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } => Ok(Predicate::PropertyEq {
            variable: variable.clone(),
            property: property.clone(),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::NotEq {
            variable,
            property,
            value,
        } => Ok(Predicate::PropertyNotEq {
            variable: variable.clone(),
            property: property.clone(),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::Compare {
            variable,
            property,
            op,
            value,
        } => Ok(Predicate::PropertyCompare {
            variable: variable.clone(),
            property: property.clone(),
            op: plan_comparison_op(*op),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::ExpressionEq { expression, value } => Ok(Predicate::ExpressionEq {
            expression: plan_scalar_expression(scope, expression, parameters)?,
            value: plan_scalar_expression(scope, value, parameters)?,
        }),
        PropertyPredicate::ExpressionNotEq { expression, value } => {
            Ok(Predicate::ExpressionNotEq {
                expression: plan_scalar_expression(scope, expression, parameters)?,
                value: plan_scalar_expression(scope, value, parameters)?,
            })
        }
        PropertyPredicate::ExpressionCompare {
            expression,
            op,
            value,
        } => Ok(Predicate::ExpressionCompare {
            expression: plan_scalar_expression(scope, expression, parameters)?,
            op: plan_comparison_op(*op),
            value: plan_scalar_expression(scope, value, parameters)?,
        }),
        PropertyPredicate::ExpressionContains { expression, value } => {
            Ok(Predicate::ExpressionContains {
                expression: plan_scalar_expression(scope, expression, parameters)?,
                value: plan_scalar_expression(scope, value, parameters)?,
            })
        }
        PropertyPredicate::ListContains {
            variable,
            property,
            value,
        } => Ok(Predicate::PropertyListContains {
            variable: variable.clone(),
            property: property.clone(),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::ListContainsLower {
            variable,
            property,
            value,
        } => match bind_value(value, parameters)? {
            Value::String(value) => Ok(Predicate::PropertyListContainsLower {
                variable: variable.clone(),
                property: property.clone(),
                value: value.to_lowercase(),
            }),
            value => Err(SkeinError::Semantic(format!(
                "list_contains_lower predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::Contains {
            variable,
            property,
            value,
        } => match bind_value(value, parameters)? {
            Value::String(value) => Ok(Predicate::PropertyContains {
                variable: variable.clone(),
                property: property.clone(),
                value,
            }),
            value => Err(SkeinError::Semantic(format!(
                "CONTAINS predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::StartsWith {
            variable,
            property,
            value,
        } => match bind_value(value, parameters)? {
            Value::String(value) => Ok(Predicate::PropertyStartsWith {
                variable: variable.clone(),
                property: property.clone(),
                value,
            }),
            value => Err(SkeinError::Semantic(format!(
                "STARTS WITH predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::EndsWith {
            variable,
            property,
            value,
        } => match bind_value(value, parameters)? {
            Value::String(value) => Ok(Predicate::PropertyEndsWith {
                variable: variable.clone(),
                property: property.clone(),
                value,
            }),
            value => Err(SkeinError::Semantic(format!(
                "ENDS WITH predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::RegexMatch {
            variable,
            property,
            pattern,
        } => match bind_value(pattern, parameters)? {
            Value::String(pattern) => Ok(Predicate::PropertyRegexMatch {
                variable: variable.clone(),
                property: property.clone(),
                pattern: ValidatedRegex::new(pattern)?,
            }),
            value => Err(SkeinError::Semantic(format!(
                "regex match predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::IsNull { variable, property } => Ok(Predicate::PropertyIsNull {
            variable: variable.clone(),
            property: property.clone(),
        }),
        PropertyPredicate::IsNotNull { variable, property } => Ok(Predicate::PropertyIsNotNull {
            variable: variable.clone(),
            property: property.clone(),
        }),
        PropertyPredicate::ParameterIsNull { parameter } => Ok(Predicate::ConstantBool(
            bind_value(
                &AstNode::synthetic(ValueExpressionKind::Parameter(parameter.clone())),
                parameters,
            )? == Value::Null,
        )),
        PropertyPredicate::ParameterIsNotNull { parameter } => Ok(Predicate::ConstantBool(
            bind_value(
                &AstNode::synthetic(ValueExpressionKind::Parameter(parameter.clone())),
                parameters,
            )? != Value::Null,
        )),
        PropertyPredicate::ParameterEq { left, right } => Ok(Predicate::ConstantBool(
            bind_value(
                &AstNode::synthetic(ValueExpressionKind::Parameter(left.clone())),
                parameters,
            )? == bind_value(right, parameters)?,
        )),
        PropertyPredicate::ParameterNotEq { left, right } => Ok(Predicate::ConstantBool(
            bind_value(
                &AstNode::synthetic(ValueExpressionKind::Parameter(left.clone())),
                parameters,
            )? != bind_value(right, parameters)?,
        )),
        PropertyPredicate::In {
            variable,
            property,
            values,
        } => match bind_value(values, parameters)? {
            Value::List(values) => Ok(Predicate::PropertyIn {
                variable: variable.clone(),
                property: property.clone(),
                values,
            }),
            value => Err(SkeinError::Semantic(format!(
                "IN predicate requires a list value, got {value:?}"
            ))),
        },
    }
}

pub(super) struct RelationshipMutationPredicatePlan {
    pub(super) source_predicate: Option<Predicate>,
    pub(super) rel_predicate: Option<Predicate>,
    pub(super) target_properties: BTreeMap<String, Value>,
}

pub(super) fn plan_relationship_mutation_predicate(
    predicate: Option<&PropertyPredicate>,
    source_variable: &str,
    rel_variable: &str,
    target_variable: &str,
    target_pattern_properties: &BTreeMap<String, ValueExpression>,
    parameters: &BTreeMap<String, Value>,
) -> Result<RelationshipMutationPredicatePlan> {
    let mut target_properties = bind_properties(target_pattern_properties, parameters)?;
    let Some(predicate) = predicate else {
        return Ok(RelationshipMutationPredicatePlan {
            source_predicate: None,
            rel_predicate: None,
            target_properties,
        });
    };
    let (source_predicate, rel_predicate) = split_relationship_mutation_predicate(
        predicate,
        source_variable,
        rel_variable,
        target_variable,
        &mut target_properties,
        parameters,
    )?;
    Ok(RelationshipMutationPredicatePlan {
        source_predicate,
        rel_predicate,
        target_properties,
    })
}

pub(super) fn split_relationship_mutation_predicate(
    predicate: &PropertyPredicate,
    source_variable: &str,
    rel_variable: &str,
    target_variable: &str,
    target_properties: &mut BTreeMap<String, Value>,
    parameters: &BTreeMap<String, Value>,
) -> Result<(Option<Predicate>, Option<Predicate>)> {
    let scope = BTreeSet::from([
        source_variable.to_string(),
        rel_variable.to_string(),
        target_variable.to_string(),
    ]);
    match predicate {
        PropertyPredicate::And(predicates) => {
            let mut source_predicates = Vec::new();
            let mut rel_predicates = Vec::new();
            for predicate in predicates {
                let (source, rel) = split_relationship_mutation_predicate(
                    predicate,
                    source_variable,
                    rel_variable,
                    target_variable,
                    target_properties,
                    parameters,
                )?;
                if let Some(source) = source {
                    source_predicates.push(source);
                }
                if let Some(rel) = rel {
                    rel_predicates.push(rel);
                }
            }
            Ok((
                combine_predicates(source_predicates),
                combine_predicates(rel_predicates),
            ))
        }
        PropertyPredicate::Or(_) => {
            let variables = predicate_variables(predicate);
            if variables.len() != 1 {
                return Err(SkeinError::Semantic(
                    "relationship mutation OR predicates cannot mix node and relationship variables"
                        .to_string(),
                ));
            }
            let variable = variables
                .iter()
                .next()
                .expect("checked exactly one predicate variable");
            let planned = plan_predicate(predicate, &scope, parameters)?;
            predicate_for_relationship_mutation_variable(
                variable,
                source_variable,
                rel_variable,
                target_variable,
                planned,
            )
        }
        PropertyPredicate::Not(_) => {
            let variables = predicate_variables(predicate);
            if variables.len() != 1 {
                return Err(SkeinError::Semantic(
                    "relationship mutation NOT predicates cannot mix node and relationship variables"
                        .to_string(),
                ));
            }
            let variable = variables
                .iter()
                .next()
                .expect("checked exactly one predicate variable");
            let planned = plan_predicate(predicate, &scope, parameters)?;
            predicate_for_relationship_mutation_variable(
                variable,
                source_variable,
                rel_variable,
                target_variable,
                planned,
            )
        }
        _ => {
            let variables = predicate_variables(predicate);
            if variables.len() != 1 {
                return Err(SkeinError::Semantic(
                    "relationship mutation predicate must bind one variable".to_string(),
                ));
            }
            let variable = variables.iter().next().ok_or_else(|| {
                SkeinError::Semantic(
                    "relationship mutation predicate must bind one variable".to_string(),
                )
            })?;
            if variable == target_variable {
                return bind_relationship_mutation_target_predicate(
                    predicate,
                    target_variable,
                    target_properties,
                    parameters,
                );
            }
            let planned = plan_predicate(predicate, &scope, parameters)?;
            predicate_for_relationship_mutation_variable(
                variable,
                source_variable,
                rel_variable,
                target_variable,
                planned,
            )
        }
    }
}

pub(super) fn predicate_for_relationship_mutation_variable(
    variable: &str,
    source_variable: &str,
    rel_variable: &str,
    target_variable: &str,
    predicate: Predicate,
) -> Result<(Option<Predicate>, Option<Predicate>)> {
    if variable == source_variable {
        return Ok((Some(predicate), None));
    }
    if variable == rel_variable {
        return Ok((None, Some(predicate)));
    }
    if variable == target_variable {
        return Err(SkeinError::Semantic(
            "relationship mutation target predicates support only equality filters".to_string(),
        ));
    }
    Err(SkeinError::Semantic(format!(
        "unknown variable '{variable}' in relationship mutation predicate"
    )))
}

pub(super) fn bind_relationship_mutation_target_predicate(
    predicate: &PropertyPredicate,
    target_variable: &str,
    target_properties: &mut BTreeMap<String, Value>,
    parameters: &BTreeMap<String, Value>,
) -> Result<(Option<Predicate>, Option<Predicate>)> {
    match predicate {
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if variable == target_variable => {
            insert_endpoint_property(target_properties, property, bind_value(value, parameters)?)?;
            Ok((None, None))
        }
        _ => Err(SkeinError::Semantic(
            "relationship mutation target predicates support only equality filters".to_string(),
        )),
    }
}

pub(super) fn combine_predicates(predicates: Vec<Predicate>) -> Option<Predicate> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(Predicate::And(predicates)),
    }
}

pub(super) fn predicate_variables(predicate: &PropertyPredicate) -> BTreeSet<String> {
    let mut variables = BTreeSet::new();
    collect_predicate_variables(predicate, &mut variables);
    variables
}

pub(super) fn collect_predicate_variables(
    predicate: &PropertyPredicate,
    variables: &mut BTreeSet<String>,
) {
    match predicate {
        PropertyPredicate::And(predicates) | PropertyPredicate::Or(predicates) => {
            for predicate in predicates {
                collect_predicate_variables(predicate, variables);
            }
        }
        PropertyPredicate::Not(predicate) => {
            collect_predicate_variables(predicate, variables);
        }
        PropertyPredicate::RelationshipExists { variable, .. } => {
            variables.insert(variable.clone());
        }
        PropertyPredicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => {
            variables.insert(source_variable.clone());
            variables.insert(target_variable.clone());
        }
        PropertyPredicate::ExpressionEq { expression, .. }
        | PropertyPredicate::ExpressionNotEq { expression, .. }
        | PropertyPredicate::ExpressionCompare { expression, .. }
        | PropertyPredicate::ExpressionContains { expression, .. } => {
            collect_scalar_expression_variables(expression, variables);
            let value = match predicate {
                PropertyPredicate::ExpressionEq { value, .. }
                | PropertyPredicate::ExpressionNotEq { value, .. }
                | PropertyPredicate::ExpressionCompare { value, .. }
                | PropertyPredicate::ExpressionContains { value, .. } => Some(value),
                _ => None,
            };
            if let Some(value) = value {
                collect_scalar_expression_variables(value, variables);
            }
        }
        _ => {
            if let Some(variable) = predicate_variable(predicate) {
                variables.insert(variable.to_string());
            }
        }
    }
}

pub(super) fn predicate_variable(predicate: &PropertyPredicate) -> Option<&str> {
    match predicate {
        PropertyPredicate::Eq { variable, .. }
        | PropertyPredicate::NotEq { variable, .. }
        | PropertyPredicate::IdEq { variable, .. }
        | PropertyPredicate::IdNotEq { variable, .. }
        | PropertyPredicate::IdCompare { variable, .. }
        | PropertyPredicate::IdIn { variable, .. }
        | PropertyPredicate::Compare { variable, .. }
        | PropertyPredicate::ListContains { variable, .. }
        | PropertyPredicate::ListContainsLower { variable, .. }
        | PropertyPredicate::Contains { variable, .. }
        | PropertyPredicate::StartsWith { variable, .. }
        | PropertyPredicate::EndsWith { variable, .. }
        | PropertyPredicate::RegexMatch { variable, .. }
        | PropertyPredicate::IsNull { variable, .. }
        | PropertyPredicate::IsNotNull { variable, .. }
        | PropertyPredicate::In { variable, .. } => Some(variable),
        PropertyPredicate::RelationshipExists { variable, .. } => Some(variable),
        PropertyPredicate::And(_)
        | PropertyPredicate::Or(_)
        | PropertyPredicate::Not(_)
        | PropertyPredicate::ExpressionEq { .. }
        | PropertyPredicate::ExpressionNotEq { .. }
        | PropertyPredicate::ExpressionCompare { .. }
        | PropertyPredicate::ExpressionContains { .. }
        | PropertyPredicate::ParameterIsNull { .. }
        | PropertyPredicate::ParameterIsNotNull { .. }
        | PropertyPredicate::ParameterEq { .. }
        | PropertyPredicate::ParameterNotEq { .. }
        | PropertyPredicate::BoundRelationshipExists { .. } => None,
    }
}

pub(super) fn collect_scalar_expression_variables(
    expression: &ScalarExpression,
    variables: &mut BTreeSet<String>,
) {
    match &expression.kind {
        ScalarExpressionKind::Variable(variable)
        | ScalarExpressionKind::Property { variable, .. }
        | ScalarExpressionKind::Id(variable)
        | ScalarExpressionKind::RelationshipType(variable) => {
            variables.insert(variable.clone());
        }
        ScalarExpressionKind::Value(_) => {}
        ScalarExpressionKind::Coalesce(expressions) => {
            for expression in expressions {
                collect_scalar_expression_variables(expression, variables);
            }
        }
        ScalarExpressionKind::Left { expression, .. } => {
            collect_scalar_expression_variables(expression, variables);
        }
        ScalarExpressionKind::Lower(expression) => {
            collect_scalar_expression_variables(expression, variables);
        }
        ScalarExpressionKind::DatePart { variable, .. } => {
            variables.insert(variable.clone());
        }
        ScalarExpressionKind::DefaultIfNullOrEq { variable, .. }
        | ScalarExpressionKind::DefaultIfNull { variable, .. } => {
            variables.insert(variable.clone());
        }
        ScalarExpressionKind::CasePropertyNotNullOrEq { variable, .. } => {
            variables.insert(variable.clone());
        }
        ScalarExpressionKind::CasePropertyEqualsRank { variable, .. } => {
            variables.insert(variable.clone());
        }
        ScalarExpressionKind::CaseLowerPropertyDefault { variable, .. } => {
            variables.insert(variable.clone());
        }
        ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            variables.insert(variable.clone());
        }
        ScalarExpressionKind::Case { .. }
        | ScalarExpressionKind::Binary { .. }
        | ScalarExpressionKind::Not(_)
        | ScalarExpressionKind::IsNull { .. } => {
            expression.kind.all_children(|child| {
                collect_scalar_expression_variables(child, variables);
                true
            });
        }
    }
}
