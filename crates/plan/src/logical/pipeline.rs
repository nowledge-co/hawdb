use super::*;
use hawdb_cypher::{
    ArithmeticOp, Clause, ClauseKind, NodePattern, PathSearch, PredicateExpression,
    ProjectionClause, QueryPipeline, RelationshipPattern,
};

mod imports;
mod mutation;
mod normalize;
mod path;
mod procedure;
#[cfg(test)]
mod tests;

/// Migration entrypoint for the ordered-clause binder.
#[doc(hidden)]
pub fn plan_pipeline_query(
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    plan_pipeline(&hawdb_cypher::parse_pipeline(query)?, parameters)
}

#[doc(hidden)]
pub fn plan_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    bind_pipeline(query, parameters)
}

/// Plans a pipeline already parsed through a supported statement entrypoint.
#[doc(hidden)]
pub fn plan_parsed_pipeline_query(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    bind_pipeline(query, parameters)
}

/// Migration entrypoint with structural read-plan normalization enabled.
#[doc(hidden)]
pub fn plan_normalized_pipeline_query(
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    plan_pipeline_query(query, parameters).map(normalize::normalize)
}

#[derive(Clone)]
enum BindingType {
    Graph {
        kind: GraphEntityKind,
        column: Option<String>,
    },
    Scalar,
    UnmaterializedPath,
}

#[derive(Clone, Default)]
struct Scope(BTreeMap<String, BindingType>);

impl Scope {
    fn graph_names(&self) -> BTreeSet<String> {
        self.0
            .iter()
            .filter_map(|(name, kind)| {
                matches!(kind, BindingType::Graph { .. }).then_some(name.clone())
            })
            .collect()
    }

    fn columns(&self) -> BTreeSet<String> {
        self.0
            .iter()
            .filter_map(|(name, kind)| {
                matches!(
                    kind,
                    BindingType::Scalar
                        | BindingType::Graph {
                            column: Some(_),
                            ..
                        }
                )
                .then_some(name.clone())
            })
            .collect()
    }

    fn imports(&self) -> Vec<GraphBindingImport> {
        self.0
            .iter()
            .filter_map(|(variable, binding)| match binding {
                BindingType::Graph {
                    kind,
                    column: Some(column),
                } => Some(GraphBindingImport {
                    variable: variable.clone(),
                    column: column.clone(),
                    kind: *kind,
                }),
                _ => None,
            })
            .collect()
    }

    fn bind_graph(
        &mut self,
        name: &str,
        kind: GraphEntityKind,
        introduced: &mut Vec<String>,
    ) -> Result<()> {
        match self.0.get(name) {
            Some(BindingType::Graph { kind: previous, .. }) if *previous == kind => Ok(()),
            Some(_) => Err(HawDBError::Semantic(format!(
                "variable '{name}' has an incompatible MATCH type"
            ))),
            None => {
                self.0
                    .insert(name.to_string(), BindingType::Graph { kind, column: None });
                introduced.push(name.to_string());
                Ok(())
            }
        }
    }

    fn materialized(&mut self) {
        for binding in self.0.values_mut() {
            if let BindingType::Graph { column, .. } = binding {
                *column = None;
            }
        }
    }
}

fn bind_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if query.clauses.len() > 32 {
        return Err(HawDBError::Semantic(
            "query exceeds maximum clause depth".to_string(),
        ));
    }
    if matches!(
        query.clauses.first().map(|clause| &clause.kind),
        Some(ClauseKind::Unwind { .. })
    ) {
        return mutation::bind_unwind_mutation_pipeline(query, parameters);
    }
    if let Some(plan) = bind_optional_relationship_count_sum(query, parameters) {
        return plan;
    }
    if let Some(plan) = bind_thread_repair_stats(query) {
        return Ok(plan);
    }
    if let Some(plan) = bind_optional_degree_pipeline(query, parameters) {
        return plan;
    }
    if query.clauses.iter().any(|clause| {
        matches!(
            clause.kind,
            ClauseKind::Create(_)
                | ClauseKind::Merge { .. }
                | ClauseKind::Set(_)
                | ClauseKind::Delete { .. }
        )
    }) {
        return mutation::bind_mutation_pipeline(query, parameters);
    }
    if matches!(
        query.clauses.first().map(|clause| &clause.kind),
        Some(ClauseKind::Call { .. })
    ) {
        return procedure::bind_procedure_pipeline(query, parameters);
    }
    if query.clauses.iter().any(|clause| {
        matches!(&clause.kind,
        ClauseKind::Match { patterns, .. } if patterns.iter().any(|pattern|
            pattern.steps.iter().any(|step| step.relationship.search == PathSearch::AllShortest)))
    }) {
        return path::bind_shortest_path_pipeline(query, parameters);
    }
    bind_read_clauses(&query.clauses, None, Scope::default(), parameters)
}

fn bind_optional_relationship_count_sum(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Option<Result<LogicalPlan>> {
    let [initial, first_optional, second_optional, final_return] = query.clauses.as_slice() else {
        return None;
    };
    let ClauseKind::Match {
        optional: false,
        patterns,
        predicate: None,
    } = &initial.kind
    else {
        return None;
    };
    let [initial_pattern] = patterns.as_slice() else {
        return None;
    };
    if initial_pattern.variable.is_some() || !initial_pattern.steps.is_empty() {
        return None;
    }
    let source = &initial_pattern.first;
    if source.anonymous {
        return None;
    }
    let source_properties = match bind_properties(&source.properties, parameters) {
        Ok(properties) => properties,
        Err(error) => return Some(Err(error)),
    };
    let first_leg = bind_optional_count_leg(first_optional, source, parameters)?;
    let (first_variable, mut first_leg) = match first_leg {
        Ok(leg) => leg,
        Err(error) => return Some(Err(error)),
    };
    let second_leg = bind_optional_count_leg(second_optional, source, parameters)?;
    let (second_variable, mut second_leg) = match second_leg {
        Ok(leg) => leg,
        Err(error) => return Some(Err(error)),
    };
    let (first_distinct, second_distinct) =
        count_sum_return(final_return, &first_variable, &second_variable)?;
    first_leg.distinct = first_distinct;
    second_leg.distinct = second_distinct;
    Some(Ok(LogicalPlan::OptionalRelationshipCountSum {
        variable: source.variable.clone(),
        label: source.label.clone(),
        properties: source_properties,
        legs: vec![first_leg, second_leg],
        output: format!(
            "(count({}) + count({}))",
            display_count_variable(&first_variable, first_distinct),
            display_count_variable(&second_variable, second_distinct)
        ),
    }))
}

fn bind_optional_count_leg(
    clause: &Clause,
    source: &NodePattern,
    parameters: &BTreeMap<String, Value>,
) -> Option<Result<(String, RelationshipCountLeg)>> {
    let ClauseKind::Match {
        optional: true,
        patterns,
        predicate,
    } = &clause.kind
    else {
        return None;
    };
    let [pattern] = patterns.as_slice() else {
        return None;
    };
    if pattern.variable.is_some() {
        return None;
    }
    let [step] = pattern.steps.as_slice() else {
        return None;
    };
    let relationship = &step.relationship;
    let Some(variable) = &relationship.variable else {
        return None;
    };
    if relationship.search != PathSearch::All
        || relationship.min_hops != 1
        || relationship.max_hops != 1
        || !relationship.properties.is_empty()
    {
        return None;
    }
    let direction = if pattern.first.variable == source.variable
        && pattern.first.label.is_empty()
        && pattern.first.properties.is_empty()
        && step.target.anonymous
        && step.target.properties.is_empty()
    {
        relationship.direction
    } else if step.target.variable == source.variable
        && step.target.label.is_empty()
        && step.target.properties.is_empty()
        && pattern.first.anonymous
        && pattern.first.properties.is_empty()
    {
        reverse_relationship_direction(relationship.direction)
    } else {
        return None;
    };
    let filter = match bind_optional_count_filter(predicate.as_ref(), variable, parameters) {
        Some(Ok(filter)) => filter,
        Some(Err(error)) => return Some(Err(error)),
        None => return None,
    };
    Some(Ok((
        variable.clone(),
        RelationshipCountLeg {
            rel_type: relationship.rel_type.clone(),
            direction,
            distinct: false,
            filter,
        },
    )))
}

fn bind_optional_count_filter(
    predicate: Option<&PredicateExpression>,
    relationship_variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Option<Result<Option<RelationshipCountFilter>>> {
    let Some(predicate) = predicate else {
        return Some(Ok(None));
    };
    let mut terms = Vec::new();
    collect_or_terms(&predicate.kind, &mut terms);
    if terms.len() != 3 {
        return None;
    }
    let mut property = None;
    let mut non_empty = None;
    let mut saw_null = false;
    let mut saw_empty = false;
    for term in &terms {
        match term {
            PropertyPredicate::NotEq {
                variable,
                property: candidate,
                value,
            } if variable == relationship_variable => {
                if property.is_some_and(|property| property != candidate)
                    || non_empty.replace(value).is_some()
                {
                    return None;
                }
                property = Some(candidate);
            }
            PropertyPredicate::IsNull {
                variable,
                property: candidate,
            } if variable == relationship_variable => {
                if property.is_some_and(|property| property != candidate) || saw_null {
                    return None;
                }
                property = Some(candidate);
                saw_null = true;
            }
            PropertyPredicate::Eq {
                variable,
                property: candidate,
                value,
            } if variable == relationship_variable
                && matches!(value.kind, ValueExpressionKind::Literal(Value::String(ref value)) if value.is_empty()) =>
            {
                if property.is_some_and(|property| property != candidate) || saw_empty {
                    return None;
                }
                property = Some(candidate);
                saw_empty = true;
            }
            _ => return None,
        }
    }
    let (Some(property), Some(value)) = (property, non_empty) else {
        return None;
    };
    if !saw_null || !saw_empty {
        return None;
    }
    Some(bind_value(value, parameters).map(|value| {
        Some(RelationshipCountFilter::PropertyNotEqOrEmpty {
            property: property.clone(),
            value,
        })
    }))
}

fn collect_or_terms<'a>(predicate: &'a PropertyPredicate, terms: &mut Vec<&'a PropertyPredicate>) {
    match predicate {
        PropertyPredicate::Or(children) => {
            for child in children {
                collect_or_terms(child, terms);
            }
        }
        predicate => terms.push(predicate),
    }
}

fn count_sum_return(
    clause: &Clause,
    first_variable: &str,
    second_variable: &str,
) -> Option<(bool, bool)> {
    let ClauseKind::Return(projection) = &clause.kind else {
        return None;
    };
    if projection.distinct
        || projection.predicate.is_some()
        || !projection.order_by.is_empty()
        || projection.offset.is_some()
        || projection.limit.is_some()
    {
        return None;
    }
    let [item] = projection.items.as_slice() else {
        return None;
    };
    if item.alias.is_some() {
        return None;
    }
    let ReturnExpressionKind::Arithmetic { first, rest } = &item.expression.kind else {
        return None;
    };
    let [(ArithmeticOp::Add, second)] = rest.as_slice() else {
        return None;
    };
    Some((
        count_variable(first, first_variable)?,
        count_variable(second, second_variable)?,
    ))
}

fn count_variable(expression: &ReturnExpression, variable: &str) -> Option<bool> {
    let ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
        variable: candidate,
        distinct,
    }) = &expression.kind
    else {
        return None;
    };
    (candidate == variable).then_some(*distinct)
}

fn display_count_variable(variable: &str, distinct: bool) -> String {
    if distinct {
        format!("DISTINCT {variable}")
    } else {
        variable.to_string()
    }
}

/// Derives the fixed-schema thread repair summary only after validating every
/// clause that contributes to the specialized executor's output. Similar
/// optional-match pipelines keep their general logical representation.
fn bind_thread_repair_stats(query: &QueryPipeline) -> Option<LogicalPlan> {
    let [initial, identity_match, first_with, message_match, second_with, memory_match, final_return] =
        query.clauses.as_slice()
    else {
        return None;
    };
    let source = required_single_node(initial)?;
    let (identity, identity_ref_property) = thread_repair_identity_match(identity_match, source)?;
    if !thread_repair_projection(
        first_with,
        source.variable.as_str(),
        &identity.variable,
        "identity_refs",
    ) {
        return None;
    }
    let (message_rel_type, message) = thread_repair_optional_expand(message_match, source)?;
    if !thread_repair_second_projection(
        second_with,
        source.variable.as_str(),
        "identity_refs",
        &message.variable,
        "legacy_messages",
    ) {
        return None;
    }
    let (memory_rel_type, memory) = thread_repair_optional_expand(memory_match, source)?;
    thread_repair_return(
        final_return,
        source.variable.as_str(),
        "identity_refs",
        "legacy_messages",
        &memory.variable,
    )?;

    Some(LogicalPlan::ThreadRepairStats {
        label: source.label.clone(),
        identity_label: identity.label.clone(),
        identity_ref_property,
        thread_id_property: "id".to_string(),
        message_rel_type,
        message_label: message.label.clone(),
        memory_rel_type,
        memory_label: memory.label.clone(),
    })
}

fn required_single_node(clause: &Clause) -> Option<&NodePattern> {
    let ClauseKind::Match {
        optional: false,
        patterns,
        predicate: None,
    } = &clause.kind
    else {
        return None;
    };
    let [pattern] = patterns.as_slice() else {
        return None;
    };
    (!pattern.variable.is_some()
        && pattern.steps.is_empty()
        && !pattern.first.anonymous
        && !pattern.first.variable.is_empty()
        && pattern.first.properties.is_empty())
    .then_some(&pattern.first)
}

fn thread_repair_identity_match<'a>(
    clause: &'a Clause,
    source: &NodePattern,
) -> Option<(&'a NodePattern, String)> {
    let ClauseKind::Match {
        optional: true,
        patterns,
        predicate: Some(predicate),
    } = &clause.kind
    else {
        return None;
    };
    let [pattern] = patterns.as_slice() else {
        return None;
    };
    let identity = &pattern.first;
    if pattern.variable.is_some()
        || !pattern.steps.is_empty()
        || identity.anonymous
        || identity.variable.is_empty()
        || !identity.properties.is_empty()
    {
        return None;
    }
    let PropertyPredicate::ExpressionEq { expression, value } = &predicate.kind else {
        return None;
    };
    let (identity_variable, identity_property) = scalar_property(expression)?;
    (identity_variable == identity.variable
        && scalar_property(value) == Some((source.variable.as_str(), "id")))
    .then_some((identity, identity_property.to_string()))
}

fn thread_repair_optional_expand<'a>(
    clause: &'a Clause,
    source: &NodePattern,
) -> Option<(String, &'a NodePattern)> {
    let ClauseKind::Match {
        optional: true,
        patterns,
        predicate: None,
    } = &clause.kind
    else {
        return None;
    };
    let [pattern] = patterns.as_slice() else {
        return None;
    };
    let [step] = pattern.steps.as_slice() else {
        return None;
    };
    let relationship = &step.relationship;
    if pattern.variable.is_some()
        || pattern.first.variable != source.variable
        || pattern.first.anonymous
        || !pattern.first.label.is_empty()
        || !pattern.first.properties.is_empty()
        || relationship.variable.is_some()
        || relationship.rel_type.is_empty()
        || !relationship.properties.is_empty()
        || relationship.direction != RelationshipDirection::Outgoing
        || relationship.search != PathSearch::All
        || relationship.min_hops != 1
        || relationship.max_hops != 1
        || step.target.anonymous
        || step.target.variable.is_empty()
        || !step.target.properties.is_empty()
    {
        return None;
    }
    Some((relationship.rel_type.clone(), &step.target))
}

fn thread_repair_projection(clause: &Clause, source: &str, counted: &str, alias: &str) -> bool {
    let Some(projection) = plain_with_projection(clause) else {
        return false;
    };
    let [source_item, count] = projection.items.as_slice() else {
        return false;
    };
    is_unaliased_variable(source_item, source) && is_count_alias(count, counted, alias)
}

fn thread_repair_second_projection(
    clause: &Clause,
    source: &str,
    retained: &str,
    counted: &str,
    alias: &str,
) -> bool {
    let Some(projection) = plain_with_projection(clause) else {
        return false;
    };
    let [source_item, retained_item, count] = projection.items.as_slice() else {
        return false;
    };
    is_unaliased_variable(source_item, source)
        && is_unaliased_variable(retained_item, retained)
        && is_count_alias(count, counted, alias)
}

fn plain_with_projection(clause: &Clause) -> Option<&ProjectionClause> {
    let ClauseKind::With(projection) = &clause.kind else {
        return None;
    };
    (!projection.distinct
        && projection.predicate.is_none()
        && projection.order_by.is_empty()
        && projection.offset.is_none()
        && projection.limit.is_none())
    .then_some(projection)
}

fn is_count_alias(item: &ReturnItem, variable: &str, alias: &str) -> bool {
    item.alias.as_deref() == Some(alias)
        && matches!(
            &item.expression.kind,
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                variable: candidate,
                distinct: false,
            }) if candidate == variable
        )
}

fn thread_repair_return(
    clause: &Clause,
    source: &str,
    identity_refs: &str,
    legacy_messages: &str,
    memory: &str,
) -> Option<()> {
    let ClauseKind::Return(projection) = &clause.kind else {
        return None;
    };
    if projection.distinct
        || projection.predicate.is_some()
        || projection.offset.is_some()
        || projection.limit.is_some()
    {
        return None;
    }
    let [id, thread_id, space_id, message_count, identity, messages, memories] =
        projection.items.as_slice()
    else {
        return None;
    };
    if !is_unaliased_property(id, source, "id")
        || !is_unaliased_property(thread_id, source, "thread_id")
        || !is_thread_repair_space_id(space_id, source)
        || !is_thread_repair_message_count(message_count, source)
        || !is_unaliased_variable(identity, identity_refs)
        || !is_unaliased_variable(messages, legacy_messages)
        || !is_count_variable(memories, memory)
    {
        return None;
    }
    let [order] = projection.order_by.as_slice() else {
        return None;
    };
    matches!(
        (&order.expression, order.direction),
        (OrderExpression::Property { variable, property }, hawdb_cypher::OrderDirection::Asc)
            if variable == source && property == "id"
    )
    .then_some(())
}

fn is_unaliased_property(item: &ReturnItem, variable: &str, property: &str) -> bool {
    item.alias.is_none()
        && matches!(
            &item.expression.kind,
            ReturnExpressionKind::Value(expression)
                if scalar_property(expression) == Some((variable, property))
        )
}

fn is_thread_repair_space_id(item: &ReturnItem, source: &str) -> bool {
    if item.alias.is_some() {
        return false;
    }
    let ReturnExpressionKind::Value(expression) = &item.expression.kind else {
        return false;
    };
    matches!(
        &expression.kind,
        ScalarExpressionKind::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } if variable == source
            && property == "space_id"
            && is_string_literal(empty, "")
            && is_string_literal(default, "default")
    )
}

fn is_thread_repair_message_count(item: &ReturnItem, source: &str) -> bool {
    if item.alias.is_some() {
        return false;
    }
    let ReturnExpressionKind::Value(expression) = &item.expression.kind else {
        return false;
    };
    let ScalarExpressionKind::Coalesce(expressions) = &expression.kind else {
        return false;
    };
    let [property, default] = expressions.as_slice() else {
        return false;
    };
    scalar_property(property) == Some((source, "message_count")) && is_int_scalar(default, 0)
}

fn is_count_variable(item: &ReturnItem, variable: &str) -> bool {
    item.alias.is_none()
        && matches!(
            &item.expression.kind,
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                variable: candidate,
                distinct: false,
            }) if candidate == variable
        )
}

fn scalar_property(expression: &ScalarExpression) -> Option<(&str, &str)> {
    let ScalarExpressionKind::Property { variable, property } = &expression.kind else {
        return None;
    };
    Some((variable, property))
}

fn is_string_literal(expression: &ValueExpression, expected: &str) -> bool {
    matches!(
        &expression.kind,
        ValueExpressionKind::Literal(Value::String(value)) if value == expected
    )
}

fn is_int_scalar(expression: &ScalarExpression, expected: i64) -> bool {
    matches!(
        &expression.kind,
        ScalarExpressionKind::Value(value)
            if matches!(&value.kind, ValueExpressionKind::Literal(Value::Int(value)) if *value == expected)
    )
}

fn reverse_relationship_direction(direction: RelationshipDirection) -> RelationshipDirection {
    match direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Undirected => RelationshipDirection::Undirected,
    }
}

fn bind_optional_degree_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Option<Result<LogicalPlan>> {
    if let Some(plan) = bind_direct_optional_degree_pipeline(query, parameters) {
        return Some(plan);
    }
    let [initial, optional, with, final_return] = query.clauses.as_slice() else {
        return None;
    };
    let ClauseKind::Match {
        optional: false,
        patterns: initial_patterns,
        ..
    } = &initial.kind
    else {
        return None;
    };
    let [initial_pattern] = initial_patterns.as_slice() else {
        return None;
    };
    if initial_pattern.variable.is_some() || !initial_pattern.steps.is_empty() {
        return None;
    }
    let source = &initial_pattern.first;
    if source.anonymous {
        return None;
    }

    let optional_match = optional_degree_match(optional, source)?;

    let ClauseKind::With(with_projection) = &with.kind else {
        return None;
    };
    if with_projection.distinct
        || !with_projection.order_by.is_empty()
        || with_projection.offset.is_some()
        || with_projection.limit.is_some()
    {
        return None;
    }
    let [group, count] = with_projection.items.as_slice() else {
        return None;
    };
    if !is_unaliased_variable(group, &source.variable) {
        return None;
    }
    let ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
        variable,
        distinct: false,
    }) = &count.expression.kind
    else {
        return None;
    };
    let counts_relationship = optional_match.relationship.variable.as_deref() == Some(variable);
    let counts_target = optional_match.target.variable == *variable;
    if !counts_relationship && !counts_target {
        return None;
    }
    let Some(alias) = &count.alias else {
        return None;
    };

    let ClauseKind::Return(final_projection) = &final_return.kind else {
        return None;
    };
    let initial = match bind_read_clauses(
        std::slice::from_ref(initial),
        None,
        Scope::default(),
        parameters,
    ) {
        Ok(plan) => normalize::normalize(plan),
        Err(error) => return Some(Err(error)),
    };
    let mut degree = match build_optional_degree(initial, source, optional_match, alias, parameters)
    {
        Ok(plan) => plan,
        Err(error) => return Some(Err(error)),
    };
    let mut scope = Scope::default();
    scope.0.insert(
        source.variable.clone(),
        BindingType::Graph {
            kind: GraphEntityKind::Node,
            column: None,
        },
    );
    scope.0.insert(alias.clone(), BindingType::Scalar);
    if let Some(predicate) = &with_projection.predicate {
        let predicate = match bind_predicate(&predicate.kind, &scope, parameters) {
            Ok(predicate) => predicate,
            Err(error) => return Some(Err(error)),
        };
        degree = LogicalPlan::Filter {
            predicate,
            input: Box::new(degree),
        };
    }
    Some(bind_projection(degree, &scope, final_projection, parameters).map(|(plan, _)| plan))
}

fn bind_direct_optional_degree_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Option<Result<LogicalPlan>> {
    let [initial, optional, final_return] = query.clauses.as_slice() else {
        return None;
    };
    let ClauseKind::Match {
        optional: false,
        patterns: initial_patterns,
        ..
    } = &initial.kind
    else {
        return None;
    };
    let [initial_pattern] = initial_patterns.as_slice() else {
        return None;
    };
    if initial_pattern.variable.is_some() || !initial_pattern.steps.is_empty() {
        return None;
    }
    let source = &initial_pattern.first;
    if source.anonymous {
        return None;
    }
    let optional_match = optional_degree_match(optional, source)?;
    let ClauseKind::Return(final_projection) = &final_return.kind else {
        return None;
    };
    if final_projection.distinct {
        return None;
    }
    let [(count_index, counted, alias)] = final_projection
        .items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match &item.expression.kind {
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                variable,
                distinct: false,
            }) => item
                .alias
                .as_ref()
                .map(|alias| (index, variable.as_str(), alias.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>()[..]
    else {
        return None;
    };
    let counts_relationship = optional_match.relationship.variable.as_deref() == Some(counted);
    let counts_target = optional_match.target.variable == counted;
    if !counts_relationship && !counts_target {
        return None;
    }
    let initial = match bind_read_clauses(
        std::slice::from_ref(initial),
        None,
        Scope::default(),
        parameters,
    ) {
        Ok(plan) => normalize::normalize(plan),
        Err(error) => return Some(Err(error)),
    };
    let degree = match build_optional_degree(initial, source, optional_match, alias, parameters) {
        Ok(plan) => plan,
        Err(error) => return Some(Err(error)),
    };
    let mut projection = final_projection.clone();
    projection.items[count_index].expression = AstNode::synthetic(ReturnExpressionKind::Value(
        AstNode::synthetic(ScalarExpressionKind::Variable(alias.to_string())),
    ));
    let mut scope = Scope::default();
    scope.0.insert(
        source.variable.clone(),
        BindingType::Graph {
            kind: GraphEntityKind::Node,
            column: None,
        },
    );
    scope.0.insert(alias.to_string(), BindingType::Scalar);
    Some(bind_projection(degree, &scope, &projection, parameters).map(|(plan, _)| plan))
}

struct OptionalDegreeMatch<'a> {
    relationship: &'a RelationshipPattern,
    direction: RelationshipDirection,
    target: &'a NodePattern,
}

fn optional_degree_match<'a>(
    clause: &'a Clause,
    source: &NodePattern,
) -> Option<OptionalDegreeMatch<'a>> {
    let ClauseKind::Match {
        optional: true,
        patterns,
        predicate: None,
    } = &clause.kind
    else {
        return None;
    };
    let [pattern] = patterns.as_slice() else {
        return None;
    };
    let [step] = pattern.steps.as_slice() else {
        return None;
    };
    let relationship = &step.relationship;
    if pattern.variable.is_some()
        || relationship.search != PathSearch::All
        || relationship.min_hops != 1
        || relationship.max_hops != 1
    {
        return None;
    }
    if pattern.first.variable == source.variable
        && !pattern.first.anonymous
        && pattern.first.label.is_empty()
        && pattern.first.properties.is_empty()
    {
        return Some(OptionalDegreeMatch {
            relationship,
            direction: relationship.direction,
            target: &step.target,
        });
    }
    if step.target.variable == source.variable
        && !step.target.anonymous
        && step.target.label.is_empty()
        && step.target.properties.is_empty()
    {
        return Some(OptionalDegreeMatch {
            relationship,
            direction: reverse_relationship_direction(relationship.direction),
            target: &pattern.first,
        });
    }
    None
}

fn build_optional_degree(
    input: LogicalPlan,
    source: &NodePattern,
    optional_match: OptionalDegreeMatch<'_>,
    alias: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    Ok(LogicalPlan::OptionalDegree {
        source_variable: source.variable.clone(),
        rel_type: optional_match.relationship.rel_type.clone(),
        rel_properties: bind_properties(&optional_match.relationship.properties, parameters)?,
        direction: optional_match.direction,
        target_label: optional_match.target.label.clone(),
        target_properties: bind_properties(&optional_match.target.properties, parameters)?,
        alias: alias.to_string(),
        input: Box::new(input),
    })
}

fn is_unaliased_variable(item: &ReturnItem, variable: &str) -> bool {
    item.alias.is_none()
        && matches!(
            &item.expression.kind,
            ReturnExpressionKind::Value(expression)
                if matches!(&expression.kind, ScalarExpressionKind::Variable(name) if name == variable)
        )
}

fn bind_read_clauses(
    clauses: &[hawdb_cypher::Clause],
    mut input: Option<LogicalPlan>,
    mut scope: Scope,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    for clause in clauses {
        match &clause.kind {
            ClauseKind::Match {
                optional,
                patterns,
                predicate,
            } => {
                let imports = scope.imports();
                let mut introduced = Vec::new();
                let mut steps = Vec::new();
                for pattern in patterns {
                    if let Some(variable) = &pattern.variable {
                        if scope.0.contains_key(variable) {
                            return Err(HawDBError::Semantic(format!(
                                "path variable '{variable}' is already bound"
                            )));
                        }
                        // Bounded expands may have an unused path alias, but it must not
                        // accidentally become a node binding in a later MATCH.
                        scope
                            .0
                            .insert(variable.clone(), BindingType::UnmaterializedPath);
                    }
                    if pattern
                        .steps
                        .iter()
                        .any(|step| step.relationship.search == PathSearch::AllShortest)
                    {
                        return Err(HawDBError::Semantic(
                            "path binding requires shortest-path lowering".to_string(),
                        ));
                    }
                    scope.bind_graph(
                        &pattern.first.variable,
                        GraphEntityKind::Node,
                        &mut introduced,
                    )?;
                    steps.push(GraphMatchStep::Node(bind_node(&pattern.first, parameters)?));
                    let mut source = pattern.first.variable.clone();
                    for step in &pattern.steps {
                        let relationship = &step.relationship;
                        if let Some(variable) = &relationship.variable {
                            scope.bind_graph(
                                variable,
                                GraphEntityKind::Relationship,
                                &mut introduced,
                            )?;
                        }
                        scope.bind_graph(
                            &step.target.variable,
                            GraphEntityKind::Node,
                            &mut introduced,
                        )?;
                        steps.push(GraphMatchStep::Expand {
                            source,
                            relationship: relationship.variable.clone(),
                            rel_type: relationship.rel_type.clone(),
                            properties: bind_properties(&relationship.properties, parameters)?,
                            direction: relationship.direction,
                            min_hops: relationship.min_hops,
                            max_hops: relationship.max_hops,
                            target: bind_node(&step.target, parameters)?,
                        });
                        source = step.target.variable.clone();
                    }
                }
                if steps.len() > 32 {
                    return Err(HawDBError::Semantic(
                        "MATCH exceeds maximum pattern depth".to_string(),
                    ));
                }
                scope.materialized();
                let predicate = predicate
                    .as_ref()
                    .map(|predicate| bind_predicate(&predicate.kind, &scope, parameters))
                    .transpose()?;
                input = Some(LogicalPlan::GraphMatch {
                    program: GraphMatchProgram {
                        imports,
                        introduced,
                        steps,
                        predicate,
                        optional: *optional,
                    },
                    input: input.map(Box::new),
                });
            }
            ClauseKind::With(projection) | ClauseKind::Return(projection) => {
                let current = input
                    .take()
                    .unwrap_or_else(|| restore_bindings(None, &mut Scope::default()));
                let (next, next_scope) = bind_projection(current, &scope, projection, parameters)?;
                input = Some(next);
                scope = next_scope;
            }
            _ => {
                return Err(HawDBError::Semantic(
                    "clause requires mutation or procedure pipeline lowering".to_string(),
                ))
            }
        }
    }
    input.ok_or_else(|| HawDBError::Semantic("empty query pipeline".to_string()))
}

fn bind_node(
    node: &hawdb_cypher::NodePattern,
    parameters: &BTreeMap<String, Value>,
) -> Result<GraphMatchNode> {
    Ok(GraphMatchNode {
        variable: node.variable.clone(),
        label: node.label.clone(),
        properties: bind_properties(&node.properties, parameters)?,
    })
}

fn restore_bindings(input: Option<LogicalPlan>, scope: &mut Scope) -> LogicalPlan {
    let imports = scope.imports();
    if imports.is_empty()
        && let Some(input) = input
    {
        return input;
    }
    scope.materialized();
    LogicalPlan::GraphMatch {
        program: GraphMatchProgram {
            imports,
            introduced: Vec::new(),
            steps: Vec::new(),
            predicate: None,
            optional: false,
        },
        input: input.map(Box::new),
    }
}

fn bind_projection(
    mut input: LogicalPlan,
    scope: &Scope,
    projection: &ProjectionClause,
    parameters: &BTreeMap<String, Value>,
) -> Result<(LogicalPlan, Scope)> {
    let graph = scope.graph_names();
    let columns = scope.columns();
    let mut planned = if projection
        .items
        .iter()
        .any(|item| super::arithmetic::contains_aggregate(&item.expression))
    {
        super::arithmetic::plan_composed_returns(&graph, &columns, &projection.items, parameters)?
    } else {
        plan_return_items_with_columns(&graph, &columns, &projection.items, parameters)?
    };
    let names = unique_projection_names(planned.names());
    let mut output_scope = Scope::default();
    for (item, name) in projection.items.iter().zip(&names) {
        let kind = match &item.expression.kind {
            ReturnExpressionKind::Value(AstNode {
                kind: ScalarExpressionKind::Variable(variable),
                ..
            }) => match scope.0.get(variable) {
                Some(BindingType::Graph { kind, .. }) => BindingType::Graph {
                    kind: *kind,
                    column: Some(name.clone()),
                },
                _ => BindingType::Scalar,
            },
            _ => BindingType::Scalar,
        };
        output_scope.0.insert(name.clone(), kind);
    }
    let projected_names = names.iter().cloned().collect();
    let projected_graph = output_scope.graph_names();
    let mut sort_items = Vec::new();
    let mut hidden_order_keys = false;
    for (index, item) in projection.order_by.iter().enumerate() {
        match plan_sort_items(
            &projected_graph,
            &projected_names,
            std::slice::from_ref(item),
            parameters,
        ) {
            Ok(mut items) => sort_items.append(&mut items),
            Err(error) if projection.distinct => return Err(error),
            Err(_) => {
                let mut items =
                    plan_sort_items(&graph, &columns, std::slice::from_ref(item), parameters)?;
                let item = items.pop().expect("one ORDER BY expression");
                let expression = match item.key {
                    SortKey::Column(column) => ProjectionExpression::Column(column),
                    SortKey::Property { variable, property } => {
                        ProjectionExpression::Property { variable, property }
                    }
                    SortKey::Id { variable } => ProjectionExpression::Id { variable },
                    SortKey::Expression(expression) => expression,
                };
                let name = format!("\0order.{index}");
                append_order_projection(
                    &mut planned,
                    Projection {
                        expression,
                        name: name.clone(),
                    },
                );
                sort_items.push(SortItem {
                    key: SortKey::Column(name),
                    direction: item.direction,
                });
                hidden_order_keys = true;
            }
        }
    }
    let needed = imports::for_returns(&mut planned, scope)?;
    input = imports::restore(input, scope, &needed);
    let mut output = planned.into_logical(input);
    if projection.distinct {
        output = LogicalPlan::Distinct {
            input: Box::new(output),
        };
    }
    if let Some(predicate) = &projection.predicate {
        let mut predicate_scope = output_scope.clone();
        predicate_scope.materialized();
        let predicate = bind_predicate(&predicate.kind, &predicate_scope, parameters)?;
        output = imports::restore(output, &output_scope, &imports::for_predicate(&predicate));
        output = LogicalPlan::Filter {
            predicate,
            input: Box::new(output),
        };
    }
    if !sort_items.is_empty() {
        let needed = imports::for_sort(&mut sort_items, &output_scope)?;
        output = imports::restore(output, &output_scope, &needed);
        output = LogicalPlan::Sort {
            items: sort_items,
            input: Box::new(output),
        };
    }
    let offset = projection
        .offset
        .as_ref()
        .map(|value| bind_pagination_value(value, parameters, "offset"))
        .transpose()?
        .unwrap_or(0);
    let limit = projection
        .limit
        .as_ref()
        .map(|value| bind_pagination_value(value, parameters, "limit"))
        .transpose()?;
    if offset != 0 || limit.is_some() {
        output = LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(output),
        };
    }
    if hidden_order_keys {
        output = LogicalPlan::Project {
            items: names
                .into_iter()
                .map(|name| Projection {
                    expression: ProjectionExpression::Column(name.clone()),
                    name,
                })
                .collect(),
            input: Box::new(output),
        };
    }
    Ok((output, output_scope))
}

fn unique_projection_names(names: Vec<String>) -> Vec<String> {
    let mut used = BTreeSet::new();
    names
        .into_iter()
        .map(|name| {
            let mut candidate = name.clone();
            let mut suffix = 2;
            while !used.insert(candidate.clone()) {
                candidate = format!("{name}#{suffix}");
                suffix += 1;
            }
            candidate
        })
        .collect()
}

fn append_order_projection(planned: &mut PlannedReturns, projection: Projection) {
    match planned {
        PlannedReturns::Projections(items) => items.push(projection),
        PlannedReturns::Aggregations { group_keys, .. } => group_keys.push(projection),
        PlannedReturns::AggregateProjection {
            group_keys,
            projections,
            ..
        } => {
            projections.push(Projection {
                expression: ProjectionExpression::Column(projection.name.clone()),
                name: projection.name.clone(),
            });
            group_keys.push(projection);
        }
    }
}

fn bind_predicate(
    predicate: &PropertyPredicate,
    scope: &Scope,
    parameters: &BTreeMap<String, Value>,
) -> Result<Predicate> {
    let graph = scope.graph_names();
    let columns = scope.columns();
    let all = graph.union(&columns).cloned().collect();
    validate_predicate(&all, predicate)?;
    let bind =
        |expression| plan_scalar_expression_with_columns(&graph, &columns, expression, parameters);
    Ok(match predicate {
        PropertyPredicate::And(children) => Predicate::And(
            children
                .iter()
                .map(|child| bind_predicate(child, scope, parameters))
                .collect::<Result<_>>()?,
        ),
        PropertyPredicate::Or(children) => Predicate::Or(
            children
                .iter()
                .map(|child| bind_predicate(child, scope, parameters))
                .collect::<Result<_>>()?,
        ),
        PropertyPredicate::Not(child) => {
            Predicate::Not(Box::new(bind_predicate(child, scope, parameters)?))
        }
        PropertyPredicate::ExpressionEq { expression, value } => Predicate::ExpressionEq {
            expression: bind(expression)?,
            value: bind(value)?,
        },
        PropertyPredicate::ExpressionNotEq { expression, value } => Predicate::ExpressionNotEq {
            expression: bind(expression)?,
            value: bind(value)?,
        },
        PropertyPredicate::ExpressionCompare {
            expression,
            op,
            value,
        } => Predicate::ExpressionCompare {
            expression: bind(expression)?,
            op: plan_comparison_op(*op),
            value: bind(value)?,
        },
        PropertyPredicate::ExpressionContains { expression, value } => {
            Predicate::ExpressionContains {
                expression: bind(expression)?,
                value: bind(value)?,
            }
        }
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if columns.contains(variable) => Predicate::ExpressionEq {
            expression: column_property(variable, property),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::NotEq {
            variable,
            property,
            value,
        } if columns.contains(variable) => Predicate::ExpressionNotEq {
            expression: column_property(variable, property),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::Compare {
            variable,
            property,
            op,
            value,
        } if columns.contains(variable) => Predicate::ExpressionCompare {
            expression: column_property(variable, property),
            op: plan_comparison_op(*op),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::Contains {
            variable,
            property,
            value,
        } if columns.contains(variable) => Predicate::ExpressionContains {
            expression: column_property(variable, property),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::IsNull { variable, property }
        | PropertyPredicate::IsNotNull { variable, property }
            if columns.contains(variable) =>
        {
            Predicate::ExpressionEq {
                expression: ProjectionExpression::IsNull {
                    expression: Box::new(column_property(variable, property)),
                    negated: matches!(predicate, PropertyPredicate::IsNotNull { .. }),
                },
                value: ProjectionExpression::Literal(Value::Bool(true)),
            }
        }
        _ => {
            let mut variables = BTreeSet::new();
            collect_predicate_variables(predicate, &mut variables);
            if variables.iter().any(|variable| columns.contains(variable)) {
                return Err(HawDBError::Semantic(
                    "predicate requires a graph binding for this operation".to_string(),
                ));
            }
            plan_predicate(predicate, &graph, parameters)?
        }
    })
}

fn column_property(column: &str, property: &str) -> ProjectionExpression {
    ProjectionExpression::ColumnProperty {
        column: column.to_string(),
        property: property.to_string(),
    }
}
