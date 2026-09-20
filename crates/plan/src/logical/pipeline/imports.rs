//! Restore typed graph identities only for expressions that require native records.

use super::*;

pub(super) fn restore(input: LogicalPlan, scope: &Scope, needed: &BTreeSet<String>) -> LogicalPlan {
    let imports = scope
        .imports()
        .into_iter()
        .filter(|import| needed.contains(&import.variable))
        .collect::<Vec<_>>();
    if imports.is_empty() {
        return input;
    }
    LogicalPlan::GraphMatch {
        program: GraphMatchProgram {
            imports,
            introduced: Vec::new(),
            steps: Vec::new(),
            predicate: None,
            optional: false,
        },
        input: Some(Box::new(input)),
    }
}

pub(super) fn for_returns(returns: &mut PlannedReturns, scope: &Scope) -> Result<BTreeSet<String>> {
    let mut needed = BTreeSet::new();
    match returns {
        PlannedReturns::Projections(items) => projections(items, scope, &mut needed)?,
        PlannedReturns::Aggregations { group_keys, items } => {
            projections(group_keys, scope, &mut needed)?;
            aggregations(items, scope, &mut needed);
        }
        PlannedReturns::AggregateProjection {
            group_keys,
            items,
            projections: output,
        } => {
            projections(group_keys, scope, &mut needed)?;
            aggregations(items, scope, &mut needed);
            projections(output, scope, &mut needed)?;
        }
    }
    Ok(needed)
}

fn projections(
    items: &mut [Projection],
    scope: &Scope,
    needed: &mut BTreeSet<String>,
) -> Result<()> {
    for item in items {
        projection(&mut item.expression, scope, needed)?;
    }
    Ok(())
}

fn aggregations(items: &mut [Aggregation], scope: &Scope, needed: &mut BTreeSet<String>) {
    for item in items {
        if let AggregateTarget::ColumnProperty { column, property } = &item.target
            && metadata_property(scope, column, property)
        {
            item.target = AggregateTarget::Property {
                variable: column.clone(),
                property: property.clone(),
            };
        }
        if let AggregateTarget::Variable(variable) | AggregateTarget::Property { variable, .. } =
            &item.target
        {
            needed.insert(variable.clone());
        }
    }
}

fn metadata_property(scope: &Scope, variable: &str, property: &str) -> bool {
    match scope.0.get(variable) {
        Some(BindingType::Graph {
            kind: GraphEntityKind::Node,
            column: Some(_),
        }) => matches!(property, "_id" | "labels"),
        Some(BindingType::Graph {
            kind: GraphEntityKind::Relationship,
            column: Some(_),
        }) => matches!(property, "_id" | "source_id" | "target_id" | "type"),
        _ => false,
    }
}

fn projection(
    expression: &mut ProjectionExpression,
    scope: &Scope,
    needed: &mut BTreeSet<String>,
) -> Result<()> {
    expression.try_for_each_child_mut(|child| projection(child, scope, needed))?;
    match expression {
        ProjectionExpression::ColumnProperty { column, property }
            if metadata_property(scope, column, property) =>
        {
            *expression = ProjectionExpression::Property {
                variable: column.clone(),
                property: property.clone(),
            };
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column,
            property,
            empty,
            default,
        } if metadata_property(scope, column, property) => {
            *expression = ProjectionExpression::DefaultIfNullOrEq {
                variable: column.clone(),
                property: property.clone(),
                empty: empty.clone(),
                default: default.clone(),
            };
        }
        _ => {}
    }
    native_projection_self(expression, needed);
    Ok(())
}

fn native_projection(expression: &ProjectionExpression, needed: &mut BTreeSet<String>) {
    native_projection_self(expression, needed);
    expression.all_children(|child| {
        native_projection(child, needed);
        true
    });
}

fn native_projection_self(expression: &ProjectionExpression, needed: &mut BTreeSet<String>) {
    match expression {
        ProjectionExpression::Variable { variable }
        | ProjectionExpression::Property { variable, .. }
        | ProjectionExpression::Id { variable }
        | ProjectionExpression::RelationshipType { variable }
        | ProjectionExpression::DatePart { variable, .. }
        | ProjectionExpression::DefaultIfNullOrEq { variable, .. }
        | ProjectionExpression::DefaultIfNull { variable, .. }
        | ProjectionExpression::CasePropertyNotNullOrEq { variable, .. }
        | ProjectionExpression::CasePropertyEqualsRank { variable, .. }
        | ProjectionExpression::CaseLowerPropertyDefault { variable, .. }
        | ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            needed.insert(variable.clone());
        }
        ProjectionExpression::CaseEntitySearchRank(rank) => {
            needed.insert(rank.variable.clone());
        }
        ProjectionExpression::Literal(_)
        | ProjectionExpression::Coalesce(_)
        | ProjectionExpression::Left { .. }
        | ProjectionExpression::Lower(_)
        | ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. }
        | ProjectionExpression::CaseColumnSearchRank(_)
        | ProjectionExpression::ColumnDefaultIfNullOrEq { .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { .. }
        | ProjectionExpression::Column(_)
        | ProjectionExpression::ColumnProperty { .. } => {}
    }
}

pub(super) fn for_predicate(predicate: &Predicate) -> BTreeSet<String> {
    let mut needed = BTreeSet::new();
    collect_predicate(predicate, &mut needed);
    needed
}

fn collect_predicate(predicate: &Predicate, needed: &mut BTreeSet<String>) {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_predicate(child, needed);
            }
        }
        Predicate::Not(child) => collect_predicate(child, needed),
        Predicate::ConstantBool(_) => {}
        Predicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => {
            needed.insert(source_variable.clone());
            needed.insert(target_variable.clone());
        }
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            native_projection(expression, needed);
            native_projection(value, needed);
        }
        Predicate::RelationshipExists { variable, .. }
        | Predicate::IdEq { variable, .. }
        | Predicate::IdNotEq { variable, .. }
        | Predicate::IdCompare { variable, .. }
        | Predicate::IdIn { variable, .. }
        | Predicate::PropertyEq { variable, .. }
        | Predicate::PropertyNotEq { variable, .. }
        | Predicate::PropertyCompare { variable, .. }
        | Predicate::PropertyListContains { variable, .. }
        | Predicate::PropertyListContainsLower { variable, .. }
        | Predicate::PropertyContains { variable, .. }
        | Predicate::PropertyStartsWith { variable, .. }
        | Predicate::PropertyEndsWith { variable, .. }
        | Predicate::PropertyRegexMatch { variable, .. }
        | Predicate::PropertyIsNull { variable, .. }
        | Predicate::PropertyIsNotNull { variable, .. }
        | Predicate::PropertyIn { variable, .. } => {
            needed.insert(variable.clone());
        }
    }
}

pub(super) fn for_sort(items: &mut [SortItem], scope: &Scope) -> Result<BTreeSet<String>> {
    let mut needed = BTreeSet::new();
    for item in items {
        match &mut item.key {
            SortKey::Property { variable, .. } | SortKey::Id { variable } => {
                needed.insert(variable.clone());
            }
            SortKey::Expression(expression) => projection(expression, scope, &mut needed)?,
            SortKey::Column(_) => {}
        }
    }
    Ok(needed)
}
