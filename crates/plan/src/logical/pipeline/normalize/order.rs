use super::*;

pub(super) fn inline_keys(
    projections: &[Projection],
    input: &LogicalPlan,
) -> Option<Vec<SortItem>> {
    let input = match input {
        LogicalPlan::Limit { input, .. } => input,
        input => input,
    };
    let LogicalPlan::Sort { items: keys, input } = input else {
        return None;
    };
    let LogicalPlan::Project { items, input } = input.as_ref() else {
        return None;
    };
    if projections.len() >= items.len()
        || !projections.iter().zip(items).all(|(outer, inner)| {
            outer.name == inner.name
                && matches!(&outer.expression, ProjectionExpression::Column(column) if column == &inner.name)
        })
    {
        return None;
    }
    let mut replacements = BTreeMap::new();
    for item in &items[projections.len()..] {
        if !item.name.starts_with("\0order.") {
            return None;
        }
        let key = match &item.expression {
            ProjectionExpression::Property { variable, property }
                if has_native_binding(input, variable) =>
            {
                SortKey::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                }
            }
            ProjectionExpression::Id { variable } if has_native_binding(input, variable) => {
                SortKey::Id {
                    variable: variable.clone(),
                }
            }
            expression if has_native_bindings(expression, input) => {
                SortKey::Expression(expression.clone())
            }
            _ => return None,
        };
        replacements.insert(&item.name, key);
    }
    let mut used = BTreeSet::new();
    let keys = keys
        .iter()
        .map(|item| {
            let key = match &item.key {
                SortKey::Column(column) if replacements.contains_key(column) => {
                    used.insert(column);
                    replacements[column].clone()
                }
                key => key.clone(),
            };
            SortItem {
                key,
                direction: item.direction,
            }
        })
        .collect();
    (used.len() == replacements.len()).then_some(keys)
}

fn has_native_binding(input: &LogicalPlan, variable: &str) -> bool {
    match input {
        LogicalPlan::NodeScan {
            variable: source, ..
        } => variable == source,
        LogicalPlan::Expand {
            target_variable,
            rel_variable,
            input,
            ..
        } => {
            variable == target_variable
                || rel_variable.as_deref() == Some(variable)
                || has_native_binding(input, variable)
        }
        LogicalPlan::Filter { input, .. } => has_native_binding(input, variable),
        _ => false,
    }
}

fn has_native_bindings(expression: &ProjectionExpression, input: &LogicalPlan) -> bool {
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
            has_native_binding(input, variable)
        }
        ProjectionExpression::Literal(_) => true,
        ProjectionExpression::Coalesce(expressions) => expressions
            .iter()
            .all(|expression| has_native_bindings(expression, input)),
        ProjectionExpression::Left { expression, .. }
        | ProjectionExpression::Lower(expression)
        | ProjectionExpression::Not(expression) => has_native_bindings(expression, input),
        ProjectionExpression::Case {
            operand,
            branches,
            otherwise,
        } => {
            operand
                .as_deref()
                .is_none_or(|expression| has_native_bindings(expression, input))
                && branches.iter().all(|(condition, result)| {
                    has_native_bindings(condition, input) && has_native_bindings(result, input)
                })
                && otherwise
                    .as_deref()
                    .is_none_or(|expression| has_native_bindings(expression, input))
        }
        ProjectionExpression::Binary { left, right, .. } => {
            has_native_bindings(left, input) && has_native_bindings(right, input)
        }
        ProjectionExpression::IsNull { expression, .. } => has_native_bindings(expression, input),
        ProjectionExpression::CaseEntitySearchRank(rank) => {
            has_native_binding(input, &rank.variable)
        }
        ProjectionExpression::CaseColumnSearchRank(_)
        | ProjectionExpression::ColumnDefaultIfNullOrEq { .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { .. }
        | ProjectionExpression::Column(_)
        | ProjectionExpression::ColumnProperty { .. } => false,
    }
}

pub(super) fn remove_hidden_keys(
    count: usize,
    input: LogicalPlan,
    keys: Vec<SortItem>,
) -> LogicalPlan {
    let (pagination, input) = match input {
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => (Some((offset, limit)), *input),
        input => (None, input),
    };
    let LogicalPlan::Sort { input, .. } = input else {
        unreachable!()
    };
    let LogicalPlan::Project { mut items, input } = *input else {
        unreachable!()
    };
    items.truncate(count);
    let sorted = LogicalPlan::Sort {
        items: keys,
        input: Box::new(LogicalPlan::Project { items, input }),
    };
    match pagination {
        Some((offset, limit)) => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(sorted),
        },
        None => sorted,
    }
}
