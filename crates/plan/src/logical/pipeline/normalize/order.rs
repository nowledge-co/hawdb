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
        let (variable, key) = match &item.expression {
            ProjectionExpression::Property { variable, property } => (
                variable,
                SortKey::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
            ),
            ProjectionExpression::Id { variable } => (
                variable,
                SortKey::Id {
                    variable: variable.clone(),
                },
            ),
            _ => return None,
        };
        if !has_native_binding(input, variable) {
            return None;
        }
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
