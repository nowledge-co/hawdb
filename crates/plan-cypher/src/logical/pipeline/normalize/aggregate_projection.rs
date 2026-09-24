//! Move infallible selectors across sort/limit without changing aggregation scope.

use super::*;

struct Columns<'a> {
    names: BTreeSet<&'a str>,
    entities: BTreeSet<&'a str>,
}

pub(super) enum ProjectRewrite {
    Applied(LogicalPlan),
    Unchanged {
        items: Vec<Projection>,
        input: LogicalPlan,
    },
}

fn columns(input: &LogicalPlan) -> Option<Columns<'_>> {
    match input {
        LogicalPlan::Aggregate {
            group_keys, items, ..
        } => Some(Columns {
            names: group_keys
                .iter()
                .map(|item| item.name.as_str())
                .chain(items.iter().map(|item| item.name.as_str()))
                .collect(),
            entities: group_keys
                .iter()
                .filter_map(|item| {
                    matches!(item.expression, ProjectionExpression::Variable { .. })
                        .then_some(item.name.as_str())
                })
                .collect(),
        }),
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. } => columns(input),
        _ => None,
    }
}

fn selector(expression: &ProjectionExpression, columns: &Columns<'_>) -> bool {
    match expression {
        ProjectionExpression::Column(name) => columns.names.contains(name.as_str()),
        ProjectionExpression::ColumnProperty { column, .. } => {
            columns.entities.contains(column.as_str())
        }
        ProjectionExpression::Coalesce(children) => {
            children.iter().all(|child| selector(child, columns))
        }
        ProjectionExpression::Literal(_) => true,
        _ => false,
    }
}

fn selectors(items: &[Projection], input: &LogicalPlan) -> bool {
    let Some(columns) = columns(input) else {
        return false;
    };
    let mut names = BTreeSet::new();
    items
        .iter()
        .all(|item| names.insert(&item.name) && selector(&item.expression, &columns))
}

fn remap(expression: &ProjectionExpression, items: &[Projection]) -> Option<ProjectionExpression> {
    let lookup = |name: &str| {
        items
            .iter()
            .find(|item| item.name == name)
            .map(|item| &item.expression)
    };
    Some(match expression {
        ProjectionExpression::Column(name) => lookup(name)?.clone(),
        ProjectionExpression::ColumnProperty { column, property } => {
            let ProjectionExpression::Column(source) = lookup(column)? else {
                return None;
            };
            ProjectionExpression::ColumnProperty {
                column: source.clone(),
                property: property.clone(),
            }
        }
        ProjectionExpression::Coalesce(children) => ProjectionExpression::Coalesce(
            children
                .iter()
                .map(|child| remap(child, items))
                .collect::<Option<_>>()?,
        ),
        ProjectionExpression::Literal(value) => ProjectionExpression::Literal(value.clone()),
        _ => return None,
    })
}

fn remap_keys(
    keys: &[SortItem],
    items: &[Projection],
    input: &LogicalPlan,
) -> Option<Vec<SortItem>> {
    if !selectors(items, input) {
        return None;
    }
    let columns = columns(input)?;
    keys.iter()
        .map(|item| {
            let expression = match &item.key {
                SortKey::Column(name) => remap(&ProjectionExpression::Column(name.clone()), items)?,
                SortKey::Expression(expression) => remap(expression, items)?,
                _ => return None,
            };
            if !selector(&expression, &columns) {
                return None;
            }
            let key = match expression {
                ProjectionExpression::Column(column) => SortKey::Column(column),
                expression => SortKey::Expression(expression),
            };
            Some(SortItem {
                key,
                direction: item.direction,
            })
        })
        .collect()
}

pub(super) fn sort(keys: Vec<SortItem>, input: LogicalPlan) -> LogicalPlan {
    let rewritten = match &input {
        LogicalPlan::Project { items, input } => remap_keys(&keys, items, input),
        _ => None,
    };
    if let Some(keys) = rewritten {
        let LogicalPlan::Project { items, input } = input else {
            unreachable!()
        };
        return LogicalPlan::Project {
            items,
            input: Box::new(LogicalPlan::Sort { items: keys, input }),
        };
    }
    LogicalPlan::Sort {
        items: keys,
        input: Box::new(input),
    }
}

fn hidden_prefix(projections: &[Projection], input: &LogicalPlan) -> bool {
    let input = match input {
        LogicalPlan::Limit { input, .. } => input,
        input => input,
    };
    let LogicalPlan::Project { items, input } = input else {
        return false;
    };
    projections.len() < items.len()
        && selectors(items, input)
        && projections.iter().zip(items).all(|(outer, inner)| outer.name == inner.name
            && matches!(&outer.expression, ProjectionExpression::Column(column) if column == &inner.name))
        && items[projections.len()..].iter().all(|item| item.name.starts_with("\0order."))
}

pub(super) fn project(projections: Vec<Projection>, input: LogicalPlan) -> ProjectRewrite {
    if hidden_prefix(&projections, &input) {
        let (limit, input) = match input {
            LogicalPlan::Limit {
                offset,
                limit,
                input,
            } => (Some((offset, limit)), *input),
            input => (None, input),
        };
        let LogicalPlan::Project { mut items, input } = input else {
            unreachable!()
        };
        items.truncate(projections.len());
        let output = LogicalPlan::Project { items, input };
        return ProjectRewrite::Applied(match limit {
            Some((offset, limit)) => LogicalPlan::Limit {
                offset,
                limit,
                input: Box::new(output),
            },
            None => output,
        });
    }
    if let LogicalPlan::Limit { input: limited, .. } = &input
        && selectors(&projections, limited)
    {
        let LogicalPlan::Limit {
            offset,
            limit,
            input,
        } = input
        else {
            unreachable!()
        };
        return ProjectRewrite::Applied(LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(LogicalPlan::Project {
                items: projections,
                input,
            }),
        });
    }
    ProjectRewrite::Unchanged {
        items: projections,
        input,
    }
}
