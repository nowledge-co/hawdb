use skein_core::{Result, SkeinError, Value};
use skein_cypher as cypher;
use skein_plan::{
    self as planner, LogicalPlan, PhysicalPlan, Predicate, Projection, ProjectionExpression,
    RelationshipCountFilter, SortItem, SortKey,
};
use std::collections::BTreeMap;

const PARAMETER_SLOT_NAME_KEY: &str = "\0skein_parameter_slot";
const PARAMETER_SLOT_PATH_KEY: &str = "\0skein_parameter_path";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ParameterCacheValue {
    Slot(ParameterValueShape),
    Exact(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ParameterValueShape {
    Null,
    Bool,
    Int,
    Float,
    String,
    Binary,
    Uuid,
    List(Vec<ParameterValueShape>),
    Map(BTreeMap<String, ParameterValueShape>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParameterizedLogicalPlan {
    logical: LogicalPlan,
    cache_key: PlanParameterCacheKey,
    slot_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlanParameterCacheKey {
    values: BTreeMap<String, ParameterCacheValue>,
}

impl ParameterizedLogicalPlan {
    pub fn logical(&self) -> &LogicalPlan {
        &self.logical
    }

    pub fn cache_key(&self) -> &PlanParameterCacheKey {
        &self.cache_key
    }

    pub fn slot_count(&self) -> usize {
        self.slot_count
    }

    pub fn exact_variant_count(&self) -> usize {
        self.cache_key.values.len() - self.slot_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerUse {
    None,
    Safe,
    Unsafe,
}

impl MarkerUse {
    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unsafe, _) | (_, Self::Unsafe) => Self::Unsafe,
            (Self::Safe, _) | (_, Self::Safe) => Self::Safe,
            (Self::None, Self::None) => Self::None,
        }
    }
}

pub fn parameterize_logical_plan(
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<ParameterizedLogicalPlan> {
    let actual = planner::plan_with_params(statement, parameters)?;
    if parameters.values().any(value_contains_any_marker) {
        return Ok(ParameterizedLogicalPlan {
            logical: actual,
            cache_key: PlanParameterCacheKey {
                values: parameters
                    .iter()
                    .map(|(name, value)| (name.clone(), ParameterCacheValue::Exact(value.clone())))
                    .collect(),
            },
            slot_count: 0,
        });
    }
    let mut template_parameters = parameters.clone();
    let mut cache_values = BTreeMap::new();
    let mut slot_count = 0;

    for (name, value) in parameters {
        let marker = parameter_marker(name, value, &[]);
        let mut candidate_parameters = parameters.clone();
        candidate_parameters.insert(name.clone(), marker.clone());
        let is_safe = planner::plan_with_params(statement, &candidate_parameters)
            .ok()
            .is_some_and(|mut candidate| {
                marker_use_in_logical_plan(&candidate, name) == MarkerUse::Safe
                    && bind_logical_plan(&mut candidate, parameters).is_ok()
                    && candidate == actual
            });
        if is_safe {
            template_parameters.insert(name.clone(), marker);
            cache_values.insert(
                name.clone(),
                ParameterCacheValue::Slot(parameter_value_shape(value)),
            );
            slot_count += 1;
        } else {
            cache_values.insert(name.clone(), ParameterCacheValue::Exact(value.clone()));
        }
    }

    let logical = if slot_count == 0 {
        actual.clone()
    } else {
        let candidate = planner::plan_with_params(statement, &template_parameters)?;
        let all_slots_are_safe = cache_values.iter().all(|(name, cached)| {
            !matches!(cached, ParameterCacheValue::Slot(_))
                || marker_use_in_logical_plan(&candidate, name) == MarkerUse::Safe
        });
        let mut rebound = candidate.clone();
        if all_slots_are_safe
            && bind_logical_plan(&mut rebound, parameters).is_ok()
            && rebound == actual
        {
            candidate
        } else {
            cache_values = parameters
                .iter()
                .map(|(name, value)| (name.clone(), ParameterCacheValue::Exact(value.clone())))
                .collect();
            slot_count = 0;
            actual.clone()
        }
    };
    Ok(ParameterizedLogicalPlan {
        logical,
        cache_key: PlanParameterCacheKey {
            values: cache_values,
        },
        slot_count,
    })
}

/// Creates internal parameter markers for a list whose shape is part of a
/// cached plan while its values are bound for every execution.
///
/// The caller must include `Value::List(values.to_vec())` under `name` when
/// calling `bind_physical_plan_parameters`.
pub fn parameterize_value_list(name: &str, values: &[Value]) -> Vec<Value> {
    match parameter_marker(name, &Value::List(values.to_vec()), &[]) {
        Value::List(markers) => markers,
        _ => unreachable!("list parameterization always produces a list"),
    }
}

pub fn bind_physical_plan_parameters(
    template: &PhysicalPlan,
    parameters: &BTreeMap<String, Value>,
    has_slots: bool,
) -> Result<PhysicalPlan> {
    let mut plan = template.clone();
    if has_slots {
        bind_physical_plan(&mut plan, parameters)?;
    }
    Ok(plan)
}

fn parameter_value_shape(value: &Value) -> ParameterValueShape {
    match value {
        Value::Null => ParameterValueShape::Null,
        Value::Bool(_) => ParameterValueShape::Bool,
        Value::Int(_) => ParameterValueShape::Int,
        Value::Float(_) => ParameterValueShape::Float,
        Value::String(_) => ParameterValueShape::String,
        Value::Binary(_) => ParameterValueShape::Binary,
        Value::Uuid(_) => ParameterValueShape::Uuid,
        Value::List(values) => {
            ParameterValueShape::List(values.iter().map(parameter_value_shape).collect())
        }
        Value::Map(values) => ParameterValueShape::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), parameter_value_shape(value)))
                .collect(),
        ),
    }
}

fn parameter_marker(name: &str, value: &Value, path: &[usize]) -> Value {
    match value {
        Value::List(values) => Value::List(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let mut nested_path = path.to_vec();
                    nested_path.push(index);
                    parameter_marker(name, value, &nested_path)
                })
                .collect(),
        ),
        _ => Value::Map(BTreeMap::from([
            (
                PARAMETER_SLOT_NAME_KEY.to_string(),
                Value::String(name.to_string()),
            ),
            (
                PARAMETER_SLOT_PATH_KEY.to_string(),
                Value::List(path.iter().map(|index| Value::Int(*index as i64)).collect()),
            ),
        ])),
    }
}

fn marker_name_and_path(value: &Value) -> Option<(&str, Vec<usize>)> {
    let Value::Map(values) = value else {
        return None;
    };
    if values.len() != 2 {
        return None;
    }
    let Value::String(name) = values.get(PARAMETER_SLOT_NAME_KEY)? else {
        return None;
    };
    let Value::List(path) = values.get(PARAMETER_SLOT_PATH_KEY)? else {
        return None;
    };
    let path = path
        .iter()
        .map(|index| match index {
            Value::Int(index) if *index >= 0 => usize::try_from(*index).ok(),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some((name, path))
}

fn value_contains_marker(value: &Value, name: &str) -> bool {
    if marker_name_and_path(value).is_some_and(|(marker_name, _)| marker_name == name) {
        return true;
    }
    match value {
        Value::List(values) => values
            .iter()
            .any(|value| value_contains_marker(value, name)),
        Value::Map(values) => values
            .values()
            .any(|value| value_contains_marker(value, name)),
        _ => false,
    }
}

fn value_contains_any_marker(value: &Value) -> bool {
    if marker_name_and_path(value).is_some() {
        return true;
    }
    match value {
        Value::List(values) => values.iter().any(value_contains_any_marker),
        Value::Map(values) => values.values().any(value_contains_any_marker),
        _ => false,
    }
}

fn values_marker_use<'a>(values: impl IntoIterator<Item = &'a Value>, name: &str) -> MarkerUse {
    if values
        .into_iter()
        .any(|value| value_contains_marker(value, name))
    {
        MarkerUse::Safe
    } else {
        MarkerUse::None
    }
}

fn marker_use_in_logical_plan(plan: &LogicalPlan, name: &str) -> MarkerUse {
    match plan {
        LogicalPlan::NodeScan { .. } | LogicalPlan::ThreadRepairStats { .. } => MarkerUse::None,
        LogicalPlan::NodeCartesianProduct { left, right } => {
            marker_use_in_logical_plan(left, name).combine(marker_use_in_logical_plan(right, name))
        }
        LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Limit { input, .. } => marker_use_in_logical_plan(input, name),
        LogicalPlan::Expand {
            rel_properties,
            input,
            ..
        } => values_marker_use(rel_properties.values(), name)
            .combine(marker_use_in_logical_plan(input, name)),
        LogicalPlan::OptionalDegree {
            rel_properties,
            target_properties,
            input,
            ..
        } => values_marker_use(rel_properties.values(), name)
            .combine(values_marker_use(target_properties.values(), name))
            .combine(marker_use_in_logical_plan(input, name)),
        LogicalPlan::OptionalRelationshipCountSum {
            properties, legs, ..
        } => {
            let mut marker_use = values_marker_use(properties.values(), name);
            for leg in legs {
                if let Some(RelationshipCountFilter::PropertyNotEqOrEmpty { value, .. }) =
                    &leg.filter
                {
                    marker_use = marker_use.combine(if value_contains_marker(value, name) {
                        MarkerUse::Safe
                    } else {
                        MarkerUse::None
                    });
                }
            }
            marker_use
        }
        LogicalPlan::Filter {
            predicate, input, ..
        } => marker_use_in_predicate(predicate, name)
            .combine(marker_use_in_logical_plan(input, name)),
        LogicalPlan::Project { items, input } => marker_use_in_projections(items, name, false)
            .combine(marker_use_in_logical_plan(input, name)),
        LogicalPlan::Aggregate {
            group_keys, input, ..
        } => marker_use_in_projections(group_keys, name, false)
            .combine(marker_use_in_logical_plan(input, name)),
        LogicalPlan::Sort { items, input } => {
            let marker_use = items.iter().fold(MarkerUse::None, |marker_use, item| {
                marker_use.combine(marker_use_in_sort_item(item, name))
            });
            marker_use.combine(marker_use_in_logical_plan(input, name))
        }
        LogicalPlan::ShortestPath {
            source_id,
            source_visibility_predicate,
            target_id,
            target_visibility_predicate,
            ..
        } => {
            let mut marker_use = values_marker_use([source_id, target_id], name);
            if marker_use == MarkerUse::Safe {
                marker_use = MarkerUse::Unsafe;
            }
            for predicate in [source_visibility_predicate, target_visibility_predicate]
                .into_iter()
                .flatten()
            {
                marker_use = marker_use.combine(marker_use_in_predicate(predicate, name));
            }
            marker_use
        }
        LogicalPlan::GraphAlgorithm {
            node_visibility_predicate,
            ..
        } => node_visibility_predicate
            .as_ref()
            .map(|predicate| marker_use_in_predicate(predicate, name))
            .unwrap_or(MarkerUse::None),
        _ => MarkerUse::None,
    }
}

fn marker_use_in_predicate(predicate: &Predicate, name: &str) -> MarkerUse {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .fold(MarkerUse::None, |marker_use, predicate| {
                marker_use.combine(marker_use_in_predicate(predicate, name))
            }),
        Predicate::Not(predicate) => marker_use_in_predicate(predicate, name),
        Predicate::PropertyEq { value, .. } | Predicate::PropertyNotEq { value, .. } => {
            values_marker_use([value], name)
        }
        Predicate::PropertyIn { values, .. } => values_marker_use(values, name),
        Predicate::IdEq { value, .. }
        | Predicate::IdNotEq { value, .. }
        | Predicate::IdCompare { value, .. }
        | Predicate::PropertyCompare { value, .. }
        | Predicate::PropertyListContains { value, .. } => {
            if value_contains_marker(value, name) {
                MarkerUse::Unsafe
            } else {
                MarkerUse::None
            }
        }
        Predicate::IdIn { values, .. } => {
            if values
                .iter()
                .any(|value| value_contains_marker(value, name))
            {
                MarkerUse::Unsafe
            } else {
                MarkerUse::None
            }
        }
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionContains { expression, value } => {
            marker_use_in_projection_expression(expression, name, false)
                .combine(marker_use_in_projection_expression(value, name, false))
        }
        Predicate::ExpressionCompare {
            expression, value, ..
        } => marker_use_in_projection_expression(expression, name, false)
            .combine(marker_use_in_projection_expression(value, name, false)),
        _ => MarkerUse::None,
    }
}

fn marker_use_in_projections(projections: &[Projection], name: &str, safe: bool) -> MarkerUse {
    projections
        .iter()
        .fold(MarkerUse::None, |marker_use, projection| {
            marker_use.combine(marker_use_in_projection_expression(
                &projection.expression,
                name,
                safe,
            ))
        })
}

fn marker_use_in_sort_item(item: &SortItem, name: &str) -> MarkerUse {
    match &item.key {
        SortKey::Expression(expression) => {
            marker_use_in_projection_expression(expression, name, false)
        }
        _ => MarkerUse::None,
    }
}

fn marker_use_in_projection_expression(
    expression: &ProjectionExpression,
    name: &str,
    safe: bool,
) -> MarkerUse {
    let marker_use = match expression {
        ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. } => {
            let mut result = MarkerUse::None;
            expression.all_children(|child| {
                result = result.combine(marker_use_in_projection_expression(child, name, safe));
                true
            });
            result
        }

        ProjectionExpression::Literal(value) => values_marker_use([value], name),
        ProjectionExpression::Coalesce(expressions) => {
            expressions
                .iter()
                .fold(MarkerUse::None, |marker_use, expression| {
                    marker_use.combine(marker_use_in_projection_expression(expression, name, safe))
                })
        }
        ProjectionExpression::Left { expression, .. } | ProjectionExpression::Lower(expression) => {
            marker_use_in_projection_expression(expression, name, safe)
        }
        ProjectionExpression::DefaultIfNullOrEq { empty, default, .. }
        | ProjectionExpression::ColumnDefaultIfNullOrEq { empty, default, .. } => {
            values_marker_use([empty, default], name)
        }
        ProjectionExpression::DefaultIfNull { default, .. }
        | ProjectionExpression::CaseLowerPropertyDefault { default, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { default, .. } => {
            values_marker_use([default], name)
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            empty,
            non_empty,
            null_or_empty,
            ..
        }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            empty,
            non_empty,
            null_or_empty,
            ..
        } => values_marker_use([empty, non_empty, null_or_empty], name),
        ProjectionExpression::CasePropertyEqualsRank {
            branches, default, ..
        } => values_marker_use(
            branches
                .iter()
                .flat_map(|(left, right)| [left, right])
                .chain([default]),
            name,
        ),
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { terms, .. } => {
            values_marker_use(terms.iter().map(|term| &term.default), name)
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => values_marker_use(
            [
                &expression.raw_query,
                &expression.normalized_query,
                &expression.raw_input,
                &expression.exact_rank,
                &expression.alias_rank,
                &expression.fallback_rank,
            ],
            name,
        ),
        ProjectionExpression::CaseColumnSearchRank(expression) => values_marker_use(
            [
                &expression.raw_query,
                &expression.normalized_query,
                &expression.exact_rank,
                &expression.contains_rank,
                &expression.fallback_rank,
            ],
            name,
        ),
        _ => MarkerUse::None,
    };
    if !safe && marker_use == MarkerUse::Safe {
        MarkerUse::Unsafe
    } else {
        marker_use
    }
}

fn bind_physical_plan(plan: &mut PhysicalPlan, parameters: &BTreeMap<String, Value>) -> Result<()> {
    match plan {
        PhysicalPlan::GraphAlgorithm {
            node_visibility_predicate,
            ..
        } => bind_optional_predicate(node_visibility_predicate, parameters)?,
        PhysicalPlan::SourceSegmentScan { predicate, .. }
        | PhysicalPlan::FilterExec { predicate, .. } => {
            bind_predicate(predicate, parameters)?;
        }
        PhysicalPlan::NodeProjectionScanExec {
            access,
            predicate,
            items,
            ..
        } => {
            bind_node_projection_access(access, parameters)?;
            bind_optional_predicate(predicate, parameters)?;
            bind_projections(items, parameters)?;
        }
        PhysicalPlan::NodeCartesianProductExec { left, right }
        | PhysicalPlan::HashJoinExec { left, right, .. } => {
            bind_physical_plan(left, parameters)?;
            bind_physical_plan(right, parameters)?;
        }
        PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::LimitExec { input, .. } => bind_physical_plan(input, parameters)?,
        PhysicalPlan::IndexNodeSeek { value, .. } => bind_value(value, parameters)?,
        PhysicalPlan::IndexNodeMultiSeek { values, .. } => {
            bind_values(values, parameters)?;
        }
        PhysicalPlan::IndexNodeUnionSeek { branches, .. } => {
            for branch in branches {
                bind_values(&mut branch.values, parameters)?;
            }
        }
        PhysicalPlan::IndexNodeCompositeSeek { predicates, .. } => {
            for (_, value) in predicates {
                bind_value(value, parameters)?;
            }
        }
        PhysicalPlan::IndexNodeCompositeRangeSeek { seek, .. } => {
            bind_composite_range_seek(seek, parameters)?;
        }
        PhysicalPlan::IndexNodeRangeSeek { lower, upper, .. } => {
            for (value, _) in lower.iter_mut().chain(upper.iter_mut()) {
                bind_value(value, parameters)?;
            }
        }
        PhysicalPlan::AdjacencyExpandExec {
            rel_properties,
            input,
            ..
        } => {
            bind_value_map(rel_properties, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::OptionalDegreeExec {
            rel_properties,
            target_properties,
            input,
            ..
        } => {
            bind_value_map(rel_properties, parameters)?;
            bind_value_map(target_properties, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::OptionalRelationshipCountSumExec {
            properties, legs, ..
        } => {
            bind_value_map(properties, parameters)?;
            for leg in legs {
                if let Some(RelationshipCountFilter::PropertyNotEqOrEmpty { value, .. }) =
                    &mut leg.filter
                {
                    bind_value(value, parameters)?;
                }
            }
        }
        PhysicalPlan::ShortestPathExec {
            source_id,
            source_visibility_predicate,
            target_id,
            target_visibility_predicate,
            ..
        } => {
            bind_value(source_id, parameters)?;
            bind_optional_predicate(source_visibility_predicate, parameters)?;
            bind_value(target_id, parameters)?;
            bind_optional_predicate(target_visibility_predicate, parameters)?;
        }
        PhysicalPlan::ProjectExec { items, input } => {
            bind_projections(items, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::AggregateExec {
            group_keys, input, ..
        } => {
            bind_projections(group_keys, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::SortExec { items, input } | PhysicalPlan::TopNExec { items, input, .. } => {
            bind_sort_items(items, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        _ => {}
    }
    if let PhysicalPlan::SourceSegmentScan { .. } | PhysicalPlan::FilterExec { .. } = plan {
        match plan {
            PhysicalPlan::FilterExec { input, .. } => bind_physical_plan(input, parameters)?,
            PhysicalPlan::SourceSegmentScan { .. } => {}
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn bind_node_projection_access(
    access: &mut skein_plan::NodeProjectionAccess,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    match access {
        skein_plan::NodeProjectionAccess::LabelScan => Ok(()),
        skein_plan::NodeProjectionAccess::PropertyValues { values, .. } => {
            bind_values(values, parameters)
        }
        skein_plan::NodeProjectionAccess::PropertyUnion { branches } => {
            for branch in branches {
                bind_values(&mut branch.values, parameters)?;
            }
            Ok(())
        }
        skein_plan::NodeProjectionAccess::CompositeEquality { predicates } => {
            for (_, value) in predicates {
                bind_value(value, parameters)?;
            }
            Ok(())
        }
        skein_plan::NodeProjectionAccess::CompositeRange { seek } => {
            bind_composite_range_seek(seek, parameters)
        }
        skein_plan::NodeProjectionAccess::PropertyRange { lower, upper, .. } => {
            for (value, _) in lower.iter_mut().chain(upper.iter_mut()) {
                bind_value(value, parameters)?;
            }
            Ok(())
        }
        skein_plan::NodeProjectionAccess::FullText { .. } => Ok(()),
    }
}

fn bind_composite_range_seek(
    seek: &mut skein_plan::CompositeRangeSeek,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    for (_, value) in &mut seek.equality_prefix {
        bind_value(value, parameters)?;
    }
    for (value, _) in seek.lower.iter_mut().chain(seek.upper.iter_mut()) {
        bind_value(value, parameters)?;
    }
    Ok(())
}

fn bind_logical_plan(plan: &mut LogicalPlan, parameters: &BTreeMap<String, Value>) -> Result<()> {
    match plan {
        LogicalPlan::GraphAlgorithm {
            node_visibility_predicate,
            ..
        } => bind_optional_predicate(node_visibility_predicate, parameters)?,
        LogicalPlan::NodeCartesianProduct { left, right } => {
            bind_logical_plan(left, parameters)?;
            bind_logical_plan(right, parameters)?;
        }
        LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Limit { input, .. } => bind_logical_plan(input, parameters)?,
        LogicalPlan::Expand {
            rel_properties,
            input,
            ..
        } => {
            bind_value_map(rel_properties, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        LogicalPlan::OptionalDegree {
            rel_properties,
            target_properties,
            input,
            ..
        } => {
            bind_value_map(rel_properties, parameters)?;
            bind_value_map(target_properties, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        LogicalPlan::OptionalRelationshipCountSum {
            properties, legs, ..
        } => {
            bind_value_map(properties, parameters)?;
            for leg in legs {
                if let Some(RelationshipCountFilter::PropertyNotEqOrEmpty { value, .. }) =
                    &mut leg.filter
                {
                    bind_value(value, parameters)?;
                }
            }
        }
        LogicalPlan::ShortestPath {
            source_id,
            source_visibility_predicate,
            target_id,
            target_visibility_predicate,
            ..
        } => {
            bind_value(source_id, parameters)?;
            bind_optional_predicate(source_visibility_predicate, parameters)?;
            bind_value(target_id, parameters)?;
            bind_optional_predicate(target_visibility_predicate, parameters)?;
        }
        LogicalPlan::Filter {
            predicate, input, ..
        } => {
            bind_predicate(predicate, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        LogicalPlan::Project { items, input } => {
            bind_projections(items, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        LogicalPlan::Aggregate {
            group_keys, input, ..
        } => {
            bind_projections(group_keys, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        LogicalPlan::Sort { items, input } => {
            bind_sort_items(items, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        _ => {}
    }
    Ok(())
}

fn bind_optional_predicate(
    predicate: &mut Option<Predicate>,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    if let Some(predicate) = predicate {
        bind_predicate(predicate, parameters)?;
    }
    Ok(())
}

fn bind_predicate(predicate: &mut Predicate, parameters: &BTreeMap<String, Value>) -> Result<()> {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => {
            for predicate in predicates {
                bind_predicate(predicate, parameters)?;
            }
        }
        Predicate::Not(predicate) => bind_predicate(predicate, parameters)?,
        Predicate::IdEq { value, .. }
        | Predicate::IdNotEq { value, .. }
        | Predicate::IdCompare { value, .. }
        | Predicate::PropertyEq { value, .. }
        | Predicate::PropertyNotEq { value, .. }
        | Predicate::PropertyCompare { value, .. }
        | Predicate::PropertyListContains { value, .. } => bind_value(value, parameters)?,
        Predicate::IdIn { values, .. } | Predicate::PropertyIn { values, .. } => {
            bind_values(values, parameters)?;
        }
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionContains { expression, value } => {
            bind_projection_expression(expression, parameters)?;
            bind_projection_expression(value, parameters)?;
        }
        Predicate::ExpressionCompare {
            expression, value, ..
        } => {
            bind_projection_expression(expression, parameters)?;
            bind_projection_expression(value, parameters)?;
        }
        _ => {}
    }
    Ok(())
}

fn bind_projections(
    projections: &mut [Projection],
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    for projection in projections {
        bind_projection_expression(&mut projection.expression, parameters)?;
    }
    Ok(())
}

fn bind_sort_items(items: &mut [SortItem], parameters: &BTreeMap<String, Value>) -> Result<()> {
    for item in items {
        if let SortKey::Expression(expression) = &mut item.key {
            bind_projection_expression(expression, parameters)?;
        }
    }
    Ok(())
}

fn bind_projection_expression(
    expression: &mut ProjectionExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    match expression {
        ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. } => expression
            .try_for_each_child_mut(|child| bind_projection_expression(child, parameters))?,

        ProjectionExpression::Literal(value) => bind_value(value, parameters)?,
        ProjectionExpression::Coalesce(expressions) => {
            for expression in expressions {
                bind_projection_expression(expression, parameters)?;
            }
        }
        ProjectionExpression::Left { expression, .. } | ProjectionExpression::Lower(expression) => {
            bind_projection_expression(expression, parameters)?;
        }
        ProjectionExpression::DefaultIfNullOrEq { empty, default, .. }
        | ProjectionExpression::ColumnDefaultIfNullOrEq { empty, default, .. } => {
            bind_value(empty, parameters)?;
            bind_value(default, parameters)?;
        }
        ProjectionExpression::DefaultIfNull { default, .. }
        | ProjectionExpression::CaseLowerPropertyDefault { default, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { default, .. } => {
            bind_value(default, parameters)?;
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            empty,
            non_empty,
            null_or_empty,
            ..
        }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            empty,
            non_empty,
            null_or_empty,
            ..
        } => {
            bind_value(empty, parameters)?;
            bind_value(non_empty, parameters)?;
            bind_value(null_or_empty, parameters)?;
        }
        ProjectionExpression::CasePropertyEqualsRank {
            branches, default, ..
        } => {
            for (left, right) in branches {
                bind_value(left, parameters)?;
                bind_value(right, parameters)?;
            }
            bind_value(default, parameters)?;
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { terms, .. } => {
            for term in terms {
                bind_value(&mut term.default, parameters)?;
            }
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            for value in [
                &mut expression.raw_query,
                &mut expression.normalized_query,
                &mut expression.raw_input,
                &mut expression.exact_rank,
                &mut expression.alias_rank,
                &mut expression.fallback_rank,
            ] {
                bind_value(value, parameters)?;
            }
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            for value in [
                &mut expression.raw_query,
                &mut expression.normalized_query,
                &mut expression.exact_rank,
                &mut expression.contains_rank,
                &mut expression.fallback_rank,
            ] {
                bind_value(value, parameters)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn bind_value_map(
    values: &mut BTreeMap<String, Value>,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    for value in values.values_mut() {
        bind_value(value, parameters)?;
    }
    Ok(())
}

fn bind_values(values: &mut [Value], parameters: &BTreeMap<String, Value>) -> Result<()> {
    for value in values {
        bind_value(value, parameters)?;
    }
    Ok(())
}

fn bind_value(value: &mut Value, parameters: &BTreeMap<String, Value>) -> Result<()> {
    if let Some((name, path)) = marker_name_and_path(value) {
        let mut bound = parameters
            .get(name)
            .ok_or_else(|| SkeinError::Semantic(format!("missing parameter '${name}'")))?;
        for index in path {
            let Value::List(values) = bound else {
                return Err(SkeinError::Semantic(format!(
                    "parameter '${name}' changed shape while binding a cached plan"
                )));
            };
            bound = values.get(index).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "parameter '${name}' changed shape while binding a cached plan"
                ))
            })?;
        }
        *value = bound.clone();
        return Ok(());
    }
    match value {
        Value::List(values) => bind_values(values, parameters),
        Value::Map(values) => bind_value_map(values, parameters),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bind_physical_plan_parameters, parameter_marker, parameterize_logical_plan,
        ParameterCacheValue, PARAMETER_SLOT_NAME_KEY, PARAMETER_SLOT_PATH_KEY,
    };
    use skein_core::Value;
    use skein_cypher as cypher;
    use skein_optimizer::{CascadesOptimizer, OptimizerCatalog};
    use skein_plan::{
        CompositeRangeSeek, ExactPropertySeekBranch, LogicalPlanRoot, NodeProjectionAccess,
        PhysicalPlan,
    };
    use std::collections::BTreeMap;

    #[test]
    fn general_case_preserves_exact_cache_keys_and_rebinds_explicit_template_slots() {
        let statement = cypher::parse("MATCH (n:Item) RETURN CASE WHEN n.id = $chosen THEN lower($label) ELSE $fallback END AS result").unwrap();
        let parameters = BTreeMap::from([
            ("chosen".into(), Value::Int(1)),
            ("label".into(), Value::String("FIRST".into())),
            ("fallback".into(), Value::String("old".into())),
        ]);
        let parameterized = parameterize_logical_plan(&statement, &parameters).unwrap();
        assert_eq!(parameterized.slot_count, 0);
        assert!(parameterized
            .cache_key
            .values
            .values()
            .all(|value| matches!(value, ParameterCacheValue::Exact(_))));
        // Projection parameters keep their existing conservative cache policy.
        // Explicit templates must nevertheless rebind all scalar descendants.
        let markers = parameters
            .iter()
            .map(|(name, value)| (name.clone(), parameter_marker(name, value, &[])))
            .collect();
        let logical = skein_plan::plan_with_params(&statement, &markers).unwrap();
        let root = CascadesOptimizer::default().optimize_root_with_catalog(
            &LogicalPlanRoot::new(logical),
            &OptimizerCatalog::default(),
        );
        let (template, _) = root.into_parts();
        let parameters = BTreeMap::from([
            ("chosen".into(), Value::Int(2)),
            ("label".into(), Value::String("SECOND".into())),
            ("fallback".into(), Value::String("new".into())),
        ]);
        let rebound = bind_physical_plan_parameters(&template, &parameters, true).unwrap();
        let text = format!("{rebound:?}");
        assert!(text.contains("SECOND") && text.contains("new"), "{text}");
        assert!(!text.contains("skein_parameter_slot"), "{text}");
    }

    #[test]
    fn equality_parameters_become_rebindable_slots() {
        let statement =
            cypher::parse("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title").unwrap();
        let first_parameters =
            BTreeMap::from([("id".to_string(), Value::String("first".to_string()))]);
        let parameterized = parameterize_logical_plan(&statement, &first_parameters).unwrap();
        assert!(matches!(
            parameterized.cache_key.values.get("id"),
            Some(ParameterCacheValue::Slot(_))
        ));

        let root = CascadesOptimizer::default().optimize_root_with_catalog(
            &LogicalPlanRoot::new(parameterized.logical),
            &OptimizerCatalog::default(),
        );
        let (template, _) = root.into_parts();
        let rebound = bind_physical_plan_parameters(
            &template,
            &BTreeMap::from([("id".to_string(), Value::String("second".to_string()))]),
            true,
        )
        .unwrap();

        assert!(rebound.explain(0).contains("second"));
        assert!(!rebound.explain(0).contains("skein_parameter_slot"));
    }

    #[test]
    fn graph_hash_join_rebinds_both_inputs_without_changing_keys() {
        let seek = |variable: &str, parameter: &str| PhysicalPlan::IndexNodeSeek {
            variable: variable.into(),
            label: "Item".into(),
            property: "scope".into(),
            value: parameter_marker(parameter, &Value::Int(0), &[]),
        };
        let template = PhysicalPlan::HashJoinExec {
            left_key: skein_plan::HashJoinKey {
                variable: "a".into(),
                property: "key".into(),
            },
            right_key: skein_plan::HashJoinKey {
                variable: "b".into(),
                property: "key".into(),
            },
            left: Box::new(seek("a", "left_scope")),
            right: Box::new(seek("b", "right_scope")),
        };
        for (a, b) in [(1, 2), (3, 4)] {
            let bound = bind_physical_plan_parameters(
                &template,
                &BTreeMap::from([
                    ("left_scope".into(), Value::Int(a)),
                    ("right_scope".into(), Value::Int(b)),
                ]),
                true,
            )
            .unwrap();
            let PhysicalPlan::HashJoinExec {
                left_key,
                right_key,
                left,
                right,
            } = bound
            else {
                unreachable!()
            };
            assert_eq!(left_key.variable, "a");
            assert_eq!(right_key.variable, "b");
            assert!(
                matches!(*left, PhysicalPlan::IndexNodeSeek { value: Value::Int(value), .. } if value == a)
            );
            assert!(
                matches!(*right, PhysicalPlan::IndexNodeSeek { value: Value::Int(value), .. } if value == b)
            );
        }
    }

    #[test]
    fn projected_index_access_rebinds_parameter_slots() {
        let template = PhysicalPlan::NodeProjectionScanExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            access: NodeProjectionAccess::PropertyValues {
                property: "stable_id".to_string(),
                values: vec![parameter_marker(
                    "id",
                    &Value::String("first".to_string()),
                    &[],
                )],
            },
            required_properties: vec!["stable_id".to_string()],
            predicate: None,
            items: Vec::new(),
        };

        let rebound = bind_physical_plan_parameters(
            &template,
            &BTreeMap::from([("id".to_string(), Value::String("second".to_string()))]),
            true,
        )
        .unwrap();

        assert!(matches!(
            rebound,
            PhysicalPlan::NodeProjectionScanExec {
                access: NodeProjectionAccess::PropertyValues { values, .. },
                ..
            } if values == vec![Value::String("second".to_string())]
        ));
    }

    #[test]
    fn projected_union_access_rebinds_every_branch() {
        let marker = || parameter_marker("id", &Value::String("first".to_string()), &[]);
        let template = PhysicalPlan::NodeProjectionScanExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            access: NodeProjectionAccess::PropertyUnion {
                branches: vec![
                    ExactPropertySeekBranch {
                        property: "id".to_string(),
                        values: vec![marker()],
                    },
                    ExactPropertySeekBranch {
                        property: "external_id".to_string(),
                        values: vec![marker()],
                    },
                ],
            },
            required_properties: vec!["id".to_string(), "external_id".to_string()],
            predicate: None,
            items: Vec::new(),
        };

        let rebound = bind_physical_plan_parameters(
            &template,
            &BTreeMap::from([("id".to_string(), Value::String("second".to_string()))]),
            true,
        )
        .unwrap();

        let PhysicalPlan::NodeProjectionScanExec {
            access: NodeProjectionAccess::PropertyUnion { branches },
            ..
        } = rebound
        else {
            panic!("expected projected union access");
        };
        assert!(branches
            .iter()
            .all(|branch| { branch.values == vec![Value::String("second".to_string())] }));
    }

    #[test]
    fn projected_composite_range_rebinds_prefix_and_bounds() {
        let template = PhysicalPlan::NodeProjectionScanExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            access: NodeProjectionAccess::CompositeRange {
                seek: CompositeRangeSeek {
                    index_properties: vec!["space_id".to_string(), "created_at".to_string()],
                    equality_prefix: vec![(
                        "space_id".to_string(),
                        parameter_marker(
                            "space_id",
                            &Value::String("space:first".to_string()),
                            &[],
                        ),
                    )],
                    range_property: "created_at".to_string(),
                    lower: Some((parameter_marker("lower", &Value::Int(10), &[]), true)),
                    upper: Some((parameter_marker("upper", &Value::Int(20), &[]), false)),
                },
            },
            required_properties: vec!["space_id".to_string(), "created_at".to_string()],
            predicate: None,
            items: Vec::new(),
        };

        let rebound = bind_physical_plan_parameters(
            &template,
            &BTreeMap::from([
                (
                    "space_id".to_string(),
                    Value::String("space:second".to_string()),
                ),
                ("lower".to_string(), Value::Int(30)),
                ("upper".to_string(), Value::Int(40)),
            ]),
            true,
        )
        .unwrap();

        assert!(matches!(
            rebound,
            PhysicalPlan::NodeProjectionScanExec {
                access: NodeProjectionAccess::CompositeRange { seek },
                ..
            } if seek.equality_prefix
                == vec![(
                    "space_id".to_string(),
                    Value::String("space:second".to_string()),
                )]
                && seek.lower == Some((Value::Int(30), true))
                && seek.upper == Some((Value::Int(40), false))
        ));
    }

    #[test]
    fn pagination_parameters_remain_exact_variants() {
        let statement =
            cypher::parse("MATCH (m:Memory) RETURN m.id AS id ORDER BY m.id LIMIT $limit").unwrap();
        let parameterized = parameterize_logical_plan(
            &statement,
            &BTreeMap::from([("limit".to_string(), Value::Int(10))]),
        )
        .unwrap();

        assert_eq!(
            parameterized.cache_key.values.get("limit"),
            Some(&ParameterCacheValue::Exact(Value::Int(10)))
        );
    }

    #[test]
    fn parameters_that_change_logical_simplification_remain_exact_variants() {
        let statement = cypher::parse(
            "MATCH (t:Thread) WHERE $source IS NULL OR t.source = $source RETURN t.id AS id",
        )
        .unwrap();
        let parameterized = parameterize_logical_plan(
            &statement,
            &BTreeMap::from([("source".to_string(), Value::Null)]),
        )
        .unwrap();

        assert_eq!(
            parameterized.cache_key.values.get("source"),
            Some(&ParameterCacheValue::Exact(Value::Null))
        );
    }

    #[test]
    fn multiple_equality_parameters_share_one_rebindable_template() {
        let statement =
            cypher::parse("MATCH (m:Memory) WHERE m.id = $id AND m.kind = $kind RETURN m.id AS id")
                .unwrap();
        let parameterized = parameterize_logical_plan(
            &statement,
            &BTreeMap::from([
                ("id".to_string(), Value::String("memory-1".to_string())),
                ("kind".to_string(), Value::String("note".to_string())),
            ]),
        )
        .unwrap();

        assert_eq!(parameterized.slot_count, 2);
        assert!(parameterized
            .cache_key
            .values
            .values()
            .all(|cached| matches!(cached, ParameterCacheValue::Slot(_))));
    }

    #[test]
    fn marker_shaped_parameter_values_disable_slots() {
        let statement =
            cypher::parse("MATCH (m:Memory) WHERE m.metadata = $metadata RETURN m.id AS id")
                .unwrap();
        let marker_shaped = Value::Map(BTreeMap::from([
            (
                PARAMETER_SLOT_NAME_KEY.to_string(),
                Value::String("other".to_string()),
            ),
            (PARAMETER_SLOT_PATH_KEY.to_string(), Value::List(Vec::new())),
        ]));
        let parameterized = parameterize_logical_plan(
            &statement,
            &BTreeMap::from([("metadata".to_string(), marker_shaped.clone())]),
        )
        .unwrap();

        assert_eq!(parameterized.slot_count, 0);
        assert_eq!(
            parameterized.cache_key.values.get("metadata"),
            Some(&ParameterCacheValue::Exact(marker_shaped))
        );
    }
}
