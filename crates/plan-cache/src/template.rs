// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::{HawDBError, Result, Value};
use hawdb_cypher as cypher;
use hawdb_plan_cypher::{
    self as planner, AggregateTarget, Aggregation, GraphMatchStep, LogicalPlan, PathFunction,
    PhysicalPlan, Predicate, Projection, ProjectionExpression, RelationshipCountFilter, SortItem,
    SortKey,
};
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

const PARAMETER_SLOT_NAME_KEY: &str = "\0skein_parameter_slot";
const PARAMETER_SLOT_PATH_KEY: &str = "\0skein_parameter_path";

mod retained;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ParameterCacheValue {
    Slot(ParameterValueShape),
    CaseSlot(ParameterValueShape),
    Exact(Value),
}

impl ParameterCacheValue {
    fn validate_slot_use(&mut self, marker_use: MarkerUse) -> bool {
        if matches!(self, Self::Exact(_)) {
            return true;
        }
        if !marker_use.is_safe() {
            return false;
        }
        // Reclassify after all exact guards have been restored. A planner
        // rewrite may move a slot into or out of CASE without changing its
        // validity domain; its physical retention policy must follow that move.
        *self = match std::mem::replace(self, Self::Exact(Value::Null)) {
            Self::Slot(shape) | Self::CaseSlot(shape) => {
                if marker_use == MarkerUse::SafeCase {
                    Self::CaseSlot(shape)
                } else {
                    Self::Slot(shape)
                }
            }
            exact @ Self::Exact(_) => exact,
        };
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ParameterValueShape {
    Null,
    Bool,
    Int,
    Float,
    String,
    Binary,
    Vector(hawdb_core::VectorDimension),
    Uuid,
    Date,
    LocalTime,
    LocalDateTime,
    DateTime,
    Duration,
    List(Vec<ParameterValueShape>),
    Map(BTreeMap<String, ParameterValueShape>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParameterizedLogicalPlan {
    // Immutable proven state is shared by cache admission and readers.
    // Cloning a handle must not deep-copy the marker tree or exact guards.
    logical: Arc<LogicalPlan>,
    cache_key: Arc<PlanParameterCacheKey>,
    slot_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlanParameterCacheKey {
    values: BTreeMap<String, ParameterCacheValue>,
}

/// A proven logical template and its complete parameter validity boundary.
/// Schema, statistics, access control and scoring remain physical-cache keys.
#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct ParameterBindingTemplate {
    plan: ParameterizedLogicalPlan,
}

impl ParameterBindingTemplate {
    /// Conservative requested heap retained by this template, including Arc
    /// allocations even when shared with another owner. Unknown plan shapes or
    /// arithmetic overflow decline admission instead of undercharging.
    pub fn retained_heap_bytes(&self) -> Option<usize> {
        retained::template_bytes(self)
    }

    pub fn new(
        plan: &ParameterizedLogicalPlan,
        parameters: &BTreeMap<String, Value>,
    ) -> Option<Self> {
        // An omitted binding may influence planning without leaving a marker.
        // Do not infer its validity domain or retain unused request payloads.
        (plan.cache_key.values.len() == parameters.len()).then(|| Self { plan: plan.clone() })
    }

    pub fn reuse(&self, parameters: &BTreeMap<String, Value>) -> Option<ParameterizedLogicalPlan> {
        if self.plan.cache_key.values.len() != parameters.len()
            || parameters.values().any(value_contains_any_marker)
            || self.plan.cache_key.values.iter().any(|(name, cached)| {
                parameters.get(name).is_none_or(|value| match cached {
                    ParameterCacheValue::Slot(shape) | ParameterCacheValue::CaseSlot(shape) => {
                        !shape.matches(value)
                    }
                    ParameterCacheValue::Exact(expected) => value != expected,
                })
            })
        {
            return None;
        }
        Some(self.plan.clone())
    }
}

impl ParameterValueShape {
    fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            (Self::Null, Value::Null)
            | (Self::Bool, Value::Bool(_))
            | (Self::Int, Value::Int(_))
            | (Self::Float, Value::Float(_))
            | (Self::String, Value::String(_))
            | (Self::Binary, Value::Binary(_))
            | (Self::Uuid, Value::Uuid(_))
            | (Self::Date, Value::Date(_))
            | (Self::LocalTime, Value::LocalTime(_))
            | (Self::LocalDateTime, Value::LocalDateTime(_))
            | (Self::DateTime, Value::DateTime(_))
            | (Self::Duration, Value::Duration(_)) => true,
            (Self::Vector(dimension), Value::Vector(value)) => *dimension == value.dimension(),
            (Self::List(shapes), Value::List(values)) => {
                shapes.len() == values.len()
                    && shapes
                        .iter()
                        .zip(values)
                        .all(|(shape, value)| shape.matches(value))
            }
            (Self::Map(shapes), Value::Map(values)) => {
                shapes.len() == values.len()
                    && shapes
                        .iter()
                        .zip(values)
                        .all(|((key, shape), (name, value))| key == name && shape.matches(value))
            }
            _ => false,
        }
    }
}

/// Allocation-free cache bucket selector. A collision can only cause a miss:
/// the template must still validate every shape and exact folded dependency.
#[doc(hidden)]
pub fn parameter_binding_shape_fingerprint(parameters: &BTreeMap<String, Value>) -> u64 {
    fn visit(value: &Value, state: &mut impl Hasher) {
        std::mem::discriminant(value).hash(state);
        match value {
            Value::List(values) => {
                values.len().hash(state);
                for value in values {
                    visit(value, state);
                }
            }
            Value::Map(values) => {
                values.len().hash(state);
                for (key, value) in values {
                    key.hash(state);
                    visit(value, state);
                }
            }
            Value::Vector(value) => value.dimension().hash(state),
            _ => {}
        }
    }
    let mut state = std::collections::hash_map::DefaultHasher::new();
    parameters.len().hash(&mut state);
    for (name, value) in parameters {
        name.hash(&mut state);
        visit(value, &mut state);
    }
    state.finish()
}

impl ParameterizedLogicalPlan {
    /// CASE marker trees stay in the byte-budgeted logical cache. Physical
    /// copies remain request-owned until the physical cache has a byte budget.
    pub fn has_case_parameter_slots(&self) -> bool {
        self.cache_key
            .values
            .values()
            .any(|value| matches!(value, ParameterCacheValue::CaseSlot(_)))
    }

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
    SafeCase,
    Unsafe,
}

#[cfg(test)]
thread_local! {
    static PARAMETER_PLANNING_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn plan_parameter_bindings(
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    #[cfg(test)]
    PARAMETER_PLANNING_CALLS.with(|calls| calls.set(calls.get() + 1));
    planner::plan_with_params(statement, parameters)
}

impl MarkerUse {
    fn is_safe(self) -> bool {
        matches!(self, Self::Safe | Self::SafeCase)
    }

    fn cache_value(self, value: &Value) -> ParameterCacheValue {
        match self {
            Self::SafeCase => ParameterCacheValue::CaseSlot(parameter_value_shape(value)),
            Self::Safe => ParameterCacheValue::Slot(parameter_value_shape(value)),
            Self::None | Self::Unsafe => ParameterCacheValue::Exact(value.clone()),
        }
    }

    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unsafe, _) | (_, Self::Unsafe) => Self::Unsafe,
            (Self::SafeCase, _) | (_, Self::SafeCase) => Self::SafeCase,
            (Self::Safe, _) | (_, Self::Safe) => Self::Safe,
            (Self::None, Self::None) => Self::None,
        }
    }
}

pub fn parameterize_logical_plan(
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<ParameterizedLogicalPlan> {
    let actual = plan_parameter_bindings(statement, parameters)?;
    if parameters.values().any(value_contains_any_marker) {
        return Ok(ParameterizedLogicalPlan {
            logical: Arc::new(actual),
            cache_key: Arc::new(PlanParameterCacheKey {
                values: parameters
                    .iter()
                    .map(|(name, value)| (name.clone(), ParameterCacheValue::Exact(value.clone())))
                    .collect(),
            }),
            slot_count: 0,
        });
    }
    if parameters.is_empty() {
        return Ok(ParameterizedLogicalPlan {
            logical: Arc::new(actual),
            cache_key: Arc::new(PlanParameterCacheKey {
                values: BTreeMap::new(),
            }),
            slot_count: 0,
        });
    }
    let folded_parameters = folded_predicate_parameters(statement);
    if let Some(parameterized) =
        parameterize_together(statement, parameters, &actual, &folded_parameters)
    {
        return Ok(parameterized);
    }
    let mut template_parameters = parameters.clone();
    let mut cache_values = BTreeMap::new();
    let mut slot_count = 0;

    for (name, value) in parameters {
        if folded_parameters.contains(name) {
            cache_values.insert(name.clone(), ParameterCacheValue::Exact(value.clone()));
            continue;
        }
        let marker = parameter_marker(name, value, &[]);
        let mut candidate_parameters = parameters.clone();
        candidate_parameters.insert(name.clone(), marker.clone());
        let candidate = plan_parameter_bindings(statement, &candidate_parameters).ok();
        if candidate.as_ref().is_some_and(|candidate| {
            marker_use_in_logical_plan(candidate, name) == MarkerUse::None && candidate == &actual
        }) {
            // Prove all omissions together below. Absence from one marked plan
            // alone does not establish that removing the binding is harmless.
            template_parameters.remove(name);
            continue;
        }
        let marker_use = candidate.map_or(MarkerUse::Unsafe, |mut candidate| {
            let marker_use = marker_use_in_logical_plan(&candidate, name);
            if marker_use.is_safe()
                && bind_logical_plan(&mut candidate, BindingAction::Execute(parameters)).is_ok()
                && candidate == actual
            {
                marker_use
            } else {
                MarkerUse::Unsafe
            }
        });
        if marker_use.is_safe() {
            template_parameters.insert(name.clone(), marker);
            cache_values.insert(name.clone(), marker_use.cache_value(value));
            slot_count += 1;
        } else {
            cache_values.insert(name.clone(), ParameterCacheValue::Exact(value.clone()));
        }
    }

    let logical = if slot_count == 0 && template_parameters.len() == parameters.len() {
        actual.clone()
    } else {
        let candidate = plan_parameter_bindings(statement, &template_parameters)
            .ok()
            .filter(|candidate| {
                let all_slots_are_safe = cache_values.iter_mut().all(|(name, cached)| {
                    matches!(cached, ParameterCacheValue::Exact(_))
                        || cached.validate_slot_use(marker_use_in_logical_plan(candidate, name))
                });
                let mut rebound = candidate.clone();
                all_slots_are_safe
                    && bind_logical_plan(&mut rebound, BindingAction::Execute(parameters)).is_ok()
                    && rebound == actual
            });
        if let Some(candidate) = candidate {
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
        logical: Arc::new(logical),
        cache_key: Arc::new(PlanParameterCacheKey {
            values: cache_values,
        }),
        slot_count,
    })
}

/// Prove a complete binding wave before trying individual parameters. Some
/// syntactic positions (for example LIMIT) cannot accept markers; those retain
/// the conservative per-parameter path below. Exact values and omissions are
/// checked together by rebinding the completed template to the actual plan.
fn parameterize_together(
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    actual: &LogicalPlan,
    folded_parameters: &BTreeSet<String>,
) -> Option<ParameterizedLogicalPlan> {
    let mut template_parameters = parameters
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                if folded_parameters.contains(name) {
                    value.clone()
                } else {
                    parameter_marker(name, value, &[])
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let marked = plan_parameter_bindings(statement, &template_parameters).ok()?;
    let mut values = BTreeMap::new();
    let mut slot_count = 0;
    let mut changed = false;
    for (name, value) in parameters {
        match marker_use_in_logical_plan(&marked, name) {
            marker_use @ (MarkerUse::Safe | MarkerUse::SafeCase) => {
                values.insert(name.clone(), marker_use.cache_value(value));
                slot_count += 1;
            }
            MarkerUse::None if folded_parameters.contains(name) => {
                // Predicate binding erased these markers. Preserve the exact
                // inputs even when other occurrences could remain slots.
                template_parameters.insert(name.clone(), value.clone());
                values.insert(name.clone(), ParameterCacheValue::Exact(value.clone()));
                changed = true;
            }
            MarkerUse::None => {
                template_parameters.remove(name);
                changed = true;
            }
            MarkerUse::Unsafe => {
                template_parameters.insert(name.clone(), value.clone());
                values.insert(name.clone(), ParameterCacheValue::Exact(value.clone()));
                changed = true;
            }
        }
    }
    let logical = if changed {
        plan_parameter_bindings(statement, &template_parameters).ok()?
    } else {
        marked
    };
    // Restoring exact inputs must not turn a previously safe marker into a
    // value-dependent specialized expression.
    if !values.iter_mut().all(|(name, value)| {
        matches!(value, ParameterCacheValue::Exact(_))
            || value.validate_slot_use(marker_use_in_logical_plan(&logical, name))
    }) {
        return None;
    }
    let mut rebound = logical.clone();
    bind_logical_plan(&mut rebound, BindingAction::Execute(parameters)).ok()?;
    if &rebound != actual {
        return None;
    }
    Some(ParameterizedLogicalPlan {
        logical: Arc::new(logical),
        cache_key: Arc::new(PlanParameterCacheKey { values }),
        slot_count,
    })
}

/// Binding folds these predicate inputs before their markers can reach the
/// logical plan. Another use (notably simple CASE) may still retain a marker;
/// logical marker presence therefore cannot prove the input's whole domain.
fn folded_predicate_parameters(statement: &cypher::Statement) -> BTreeSet<String> {
    fn collect_value(value: &cypher::ValueExpression, exact: &mut BTreeSet<String>) {
        match &value.kind {
            cypher::ValueExpressionKind::Parameter(name) => {
                exact.insert(name.clone());
            }
            cypher::ValueExpressionKind::List(values) => {
                for item in values {
                    collect_value(item, exact);
                }
            }
            cypher::ValueExpressionKind::Timestamp(inner)
            | cypher::ValueExpressionKind::Conversion {
                argument: inner, ..
            } => collect_value(inner, exact),
            // Temporal calls are evaluated while binding: like `timestamp()`,
            // their inputs are folded and must be exact cache keys.
            cypher::ValueExpressionKind::Temporal { arguments, .. } => {
                for argument in arguments {
                    collect_value(argument, exact);
                }
            }
            cypher::ValueExpressionKind::Map(entries) => {
                for value in entries.values() {
                    collect_value(value, exact);
                }
            }
            cypher::ValueExpressionKind::Literal(_)
            | cypher::ValueExpressionKind::Binding(_)
            | cypher::ValueExpressionKind::BindingProperty { .. }
            | cypher::ValueExpressionKind::BindingIndex { .. }
            | cypher::ValueExpressionKind::CurrentTimestamp => {}
        }
    }
    fn collect_predicate(predicate: &cypher::PropertyPredicate, exact: &mut BTreeSet<String>) {
        use cypher::PropertyPredicate as P;
        match predicate {
            P::And(items) | P::Or(items) => {
                for item in items {
                    collect_predicate(item, exact);
                }
            }
            P::Not(inner) => collect_predicate(inner, exact),
            P::ParameterEq { left, right } | P::ParameterNotEq { left, right } => {
                exact.insert(left.clone());
                collect_value(right, exact);
            }
            P::RelationshipExists { .. }
            | P::BoundRelationshipExists { .. }
            | P::IdEq { .. }
            | P::IdNotEq { .. }
            | P::IdCompare { .. }
            | P::IdIn { .. }
            | P::Eq { .. }
            | P::NotEq { .. }
            | P::Compare { .. }
            | P::ExpressionEq { .. }
            | P::ExpressionNotEq { .. }
            | P::ExpressionCompare { .. }
            | P::ExpressionContains { .. }
            | P::ExpressionIn { .. }
            | P::ListContains { .. }
            | P::ListContainsLower { .. }
            | P::Contains { .. }
            | P::StartsWith { .. }
            | P::EndsWith { .. }
            | P::RegexMatch { .. }
            // Nullness is already fixed by the complete parameter shape.
            | P::ParameterIsNull { .. }
            | P::ParameterIsNotNull { .. }
            | P::IsNull { .. }
            | P::IsNotNull { .. }
            | P::In { .. } => {}
        }
    }
    fn pipeline(query: &cypher::QueryPipeline, exact: &mut BTreeSet<String>) {
        for clause in &query.clauses {
            let condition = match &clause.kind {
                cypher::ClauseKind::Match { predicate, .. } => predicate.as_ref(),
                cypher::ClauseKind::With(projection) | cypher::ClauseKind::Return(projection) => {
                    projection.predicate.as_ref()
                }
                _ => None,
            };
            if let Some(condition) = condition {
                collect_predicate(&condition.kind, exact);
            }
        }
    }
    let mut exact = BTreeSet::new();
    match statement {
        cypher::Statement::Pipeline(query) | cypher::Statement::UnwindMutation(query) => {
            pipeline(query, &mut exact)
        }
        cypher::Statement::CypherQuery(query) => {
            return folded_predicate_parameters(&query.statement)
        }
        cypher::Statement::Explain(explain) => {
            return folded_predicate_parameters(&explain.statement)
        }
        cypher::Statement::ProjectGraph(query) => {
            for condition in query.relationship_predicates.values() {
                collect_predicate(condition, &mut exact);
            }
        }
        _ => {}
    }
    exact
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
        bind_physical_plan(&mut plan, BindingAction::Execute(parameters))?;
    }
    Ok(plan)
}

/// Resolves a borrowed cost input from the same template as the physical plan.
/// Keep marker interpretation here so planning never reconstructs its encoding.
#[doc(hidden)]
pub fn resolve_plan_template_value<'a>(
    template: &'a Value,
    parameters: &'a BTreeMap<String, Value>,
    has_slots: bool,
) -> Result<&'a Value> {
    if has_slots && let Some((name, path)) = marker_name_and_path(template) {
        return resolve_parameter_marker(parameters, name, path);
    }
    Ok(template)
}

/// Returns scalar type proofs using the same private marker owner as binding.
/// The caller must use a parameterized plan whose cache key retains value shapes.
#[doc(hidden)]
pub fn parameter_scalar_types(
    plan: &ParameterizedLogicalPlan,
    parameters: &BTreeMap<String, Value>,
) -> BTreeMap<Value, hawdb_core::PropertyType> {
    fn visit(
        name: &str,
        value: &Value,
        path: &mut Vec<usize>,
        output: &mut BTreeMap<Value, hawdb_core::PropertyType>,
    ) {
        use hawdb_core::PropertyType;
        if let Value::List(values) = value {
            for (index, value) in values.iter().enumerate() {
                path.push(index);
                visit(name, value, path, output);
                path.pop();
            }
        } else {
            let kind = match value {
                Value::Int(_) => PropertyType::Int,
                Value::String(_) => PropertyType::String,
                Value::Bool(_) => PropertyType::Bool,
                _ => return,
            };
            output.insert(parameter_marker(name, value, path), kind);
        }
    }
    let mut output = BTreeMap::new();
    for name in plan.cache_key.values.keys() {
        if let Some(value) = parameters.get(name) {
            visit(name, value, &mut Vec::new(), &mut output);
        }
    }
    output
}

fn parameter_value_shape(value: &Value) -> ParameterValueShape {
    match value {
        Value::Null => ParameterValueShape::Null,
        Value::Bool(_) => ParameterValueShape::Bool,
        Value::Int(_) => ParameterValueShape::Int,
        Value::Float(_) => ParameterValueShape::Float,
        Value::String(_) => ParameterValueShape::String,
        Value::Binary(_) => ParameterValueShape::Binary,
        Value::Vector(value) => ParameterValueShape::Vector(value.dimension()),
        Value::Uuid(_) => ParameterValueShape::Uuid,
        Value::Date(_) => ParameterValueShape::Date,
        Value::LocalTime(_) => ParameterValueShape::LocalTime,
        Value::LocalDateTime(_) => ParameterValueShape::LocalDateTime,
        Value::DateTime(_) => ParameterValueShape::DateTime,
        Value::Duration(_) => ParameterValueShape::Duration,
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
        LogicalPlan::Unwind { rows, .. } => values_marker_use(rows, name),
        LogicalPlan::UnwindRows {
            expression, input, ..
        } => marker_use_in_projection_expression(expression, name, true)
            .combine(marker_use_in_logical_plan(input, name)),
        LogicalPlan::NodeScan { .. } | LogicalPlan::ThreadRepairStats { .. } => MarkerUse::None,
        LogicalPlan::NodeCartesianProduct { left, right } => {
            marker_use_in_logical_plan(left, name).combine(marker_use_in_logical_plan(right, name))
        }
        LogicalPlan::GraphMatch { program, input } => {
            let mut marker_use = input.as_ref().map_or(MarkerUse::None, |input| {
                marker_use_in_logical_plan(input, name)
            });
            for step in &program.steps {
                let node = match step {
                    GraphMatchStep::Node(node) => node,
                    GraphMatchStep::Expand {
                        target, properties, ..
                    } => {
                        marker_use =
                            marker_use.combine(values_marker_use(properties.values(), name));
                        target
                    }
                    // Hop bounds and types are literals; endpoints are Node steps.
                    GraphMatchStep::ShortestPath(_) => continue,
                };
                marker_use = marker_use.combine(values_marker_use(node.properties.values(), name));
            }
            if let Some(predicate) = &program.predicate {
                marker_use = marker_use.combine(marker_use_in_predicate(predicate, name));
            }
            marker_use
        }
        LogicalPlan::NodeColumnLookup {
            input,
            node_visibility_predicate,
            ..
        } => {
            let policy = node_visibility_predicate
                .as_ref()
                .map_or(MarkerUse::None, |p| marker_use_in_predicate(p, name));
            policy.combine(marker_use_in_logical_plan(input, name))
        }
        LogicalPlan::Distinct { input } | LogicalPlan::Limit { input, .. } => {
            marker_use_in_logical_plan(input, name)
        }
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
            group_keys,
            items,
            input,
        } => marker_use_in_projections(group_keys, name, false)
            .combine(marker_use_in_aggregations(items, name))
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
            if marker_use.is_safe() {
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
        LogicalPlan::VectorIndexSearch(scan) => {
            let visibility = scan
                .node_visibility_predicate
                .as_ref()
                .map_or(MarkerUse::None, |predicate| {
                    marker_use_in_predicate(predicate, name)
                });
            visibility.combine(scan.input.as_deref().map_or(MarkerUse::None, |input| {
                marker_use_in_logical_plan(input, name)
            }))
        }
        LogicalPlan::GraphAlgorithmOutput(output) => output
            .node_visibility_predicate
            .as_ref()
            .map(|predicate| marker_use_in_predicate(predicate, name))
            .unwrap_or(MarkerUse::None),
        LogicalPlan::GraphAlgorithm {
            node_visibility_predicate,
            ..
        }
        | LogicalPlan::GraphSeed {
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
        Predicate::ExpressionIn { expression, values } => {
            marker_use_in_projection_expression(expression, name, false)
                .combine(values_marker_use(values, name))
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

fn marker_use_in_aggregations(items: &[Aggregation], name: &str) -> MarkerUse {
    items.iter().fold(MarkerUse::None, |marker_use, item| {
        marker_use.combine(match &item.target {
            AggregateTarget::Expression(expression) => {
                marker_use_in_projection_expression(expression, name, false)
            }
            _ => MarkerUse::None,
        })
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
    if let ProjectionExpression::Case {
        operand: Some(operand),
        branches,
        otherwise,
    } = expression
        && matches!(
            operand.as_ref(),
            ProjectionExpression::Property { .. } | ProjectionExpression::Column(_)
        )
        && branches.iter().all(|(condition, result)| {
            matches!(condition, ProjectionExpression::Literal(_))
                && matches!(result, ProjectionExpression::Literal(_))
        })
        && otherwise
            .as_deref()
            .is_none_or(|value| matches!(value, ProjectionExpression::Literal(_)))
    {
        // Simple CASE with literal branches has no value-driven planner
        // specialization. Binding changes values, never its branch structure.
        let marker_use = values_marker_use(
            branches
                .iter()
                .flat_map(|(condition, result)| [condition, result])
                .chain(otherwise.iter().map(Box::as_ref))
                .filter_map(|value| match value {
                    ProjectionExpression::Literal(value) => Some(value),
                    _ => None,
                }),
            name,
        );
        return if marker_use == MarkerUse::Safe {
            MarkerUse::SafeCase
        } else {
            marker_use
        };
    }
    let marker_use = match expression {
        ProjectionExpression::Case { .. }
        | ProjectionExpression::VectorDistance { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. }
        | ProjectionExpression::ListFunction { .. }
        | ProjectionExpression::ListComprehension(_)
        | ProjectionExpression::List(_)
        | ProjectionExpression::ListIndex { .. }
        | ProjectionExpression::Conversion { .. }
        | ProjectionExpression::ListSlice { .. }
        | ProjectionExpression::TemporalFunction { .. }
        | ProjectionExpression::PropertyAccess { .. }
        | ProjectionExpression::MapLiteral(_) => {
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
        ProjectionExpression::Left { expression, .. }
        | ProjectionExpression::Lower(expression)
        | ProjectionExpression::JsonValue { expression, .. } => {
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
    if !safe && marker_use.is_safe() {
        MarkerUse::Unsafe
    } else {
        marker_use
    }
}

#[derive(Clone, Copy)]
enum BindingAction<'a> {
    Execute(&'a BTreeMap<String, Value>),
    PrepareRegex,
    RetainRegex(&'a std::cell::Cell<bool>),
}

/// Retain validated pattern sources without compiled automata in a cache owner.
/// Returns whether execution binding must prepare regex state, even without slots.
#[doc(hidden)]
pub fn prepare_physical_plan_for_cache(plan: &mut PhysicalPlan) -> Result<bool> {
    let found = std::cell::Cell::new(false);
    bind_physical_plan(plan, BindingAction::RetainRegex(&found))?;
    Ok(found.get())
}

/// Prepare only source-backed regex state in an execution clone. Literal values
/// are left untouched when the template has no actual parameter slots.
#[doc(hidden)]
pub fn prepare_physical_regexes_for_execution(plan: &mut PhysicalPlan) -> Result<()> {
    bind_physical_plan(plan, BindingAction::PrepareRegex)
}

fn bind_physical_plan(plan: &mut PhysicalPlan, parameters: BindingAction<'_>) -> Result<()> {
    match plan {
        PhysicalPlan::VectorIndexScan(scan) => {
            bind_optional_predicate(&mut scan.node_visibility_predicate, parameters)?;
            if let Some(input) = &mut scan.input {
                bind_physical_plan(input, parameters)?;
            }
        }
        PhysicalPlan::GraphAlgorithmOutput(output) => {
            bind_optional_predicate(&mut output.node_visibility_predicate, parameters)?
        }
        PhysicalPlan::GraphAlgorithm {
            node_visibility_predicate,
            ..
        }
        | PhysicalPlan::GraphSeedScan {
            node_visibility_predicate,
            ..
        } => bind_optional_predicate(node_visibility_predicate, parameters)?,
        PhysicalPlan::SourceSegmentScan { predicate, .. } => {
            bind_predicate(predicate, parameters)?;
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            bind_predicate(predicate, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::UnwindRowsExec {
            expression, input, ..
        } => {
            bind_projection_expression(expression, parameters)?;
            bind_physical_plan(input, parameters)?;
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
        PhysicalPlan::GraphMatchExec { program, input } => {
            for step in &mut program.steps {
                let node = match step {
                    GraphMatchStep::Node(node) => node,
                    GraphMatchStep::Expand {
                        target, properties, ..
                    } => {
                        bind_value_map(properties, parameters)?;
                        target
                    }
                    GraphMatchStep::ShortestPath(_) => continue,
                };
                bind_value_map(&mut node.properties, parameters)?;
            }
            bind_optional_predicate(&mut program.predicate, parameters)?;
            if let Some(input) = input {
                bind_physical_plan(input, parameters)?;
            }
        }
        PhysicalPlan::NodeColumnLookupExec {
            input,
            node_visibility_predicate,
            ..
        } => {
            bind_optional_predicate(node_visibility_predicate, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::AdjacencyExistsExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::ScoringRerankExec { input, .. }
        | PhysicalPlan::ScoringProgramExec { input, .. }
        | PhysicalPlan::HostScoringExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => bind_physical_plan(input, parameters)?,
        PhysicalPlan::IndexNodeSeek { value, .. } => bind_value(value, parameters)?,
        PhysicalPlan::UnwindExec { rows: values, .. }
        | PhysicalPlan::IndexNodeMultiSeek { values, .. } => {
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
            group_keys,
            items,
            input,
        } => {
            bind_projections(group_keys, parameters)?;
            bind_aggregations(items, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::SortExec { items, input } | PhysicalPlan::TopNExec { items, input, .. } => {
            bind_sort_items(items, parameters)?;
            bind_physical_plan(input, parameters)?;
        }
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::VectorSeedScan { .. }
        | PhysicalPlan::TextSeedScan { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::UnwindMutation { .. }
        // LOAD CSV keeps its parameter name, resolved again at execution.
        | PhysicalPlan::LoadCsvMutation { .. }
        | PhysicalPlan::LoadCsvExec { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. }
        | PhysicalPlan::EmptyExec
        | PhysicalPlan::SeqNodeScan { .. }
        | PhysicalPlan::IndexNodeTextSeek { .. }
        | PhysicalPlan::NodeCountExec { .. }
        | PhysicalPlan::RelationshipCountExec { .. }
        | PhysicalPlan::ThreadRepairStatsExec { .. } => {}
    }
    Ok(())
}

fn bind_node_projection_access(
    access: &mut hawdb_plan_cypher::NodeProjectionAccess,
    parameters: BindingAction<'_>,
) -> Result<()> {
    match access {
        hawdb_plan_cypher::NodeProjectionAccess::LabelScan => Ok(()),
        hawdb_plan_cypher::NodeProjectionAccess::PropertyValues { values, .. } => {
            bind_values(values, parameters)
        }
        hawdb_plan_cypher::NodeProjectionAccess::PropertyUnion { branches } => {
            for branch in branches {
                bind_values(&mut branch.values, parameters)?;
            }
            Ok(())
        }
        hawdb_plan_cypher::NodeProjectionAccess::CompositeEquality { predicates } => {
            for (_, value) in predicates {
                bind_value(value, parameters)?;
            }
            Ok(())
        }
        hawdb_plan_cypher::NodeProjectionAccess::CompositeRange { seek } => {
            bind_composite_range_seek(seek, parameters)
        }
        hawdb_plan_cypher::NodeProjectionAccess::CompositeOrdered { seek } => {
            for (_, value) in &mut seek.equality_prefix {
                bind_value(value, parameters)?;
            }
            if let Some(after) = &mut seek.after {
                bind_values(after, parameters)?;
            }
            Ok(())
        }
        hawdb_plan_cypher::NodeProjectionAccess::PropertyRange { lower, upper, .. } => {
            for (value, _) in lower.iter_mut().chain(upper.iter_mut()) {
                bind_value(value, parameters)?;
            }
            Ok(())
        }
        hawdb_plan_cypher::NodeProjectionAccess::FullText { .. } => Ok(()),
    }
}

fn bind_composite_range_seek(
    seek: &mut hawdb_plan_cypher::CompositeRangeSeek,
    parameters: BindingAction<'_>,
) -> Result<()> {
    for (_, value) in &mut seek.equality_prefix {
        bind_value(value, parameters)?;
    }
    for (value, _) in seek.lower.iter_mut().chain(seek.upper.iter_mut()) {
        bind_value(value, parameters)?;
    }
    Ok(())
}

fn bind_logical_plan(plan: &mut LogicalPlan, parameters: BindingAction<'_>) -> Result<()> {
    match plan {
        LogicalPlan::Unwind { rows, .. } => bind_values(rows, parameters)?,
        LogicalPlan::UnwindRows {
            expression, input, ..
        } => {
            bind_projection_expression(expression, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        LogicalPlan::VectorIndexSearch(scan) => {
            bind_optional_predicate(&mut scan.node_visibility_predicate, parameters)?;
            if let Some(input) = &mut scan.input {
                bind_logical_plan(input, parameters)?;
            }
        }
        LogicalPlan::GraphAlgorithmOutput(output) => {
            bind_optional_predicate(&mut output.node_visibility_predicate, parameters)?
        }
        LogicalPlan::GraphAlgorithm {
            node_visibility_predicate,
            ..
        }
        | LogicalPlan::GraphSeed {
            node_visibility_predicate,
            ..
        } => bind_optional_predicate(node_visibility_predicate, parameters)?,
        LogicalPlan::NodeCartesianProduct { left, right } => {
            bind_logical_plan(left, parameters)?;
            bind_logical_plan(right, parameters)?;
        }
        LogicalPlan::NodeColumnLookup {
            input,
            node_visibility_predicate,
            ..
        } => {
            bind_optional_predicate(node_visibility_predicate, parameters)?;
            bind_logical_plan(input, parameters)?;
        }
        LogicalPlan::Distinct { input } | LogicalPlan::Limit { input, .. } => {
            bind_logical_plan(input, parameters)?
        }
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
            group_keys,
            items,
            input,
        } => {
            bind_projections(group_keys, parameters)?;
            bind_aggregations(items, parameters)?;
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
    parameters: BindingAction<'_>,
) -> Result<()> {
    if let Some(predicate) = predicate {
        bind_predicate(predicate, parameters)?;
    }
    Ok(())
}

fn bind_predicate(predicate: &mut Predicate, parameters: BindingAction<'_>) -> Result<()> {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => {
            for predicate in predicates {
                bind_predicate(predicate, parameters)?;
            }
        }
        Predicate::Not(predicate) => bind_predicate(predicate, parameters)?,
        Predicate::PropertyRegexMatch { pattern, .. } => match parameters {
            BindingAction::Execute(_) | BindingAction::PrepareRegex => {
                pattern.prepare_for_execution()?
            }
            BindingAction::RetainRegex(found) => {
                pattern.retain_source_only();
                found.set(true);
            }
        },
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
        Predicate::ExpressionIn { expression, values } => {
            bind_projection_expression(expression, parameters)?;
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

fn bind_projections(projections: &mut [Projection], parameters: BindingAction<'_>) -> Result<()> {
    for projection in projections {
        bind_projection_expression(&mut projection.expression, parameters)?;
    }
    Ok(())
}

fn bind_aggregations(items: &mut [Aggregation], parameters: BindingAction<'_>) -> Result<()> {
    for item in items {
        if let AggregateTarget::Expression(expression) = &mut item.target {
            bind_projection_expression(expression, parameters)?;
        }
    }
    Ok(())
}

fn bind_sort_items(items: &mut [SortItem], parameters: BindingAction<'_>) -> Result<()> {
    for item in items {
        if let SortKey::Expression(expression) = &mut item.key {
            bind_projection_expression(expression, parameters)?;
        }
    }
    Ok(())
}

fn bind_projection_expression(
    expression: &mut ProjectionExpression,
    parameters: BindingAction<'_>,
) -> Result<()> {
    match expression {
        ProjectionExpression::Case { .. }
        | ProjectionExpression::VectorDistance { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. }
        | ProjectionExpression::ListFunction { .. }
        | ProjectionExpression::ListComprehension(_)
        | ProjectionExpression::List(_)
        | ProjectionExpression::ListIndex { .. }
        | ProjectionExpression::Conversion { .. }
        | ProjectionExpression::ListSlice { .. }
        | ProjectionExpression::TemporalFunction { .. }
        | ProjectionExpression::PropertyAccess { .. }
        | ProjectionExpression::MapLiteral(_) => expression
            .try_for_each_child_mut(|child| bind_projection_expression(child, parameters))?,

        ProjectionExpression::Literal(value) => bind_value(value, parameters)?,
        ProjectionExpression::Coalesce(expressions) => {
            for expression in expressions {
                bind_projection_expression(expression, parameters)?;
            }
        }
        ProjectionExpression::Left { expression, .. }
        | ProjectionExpression::Lower(expression)
        | ProjectionExpression::JsonValue { expression, .. } => {
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
    parameters: BindingAction<'_>,
) -> Result<()> {
    for value in values.values_mut() {
        bind_value(value, parameters)?;
    }
    Ok(())
}

fn bind_values(values: &mut [Value], parameters: BindingAction<'_>) -> Result<()> {
    for value in values {
        bind_value(value, parameters)?;
    }
    Ok(())
}

fn resolve_parameter_marker<'a>(
    parameters: &'a BTreeMap<String, Value>,
    name: &str,
    path: Vec<usize>,
) -> Result<&'a Value> {
    let mut bound = parameters
        .get(name)
        .ok_or_else(|| HawDBError::Semantic(format!("missing parameter '${name}'")))?;
    for index in path {
        let Value::List(values) = bound else {
            return Err(HawDBError::Semantic(format!(
                "parameter '${name}' changed shape while binding a cached plan"
            )));
        };
        bound = values.get(index).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "parameter '${name}' changed shape while binding a cached plan"
            ))
        })?;
    }
    Ok(bound)
}

fn bind_value(value: &mut Value, parameters: BindingAction<'_>) -> Result<()> {
    let BindingAction::Execute(values) = parameters else {
        return Ok(());
    };
    if let Some((name, path)) = marker_name_and_path(value) {
        *value = resolve_parameter_marker(values, name, path)?.clone();
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
        bind_physical_plan_parameters, marker_use_in_logical_plan, parameter_marker,
        parameter_scalar_types, parameter_value_shape, parameterize_logical_plan,
        resolve_plan_template_value, MarkerUse, ParameterCacheValue, PARAMETER_SLOT_NAME_KEY,
        PARAMETER_SLOT_PATH_KEY,
    };
    use hawdb_core::Value;
    use hawdb_cypher as cypher;
    use hawdb_optimizer_graph::{CascadesOptimizer, OptimizerCatalog};
    use hawdb_plan_cypher::{
        CompositeRangeSeek, ExactPropertySeekBranch, LogicalPlanRoot, NodeProjectionAccess,
        PhysicalPlan,
    };
    use std::collections::BTreeMap;

    #[test]
    fn unwind_read_parameters_rebind_in_logical_and_physical_templates() {
        let statement = cypher::parse(
            "UNWIND $ids AS candidate_id OPTIONAL MATCH (n:Item {id: candidate_id}) RETURN n.id",
        )
        .unwrap();
        let parameters = BTreeMap::from([(
            "ids".into(),
            Value::List(vec![Value::Int(1), Value::Int(1), Value::Null]),
        )]);
        let template = parameterize_logical_plan(&statement, &parameters).unwrap();
        assert_eq!(template.slot_count(), 1);
        let changed = BTreeMap::from([(
            "ids".into(),
            Value::List(vec![Value::Int(2), Value::Int(3), Value::Null]),
        )]);
        let mut rebound = template.logical().clone();
        super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&changed)).unwrap();
        assert_eq!(
            rebound,
            hawdb_plan_cypher::plan_with_params(&statement, &changed).unwrap()
        );
        let physical = CascadesOptimizer::default().optimize(template.logical());
        let physical = bind_physical_plan_parameters(&physical, &changed, true).unwrap();
        let mut sources = 0;
        hawdb_plan_cypher::visit_plan(&physical, &mut |node| {
            if let PhysicalPlan::UnwindExec { rows, .. } = node {
                assert_eq!(rows, &[Value::Int(2), Value::Int(3), Value::Null]);
                sources += 1;
            }
        });
        assert_eq!(sources, 1);
    }

    #[test]
    fn row_unwind_parameters_rebind_in_logical_and_physical_templates() {
        let statement =
            cypher::parse("MATCH (n:Item) UNWIND [$candidate, n.id, null] AS item RETURN item")
                .unwrap();
        let parameters = BTreeMap::from([("candidate".into(), Value::String("first".into()))]);
        let changed = BTreeMap::from([("candidate".into(), Value::String("second".into()))]);
        let template = parameterize_logical_plan(&statement, &parameters).unwrap();
        assert_eq!(template.slot_count(), 1);
        let mut rebound = template.logical().clone();
        super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&changed)).unwrap();
        let expected = hawdb_plan_cypher::plan_with_params(&statement, &changed).unwrap();
        assert_eq!(rebound, expected);
        let physical = CascadesOptimizer::default().optimize(template.logical());
        let bound = bind_physical_plan_parameters(&physical, &changed, true).unwrap();
        assert_eq!(bound, CascadesOptimizer::default().optimize(&expected));
        let mut sources = 0;
        hawdb_plan_cypher::visit_plan(&bound, &mut |node| {
            if let PhysicalPlan::UnwindRowsExec { .. } = node {
                sources += 1;
            }
        });
        assert_eq!(sources, 1);
    }

    #[test]
    fn proven_binding_template_skips_planning_and_validates_complete_signature() {
        let statement =
            cypher::parse("MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id LIMIT $limit")
                .unwrap();
        let parameters = BTreeMap::from([
            ("id".into(), Value::String("a".into())),
            ("limit".into(), Value::Int(3)),
        ]);
        let first = parameterize_logical_plan(&statement, &parameters).unwrap();
        let template = super::ParameterBindingTemplate::new(&first, &parameters).unwrap();
        let mut changed = parameters.clone();
        changed.insert("id".into(), Value::String("b".into()));
        super::PARAMETER_PLANNING_CALLS.with(|calls| calls.set(0));
        let reused = template.reuse(&changed).unwrap();
        assert!(std::sync::Arc::ptr_eq(&first.logical, &reused.logical));
        assert!(std::sync::Arc::ptr_eq(&first.cache_key, &reused.cache_key));
        assert_eq!(
            super::PARAMETER_PLANNING_CALLS.with(std::cell::Cell::get),
            0
        );
        let mut rebound = reused.logical().clone();
        super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&changed)).unwrap();
        assert_eq!(
            rebound,
            hawdb_plan_cypher::plan_with_params(&statement, &changed).unwrap()
        );
        changed.insert("limit".into(), Value::Int(4));
        assert!(template.reuse(&changed).is_none());
        changed = parameters.clone();
        changed.insert("id".into(), Value::Int(1));
        assert!(template.reuse(&changed).is_none());
        changed = parameters.clone();
        changed.remove("id");
        assert!(template.reuse(&changed).is_none());
        changed = parameters.clone();
        changed.insert("extra".into(), Value::Int(1));
        assert!(template.reuse(&changed).is_none());
        let unused = parameterize_logical_plan(&statement, &changed).unwrap();
        assert!(super::ParameterBindingTemplate::new(&unused, &changed).is_none());
    }

    #[test]
    fn folded_null_guards_keep_shape_valid_rebinding() {
        for predicate in ["$gate IS NULL", "$gate IS NOT NULL"] {
            let statement = cypher::parse(&format!(
                "MATCH (n:Item) WHERE ({predicate} OR n.id = $gate) \
                 RETURN CASE n.id WHEN 'a' THEN $gate ELSE $gate END AS gate"
            ))
            .unwrap();
            let parameters = BTreeMap::from([("gate".into(), Value::Int(1))]);
            let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
            let template = super::ParameterBindingTemplate::new(&plan, &parameters).unwrap();
            let changed = BTreeMap::from([("gate".into(), Value::Int(2))]);
            super::PARAMETER_PLANNING_CALLS.with(|calls| calls.set(0));
            let reused = template.reuse(&changed).unwrap();
            assert_eq!(
                super::PARAMETER_PLANNING_CALLS.with(std::cell::Cell::get),
                0
            );
            let mut rebound = reused.logical().clone();
            super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&changed))
                .unwrap();
            assert_eq!(
                rebound,
                hawdb_plan_cypher::plan_with_params(&statement, &changed).unwrap()
            );
            assert!(template
                .reuse(&BTreeMap::from([("gate".into(), Value::Null)]))
                .is_none());
        }
    }

    #[test]
    fn folded_predicate_parameters_are_exact_even_when_also_projected() {
        for (predicate, initial, changed) in [
            ("$gate = 0", Value::Int(1), Value::Int(0)),
            ("$gate <> 1", Value::Int(2), Value::Int(1)),
            ("$gate = true", Value::Bool(false), Value::Bool(true)),
            ("$gate <> false", Value::Bool(true), Value::Bool(false)),
            ("$other = $gate", Value::Int(1), Value::Int(0)),
            ("$other <> $gate", Value::Int(1), Value::Int(0)),
        ] {
            let statement = cypher::parse(&format!(
                "MATCH (n:Item) WHERE ({predicate} OR n.id = $gate) \
                 RETURN n.id AS id, CASE n.id WHEN 'a' THEN $gate ELSE $gate END AS gate, \
                 CASE n.id WHEN 'a' THEN $other ELSE $other END AS other, \
                 CASE n.id WHEN 'a' THEN $score ELSE $score END AS score"
            ))
            .unwrap();
            let parameters = BTreeMap::from([
                ("gate".into(), initial),
                ("other".into(), Value::Int(0)),
                ("score".into(), Value::Float(0.5)),
            ]);
            let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
            let template = super::ParameterBindingTemplate::new(&plan, &parameters).unwrap();
            let mut altered = parameters.clone();
            altered.insert("gate".into(), changed);
            assert!(
                template.reuse(&altered).is_none(),
                "folded guard {predicate}"
            );
            altered = parameters.clone();
            altered.insert("score".into(), Value::Float(0.75));
            let reused = template.reuse(&altered).unwrap();
            let mut rebound = reused.logical().clone();
            super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&altered))
                .unwrap();
            assert_eq!(
                rebound,
                hawdb_plan_cypher::plan_with_params(&statement, &altered).unwrap()
            );
        }
    }

    #[test]
    fn scoped_case_template_retains_folded_booleans_as_exact_guards() {
        let mut query = "MATCH (n:Item) WHERE n.id IN $ids AND ($cross_space = true OR \
            n.space_id IN $spaces OR ($default_requested = true AND \
            (n.space_id IS NULL OR n.space_id = ''))) RETURN CASE n.id"
            .to_owned();
        let mut parameters = BTreeMap::from([
            ("cross_space".into(), Value::Bool(false)),
            (
                "spaces".into(),
                Value::List(vec![Value::String("default".into())]),
            ),
            ("default_requested".into(), Value::Bool(true)),
        ]);
        let mut ids = Vec::new();
        for index in 0..20 {
            query.push_str(&format!(" WHEN $id_{index} THEN $score_{index}"));
            let id = Value::String(format!("a{index}"));
            ids.push(id.clone());
            parameters.insert(format!("id_{index}"), id);
            parameters.insert(format!("score_{index}"), Value::Float(0.5));
        }
        query.push_str(" ELSE null END AS score");
        parameters.insert("ids".into(), Value::List(ids));
        let statement = cypher::parse(&query).unwrap();
        let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
        let template = super::ParameterBindingTemplate::new(&plan, &parameters)
            .expect("folded boolean scope guards must permit safe template reuse");
        let mut changed = parameters.clone();
        changed.insert("score_0".into(), Value::Float(0.75));
        super::PARAMETER_PLANNING_CALLS.with(|calls| calls.set(0));
        let reused = template.reuse(&changed).unwrap();
        assert_eq!(
            super::PARAMETER_PLANNING_CALLS.with(std::cell::Cell::get),
            0
        );
        let mut rebound = reused.logical().clone();
        super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&changed)).unwrap();
        assert_eq!(
            rebound,
            hawdb_plan_cypher::plan_with_params(&statement, &changed).unwrap()
        );
        for name in ["cross_space", "default_requested"] {
            let mut altered = changed.clone();
            let Value::Bool(value) = altered[name] else {
                panic!("boolean fixture")
            };
            altered.insert(name.into(), Value::Bool(!value));
            assert!(template.reuse(&altered).is_none(), "changed scope {name}");
            altered.remove(name);
            assert!(template.reuse(&altered).is_none(), "missing scope {name}");
            altered.insert(name.into(), Value::Int(i64::from(value)));
            assert!(
                template.reuse(&altered).is_none(),
                "changed scope type {name}"
            );
        }
    }

    #[test]
    fn simple_case_cohorts_parameterize_without_per_value_replanning() {
        for count in [20, 100, 512] {
            let mut query = "MATCH (n:Item) WHERE n.id IN $ids RETURN CASE n.id".to_owned();
            let mut parameters = BTreeMap::new();
            let mut ids = Vec::new();
            for index in 0..count {
                query.push_str(&format!(" WHEN $id_{index} THEN $score_{index}"));
                let id = Value::String(format!("item-{index}"));
                ids.push(id.clone());
                parameters.insert(format!("id_{index}"), id);
                parameters.insert(format!("score_{index}"), Value::Float(index as f64));
            }
            query.push_str(" ELSE null END AS score");
            parameters.insert("ids".into(), Value::List(ids));
            let statement = cypher::parse(&query).unwrap();
            super::PARAMETER_PLANNING_CALLS.with(|calls| calls.set(0));
            let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
            let calls = super::PARAMETER_PLANNING_CALLS.with(std::cell::Cell::get);
            assert!(
                calls <= 3,
                "{count} candidates required {calls} logical plans"
            );
            assert_eq!(plan.slot_count(), parameters.len());
            assert!(plan.has_case_parameter_slots());
            let mut changed = parameters.clone();
            changed.insert("score_0".into(), Value::Float(-3.0));
            assert_eq!(
                plan.cache_key(),
                parameterize_logical_plan(&statement, &changed)
                    .unwrap()
                    .cache_key()
            );
            let mut rebound = plan.logical().clone();
            super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&changed))
                .unwrap();
            assert_eq!(
                rebound,
                hawdb_plan_cypher::plan_with_params(&statement, &changed).unwrap()
            );
        }
    }

    #[test]
    fn case_retention_classification_preserves_non_case_slots_and_fallback() {
        for (query, parameters, case_slots) in [
            (
                "MATCH (n:Item) WHERE n.id = $id RETURN CASE n.id WHEN 'a' THEN 1 ELSE 0 END AS rank",
                BTreeMap::from([("id".into(), Value::String("a".into()))]),
                false,
            ),
            (
                "MATCH (n:Item) RETURN CASE n.id WHEN 'a' THEN $score ELSE 0 END AS rank LIMIT $take",
                BTreeMap::from([("score".into(), Value::Int(1)), ("take".into(), Value::Int(2))]),
                true,
            ),
        ] {
            let statement = cypher::parse(query).unwrap();
            let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
            assert!(plan.slot_count() > 0);
            assert_eq!(plan.has_case_parameter_slots(), case_slots, "{query}");
            let mut rebound = plan.logical().clone();
            super::bind_logical_plan(&mut rebound, super::BindingAction::Execute(&parameters)).unwrap();
            assert_eq!(rebound, hawdb_plan_cypher::plan_with_params(&statement, &parameters).unwrap());
        }
    }

    #[test]
    fn parameter_type_proofs_share_binding_markers_and_distinguish_numeric_shapes() {
        use hawdb_core::PropertyType;
        let parameters = BTreeMap::from([
            ("rank".into(), Value::Int(12)),
            (
                "keys".into(),
                Value::List(vec![
                    Value::String("id".into()),
                    Value::List(vec![Value::Bool(true), Value::Float(12.5)]),
                ]),
            ),
        ]);
        let mut parameters = parameters;
        parameters.insert(
            "unused".into(),
            Value::List(vec![Value::String("omitted".into()); 64]),
        );
        let statement = cypher::parse(
            "MATCH (n:Item) WHERE n.rank > $rank AND n.id IN $keys RETURN n.id AS id",
        )
        .unwrap();
        let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
        let types = parameter_scalar_types(&plan, &parameters);
        assert_eq!(types.len(), 3);
        for (marker, kind) in &types {
            let value = resolve_plan_template_value(marker, &parameters, true).unwrap();
            assert!(matches!(
                (kind, value),
                (PropertyType::Int, Value::Int(_))
                    | (PropertyType::String, Value::String(_))
                    | (PropertyType::Bool, Value::Bool(_))
            ));
        }
        let int_marker = parameter_marker("rank", &Value::Int(12), &[]);
        assert_eq!(types.get(&int_marker), Some(&PropertyType::Int));
        let mut changed = parameters.clone();
        changed.insert("rank".into(), Value::Float(12.5));
        let changed_plan = parameterize_logical_plan(&statement, &changed).unwrap();
        assert!(!parameter_scalar_types(&changed_plan, &changed).contains_key(&int_marker));
        assert_ne!(plan.cache_key(), changed_plan.cache_key());
        assert_ne!(
            parameter_value_shape(&parameters["rank"]),
            parameter_value_shape(&changed["rank"])
        );
    }

    #[test]
    fn cost_operand_resolution_borrows_values_and_preserves_literal_markers() {
        let parameters = BTreeMap::from([("id".into(), Value::String("point".into()))]);
        let marker = parameter_marker("id", &parameters["id"], &[]);
        let resolved = resolve_plan_template_value(&marker, &parameters, true).unwrap();
        assert!(std::ptr::eq(resolved, &parameters["id"]));
        assert!(std::ptr::eq(
            resolve_plan_template_value(&marker, &BTreeMap::new(), false).unwrap(),
            &marker
        ));
        assert!(resolve_plan_template_value(&marker, &BTreeMap::new(), true).is_err());
        let parameters = BTreeMap::from([("ids".into(), Value::List(vec![Value::Int(7)]))]);
        let Value::List(markers) = parameter_marker("ids", &parameters["ids"], &[]) else {
            panic!("expected list markers")
        };
        let resolved = resolve_plan_template_value(&markers[0], &parameters, true).unwrap();
        let Value::List(values) = &parameters["ids"] else {
            panic!("expected list operand")
        };
        assert!(std::ptr::eq(resolved, &values[0]));
        assert!(resolve_plan_template_value(
            &markers[0],
            &BTreeMap::from([("ids".into(), Value::Int(7))]),
            true
        )
        .is_err());
    }

    #[test]
    fn unused_bindings_share_a_template_without_hiding_exact_parameters() {
        let statement =
            cypher::parse("MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id LIMIT $limit")
                .unwrap();
        let first = BTreeMap::from([
            ("id".into(), Value::Int(1)),
            ("limit".into(), Value::Int(3)),
            ("score".into(), Value::String("unused".into())),
        ]);
        let mut second = first.clone();
        second.insert("id".into(), Value::Int(2));
        second.insert("score".into(), Value::List(vec![Value::Int(9)]));
        let first_plan = parameterize_logical_plan(&statement, &first).unwrap();
        let second_plan = parameterize_logical_plan(&statement, &second).unwrap();
        assert_eq!(first_plan.cache_key(), second_plan.cache_key());
        assert!(!first_plan.cache_key.values.contains_key("score"));
        assert_eq!(first_plan.exact_variant_count(), 1);
        assert_eq!(first_plan.slot_count(), 1);
        second.remove("score");
        assert_eq!(
            first_plan.cache_key(),
            parameterize_logical_plan(&statement, &second)
                .unwrap()
                .cache_key()
        );
        second.insert("limit".into(), Value::Int(4));
        assert_ne!(
            first_plan.cache_key(),
            parameterize_logical_plan(&statement, &second)
                .unwrap()
                .cache_key()
        );
        second.remove("id");
        assert!(parameterize_logical_plan(&statement, &second).is_err());
    }

    #[test]
    fn unused_only_bindings_leave_no_slots_and_preserve_literal_values() {
        let statement = cypher::parse("MATCH (n:Item) WHERE n.id = 1 RETURN n.id").unwrap();
        let empty = parameterize_logical_plan(&statement, &BTreeMap::new()).unwrap();
        let extra = parameterize_logical_plan(
            &statement,
            &BTreeMap::from([("unused".into(), Value::String("payload".into()))]),
        )
        .unwrap();
        assert_eq!(empty, extra);
        assert_eq!(extra.slot_count(), 0);
        assert_eq!(extra.exact_variant_count(), 0);
    }

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
        let logical = hawdb_plan_cypher::plan_with_params(&statement, &markers).unwrap();
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
        assert!(!text.contains("hawdb_parameter_slot"), "{text}");
    }

    #[test]
    fn list_expression_parameters_are_detected_keyed_and_rebound() {
        let statement = cypher::parse(
            "MATCH (n:Item) WITH n, n.tags AS tags WHERE size(tags) > $min_size AND $needle IN tags \
             RETURN [x IN range($start, $end) WHERE x > $floor | coalesce(tags[x], $missing)] AS picked, \
             n.tags[$index..] AS rest ORDER BY head(tags[..$prefix]) DESC",
        )
        .unwrap();
        let names = [
            "min_size", "needle", "start", "end", "floor", "missing", "index", "prefix",
        ];
        let parameters = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let value = if *name == "needle" || *name == "missing" {
                    Value::String(format!("old-{index}"))
                } else {
                    Value::Int(index as i64)
                };
                (name.to_string(), value)
            })
            .collect::<BTreeMap<_, _>>();
        let parameterized = parameterize_logical_plan(&statement, &parameters).unwrap();
        // Every parameter is seen; list expressions keep the conservative
        // exact-key policy of other scalar expressions, so no value is dropped.
        assert_eq!(parameterized.slot_count, 0);
        for name in names {
            assert!(
                matches!(
                    parameterized.cache_key.values.get(name),
                    Some(ParameterCacheValue::Exact(_))
                ),
                "{name}: {:?}",
                parameterized.cache_key
            );
        }
        let markers = parameters
            .iter()
            .map(|(name, value)| (name.clone(), parameter_marker(name, value, &[])))
            .collect::<BTreeMap<_, _>>();
        let logical = hawdb_plan_cypher::plan_with_params(&statement, &markers).unwrap();
        for name in names {
            assert_ne!(
                marker_use_in_logical_plan(&logical, name),
                MarkerUse::None,
                "{name}"
            );
        }
        let root = CascadesOptimizer::default().optimize_root_with_catalog(
            &LogicalPlanRoot::new(logical),
            &OptimizerCatalog::default(),
        );
        let (template, _) = root.into_parts();
        let rebound_parameters = names
            .iter()
            .map(|name| (name.to_string(), Value::String(format!("rebound-{name}"))))
            .collect::<BTreeMap<_, _>>();
        let rebound = bind_physical_plan_parameters(&template, &rebound_parameters, true).unwrap();
        let text = format!("{rebound:?}");
        for name in names {
            assert!(text.contains(&format!("rebound-{name}")), "{name}: {text}");
        }
        assert!(!text.contains("hawdb_parameter_slot"), "{text}");
        assert!(!text.contains("skein_parameter_slot"), "{text}");
    }

    #[test]
    fn load_csv_plans_keep_parameter_sources_unbound_and_reusable() {
        let statement = cypher::parse(
            "LOAD CSV WITH HEADERS FROM $f AS row WITH row WHERE toInteger(row.id) > $min \
             RETURN row.name AS name",
        )
        .unwrap();
        let first = BTreeMap::from([
            ("f".to_string(), Value::String("/import/a.csv".into())),
            ("min".to_string(), Value::Int(1)),
        ]);
        let second = BTreeMap::from([
            ("f".to_string(), Value::String("/import/b.csv".into())),
            ("min".to_string(), Value::Int(1)),
        ]);
        let first = parameterize_logical_plan(&statement, &first).unwrap();
        let second = parameterize_logical_plan(&statement, &second).unwrap();
        // The file location is never part of the plan or its cache key, so one
        // cached plan serves every file; execution resolves `$f` again.
        assert_eq!(first.logical, second.logical);
        assert_eq!(first.cache_key, second.cache_key);
        assert!(
            !first.cache_key.values.contains_key("f"),
            "{:?}",
            first.cache_key
        );
        let text = format!("{:?}", first.logical);
        assert!(text.contains("Parameter(\"f\")"), "{text}");
        assert!(!text.contains("a.csv"), "{text}");

        let physical = CascadesOptimizer::default().optimize(&first.logical);
        let rebound = bind_physical_plan_parameters(
            &physical,
            &BTreeMap::from([("min".to_string(), Value::Int(5))]),
            first.slot_count > 0,
        )
        .unwrap();
        let mut sources = Vec::new();
        hawdb_plan_cypher::visit_plan(&rebound, &mut |node| {
            if let PhysicalPlan::LoadCsvExec { source } = node {
                sources.push(source.clone());
            }
        });
        assert_eq!(sources.len(), 1, "{rebound:?}");
        assert_eq!(
            sources[0].location,
            hawdb_plan_cypher::CsvLocation::Parameter("f".to_string())
        );
    }

    #[test]
    fn clause_match_rebinds_every_step_predicate_and_input() {
        let query = "MATCH (a:Item {id: $a}) OPTIONAL MATCH (a)-[r:LINK {code: $code}]->(b:Item {id: $b}) WHERE b.visible = $visible RETURN b";
        let slots = ["a", "b", "code", "visible"];
        let parameters = slots
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    parameter_marker(name, &Value::Int(0), &[]),
                )
            })
            .collect();
        let logical = hawdb_plan_cypher::plan_pipeline_query(query, &parameters).unwrap();
        for name in slots {
            assert_eq!(
                marker_use_in_logical_plan(&logical, name),
                MarkerUse::Safe,
                "{name}"
            );
        }
        let optimize = |logical| {
            CascadesOptimizer::default()
                .optimize_root_with_catalog(
                    &LogicalPlanRoot::new(logical),
                    &OptimizerCatalog::default(),
                )
                .into_parts()
                .0
        };
        let template = optimize(logical);
        let original = template.instance_fingerprint();
        for base in [10, 20] {
            let parameters = slots
                .into_iter()
                .enumerate()
                .map(|(index, name)| (name.to_string(), Value::Int(base + index as i64)))
                .collect();
            let rebound = bind_physical_plan_parameters(&template, &parameters, true).unwrap();
            let fresh =
                optimize(hawdb_plan_cypher::plan_pipeline_query(query, &parameters).unwrap());
            assert_eq!(rebound.instance_fingerprint(), fresh.instance_fingerprint());
            assert!(!rebound
                .instance_fingerprint()
                .contains("hawdb_parameter_slot"));
        }
        assert_eq!(template.instance_fingerprint(), original);
        assert!(bind_physical_plan_parameters(&template, &BTreeMap::new(), true).is_err());
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
            &LogicalPlanRoot::new(parameterized.logical().clone()),
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
        assert!(!rebound.explain(0).contains("hawdb_parameter_slot"));
    }

    #[test]
    fn scoring_rerank_rebinds_its_input_and_rejects_missing_parameter_slots() {
        let spec = hawdb_core::graph_rag::ScoringSpec::weighted_scores(2.0, 1.0);
        let template = PhysicalPlan::ScoringRerankExec {
            score_column: "seed_score".into(),
            spec: spec.clone(),
            limit: 3,
            input: Box::new(PhysicalPlan::IndexNodeSeek {
                variable: "n".into(),
                label: "Item".into(),
                property: "scope".into(),
                value: parameter_marker("scope", &Value::String("template".into()), &[]),
            }),
        };
        for scope in ["first", "second"] {
            let rebound = bind_physical_plan_parameters(
                &template,
                &BTreeMap::from([("scope".into(), Value::String(scope.into()))]),
                true,
            )
            .unwrap();
            let PhysicalPlan::ScoringRerankExec {
                score_column,
                spec: rebound_spec,
                limit,
                input,
            } = rebound
            else {
                panic!("scoring operator must be preserved");
            };
            assert_eq!(score_column, "seed_score");
            assert_eq!(rebound_spec, spec);
            assert_eq!(limit, 3);
            let PhysicalPlan::IndexNodeSeek { value, .. } = *input else {
                panic!("scoring input seek must be preserved");
            };
            assert_eq!(
                value,
                Value::String(scope.into()),
                "scoring input must use current request values"
            );
        }
        assert!(bind_physical_plan_parameters(&template, &BTreeMap::new(), true).is_err());
        let PhysicalPlan::ScoringRerankExec { input, .. } = &template else {
            unreachable!()
        };
        let PhysicalPlan::IndexNodeSeek { value, .. } = input.as_ref() else {
            unreachable!()
        };
        assert_eq!(
            *value,
            parameter_marker("scope", &Value::String("template".into()), &[]),
            "binding must not mutate the retained template"
        );
    }

    #[test]
    fn adjacency_exists_rebinds_its_input_and_rejects_missing_parameter_slots() {
        let marker = parameter_marker("scope", &Value::String("template".into()), &[]);
        let template = PhysicalPlan::AdjacencyExistsExec {
            source_variable: "n".into(),
            rel_type: "REL".into(),
            direction: cypher::RelationshipDirection::Outgoing,
            target_variable: "s".into(),
            input: Box::new(PhysicalPlan::IndexNodeSeek {
                variable: "n".into(),
                label: "Item".into(),
                property: "scope".into(),
                value: marker.clone(),
            }),
        };
        let original = template.clone();
        for scope in ["first", "second"] {
            let rebound = bind_physical_plan_parameters(
                &template,
                &BTreeMap::from([("scope".into(), Value::String(scope.into()))]),
                true,
            )
            .unwrap();
            let PhysicalPlan::AdjacencyExistsExec {
                source_variable,
                rel_type,
                direction,
                target_variable,
                input,
            } = rebound
            else {
                panic!("EXISTS operator must be preserved");
            };
            assert_eq!(source_variable, "n");
            assert_eq!(rel_type, "REL");
            assert_eq!(direction, cypher::RelationshipDirection::Outgoing);
            assert_eq!(target_variable, "s");
            let PhysicalPlan::IndexNodeSeek { value, .. } = *input else {
                panic!("EXISTS input seek must be preserved");
            };
            assert_eq!(value, Value::String(scope.into()));
        }
        assert!(bind_physical_plan_parameters(&template, &BTreeMap::new(), true).is_err());
        assert_eq!(
            template, original,
            "binding must preserve the cached template"
        );
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
            left_key: hawdb_plan_cypher::HashJoinKey {
                variable: "a".into(),
                property: "key".into(),
            },
            right_key: hawdb_plan_cypher::HashJoinKey {
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

// Retained query ownership: ParameterCacheValue.
impl hawdb_core::RetainedHeapBytes for ParameterCacheValue {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Slot(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::CaseSlot(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Exact(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
        }
    }
}

// Retained query ownership: ParameterValueShape.
impl hawdb_core::RetainedHeapBytes for ParameterValueShape {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Null => hawdb_core::checked_retained_heap_sum([]),
            Self::Bool => hawdb_core::checked_retained_heap_sum([]),
            Self::Int => hawdb_core::checked_retained_heap_sum([]),
            Self::Float => hawdb_core::checked_retained_heap_sum([]),
            Self::String => hawdb_core::checked_retained_heap_sum([]),
            Self::Binary => hawdb_core::checked_retained_heap_sum([]),
            Self::Vector(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Uuid => hawdb_core::checked_retained_heap_sum([]),
            Self::Date
            | Self::LocalTime
            | Self::LocalDateTime
            | Self::DateTime
            | Self::Duration => hawdb_core::checked_retained_heap_sum([]),
            Self::List(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Map(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
        }
    }
}

// Retained query ownership: PlanParameterCacheKey.
hawdb_core::impl_retained_heap_for_struct!(PlanParameterCacheKey { values });
