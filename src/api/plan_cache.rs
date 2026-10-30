// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(super) mod determinism;
mod output_schema;

use super::query_request::{BoundScoringRequest, ScoringPlanCacheKey};
use super::{
    optimizer_config_from_database_config, statement_body, DatabaseConfig,
    QueryAccessControlContext, SharedState,
};
use crate::cypher;
use crate::error::{HawDBError, Result};
use crate::optimizer::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerCatalog, OptimizerSearchDirective, OptimizerTrace,
    PhysicalPlan,
};
use crate::planner::{self, LogicalPlan, Predicate};
use crate::schema::{Catalog, GraphStatistics, IndexKind, LabelId, PropertyType, TableKind};
use crate::store::GraphStore;
use crate::value::Value;
use hawdb_optimizer::graph::{
    node_equality_index_seek_is_cheaper, optimizer_catalog_from_graph_statistics,
    NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD,
};
#[cfg(test)]
use hawdb_plan_cache::ParameterBindingTemplate;
use hawdb_plan_cache::{
    bind_physical_plan_parameters, parameter_scalar_types, parameterize_logical_plan,
    parameterize_value_list, resolve_plan_template_value, LfuCache, OptimizerSchemaKey,
    ParameterBindingCache, ParameterBindingCacheConfig, ParameterizedLogicalPlan,
    PlanParameterCacheKey,
};
pub use hawdb_plan_cache::{PlanCacheBypassReason, PlanCacheLookup, PlanCacheStats};
use hawdb_plan_cypher::{BatchMutationOperation, ProjectionExpression};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) const DEFAULT_PLAN_CACHE_MAX_ENTRIES: usize = 128;
const ACCESS_CONTROL_VALUES_PARAMETER: &str = "\0hawdb_access_control_visibility_values";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PlanCacheMode {
    Use,
    Bypass(PlanCacheBypassReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PlanTraceMode {
    Template,
    Bound,
}

pub(super) struct PlanCacheContext<'a, S = GraphStore> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a S,
    pub(super) optimizer: &'a CascadesOptimizer,
    pub(super) config: &'a DatabaseConfig,
    pub(super) cache: &'a SharedState<PlanCache>,
    pub(super) planning_cache: &'a SharedState<OptimizerPlanningCache>,
    pub(super) access_control: Option<&'a QueryAccessControlContext>,
    pub(super) optimizer_search: OptimizerSearchDirective,
    pub(super) scoring: Option<BoundScoringRequest<'a>>,
}

pub(super) struct OptimizedQueryPlan {
    // Request-owned, never retained in a shared plan-cache entry. The recorder
    // takes this after planning so cache hits preserve the current text hash.
    pub(super) query_identity: Option<hawdb_query::QueryIdentity>,
    pub(super) physical_plan: PhysicalPlan,
    pub(super) output_columns: Option<Vec<String>>,
    pub(super) trace: OptimizerTrace,
    pub(super) plan_cache_lookup: PlanCacheLookup,
    pub(super) optimizer_environment: OptimizerEnvironmentKey,
    pub(super) configured_max_optimizer_groups: Option<usize>,
    pub(super) effective_max_optimizer_groups: usize,
}

impl OptimizedQueryPlan {
    pub(super) fn collect_named_rows(
        &self,
        rows: Vec<crate::executor::Row>,
    ) -> Result<hawdb_executor::QueryRows> {
        match &self.output_columns {
            Some(columns) => hawdb_executor::QueryRows::try_from_named_rows(
                hawdb_executor::QuerySchema::try_new(columns.iter().cloned())?,
                rows,
            ),
            None => Ok(rows.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct CachedPlan {
    physical_template: PhysicalPlan,
    trace: OptimizerTrace,
    has_parameter_slots: bool,
}

pub(super) type PlanCache = LfuCache<PlanCacheKey, Arc<CachedPlan>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct PlanCacheKey {
    normalized_query: String,
    parameters: PlanParameterCacheKey,
    environment: OptimizerEnvironmentKey,
    max_optimizer_groups: Option<usize>,
    access_control: Option<AccessControlPlanCacheKey>,
    scoring: Option<ScoringPlanCacheKey>,
    point_statistics: Option<PointStatisticsKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PointStatisticsKey {
    label: String,
    property: String,
    matched_rows: u64,
    // Counts may grow within the large-label class across the access-cost
    // boundary for a duplicate key. Share that decision with the optimizer.
    index_seek_is_cheaper: bool,
}

struct PointStatistics<'plan> {
    label: &'plan str,
    property: &'plan str,
    matched_rows: u64,
    index_seek_is_cheaper: bool,
}

impl From<&PointStatistics<'_>> for PointStatisticsKey {
    fn from(point: &PointStatistics<'_>) -> Self {
        Self {
            label: point.label.to_owned(),
            property: point.property.to_owned(),
            matched_rows: point.matched_rows,
            index_seek_is_cheaper: point.index_seek_is_cheaper,
        }
    }
}

fn point_statistics<'plan, S: crate::executor::ExecutionStore>(
    context: &PlanCacheContext<'_, S>,
    logical: &'plan LogicalPlan,
    parameters: &BTreeMap<String, Value>,
    has_parameter_slots: bool,
) -> Result<Option<PointStatistics<'plan>>> {
    if context.access_control.is_some()
        || context.scoring.is_some()
        || context.store.is_out_of_core()
    {
        return Ok(None);
    }
    let LogicalPlan::Project { items, input } = logical else {
        return Ok(None);
    };
    let LogicalPlan::Filter { predicate, input } = input.as_ref() else {
        return Ok(None);
    };
    let (
        Predicate::PropertyEq {
            variable,
            property,
            value,
        },
        LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        },
    ) = (predicate, input.as_ref())
    else {
        return Ok(None);
    };
    if variable != scan_variable
        || !items.iter().all(|item| match &item.expression {
            ProjectionExpression::Variable {
                variable: projected,
            }
            | ProjectionExpression::Property {
                variable: projected,
                ..
            }
            | ProjectionExpression::Id {
                variable: projected,
            } => projected == variable,
            ProjectionExpression::Literal(_) => true,
            _ => false,
        })
    {
        return Ok(None);
    }
    let Some(label_id) = context.catalog.label_id(label) else {
        return Ok(None);
    };
    if !context
        .catalog
        .has_scalar_property_index(label_id, property)
    {
        return Ok(None);
    }
    let value = resolve_plan_template_value(value, parameters, has_parameter_slots)?;
    if !matches!(
        value,
        Value::Bool(_) | Value::Int(_) | Value::String(_) | Value::Binary(_) | Value::Uuid(_)
    ) && !matches!(value, Value::Float(value) if value.is_finite())
    {
        return Ok(None);
    }
    let Some(nodes) = context
        .store
        .scan_indexed_nodes_borrowed(label_id, property, value)
    else {
        return Ok(None);
    };
    // This is a bounded candidate probe, without materializing graph payloads.
    // A dense value falls back before requesting full optimizer statistics.
    let matched_rows = nodes
        .take(NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD as usize + 1)
        .count() as u64;
    if matched_rows > NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD {
        return Ok(None);
    }
    let label_count = context.store.node_count_for_label(Some(label_id)) as u64;
    Ok(Some(PointStatistics {
        label,
        property,
        matched_rows,
        index_seek_is_cheaper: node_equality_index_seek_is_cheaper(label_count, matched_rows),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct AccessControlPlanCacheKey {
    policy_epoch: u64,
    visibility_property: String,
    visibility_value_count: usize,
}

impl From<&QueryAccessControlContext> for AccessControlPlanCacheKey {
    fn from(access_control: &QueryAccessControlContext) -> Self {
        Self {
            policy_epoch: access_control.policy_epoch(),
            visibility_property: access_control.visibility_property().to_string(),
            visibility_value_count: access_control.allowed_visibility_values().len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct OptimizerEnvironmentKey {
    schema: OptimizerSchemaKey,
    // Query-local point estimates do not depend on published advanced statistics.
    statistics_generation: Option<u64>,
    ordered_range_indexes: Vec<(String, String)>,
    // None is an unrelated query; Some(empty) still tracks capability gains.
    ordered_composite_indexes: Option<Arc<Vec<OrderedCompositeCapability>>>,
    node_index_size_classes: Vec<(LabelId, bool)>,
}

fn node_index_size_classes<R: hawdb_storage::graph_engine::GraphReadEngine>(
    catalog: &Catalog,
    store: &R,
) -> Vec<(LabelId, bool)> {
    let mut labels = catalog
        .property_indexes()
        .filter(|index| matches!(index.kind, IndexKind::Equality | IndexKind::Range))
        .map(|index| index.label_id)
        .chain(
            catalog
                .composite_property_indexes()
                .map(|index| index.label_id),
        )
        .collect::<Vec<_>>();
    labels.sort_unstable();
    labels.dedup();
    if labels.is_empty() {
        return Vec::new();
    }
    let statistics = store.basic_statistics();
    labels
        .into_iter()
        .map(|label| {
            let count = statistics.label_counts.get(&label).copied().unwrap_or(0);
            (label, count > NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD)
        })
        .collect()
}

type OrderedCompositeCapability = (String, Vec<(String, PropertyType)>);

fn ordered_composite_indexes<R: hawdb_storage::graph_engine::GraphReadEngine>(
    catalog: &Catalog,
    store: &R,
) -> Vec<OrderedCompositeCapability> {
    catalog
        .composite_property_indexes()
        .filter_map(|index| {
            if index.properties.len() < 3
                || !store.supports_ordered_node_composite(index.label_id, &index.properties)
            {
                return None;
            }
            let label = catalog.label_name(index.label_id)?;
            let table_id = catalog.table_id(TableKind::Node, label)?;
            let table = catalog.table_descriptor(table_id)?;
            if table.state != hawdb_core::SchemaObjectState::Public
                || catalog
                    .unique_constraint_id(index.label_id, index.properties.last()?)
                    .is_none()
            {
                return None;
            }
            let suffix_start = index.properties.len() - 2;
            let columns = index
                .properties
                .iter()
                .enumerate()
                .map(|(position, property)| {
                    let descriptor = catalog
                        .property_descriptor_id(table_id, property)
                        .and_then(|id| catalog.property_descriptor(id));
                    let active = descriptor.filter(|descriptor| {
                        descriptor.state == hawdb_core::SchemaObjectState::Public
                    });
                    if position >= suffix_start {
                        let descriptor = active?;
                        if descriptor.nullable
                            || !matches!(
                                descriptor.value_type,
                                PropertyType::Int | PropertyType::String
                            )
                        {
                            return None;
                        }
                        Some((property.clone(), descriptor.value_type))
                    } else {
                        // Equality supplies prefix presence. Keep declared types for
                        // the initial conservative numeric-prefix opt-in.
                        Some((
                            property.clone(),
                            active.map_or(PropertyType::Any, |descriptor| descriptor.value_type),
                        ))
                    }
                })
                .collect::<Option<Vec<_>>>()?;
            Some((label.to_owned(), columns))
        })
        .collect()
}

fn ordered_range_indexes<R: hawdb_storage::graph_engine::GraphReadEngine>(
    catalog: &Catalog,
    store: &R,
) -> Vec<(String, String)> {
    catalog
        .property_indexes()
        .filter_map(|index| {
            (index.kind == IndexKind::Range
                && store.supports_ordered_node_range(index.label_id, &index.property))
            .then(|| {
                catalog
                    .label_name(index.label_id)
                    .map(|label| (label.to_string(), index.property.clone()))
            })
            .flatten()
        })
        .collect()
}

#[derive(Debug, Clone)]
struct CachedOptimizerCatalog {
    environment: OptimizerEnvironmentKey,
    catalog: Arc<OptimizerCatalog>,
}

/// Identity of a published advanced-statistics snapshot.
///
/// Live basic counts may change while this identity remains stable. Those changes
/// rebuild the optimizer catalog without invalidating otherwise reusable plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StatisticsPublicationKey {
    computed_at_commit_epoch: u64,
    advanced_statistics_complete: bool,
}

impl From<&GraphStatistics> for StatisticsPublicationKey {
    fn from(statistics: &GraphStatistics) -> Self {
        Self {
            computed_at_commit_epoch: statistics.computed_at_commit_epoch,
            advanced_statistics_complete: statistics.advanced_statistics_complete,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StatisticsCacheRefresh {
    snapshot_refreshed: bool,
    publication_changed: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct OptimizerPlanningCache {
    parameter_bindings: Option<ParameterBindingCache>,
    statistics: Option<Arc<GraphStatistics>>,
    statistics_schema: Option<OptimizerSchemaKey>,
    statistics_source_graph_commit_epoch: Option<u64>,
    statistics_publication: Option<StatisticsPublicationKey>,
    statistics_generation: u64,
    catalog: Option<CachedOptimizerCatalog>,
}

struct OptimizerCatalogAccess {
    environment: OptimizerEnvironmentKey,
    catalog: Arc<OptimizerCatalog>,
    decisions: Vec<String>,
}

impl OptimizerEnvironmentKey {
    fn ordered_composite_proofs(&self) -> impl Iterator<Item = OrderedCompositeCapability> + '_ {
        self.ordered_composite_indexes
            .iter()
            .flat_map(|proofs| proofs.iter())
            .cloned()
    }

    /// A prepared query may reuse a physical plan across data and statistics
    /// generations because those affect cost only. Schema and runtime ordering
    /// capabilities affect legality and therefore require re-planning.
    pub(super) fn is_execution_compatible<R: hawdb_storage::graph_engine::GraphReadEngine>(
        &self,
        catalog: &Catalog,
        store: &R,
    ) -> bool {
        self.schema == OptimizerSchemaKey::from_catalog(catalog)
            && self.ordered_range_indexes == ordered_range_indexes(catalog, store)
            && self
                .ordered_composite_indexes
                .as_ref()
                .is_none_or(|proofs| proofs.as_ref() == &ordered_composite_indexes(catalog, store))
    }
}

impl OptimizerPlanningCache {
    pub(super) fn invalidate(&mut self) {
        self.parameter_bindings = None;
        self.statistics = None;
        self.statistics_schema = None;
        self.statistics_source_graph_commit_epoch = None;
        self.statistics_publication = None;
        // In-flight snapshots can still publish plans after invalidation. Keep
        // generations monotonic so a refreshed environment cannot reuse their key.
        self.catalog = None;
    }

    fn parameterized(
        shared: &SharedState<Self>,
        normalized_query: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        max_entries: Option<usize>,
        max_bytes: usize,
    ) -> Result<(ParameterizedLogicalPlan, bool)> {
        let config = ParameterBindingCacheConfig {
            max_entries,
            max_bytes,
        };
        let key = ParameterBindingCache::key(normalized_query, parameters);
        let cached = shared
            .borrow_mut()
            .parameter_binding_cache(config)
            .lookup(&key);
        // Template validation and planning stay outside the shared lock.
        if let Some(template) = cached
            && let Some(plan) = template.reuse(parameters)
        {
            return Ok((plan, true));
        }
        let plan = parameterize_logical_plan(statement, parameters)?;
        if let Some(candidate) = ParameterBindingCache::prepare_admission(key, &plan, parameters) {
            shared
                .borrow_mut()
                .parameter_binding_cache(config)
                .admit(candidate);
        }
        Ok((plan, false))
    }

    fn parameter_binding_cache(
        &mut self,
        config: ParameterBindingCacheConfig,
    ) -> &mut ParameterBindingCache {
        if self
            .parameter_bindings
            .as_ref()
            .is_none_or(|cache| cache.config() != config)
        {
            self.parameter_bindings = Some(ParameterBindingCache::new(config));
        }
        self.parameter_bindings
            .as_mut()
            .expect("binding cache initialized above")
    }

    pub(super) fn environment_hint<R: hawdb_storage::graph_engine::GraphReadEngine>(
        &mut self,
        catalog: &Catalog,
        store: &R,
        query_local_statistics: bool,
        tracks_composite_ordering: bool,
    ) -> OptimizerEnvironmentKey {
        // A cache lookup must retain the last published statistics generation:
        // ordinary data commits do not make a physical plan illegal. A cache
        // miss refreshes the snapshot before choosing a new plan instead.
        // Maintained index-size classes prevent a tiny-label scan from staying
        // cached across the existing scan/index cost boundary. Point plans use
        // snapshot-local candidate counts instead of advanced statistics, so a
        // diagnostic publication must not invalidate their warmed variants.
        if !query_local_statistics {
            self.ensure_statistics(catalog, store, false, None);
        }
        OptimizerEnvironmentKey {
            schema: OptimizerSchemaKey::from_catalog(catalog),
            statistics_generation: (!query_local_statistics).then_some(self.statistics_generation),
            ordered_range_indexes: ordered_range_indexes(catalog, store),
            ordered_composite_indexes: tracks_composite_ordering
                .then(|| Arc::new(ordered_composite_indexes(catalog, store))),
            node_index_size_classes: node_index_size_classes(catalog, store),
        }
    }

    fn ensure_statistics<R: hawdb_storage::graph_engine::GraphReadEngine>(
        &mut self,
        catalog: &Catalog,
        store: &R,
        refresh_for_data_change: bool,
        max_commit_lag: Option<u64>,
    ) -> StatisticsCacheRefresh {
        let schema = OptimizerSchemaKey::from_catalog(catalog);
        let source_graph_commit_epoch = store.commit_epoch();
        let statistics_epoch = self.statistics_source_graph_commit_epoch;
        let commit_lag = source_graph_commit_epoch.saturating_sub(statistics_epoch.unwrap_or(0));
        // A backwards move means this call plans against an older retained
        // snapshot than the cached statistics were computed from (the planning
        // cache is shared across snapshots). Refresh regardless of the lag so
        // the snapshot sees statistics computed from its own graph state —
        // saturating_sub alone would report zero lag and skip the refresh.
        let epoch_regressed =
            statistics_epoch.is_some_and(|epoch| source_graph_commit_epoch < epoch);
        let refresh_statistics = self.statistics.is_none()
            || self.statistics_schema.as_ref() != Some(&schema)
            || (refresh_for_data_change
                && (epoch_regressed || commit_lag > max_commit_lag.unwrap_or(0)));
        if !refresh_statistics {
            return StatisticsCacheRefresh::default();
        }

        let statistics = Arc::new(store.statistics(catalog));
        let publication = StatisticsPublicationKey::from(statistics.as_ref());
        let publication_changed = self.statistics_publication != Some(publication);
        self.statistics = Some(statistics);
        self.statistics_schema = Some(schema);
        self.statistics_source_graph_commit_epoch = Some(source_graph_commit_epoch);
        self.statistics_publication = Some(publication);
        if publication_changed {
            self.statistics_generation = self.statistics_generation.saturating_add(1);
        }
        self.catalog = None;
        StatisticsCacheRefresh {
            snapshot_refreshed: true,
            publication_changed,
        }
    }

    fn optimizer_catalog<R: hawdb_storage::graph_engine::GraphReadEngine>(
        &mut self,
        catalog: &Catalog,
        store: &R,
        statistics_commit_lag: Option<u64>,
        tracks_composite_ordering: bool,
    ) -> OptimizerCatalogAccess {
        let mut decisions = Vec::new();
        // This path is reached only after the physical-plan cache missed or
        // was intentionally bypassed, so cost-based planning must observe the
        // latest committed graph statistics, bounded by the configured lag.
        let refresh = self.ensure_statistics(catalog, store, true, statistics_commit_lag);
        if refresh.snapshot_refreshed {
            let statistics = self
                .statistics
                .as_ref()
                .expect("statistics exist after refresh")
                .clone();
            decisions.push(format!(
                "optimizer statistics cache refresh: statistics_epoch={} statistics_generation={} graph_commit_epoch={} publication_changed={}",
                statistics.computed_at_commit_epoch,
                self.statistics_generation,
                store.commit_epoch(),
                refresh.publication_changed
            ));
        } else if let Some(statistics) = &self.statistics {
            decisions.push(format!(
                "optimizer statistics cache hit: statistics_epoch={} statistics_generation={} graph_commit_epoch={}",
                statistics.computed_at_commit_epoch,
                self.statistics_generation,
                store.commit_epoch()
            ));
        }

        let statistics = self
            .statistics
            .as_ref()
            .expect("optimizer statistics exist after refresh check");
        let freshness = statistics.advanced_statistics_freshness(store.commit_epoch());
        decisions.push(format!(
            "optimizer advanced statistics freshness: status={} statistics_epoch={} graph_commit_epoch={} commit_lag={} usable={}",
            freshness.as_str(),
            statistics.computed_at_commit_epoch,
            store.commit_epoch(),
            statistics.advanced_statistics_commit_lag(store.commit_epoch()),
            statistics.advanced_statistics_complete
        ));
        let environment = OptimizerEnvironmentKey {
            schema: OptimizerSchemaKey::from_catalog(catalog),
            statistics_generation: Some(self.statistics_generation),
            ordered_range_indexes: ordered_range_indexes(catalog, store),
            ordered_composite_indexes: tracks_composite_ordering
                .then(|| Arc::new(ordered_composite_indexes(catalog, store))),
            node_index_size_classes: node_index_size_classes(catalog, store),
        };
        // The catalog depends on runtime capabilities as well as schema and
        // published statistics, all captured by `environment` (statistics_generation
        // bumps whenever the published snapshot changes). Comparing against
        // the live commit epoch would miss on every unrelated commit even
        // though the inputs are unchanged.
        if let Some(cached) = &self.catalog
            && cached.environment == environment
        {
            decisions.push(format!(
                "optimizer catalog cache hit: statistics_epoch={} statistics_generation={} graph_commit_epoch={}",
                statistics.computed_at_commit_epoch,
                self.statistics_generation,
                store.commit_epoch()
            ));
            return OptimizerCatalogAccess {
                environment,
                catalog: cached.catalog.clone(),
                decisions,
            };
        }

        let optimized = Arc::new(
            optimizer_catalog_from_graph_statistics(catalog, statistics)
                .with_ordered_range_indexes(environment.ordered_range_indexes.clone())
                .with_ordered_composite_indexes(environment.ordered_composite_proofs()),
        );
        decisions.push(format!(
            "optimizer catalog cache refresh: statistics_epoch={} statistics_generation={} graph_commit_epoch={}",
            statistics.computed_at_commit_epoch,
            self.statistics_generation,
            store.commit_epoch()
        ));
        self.catalog = Some(CachedOptimizerCatalog {
            environment: environment.clone(),
            catalog: optimized.clone(),
        });
        OptimizerCatalogAccess {
            environment,
            catalog: optimized,
            decisions,
        }
    }
}

pub(super) fn optimized_query_plan_for<S: crate::executor::ExecutionStore>(
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    cache_mode: PlanCacheMode,
    trace_mode: PlanTraceMode,
    context: PlanCacheContext<'_, S>,
) -> Result<OptimizedQueryPlan> {
    #[cfg(feature = "qualification")]
    let _preparation = super::query_preparation_metrics::PlanPreparationTiming::start();
    let query_identity = hawdb_query::QueryIdentity::new("cypher", cypher_text);
    let query_optimizer =
        CascadesOptimizer::with_context(context.optimizer.context().clone().with_query_identity(
            query_identity.normalized_query(),
            query_identity.query_digest(),
        ));
    if let Some(access_control) = context.access_control {
        context
            .config
            .runtime_capabilities
            .require(hawdb_core::RuntimeCapability::AccessControl)?;
        access_control.validate()?;
    }
    if let Some(capability) = required_runtime_capability(statement_body(statement)) {
        context.config.runtime_capabilities.require(capability)?;
    }
    let effective_max_optimizer_groups =
        optimizer_config_from_database_config(context.config).max_groups;
    let normalized_plan_cache_query =
        hawdb_query::normalize_query_for_plan_cache("cypher", cypher_text);
    let access_control_cache_key = context.access_control.map(AccessControlPlanCacheKey::from);
    let execution_parameters = parameters_with_access_control(parameters, context.access_control);
    let mut binding_template_hit = false;
    let parameterized = (cache_mode == PlanCacheMode::Use)
        .then(|| {
            OptimizerPlanningCache::parameterized(
                context.planning_cache,
                &normalized_plan_cache_query,
                statement_body(statement),
                parameters,
                context.config.max_plan_cache_entries,
                context.config.max_parameter_binding_cache_bytes,
            )
        })
        .transpose()?
        .map(|(plan, hit)| {
            binding_template_hit = hit;
            plan
        });
    // The new CASE marker trees are large. Keep physical copies request-owned
    // until the physical cache has its own byte budget; logical reuse remains
    // enabled under the separate bounded cache above.
    let physical_cache_mode = if parameterized
        .as_ref()
        .is_some_and(ParameterizedLogicalPlan::has_case_parameter_slots)
    {
        PlanCacheMode::Bypass(PlanCacheBypassReason::ParameterizedCaseTemplate)
    } else {
        cache_mode
    };
    let point_statistics = if trace_mode == PlanTraceMode::Template {
        parameterized
            .as_ref()
            .map(|plan| {
                point_statistics(&context, plan.logical(), parameters, plan.slot_count() > 0)
            })
            .transpose()?
            .flatten()
    } else {
        None
    };
    let environment_hint = (cache_mode == PlanCacheMode::Use).then(|| {
        context.planning_cache.borrow_mut().environment_hint(
            context.catalog,
            context.store,
            point_statistics.is_some(),
            parameterized
                .as_ref()
                .is_some_and(|plan| ordered_query_shape(plan.logical())),
        )
    });
    let mut key = parameterized
        .as_ref()
        .filter(|_| physical_cache_mode == PlanCacheMode::Use)
        .map(|parameterized| PlanCacheKey {
            normalized_query: normalized_plan_cache_query,
            parameters: parameterized.cache_key().clone(),
            environment: environment_hint
                .clone()
                .expect("optimizer environment exists for a parameterized plan"),
            max_optimizer_groups: context.config.max_optimizer_groups,
            access_control: access_control_cache_key,
            scoring: context.scoring.map(BoundScoringRequest::cache_key),
            point_statistics: point_statistics.as_ref().map(PointStatisticsKey::from),
        });
    if physical_cache_mode == PlanCacheMode::Use {
        let key = key.as_ref().expect("cache key exists in use mode");
        let cached = context.cache.borrow_mut().get(key);
        if let Some(cached) = cached {
            let mut physical_plan = bind_physical_plan_parameters(
                &cached.physical_template,
                execution_parameters.as_ref(),
                cached.has_parameter_slots,
            )?;
            if let Some(scoring) = context.scoring {
                scoring.rebind(&mut physical_plan)?;
            }
            let mut trace = cached.trace.clone();
            trace.query_digest = Some(query_identity.query_digest().to_string());
            refresh_plan_trace(
                &query_optimizer,
                &mut trace,
                &physical_plan,
                trace_mode,
                &context,
            );
            trace
                .decisions
                .push("plan cache hit: parameterized physical plan template".to_string());
            if binding_template_hit {
                trace
                    .decisions
                    .push("parameter binding cache hit: proven logical template".into());
            }
            record_access_control_plan_decision(&mut trace, context.access_control);
            return Ok(OptimizedQueryPlan {
                query_identity: Some(query_identity),
                output_columns: output_schema::logical_output_columns(
                    parameterized
                        .as_ref()
                        .expect("cached plans have a logical template")
                        .logical(),
                    context.scoring,
                ),
                physical_plan,
                trace,
                plan_cache_lookup: PlanCacheLookup::Hit,
                optimizer_environment: key.environment.clone(),
                configured_max_optimizer_groups: context.config.max_optimizer_groups,
                effective_max_optimizer_groups,
            });
        }
    }
    let mut logical = if let Some(parameterized) = &parameterized {
        parameterized.logical().clone()
    } else {
        planner::plan_with_params(statement_body(statement), parameters)?
    };
    let output_columns = output_schema::logical_output_columns(&logical, context.scoring);
    if let Some(access_control) = context.access_control {
        logical = apply_access_control_to_logical_plan(
            logical,
            access_control,
            cache_mode == PlanCacheMode::Use,
        );
    }
    let catalog_access = optimizer_catalog_access_for_logical(
        &context,
        &logical,
        point_statistics.as_ref(),
        environment_hint.as_ref(),
    );
    // Shape proofs are needed only on an ordered-query miss. Point queries and
    // all cache hits retain the shared catalog without cloning it.
    let has_parameter_slots = parameterized
        .as_ref()
        .is_some_and(|plan| plan.slot_count() > 0);
    let query_catalog = (has_parameter_slots
        && catalog_access
            .environment
            .ordered_composite_indexes
            .as_ref()
            .is_some_and(|proofs| !proofs.is_empty())
        && ordered_query_shape(&logical))
    .then(|| {
        catalog_access
            .catalog
            .as_ref()
            .clone()
            .with_parameter_scalar_types(parameter_scalar_types(
                parameterized
                    .as_ref()
                    .expect("parameter proofs require a parameterized plan"),
                parameters,
            ))
    });
    let planning_catalog = query_catalog.as_ref().unwrap_or(&catalog_access.catalog);
    let logical_root = LogicalPlanRoot::new(logical);
    let physical_root = query_optimizer
        .optimize_root_with_catalog_and_directive(
            &logical_root,
            planning_catalog,
            context.optimizer_search,
        )
        .map_err(|error| {
            HawDBError::Execution(format!(
                "optimizer search directive could not be honored: {error}"
            ))
        })?;
    let (physical_template, mut trace) = physical_root.into_parts();
    if binding_template_hit {
        trace
            .decisions
            .push("parameter binding cache hit: proven logical template".into());
    }
    let physical_template = match context.scoring {
        Some(scoring) => scoring.attach_template(physical_template)?,
        None => physical_template,
    };
    if context.scoring.is_some() {
        query_optimizer.refresh_trace_for_physical_plan(
            &mut trace,
            &physical_template,
            &catalog_access.catalog,
        );
    }
    trace.decisions.extend(catalog_access.decisions);
    let has_parameter_slots = parameterized
        .as_ref()
        .is_some_and(|parameterized| parameterized.slot_count() > 0)
        || (cache_mode == PlanCacheMode::Use && context.access_control.is_some());
    let mut physical_plan = bind_physical_plan_parameters(
        &physical_template,
        execution_parameters.as_ref(),
        has_parameter_slots,
    )?;
    if let Some(scoring) = context.scoring {
        scoring.rebind(&mut physical_plan)?;
    }
    if let Some(parameterized) = &parameterized {
        trace.decisions.push(format!(
            "parameterized plan template: slots={} exact_variants={}",
            parameterized.slot_count(),
            parameterized.exact_variant_count()
        ));
    }
    let cached_trace = (physical_cache_mode == PlanCacheMode::Use).then(|| {
        let mut cached_trace = trace.clone();
        refresh_materialized_plan_trace(&mut cached_trace, &physical_template);
        cached_trace
    });
    match trace_mode {
        PlanTraceMode::Template => refresh_materialized_plan_trace(&mut trace, &physical_plan),
        PlanTraceMode::Bound => query_optimizer.refresh_trace_for_physical_plan(
            &mut trace,
            &physical_plan,
            &catalog_access.catalog,
        ),
    }
    record_access_control_plan_decision(&mut trace, context.access_control);
    if physical_cache_mode == PlanCacheMode::Use {
        let mut key = key.take().expect("cache key exists in use mode");
        key.environment = catalog_access.environment.clone();
        context.cache.borrow_mut().insert(
            key,
            Arc::new(CachedPlan {
                physical_template,
                trace: cached_trace.expect("cached trace exists in physical cache use mode"),
                has_parameter_slots,
            }),
        );
        trace
            .decisions
            .push("plan cache miss: optimized parameterized physical plan template".to_string());
        return Ok(OptimizedQueryPlan {
            query_identity: Some(query_identity),
            output_columns,
            physical_plan,
            trace,
            plan_cache_lookup: PlanCacheLookup::Miss,
            optimizer_environment: catalog_access.environment,
            configured_max_optimizer_groups: context.config.max_optimizer_groups,
            effective_max_optimizer_groups,
        });
    } else if let PlanCacheMode::Bypass(reason) = physical_cache_mode {
        context.cache.borrow_mut().record_bypass();
        trace
            .decisions
            .push(format!("plan cache bypass: {}", reason.as_str()));
        return Ok(OptimizedQueryPlan {
            query_identity: Some(query_identity),
            output_columns,
            physical_plan,
            trace,
            plan_cache_lookup: PlanCacheLookup::Bypass(reason),
            optimizer_environment: catalog_access.environment,
            configured_max_optimizer_groups: context.config.max_optimizer_groups,
            effective_max_optimizer_groups,
        });
    }
    unreachable!("plan cache mode must be either use or bypass")
}

fn ordered_query_shape(logical: &LogicalPlan) -> bool {
    match logical {
        LogicalPlan::Sort { .. } => true,
        LogicalPlan::Limit { input, .. } | LogicalPlan::Project { input, .. } => {
            ordered_query_shape(input)
        }
        _ => false,
    }
}

fn refresh_materialized_plan_trace(trace: &mut OptimizerTrace, physical_plan: &PhysicalPlan) {
    trace.selected_plan = physical_plan.explain(0);
    trace.selected_plan_fingerprint = physical_plan.fingerprint();
}

fn optimizer_catalog_access_for_logical<S: crate::executor::ExecutionStore>(
    context: &PlanCacheContext<'_, S>,
    logical: &LogicalPlan,
    point_statistics: Option<&PointStatistics<'_>>,
    environment_hint: Option<&OptimizerEnvironmentKey>,
) -> OptimizerCatalogAccess {
    if let Some(point) = point_statistics {
        let environment = environment_hint
            .expect("point statistics require a cache environment")
            .clone();
        let statistics = hawdb_storage::statistics::graph_statistics_from_basic(
            context.store.basic_statistics(),
            false,
        );
        let catalog = Arc::new(
            optimizer_catalog_from_graph_statistics(context.catalog, &statistics)
                .with_ordered_range_indexes(environment.ordered_range_indexes.clone())
                .with_ordered_composite_indexes(environment.ordered_composite_proofs())
                .with_property_equality_row_count(
                    point.label.to_owned(),
                    point.property.to_owned(),
                    point.matched_rows,
                ),
        );
        return OptimizerCatalogAccess {
            environment,
            catalog,
            decisions: vec![format!("optimizer statistics: bounded indexed equality on {}.{} has {} snapshot-local candidates; advanced statistics not requested", point.label, point.property, point.matched_rows)],
        };
    }
    let shape = match logical {
        LogicalPlan::CreateNode { .. } => "CreateNode",
        LogicalPlan::UnwindMutation {
            operation: BatchMutationOperation::CreateNode { .. },
            ..
        } => "UnwindCreateNode",
        _ => return optimizer_catalog_access(context, ordered_query_shape(logical)),
    };
    // These bound leaves have no graph input or competing access path. Keep
    // schema and maintained counts current without scanning data or publishing
    // a statistics generation that a subsequent graph read could reuse.
    let statistics = hawdb_storage::statistics::graph_statistics_from_basic(
        context.store.basic_statistics(),
        false,
    );
    let environment = OptimizerEnvironmentKey {
        schema: OptimizerSchemaKey::from_catalog(context.catalog),
        statistics_generation: Some(context.planning_cache.borrow().statistics_generation),
        ordered_range_indexes: ordered_range_indexes(context.catalog, context.store),
        ordered_composite_indexes: None,
        // Mutation plans bypass the physical cache and have no graph input.
        // Do not add a second basic-count clone to their planning path.
        node_index_size_classes: Vec::new(),
    };
    let catalog = Arc::new(
        optimizer_catalog_from_graph_statistics(context.catalog, &statistics)
            .with_ordered_range_indexes(environment.ordered_range_indexes.clone())
            .with_ordered_composite_indexes(environment.ordered_composite_proofs()),
    );
    OptimizerCatalogAccess {
        environment,
        catalog,
        decisions: vec![format!(
            "optimizer statistics: graph-independent {shape} uses live schema and maintained basic counts; advanced statistics not requested"
        )],
    }
}

fn optimizer_catalog_access<S: crate::executor::ExecutionStore>(
    context: &PlanCacheContext<'_, S>,
    tracks_composite_ordering: bool,
) -> OptimizerCatalogAccess {
    context.planning_cache.borrow_mut().optimizer_catalog(
        context.catalog,
        context.store,
        context.config.optimizer_statistics_inline_commit_lag,
        tracks_composite_ordering,
    )
}

fn refresh_plan_trace<S: crate::executor::ExecutionStore>(
    optimizer: &CascadesOptimizer,
    trace: &mut OptimizerTrace,
    physical_plan: &PhysicalPlan,
    trace_mode: PlanTraceMode,
    context: &PlanCacheContext<'_, S>,
) {
    match trace_mode {
        PlanTraceMode::Template => refresh_materialized_plan_trace(trace, physical_plan),
        PlanTraceMode::Bound => {
            let catalog = optimizer_catalog_access(context, true).catalog;
            optimizer.refresh_trace_for_physical_plan(trace, physical_plan, &catalog);
            trace.decisions.push(
                "selected physical plan estimates refreshed for bound parameters".to_string(),
            );
        }
    }
}

fn required_runtime_capability(
    statement: &cypher::Statement,
) -> Option<hawdb_core::RuntimeCapability> {
    match statement {
        cypher::Statement::CreateFullTextIndex(_) => {
            Some(hawdb_core::RuntimeCapability::FullTextSearch)
        }
        cypher::Statement::VectorSearch(_) => Some(hawdb_core::RuntimeCapability::VectorSearch),
        cypher::Statement::ProjectGraph(_) | cypher::Statement::GraphAlgorithm(_) => {
            Some(hawdb_core::RuntimeCapability::GraphAnalytics)
        }
        cypher::Statement::Pipeline(query) => query.clauses.iter().find_map(|clause| {
            let cypher::ClauseKind::Call { procedure, .. } = &clause.kind else {
                return None;
            };
            Some(match &procedure.kind {
                cypher::ProcedureCallKind::GraphSeedSearch(_) => return None,
                cypher::ProcedureCallKind::VectorSearch(_)
                | cypher::ProcedureCallKind::VectorIndexSearch(_) => {
                    hawdb_core::RuntimeCapability::VectorSearch
                }
                cypher::ProcedureCallKind::TextSearch(_) => {
                    hawdb_core::RuntimeCapability::FullTextSearch
                }
                cypher::ProcedureCallKind::GraphAlgorithm { .. }
                | cypher::ProcedureCallKind::ProjectGraph { .. } => {
                    hawdb_core::RuntimeCapability::GraphAnalytics
                }
            })
        }),
        _ => None,
    }
}

fn record_access_control_plan_decision(
    trace: &mut OptimizerTrace,
    access_control: Option<&QueryAccessControlContext>,
) {
    if let Some(access_control) = access_control {
        trace.decisions.push(format!(
            "access control policy epoch {} bound to plan cache key",
            access_control.policy_epoch()
        ));
        trace
            .decisions
            .push("access control scope values bound for this execution".to_string());
    }
}

fn parameters_with_access_control<'a>(
    parameters: &'a BTreeMap<String, Value>,
    access_control: Option<&QueryAccessControlContext>,
) -> Cow<'a, BTreeMap<String, Value>> {
    let Some(access_control) = access_control else {
        return Cow::Borrowed(parameters);
    };
    let mut execution_parameters = parameters.clone();
    execution_parameters.insert(
        ACCESS_CONTROL_VALUES_PARAMETER.to_string(),
        Value::List(
            access_control
                .allowed_visibility_values()
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    Cow::Owned(execution_parameters)
}

fn apply_access_control_to_logical_plan(
    logical: LogicalPlan,
    access_control: &QueryAccessControlContext,
    parameterized_values: bool,
) -> LogicalPlan {
    hawdb_plan_cypher::apply_node_visibility_predicates(logical, &|variable| {
        access_control_node_predicate(variable, access_control, parameterized_values)
    })
}

fn access_control_node_predicate(
    variable: &str,
    access_control: &QueryAccessControlContext,
    parameterized_values: bool,
) -> Predicate {
    let values = access_control
        .allowed_visibility_values()
        .iter()
        .cloned()
        .map(Value::String)
        .collect::<Vec<_>>();
    Predicate::PropertyIn {
        variable: variable.to_string(),
        property: access_control.visibility_property().to_string(),
        values: if parameterized_values {
            parameterize_value_list(ACCESS_CONTROL_VALUES_PARAMETER, &values)
        } else {
            values
        },
    }
}

pub(super) fn statement_uses_plan_cache(statement: &cypher::Statement) -> bool {
    match statement_body(statement) {
        cypher::Statement::Pipeline(query) => {
            // Preserve legacy cache boundaries when queries move to clause ASTs.
            // Eligibility is not a substitute for semantic binding validation.
            !query.clauses.is_empty()
                && query.clauses.iter().all(|clause| match &clause.kind {
                    cypher::ClauseKind::Match {
                        patterns,
                        predicate,
                        ..
                    } => determinism::match_is_deterministic(patterns, predicate.as_ref()),
                    cypher::ClauseKind::With(projection)
                    | cypher::ClauseKind::Return(projection) => {
                        determinism::projection_is_deterministic(projection)
                    }
                    cypher::ClauseKind::Call { procedure, .. } => match &procedure.kind {
                        cypher::ProcedureCallKind::GraphAlgorithm { options, .. } => {
                            determinism::graph_options_are_deterministic(options)
                        }
                        cypher::ProcedureCallKind::VectorIndexSearch(_)
                        | cypher::ProcedureCallKind::VectorSearch(_)
                        | cypher::ProcedureCallKind::TextSearch(_)
                        | cypher::ProcedureCallKind::GraphSeedSearch(_)
                        | cypher::ProcedureCallKind::ProjectGraph { .. } => false,
                    },
                    cypher::ClauseKind::Unwind { .. }
                    | cypher::ClauseKind::Create(_)
                    | cypher::ClauseKind::Merge { .. }
                    | cypher::ClauseKind::Set(_)
                    | cypher::ClauseKind::Delete { .. } => false,
                })
        }

        cypher::Statement::GraphAlgorithm(query) => {
            determinism::graph_options_are_deterministic(&query.options)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn binding_parameters(length: usize, nullable: bool) -> BTreeMap<String, Value> {
        BTreeMap::from([(
            "ids".into(),
            Value::List(
                (0..length)
                    .map(|index| {
                        if nullable && index == 0 {
                            Value::Null
                        } else {
                            Value::String(index.to_string())
                        }
                    })
                    .collect(),
            ),
        )])
    }

    #[test]
    fn default_binding_byte_cap_bounds_five_case_mem_templates() {
        let budget = DatabaseConfig::default().max_parameter_binding_cache_bytes;
        assert_eq!(budget, 16 * 1024 * 1024);
        let mut query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id AS id".to_owned();
        let mut parameters = binding_parameters(512, false);
        for field in 0..5 {
            query.push_str(", CASE n.id");
            for index in 0..512 {
                query.push_str(&format!(" WHEN $id_{index} THEN $score_{field}_{index}"));
                parameters.insert(format!("id_{index}"), Value::String(index.to_string()));
                parameters.insert(format!("score_{field}_{index}"), Value::Float(0.5));
            }
            query.push_str(&format!(" ELSE null END AS score_{field}"));
        }
        query.push_str(" ORDER BY score_0");
        let statement = cypher::parse(&query).unwrap();
        let shared = SharedState::new(OptimizerPlanningCache::default());
        for length in 512..520 {
            parameters.insert(
                "ids".into(),
                binding_parameters(length, false).remove("ids").unwrap(),
            );
            let (plan, hit) = OptimizerPlanningCache::parameterized(
                &shared,
                &query,
                &statement,
                &parameters,
                Some(128),
                budget,
            )
            .unwrap();
            assert!(!hit);
            assert_eq!(
                plan,
                parameterize_logical_plan(&statement, &parameters).unwrap()
            );
            let (reused, hit) = OptimizerPlanningCache::parameterized(
                &shared,
                &query,
                &statement,
                &parameters,
                Some(128),
                budget,
            )
            .unwrap();
            assert!(
                length != 512 || hit,
                "the default must admit a representative 512-candidate template: length={length}, charge={:?}",
                ParameterBindingTemplate::new(&plan, &parameters).and_then(|template| template.retained_heap_bytes())
            );
            assert_eq!(plan, reused);
            let state = shared.borrow();
            let cache = state.parameter_bindings.as_ref().unwrap();
            assert!(cache.charged_bytes().unwrap() <= budget);
        }
        let state = shared.borrow();
        let cache = state.parameter_bindings.as_ref().unwrap();
        assert!(cache.stats().entries < 8);
        assert!(cache.stats().evictions > 0);
    }

    #[test]
    fn parameter_binding_cache_bounds_large_shape_bursts_by_bytes() {
        let shared = SharedState::new(OptimizerPlanningCache::default());
        let budget = 1024 * 1024;
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let shared = &shared;
                scope.spawn(move || {
                    let query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id";
                    let statement = cypher::parse(query).unwrap();
                    for iteration in 0..12 {
                        let parameters = binding_parameters(512 + worker * 12 + iteration, false);
                        let (plan, _) = OptimizerPlanningCache::parameterized(
                            shared,
                            query,
                            &statement,
                            &parameters,
                            Some(128),
                            budget,
                        )
                        .unwrap();
                        assert_eq!(
                            plan,
                            parameterize_logical_plan(&statement, &parameters).unwrap()
                        );
                        let state = shared.borrow();
                        let cache = state.parameter_bindings.as_ref().unwrap();
                        assert!(cache.charged_bytes().unwrap() <= budget);
                    }
                });
            }
        });
        let state = shared.borrow();
        let cache = state.parameter_bindings.as_ref().unwrap();
        assert!(cache.stats().admissions >= 48);
        assert!(cache.stats().evictions > 0);
        assert!(cache.stats().entries < 48);
        assert!(cache.charged_bytes().unwrap() <= budget);
    }

    #[test]
    fn parameter_binding_cache_zero_and_tiny_byte_caps_decline_admission() {
        let query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id";
        let statement = cypher::parse(query).unwrap();
        let parameters = binding_parameters(512, false);
        let shared = SharedState::new(OptimizerPlanningCache::default());
        for budget in [0, 1, 1024] {
            for _ in 0..2 {
                let (plan, hit) = OptimizerPlanningCache::parameterized(
                    &shared,
                    query,
                    &statement,
                    &parameters,
                    Some(128),
                    budget,
                )
                .unwrap();
                assert!(!hit);
                assert_eq!(
                    plan,
                    parameterize_logical_plan(&statement, &parameters).unwrap()
                );
                let state = shared.borrow();
                let cache = state.parameter_bindings.as_ref().unwrap();
                assert_eq!(cache.stats().entries, 0);
                assert_eq!(cache.charged_bytes(), Some(0));
            }
        }
    }

    #[test]
    fn parameter_binding_cache_retains_alternating_lengths_and_null_masks() {
        let query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id";
        let statement = cypher::parse(query).unwrap();
        let shared = SharedState::new(OptimizerPlanningCache::default());
        for iteration in 0..24 {
            for (length, nullable) in [(20, false), (17, false), (20, true)] {
                let parameters = binding_parameters(length, nullable);
                let (plan, hit) = OptimizerPlanningCache::parameterized(
                    &shared,
                    query,
                    &statement,
                    &parameters,
                    Some(3),
                    DatabaseConfig::default().max_parameter_binding_cache_bytes,
                )
                .unwrap();
                assert_eq!(hit, iteration != 0);
                assert_eq!(
                    plan,
                    parameterize_logical_plan(&statement, &parameters).unwrap()
                );
            }
        }
        let stats = shared.borrow().parameter_bindings.as_ref().unwrap().stats();
        assert_eq!(stats.entries, 3);
        assert_eq!(stats.hits, 69);
        assert_eq!(stats.misses, 3);
        assert_eq!(stats.evictions, 0);
    }

    #[test]
    fn parameter_binding_cache_evicts_shapes_under_concurrent_churn() {
        let shared = SharedState::new(OptimizerPlanningCache::default());
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let shared = &shared;
                scope.spawn(move || {
                    let query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id";
                    let statement = cypher::parse(query).unwrap();
                    for iteration in 0..48 {
                        let parameters = binding_parameters(
                            1 + (iteration + worker * 7) % 24,
                            iteration % 2 == 0,
                        );
                        let (plan, _) = OptimizerPlanningCache::parameterized(
                            shared,
                            query,
                            &statement,
                            &parameters,
                            Some(8),
                            DatabaseConfig::default().max_parameter_binding_cache_bytes,
                        )
                        .unwrap();
                        assert_eq!(
                            plan,
                            parameterize_logical_plan(&statement, &parameters).unwrap()
                        );
                        assert!(
                            shared
                                .borrow()
                                .parameter_bindings
                                .as_ref()
                                .unwrap()
                                .stats()
                                .entries
                                <= 8
                        );
                    }
                });
            }
        });
        let stats = shared.borrow().parameter_bindings.as_ref().unwrap().stats();
        assert_eq!(stats.entries, 8);
        assert!(stats.evictions > 0);
        // An evicted shape must be rebuilt correctly, even if LFU declines
        // to retain a new cold shape among frequently used incumbents.
        let query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id";
        let statement = cypher::parse(query).unwrap();
        let parameters = binding_parameters(512, false);
        let (plan, hit) = OptimizerPlanningCache::parameterized(
            &shared,
            query,
            &statement,
            &parameters,
            Some(8),
            DatabaseConfig::default().max_parameter_binding_cache_bytes,
        )
        .unwrap();
        assert!(!hit);
        assert_eq!(
            plan,
            parameterize_logical_plan(&statement, &parameters).unwrap()
        );
        let (repeated, _) = OptimizerPlanningCache::parameterized(
            &shared,
            query,
            &statement,
            &parameters,
            Some(8),
            DatabaseConfig::default().max_parameter_binding_cache_bytes,
        )
        .unwrap();
        assert_eq!(plan, repeated);
        assert_eq!(
            shared
                .borrow()
                .parameter_bindings
                .as_ref()
                .unwrap()
                .stats()
                .entries,
            8
        );
    }

    #[test]
    fn plan_cache_rejects_volatile_values_in_read_expressions() {
        for query in [
            "MATCH (n:Item) RETURN [CURRENT_TIMESTAMP()] AS values",
            "MATCH (n:Item) RETURN CASE WHEN true THEN timestamp(CURRENT_TIMESTAMP()) ELSE 0 END AS now",
            "MATCH (n:Item) RETURN coalesce(NULL, CASE WHEN true THEN CURRENT_TIMESTAMP() ELSE 0 END) AS now",
            "MATCH (n:Item {id: CURRENT_TIMESTAMP()}) RETURN n.id",
            "MATCH (n:Item) WHERE n.id = CURRENT_TIMESTAMP() RETURN n.id",
            "MATCH (n:Item) WHERE n.id IN [CURRENT_TIMESTAMP()] RETURN n.id",
            "MATCH (n:Item)-[:LINK {at: CURRENT_TIMESTAMP()}]->(m:Item) RETURN n.id",
            "MATCH (n:Item) RETURN n.id AS id ORDER BY CASE WHEN true THEN CURRENT_TIMESTAMP() ELSE 0 END",
            "MATCH (n:Item) WITH CASE WHEN true THEN CURRENT_TIMESTAMP() ELSE 0 END AS now RETURN now",
            "MATCH (n:Item) RETURN n.id LIMIT CURRENT_TIMESTAMP()",
            "CALL page_rank('g', maxIterations := CURRENT_TIMESTAMP()) RETURN node, rank",
        ] {
            let statement = cypher::parse(query).unwrap_or_else(|error| panic!("{query}: {error}"));
            assert!(!statement_uses_plan_cache(&statement), "{query}");
        }
        for query in [
            "MATCH (n:Item) RETURN CASE WHEN true THEN timestamp($now) ELSE 0 END AS now",
            "MATCH (n:Item) RETURN 'CURRENT_TIMESTAMP()' AS value",
            "MATCH (n:Item) WHERE n.id = timestamp($now) RETURN n.id",
        ] {
            let statement = cypher::parse(query).unwrap_or_else(|error| panic!("{query}: {error}"));
            assert!(statement_uses_plan_cache(&statement), "{query}");
        }
    }

    #[test]
    fn point_statistics_borrow_plan_names_and_keep_owned_cache_identity() {
        let mut db = crate::Database::new();
        for statement in [
            "CREATE INDEX ON :Item(id)",
            "CREATE INDEX ON :Item(other_id)",
            "CREATE INDEX ON :Other(id)",
            "CREATE (:Item {id: 1, other_id: 1})",
            "CREATE (:Other {id: 1})",
        ] {
            db.query(statement).unwrap();
        }
        let runtime = db.runtime.get().unwrap();
        let context = PlanCacheContext {
            catalog: &runtime.catalog,
            store: &runtime.store,
            optimizer: &runtime.optimizer,
            config: &db.config,
            cache: &runtime.plan_cache,
            planning_cache: &runtime.optimizer_planning_cache,
            access_control: None,
            optimizer_search: OptimizerSearchDirective::Auto,
            scoring: None,
        };
        let parameters = BTreeMap::from([("id".into(), Value::Int(1))]);
        let keys = [
            "MATCH (n:Item) WHERE n.id = $id RETURN n.id",
            "MATCH (n:Item) WHERE n.other_id = $id RETURN n.id",
            "MATCH (n:Other) WHERE n.id = $id RETURN n.id",
        ]
        .map(|query| {
            let statement = cypher::parse(query).unwrap();
            let plan = parameterize_logical_plan(statement_body(&statement), &parameters).unwrap();
            let point =
                point_statistics(&context, plan.logical(), &parameters, plan.slot_count() > 0)
                    .unwrap()
                    .unwrap();
            let LogicalPlan::Project { input, .. } = plan.logical() else {
                panic!("expected projection");
            };
            let LogicalPlan::Filter {
                predicate: Predicate::PropertyEq { property, .. },
                input,
            } = input.as_ref()
            else {
                panic!("expected property equality");
            };
            let LogicalPlan::NodeScan { label, .. } = input.as_ref() else {
                panic!("expected node scan");
            };
            assert_eq!(point.label.as_ptr(), label.as_ptr());
            assert_eq!(point.property.as_ptr(), property.as_ptr());
            PointStatisticsKey::from(&point)
        });
        // Each plan has been dropped, while its independently owned identity
        // still distinguishes names with identical cardinality and cost class.
        assert_eq!(keys[0].label, "Item");
        assert_eq!(keys[0].property, "id");
        assert_eq!(keys[1].property, "other_id");
        assert_eq!(keys[2].label, "Other");
        for key in &keys {
            assert_eq!(key.matched_rows, 1);
            assert_eq!(key.index_seek_is_cheaper, keys[0].index_seek_is_cheaper);
        }
        assert_ne!(keys[0], keys[1]);
        assert_ne!(keys[0], keys[2]);
    }

    #[test]
    fn point_statistics_avoid_full_publication_for_a_cold_indexed_read() {
        let mut db = crate::Database::new();
        for statement in [
            "CREATE NODE TABLE Item",
            "CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT",
            "CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT",
            "CREATE INDEX ON :Item(id)",
        ] {
            db.query(statement).unwrap();
        }
        for id in 0..32 {
            db.query_with_params(
                "CREATE (:Item {id: $id, score: $score})",
                &BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("score".into(), Value::Int(id * 3)),
                ]),
            )
            .unwrap();
        }
        // DDL may have left an earlier statistics publication. Model a cold
        // planning cache independently of those schema statements.
        db.runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow_mut()
            .invalidate();
        let generation = db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics_generation;
        let query = "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id, n.score AS score";
        let statement = cypher::parse(query).unwrap();
        let planned = db
            .optimized_query_plan_with_access_control_and_trace_mode(
                query,
                &statement,
                &BTreeMap::from([("id".into(), Value::Int(5))]),
                None,
                PlanTraceMode::Template,
            )
            .unwrap();
        assert!(
            planned
                .trace
                .decisions
                .iter()
                .any(|decision| decision.contains("bounded indexed equality")),
            "{:?}",
            planned.trace.decisions
        );
        let output = db
            .query_with_params(query, &BTreeMap::from([("id".into(), Value::Int(5))]))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("id"), Some(&Value::Int(5)));
        assert_eq!(output.rows[0].get("score"), Some(&Value::Int(15)));
        let runtime = db.runtime.get().unwrap();
        let planning = runtime.optimizer_planning_cache.borrow();
        assert!(planning.statistics.is_none());
        assert_eq!(planning.statistics_generation, generation);
    }

    #[test]
    fn point_statistics_rebind_cardinality_without_publishing_and_preserve_snapshots() {
        let mut db = crate::Database::new();
        db.query("CREATE (:Item {id: 0, score: 0})").unwrap();
        db.query("CREATE INDEX ON :Item(id)").unwrap();
        for id in 1..32 {
            db.query(&format!("CREATE (:Item {{id: {id}, score: {id}}})"))
                .unwrap();
        }
        let before = db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics
            .clone();
        let generation = db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics_generation;
        let query = "MATCH (n:Item) WHERE n.id = $id RETURN n.score AS score";
        let parameters = |id| BTreeMap::from([("id".into(), Value::Int(id))]);
        assert_eq!(
            db.query_with_params(query, &parameters(0)).unwrap().rows[0]["score"],
            Value::Int(0)
        );
        let cache = db.plan_cache_stats().unwrap();
        assert_eq!(
            db.query_with_params(query, &parameters(1)).unwrap().rows[0]["score"],
            Value::Int(1)
        );
        assert_eq!(db.plan_cache_stats().unwrap().hits, cache.hits + 1);
        assert!(db
            .query_with_params(query, &parameters(99))
            .unwrap()
            .rows
            .is_empty());
        let mut snapshot = db.begin_read_transaction().unwrap();
        db.query("CREATE (:Item {id: 0, score: 100})").unwrap();
        let cache = db.plan_cache_stats().unwrap();
        assert_eq!(
            db.query_with_params(query, &parameters(0))
                .unwrap()
                .rows
                .len(),
            2
        );
        assert_eq!(db.plan_cache_stats().unwrap().misses, cache.misses + 1);
        assert_eq!(
            snapshot
                .query_with_params(query, &parameters(0))
                .unwrap()
                .rows
                .len(),
            1
        );
        db.query("CREATE (:Item {id: 99, score: 99})").unwrap();
        let cache = db.plan_cache_stats().unwrap();
        assert_eq!(
            db.query_with_params(query, &parameters(99))
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(db.plan_cache_stats().unwrap().hits, cache.hits + 1);
        let runtime = db.runtime.get().unwrap();
        let planning = runtime.optimizer_planning_cache.borrow();
        match (&before, &planning.statistics) {
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(before, after)),
            (None, None) => {}
            _ => panic!("point reads changed the full statistics publication"),
        }
        assert_eq!(planning.statistics_generation, generation);
    }

    #[test]
    fn point_statistics_replan_at_the_duplicate_equality_cost_boundary() {
        let mut db = crate::Database::new();
        db.query("CREATE (:Item {id: 1})").unwrap();
        db.query("CREATE INDEX ON :Item(id)").unwrap();
        for _ in 0..8 {
            db.query("CREATE (:Item {id: 0})").unwrap();
        }
        let query = "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id";
        let statement = cypher::parse(query).unwrap();
        let parameters = BTreeMap::from([("id".into(), Value::Int(0))]);
        let plan = |db: &crate::Database| {
            db.optimized_query_plan_with_access_control_and_trace_mode(
                query,
                &statement,
                &parameters,
                None,
                PlanTraceMode::Template,
            )
            .unwrap()
        };
        let initial = plan(&db);
        assert!(initial
            .physical_plan
            .explain(0)
            .contains("access=SeqNodeScan"));
        for id in 2..32 {
            db.query(&format!("CREATE (:Item {{id: {id}}})")).unwrap();
        }
        let grown = plan(&db);
        assert_eq!(grown.plan_cache_lookup, PlanCacheLookup::Miss);
        assert!(grown
            .physical_plan
            .explain(0)
            .contains("access=IndexNodeSeek"));
        assert_eq!(
            db.query_with_params(query, &parameters).unwrap().rows.len(),
            8
        );
    }

    #[test]
    fn point_statistics_dense_complex_and_bound_reads_keep_full_statistics() {
        for (query, parameters) in [
            (
                "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id",
                BTreeMap::from([("id".into(), Value::Int(0))]),
            ),
            (
                "MATCH (n:Item) WHERE n.id = $id RETURN count(n) AS count",
                BTreeMap::from([("id".into(), Value::Int(1))]),
            ),
            (
                "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id",
                BTreeMap::from([("id".into(), Value::Null)]),
            ),
        ] {
            let mut db = crate::Database::new();
            db.query("CREATE (:Item {id: 1})").unwrap();
            db.query("CREATE INDEX ON :Item(id)").unwrap();
            for _ in 0..9 {
                db.query("CREATE (:Item {id: 0})").unwrap();
            }
            db.runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow_mut()
                .invalidate();
            let output = db.query_with_params(query, &parameters).unwrap();
            if parameters.get("id") == Some(&Value::Int(0)) {
                assert_eq!(output.rows.len(), 9);
            }
            assert!(db
                .runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow()
                .statistics
                .is_some());
        }
        let mut db = crate::Database::new();
        db.query("CREATE (:Item {id: 1})").unwrap();
        db.query("CREATE INDEX ON :Item(id)").unwrap();
        db.runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow_mut()
            .invalidate();
        let query = "MATCH (n:Item) WHERE n.id = $id RETURN n AS node";
        assert!(db.query_with_params(query, &BTreeMap::new()).is_err());
        assert!(db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics
            .is_none());
        let parameters = BTreeMap::from([("id".into(), Value::Int(1))]);
        assert_eq!(
            db.query_with_params(query, &parameters).unwrap().rows.len(),
            1
        );
        assert!(db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics
            .is_none());
        db.explain_query_with_params(query, &parameters).unwrap();
        assert!(db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics
            .is_some());
    }

    #[test]
    fn diagnostic_statistics_preserve_query_local_point_cache() {
        for analyze in [false, true] {
            let mut db = crate::Database::new();
            db.query("CREATE INDEX ON :Item(id)").unwrap();
            let parameters = |id| {
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("score".into(), Value::Int(id * 3)),
                ])
            };
            for id in 0..100 {
                db.query_with_params("CREATE (:Item {id: $id, score: $score})", &parameters(id))
                    .unwrap();
            }
            db.runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow_mut()
                .invalidate();
            let query = "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id, n.score AS score";
            let reads = |db: &mut crate::Database, unused_score_bias: i64| {
                let before = db.plan_cache_stats().unwrap();
                for id in 0..100 {
                    // Vary the unused binding independently of canonical row values.
                    let mut bindings = parameters(id);
                    bindings.insert("score".into(), Value::Int(id * 3 + unused_score_bias));
                    let output = db.query_with_params(query, &bindings).unwrap();
                    assert_eq!(output.rows.len(), 1);
                    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(id)));
                    assert_eq!(output.rows[0].get("score"), Some(&Value::Int(id * 3)));
                }
                let after = db.plan_cache_stats().unwrap();
                (after.hits - before.hits, after.misses - before.misses)
            };
            assert_eq!(reads(&mut db, 0), (99, 1));
            assert_eq!(reads(&mut db, 0), (100, 0));
            let generation = db
                .runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow()
                .statistics_generation;
            if analyze {
                let output = db
                    .explain_analyze_query_with_params(query, &parameters(0))
                    .unwrap();
                assert_eq!(output.output.rows.len(), 1);
            } else {
                db.explain_query_with_params(query, &parameters(0)).unwrap();
            }
            assert!(
                db.runtime
                    .get()
                    .unwrap()
                    .optimizer_planning_cache
                    .borrow()
                    .statistics_generation
                    > generation,
                "the diagnostic must publish a new advanced-statistics snapshot"
            );
            assert_eq!(reads(&mut db, 1_000), (100, 0));
            assert_eq!(reads(&mut db, 2_000), (100, 0));
        }
    }

    #[test]
    fn graph_dependent_cache_still_tracks_statistics_publication() {
        let mut db = crate::Database::new();
        for id in 0..10 {
            db.query(&format!("CREATE (:Item {{id: {id}, score: {id}}})"))
                .unwrap();
        }
        let query = "MATCH (n:Item) WHERE n.score > $minimum RETURN n.id AS id";
        let parameters = BTreeMap::from([("minimum".into(), Value::Int(0))]);
        assert_eq!(
            db.query_with_params(query, &parameters).unwrap().rows.len(),
            9
        );
        let warm = db.plan_cache_stats().unwrap();
        db.query("CREATE (:Item {id: 10, score: 10})").unwrap();
        assert_eq!(
            db.query_with_params(query, &parameters).unwrap().rows.len(),
            10
        );
        assert_eq!(db.plan_cache_stats().unwrap().hits, warm.hits + 1);
        let generation = db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics_generation;
        db.explain_query_with_params(query, &parameters).unwrap();
        assert!(
            db.runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow()
                .statistics_generation
                > generation
        );
        let before = db.plan_cache_stats().unwrap();
        assert_eq!(
            db.query_with_params(query, &parameters).unwrap().rows.len(),
            10
        );
        let replanned = db.plan_cache_stats().unwrap();
        assert_eq!(replanned.misses, before.misses + 1);
        db.query_with_params(query, &parameters).unwrap();
        assert_eq!(db.plan_cache_stats().unwrap().hits, replanned.hits + 1);
    }

    #[test]
    fn point_plan_cache_tracks_the_small_label_index_boundary() {
        let mut db = crate::Database::new();
        for statement in [
            "CREATE NODE TABLE Item",
            "CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT",
            "CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT",
            "CREATE INDEX ON :Item(id)",
        ] {
            db.query(statement).unwrap();
        }
        let insert = |db: &mut crate::Database, id: i64| {
            db.query_with_params(
                "CREATE (:Item {id: $id, score: $score})",
                &BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("score".into(), Value::Int(id * 3)),
                ]),
            )
            .unwrap();
        };
        for id in 0..4 {
            insert(&mut db, id);
        }
        let query = "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id, n.score AS score";
        let statement = cypher::parse(query).unwrap();
        let parameters = BTreeMap::from([("id".into(), Value::Int(0))]);
        let plan = |db: &crate::Database| {
            db.optimized_query_plan_with_access_control_and_trace_mode(
                query,
                &statement,
                &parameters,
                None,
                PlanTraceMode::Template,
            )
            .unwrap()
        };
        let initial = plan(&db);
        assert_eq!(initial.plan_cache_lookup, PlanCacheLookup::Miss);
        assert!(initial
            .physical_plan
            .explain(0)
            .contains("access=SeqNodeScan"));
        let generation = db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics_generation;
        let (snapshot_catalog, snapshot_store) = {
            let runtime = db.runtime.get().unwrap();
            (runtime.catalog.clone(), runtime.store.snapshot())
        };
        for id in 4..8 {
            insert(&mut db, id);
        }
        assert_eq!(plan(&db).plan_cache_lookup, PlanCacheLookup::Hit);
        insert(&mut db, 8);
        assert_eq!(
            db.runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow()
                .statistics_generation,
            generation,
            "CREATE must not publish statistics even at the access-cost boundary"
        );
        let grown = plan(&db);
        assert_eq!(grown.plan_cache_lookup, PlanCacheLookup::Miss);
        assert!(grown
            .physical_plan
            .explain(0)
            .contains("access=IndexNodeSeek"));
        assert_eq!(
            node_index_size_classes(&snapshot_catalog, &snapshot_store),
            vec![(snapshot_catalog.label_id("Item").unwrap(), false)],
            "the old read view must not use live-head counts"
        );
        let grown_generation = db
            .runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow()
            .statistics_generation;
        insert(&mut db, 9);
        assert_eq!(plan(&db).plan_cache_lookup, PlanCacheLookup::Hit);
        assert_eq!(
            db.runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow()
                .statistics_generation,
            grown_generation
        );
        let output = db.query_with_params(query, &parameters).unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("id"), Some(&Value::Int(0)));
        assert_eq!(output.rows[0].get("score"), Some(&Value::Int(0)));
        db.query("MATCH (n:Item) WHERE n.id >= 8 DELETE n").unwrap();
        let shrunk = plan(&db);
        // Returning to the small-label class reuses its original scan plan.
        assert_eq!(shrunk.plan_cache_lookup, PlanCacheLookup::Hit);
        assert_eq!(shrunk.optimizer_environment, initial.optimizer_environment);
        assert_eq!(
            shrunk.physical_plan.fingerprint(),
            initial.physical_plan.fingerprint()
        );
        assert!(shrunk
            .physical_plan
            .explain(0)
            .contains("access=SeqNodeScan"));
        let shrunk_output = db.query_with_params(query, &parameters).unwrap();
        assert_eq!(shrunk_output.rows, output.rows);
    }

    #[test]
    fn point_plan_cache_keeps_unindexed_label_growth_reusable() {
        let mut db = crate::Database::new();
        db.query("CREATE NODE TABLE Item").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT")
            .unwrap();
        for id in 0..4 {
            db.query(&format!("CREATE (:Item {{id: {id}}})")).unwrap();
        }
        let query = "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id";
        let parameters = BTreeMap::from([("id".into(), Value::Int(0))]);
        db.query_with_params(query, &parameters).unwrap();
        for id in 4..10 {
            db.query(&format!("CREATE (:Item {{id: {id}}})")).unwrap();
        }
        let before = db.plan_cache_stats().unwrap();
        let output = db.query_with_params(query, &parameters).unwrap();
        let after = db.plan_cache_stats().unwrap();
        assert_eq!(after.hits - before.hits, 1);
        assert_eq!(after.misses, before.misses);
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("id"), Some(&Value::Int(0)));
    }

    #[test]
    fn creation_planning_does_not_publish_advanced_statistics() {
        let mut db = crate::Database::new();
        db.query_with_params(
            "CREATE (:Item {score: $score})",
            &BTreeMap::from([("score".into(), Value::Int(1))]),
        )
        .unwrap();
        db.query_with_params(
            "UNWIND $rows AS row CREATE (:Item {score: row.score})",
            &BTreeMap::from([(
                "rows".into(),
                Value::List(vec![
                    Value::Map(BTreeMap::from([("score".into(), Value::Int(2))])),
                    Value::Map(BTreeMap::from([("score".into(), Value::Int(3))])),
                ]),
            )]),
        )
        .unwrap();
        db.query_with_params(
            "UNWIND $rows AS row CREATE (:Item {score: row.score})",
            &BTreeMap::from([("rows".into(), Value::List(Vec::new()))]),
        )
        .unwrap();
        {
            let runtime = db.runtime.get().unwrap();
            let cache = runtime.optimizer_planning_cache.borrow();
            assert!(cache.statistics.is_none());
            assert!(cache.statistics_source_graph_commit_epoch.is_none());
            assert_eq!(cache.statistics_generation, 0);
        }
        let output = db.query("MATCH (n:Item) RETURN n.score AS score").unwrap();
        assert_eq!(output.rows.len(), 3);
        let (published_epoch, generation) = {
            let runtime = db.runtime.get().unwrap();
            let cache = runtime.optimizer_planning_cache.borrow();
            assert!(cache.statistics.is_some());
            assert_eq!(
                cache.statistics_source_graph_commit_epoch,
                Some(runtime.store.commit_epoch())
            );
            (
                cache.statistics_source_graph_commit_epoch,
                cache.statistics_generation,
            )
        };
        db.query("CREATE (:Item {score: 4})").unwrap();
        {
            let runtime = db.runtime.get().unwrap();
            let cache = runtime.optimizer_planning_cache.borrow();
            assert_eq!(cache.statistics_source_graph_commit_epoch, published_epoch);
            assert_eq!(cache.statistics_generation, generation);
        }
        let before_merge = db.commit_epoch().unwrap();
        db.query("MERGE (:Item {score: 4})").unwrap();
        assert_eq!(
            db.runtime
                .get()
                .unwrap()
                .optimizer_planning_cache
                .borrow()
                .statistics_source_graph_commit_epoch,
            Some(before_merge)
        );
    }

    #[test]
    fn creation_planning_preserves_live_schema_and_failed_write_atomicity() {
        let mut db = crate::Database::new();
        db.query("CREATE NODE TABLE Item").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
            .unwrap();
        let mut old = db.begin_read_transaction().unwrap();
        let before = db.commit_epoch().unwrap();
        assert!(db
            .query_with_params("CREATE (:Item {score: $score})", &BTreeMap::new())
            .is_err());
        assert!(db
            .query_with_params(
                "CREATE (:Item {score: $score})",
                &BTreeMap::from([("score".into(), Value::String("invalid".into()))]),
            )
            .is_err());
        assert_eq!(db.commit_epoch().unwrap(), before);
        db.query_with_params(
            "CREATE (:Item {score: $score})",
            &BTreeMap::from([("score".into(), Value::Int(9))]),
        )
        .unwrap();
        assert_eq!(
            old.query("MATCH (n:Item) RETURN n.score AS score")
                .unwrap()
                .rows
                .len(),
            0
        );
        assert_eq!(
            db.query("MATCH (n:Item) RETURN n.score AS score")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn creation_planning_preserves_constraints_cancellation_and_wal_replay() {
        let path = unique_test_dir("creation_planning_wal");
        let parameters = |scores: &[i64]| {
            BTreeMap::from([(
                "rows".into(),
                Value::List(
                    scores
                        .iter()
                        .map(|score| {
                            Value::Map(BTreeMap::from([("score".into(), Value::Int(*score))]))
                        })
                        .collect(),
                ),
            )])
        };
        let bulk = "UNWIND $rows AS row CREATE (:Item {score: row.score})";
        {
            let mut db = crate::Database::open(&path).unwrap();
            db.query("CREATE (:Item {score: 1})").unwrap();
            db.query("CREATE CONSTRAINT ON :Item(score) ASSERT UNIQUE")
                .unwrap();
            let before = db.commit_epoch().unwrap();
            let error = db
                .query_with_params(bulk, &parameters(&[2, 1]))
                .unwrap_err();
            assert!(
                error.to_string().contains("unique constraint violation"),
                "{error}"
            );
            assert_eq!(db.commit_epoch().unwrap(), before);
            let context = hawdb_core::RuntimeTaskContext::default();
            context.cancellation().cancel();
            let error = db
                .query_with_params_context(bulk, &parameters(&[4]), &context)
                .unwrap_err();
            assert!(error.to_string().contains("cancelled"), "{error}");
            assert_eq!(db.commit_epoch().unwrap(), before);
            db.query_with_params(bulk, &parameters(&[2, 3])).unwrap();
        }
        {
            let mut db = crate::Database::open(&path).unwrap();
            let output = db
                .query("MATCH (n:Item) RETURN n.score AS score ORDER BY score")
                .unwrap();
            assert_eq!(
                output.rows,
                vec![
                    BTreeMap::from([("score".into(), Value::Int(1))]),
                    BTreeMap::from([("score".into(), Value::Int(2))]),
                    BTreeMap::from([("score".into(), Value::Int(3))]),
                ]
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn creation_planning_reports_shape_and_preserves_optimizer_directives() {
        let db = crate::Database::new();
        let query = "CREATE (:Item {score: $score})";
        let parameters = BTreeMap::from([("score".into(), Value::Int(7))]);
        let automatic = db.explain_query_with_params(query, &parameters).unwrap();
        assert!(automatic
            .trace
            .decisions
            .iter()
            .any(|decision| decision.contains("graph-independent CreateNode")));
        for mode in ["memo", "direct_fallback"] {
            let hinted = format!("CYPHER system.optimizer_search = '{mode}' {query}");
            let explained = db.explain_query_with_params(&hinted, &parameters).unwrap();
            assert_eq!(explained.trace.search_mode.as_str(), mode);
            assert_eq!(
                explained.physical_plan.fingerprint(),
                automatic.physical_plan.fingerprint()
            );
            assert_eq!(
                explained.plan_cache_lookup,
                PlanCacheLookup::Bypass(PlanCacheBypassReason::OptimizerDirective)
            );
        }
        let constrained = crate::Database::new_with_config(crate::DatabaseConfig {
            max_optimizer_groups: Some(0),
            ..crate::DatabaseConfig::default()
        });
        let error = constrained
            .explain_query_with_params(
                "CYPHER system.optimizer_search = 'memo' CREATE (:Item {score: $score})",
                &parameters,
            )
            .unwrap_err();
        assert!(error.to_string().contains("max_groups is 0"), "{error}");
    }

    #[test]
    fn pipeline_plan_cache_preserves_legacy_eligibility() {
        for (query, expected) in [
            ("MATCH (n:Item) RETURN n.id", true),
            ("CALL page_rank('g') RETURN node, rank", true),
            ("CALL louvain('g') RETURN node, level, louvain_id", true),
            ("CREATE (:Item {id: 1})", false),
            ("MERGE (:Item {id: 1})", false),
            ("MATCH (n:Item) SET n.id = 2", false),
            ("MATCH (n:Item) DELETE n", false),
            ("MATCH (n:Item) DETACH DELETE n", false),
            ("UNWIND $rows AS row CREATE (:Item {id: row.id})", false),
            (
                "CALL vector_search($embedding, topK := 4) RETURN id, score",
                false,
            ),
            ("CALL project_graph('g', ['Item'], ['LINK'])", false),
        ] {
            let legacy = cypher::parse(query).unwrap();
            let pipeline =
                cypher::Statement::Pipeline(Box::new(cypher::parse_pipeline(query).unwrap()));
            assert_eq!(
                statement_uses_plan_cache(&legacy),
                expected,
                "legacy: {query}"
            );
            assert_eq!(
                statement_uses_plan_cache(&pipeline),
                expected,
                "pipeline: {query}"
            );
            let wrapped = cypher::Statement::CypherQuery(Box::new(cypher::CypherQuery {
                system_variables: Vec::new(),
                statement: pipeline,
            }));
            assert_eq!(
                statement_uses_plan_cache(&wrapped),
                expected,
                "wrapped: {query}"
            );
        }
    }

    #[test]
    fn pipeline_plan_cache_checks_every_clause() {
        let read = cypher::parse_pipeline(
            "MATCH (n:Item) OPTIONAL MATCH (n)-[:LINK]->(m:Item) WITH n WITH n RETURN n.id",
        )
        .unwrap();
        assert!(statement_uses_plan_cache(&cypher::Statement::Pipeline(
            Box::new(read.clone())
        )));
        for query in [
            "UNWIND [1] AS id RETURN id",
            "CREATE (:Item)",
            "MERGE (:Item {id: 1})",
            "SET n.id = 2",
            "DELETE n",
            "DETACH DELETE n",
            "CALL vector_search($embedding, topK := 4) RETURN id, score",
            "CALL project_graph('g', ['Item'], ['LINK'])",
        ] {
            let clause = cypher::parse_pipeline(query).unwrap().clauses[0].clone();
            // Cache eligibility is independent of binding validity and clause position.
            for position in 0..=read.clauses.len() {
                let mut pipeline = read.clone();
                pipeline.clauses.insert(position, clause.clone());
                assert!(
                    !statement_uses_plan_cache(&cypher::Statement::Pipeline(Box::new(pipeline))),
                    "{query} at {position}"
                );
            }
        }
        let mut empty = read;
        empty.clauses.clear();
        assert!(!statement_uses_plan_cache(&cypher::Statement::Pipeline(
            Box::new(empty)
        )));
    }

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb_{name}_{nanos}"))
    }

    fn node_props(i: i64) -> BTreeMap<String, Value> {
        BTreeMap::from([("k".to_string(), Value::Int(i))])
    }

    #[test]
    fn statistics_commit_lag_defers_refresh_until_threshold() {
        let path = unique_test_dir("stats_commit_lag");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let mut cache = OptimizerPlanningCache::default();

        // Seed the label first: a new label mutates the schema, and schema
        // changes legitimately force a refresh regardless of the lag window.
        store.create_node(&mut catalog, "T", node_props(0)).unwrap();

        // First call always computes: there is no snapshot yet.
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(8));
        assert!(refresh.snapshot_refreshed);
        let stats_epoch = cache.statistics_source_graph_commit_epoch.unwrap();

        // Commits within the lag window must not recompute: Some(8)
        // tolerates up to eight commits of staleness.
        for i in 1..5 {
            store.create_node(&mut catalog, "T", node_props(i)).unwrap();
        }
        assert!(store.commit_epoch() - stats_epoch <= 8);
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(8));
        assert!(!refresh.snapshot_refreshed);
        assert_eq!(
            cache.statistics_source_graph_commit_epoch.unwrap(),
            stats_epoch
        );

        // The boundary itself is still tolerated: lag == 8 stays inside.
        for i in 5..9 {
            store.create_node(&mut catalog, "T", node_props(i)).unwrap();
        }
        assert_eq!(store.commit_epoch() - stats_epoch, 8);
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(8));
        assert!(!refresh.snapshot_refreshed);

        // Exceeding the window recomputes: lag == 9.
        store.create_node(&mut catalog, "T", node_props(9)).unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(8));
        assert!(refresh.snapshot_refreshed);

        // None preserves the legacy refresh-on-every-commit behavior.
        store
            .create_node(&mut catalog, "T", node_props(10))
            .unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, None);
        assert!(refresh.snapshot_refreshed);

        // Some(0) is identical to None.
        store
            .create_node(&mut catalog, "T", node_props(11))
            .unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(0));
        assert!(refresh.snapshot_refreshed);

        // Some(1) tolerates exactly one commit: the first commit does not
        // refresh, the second does — distinguishable from None/Some(0).
        store
            .create_node(&mut catalog, "T", node_props(12))
            .unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(1));
        assert!(!refresh.snapshot_refreshed);
        store
            .create_node(&mut catalog, "T", node_props(13))
            .unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(1));
        assert!(refresh.snapshot_refreshed);

        // An unchanged epoch never refreshes, whatever the lag.
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(1));
        assert!(!refresh.snapshot_refreshed);

        // A second lag window applies after a refresh: commits below the
        // threshold since the last refresh do not recompute.
        let stats_epoch = cache.statistics_source_graph_commit_epoch.unwrap();
        for i in 14..18 {
            store.create_node(&mut catalog, "T", node_props(i)).unwrap();
        }
        assert!(store.commit_epoch() - stats_epoch <= 8);
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(8));
        assert!(!refresh.snapshot_refreshed);

        let _ = std::fs::remove_dir_all(&path);
    }

    /// While the lag window keeps the statistics snapshot unchanged, the
    /// derived optimizer catalog must be reused too — keying it on the live
    /// commit epoch would force a rebuild from identical inputs on every
    /// unrelated commit.
    #[test]
    fn statistics_commit_lag_reuses_catalog_across_live_epoch_drift() {
        let path = unique_test_dir("stats_commit_lag_catalog");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let mut cache = OptimizerPlanningCache::default();
        store.create_node(&mut catalog, "T", node_props(0)).unwrap();

        let access = cache.optimizer_catalog(&catalog, &store, Some(8), false);
        assert!(
            access
                .decisions
                .iter()
                .any(|decision| decision.contains("optimizer catalog cache refresh")),
            "{:?}",
            access.decisions
        );

        store.create_node(&mut catalog, "T", node_props(1)).unwrap();
        let access = cache.optimizer_catalog(&catalog, &store, Some(8), false);
        assert!(
            access
                .decisions
                .iter()
                .any(|decision| decision.contains("optimizer statistics cache hit")),
            "{:?}",
            access.decisions
        );
        assert!(
            access
                .decisions
                .iter()
                .any(|decision| decision.contains("optimizer catalog cache hit")),
            "{:?}",
            access.decisions
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// The planning cache is shared across snapshots: a call planning against
    /// a retained older snapshot must refresh, since the cached statistics
    /// describe a newer graph than that snapshot can see — under every lag
    /// setting, including the legacy `None`.
    #[test]
    fn statistics_commit_lag_refreshes_for_older_snapshots() {
        let path = unique_test_dir("stats_older_snapshot");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store.create_node(&mut catalog, "T", node_props(0)).unwrap();
        let snapshot = store.snapshot();
        store.create_node(&mut catalog, "T", node_props(1)).unwrap();
        assert!(snapshot.commit_epoch() < store.commit_epoch());

        for lag in [None, Some(0), Some(8)] {
            let mut cache = OptimizerPlanningCache::default();
            let refresh = cache.ensure_statistics(&catalog, &store, true, lag);
            assert!(refresh.snapshot_refreshed, "lag={lag:?} populate");
            let refresh = cache.ensure_statistics(&catalog, &snapshot, true, lag);
            assert!(refresh.snapshot_refreshed, "lag={lag:?} older snapshot");
            assert_eq!(
                cache.statistics_source_graph_commit_epoch,
                Some(snapshot.commit_epoch()),
                "lag={lag:?}"
            );
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Declaring a property descriptor inside the lag window changes which
    /// properties are eligible for statistics: a previously undeclared string
    /// keeps a cached histogram that a TEXT declaration makes ineligible. The
    /// schema key must notice even though labels and indexes are unchanged.
    #[test]
    fn descriptor_ddl_inside_lag_window_refreshes_statistics() {
        let path = unique_test_dir("stats_descriptor_ddl");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let mut cache = OptimizerPlanningCache::default();

        let mut props = node_props(0);
        props.insert("s".to_string(), Value::String("x".to_string()));
        store.create_node(&mut catalog, "T", props).unwrap();
        let label_id = catalog.label_id("T").unwrap();

        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(8));
        assert!(refresh.snapshot_refreshed);
        let stats = cache.statistics.as_ref().unwrap();
        let key = (label_id, "s".to_string());
        assert!(stats.property_histograms.contains_key(&key));
        assert!(stats.property_distinct_counts.contains_key(&key));

        // The TEXT declaration commits once — well inside the lag window —
        // but makes the property ineligible for statistics.
        store
            .create_property_descriptor(
                &mut catalog,
                TableKind::Node,
                "T",
                "s",
                PropertyType::Text,
                true,
            )
            .unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(8));
        assert!(refresh.snapshot_refreshed);
        let stats = cache.statistics.as_ref().unwrap();
        assert!(!stats.property_histograms.contains_key(&key));
        assert!(!stats.property_distinct_counts.contains_key(&key));

        let _ = std::fs::remove_dir_all(&path);
    }
}
