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
use crate::schema::{Catalog, GraphStatistics, IndexKind, PropertyType, TableKind};
use crate::store::GraphStore;
use crate::value::Value;
use hawdb_optimizer::graph::optimizer_catalog_from_graph_statistics;
use hawdb_plan_cache::{
    bind_physical_plan_parameters, parameterize_logical_plan, parameterize_value_list, LfuCache,
    PlanParameterCacheKey,
};
pub use hawdb_plan_cache::{PlanCacheBypassReason, PlanCacheLookup, PlanCacheStats};
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
}

pub(super) struct OptimizedQueryPlan {
    pub(super) physical_plan: PhysicalPlan,
    pub(super) trace: OptimizerTrace,
    pub(super) plan_cache_lookup: PlanCacheLookup,
    pub(super) optimizer_environment: OptimizerEnvironmentKey,
    pub(super) configured_max_optimizer_groups: Option<usize>,
    pub(super) effective_max_optimizer_groups: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct CachedPlan {
    physical_template: PhysicalPlan,
    trace: OptimizerTrace,
    has_parameter_slots: bool,
}

pub(super) type PlanCache = LfuCache<PlanCacheKey, CachedPlan>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct PlanCacheKey {
    normalized_query: String,
    parameters: PlanParameterCacheKey,
    environment: OptimizerEnvironmentKey,
    max_optimizer_groups: Option<usize>,
    access_control: Option<AccessControlPlanCacheKey>,
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
    statistics_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OptimizerSchemaKey {
    labels: Vec<String>,
    relationship_types: Vec<String>,
    equality_indexes: Vec<(String, String)>,
    range_indexes: Vec<(String, String)>,
    full_text_indexes: Vec<(String, String)>,
    composite_indexes: Vec<(String, Vec<String>)>,
    /// Declared property descriptors, because declaring or altering a
    /// property's type/nullability/state changes which properties are
    /// eligible for optimizer statistics even when labels and indexes are
    /// unchanged. Without them a descriptor DDL inside the commit-lag window
    /// would leave now-ineligible statistics cached.
    property_descriptors: Vec<SchemaPropertyDescriptor>,
}

/// Property-descriptor identity for [`OptimizerSchemaKey`]. Lifecycle states
/// (`SchemaObjectState`) are intentionally excluded: they transition as online
/// DDL machinery backfills and would churn the shared schema key without
/// changing statistics eligibility, which depends only on the declared type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SchemaPropertyDescriptor {
    table: String,
    table_kind: TableKind,
    property: String,
    value_type: PropertyType,
    nullable: bool,
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

impl OptimizerSchemaKey {
    fn from_catalog(catalog: &Catalog) -> Self {
        let labels = catalog.labels().map(|label| label.name.clone()).collect();
        let relationship_types = catalog
            .rel_types()
            .map(|rel_type| rel_type.name.clone())
            .collect();
        let mut equality_indexes = Vec::new();
        let mut range_indexes = Vec::new();
        let mut full_text_indexes = Vec::new();
        for index in catalog.property_indexes() {
            let Some(label) = catalog.label_name(index.label_id) else {
                continue;
            };
            let descriptor = (label.to_string(), index.property.clone());
            match index.kind {
                IndexKind::Equality => equality_indexes.push(descriptor),
                IndexKind::Range => range_indexes.push(descriptor),
                IndexKind::FullText => full_text_indexes.push(descriptor),
            }
        }
        let composite_indexes = catalog
            .composite_property_indexes()
            .filter_map(|index| {
                catalog
                    .label_name(index.label_id)
                    .map(|label| (label.to_string(), index.properties.clone()))
            })
            .collect();
        let mut property_descriptors = catalog
            .property_descriptors()
            .filter_map(|property| {
                let table = catalog.table_descriptor(property.table_id)?;
                Some(SchemaPropertyDescriptor {
                    table: table.name.clone(),
                    table_kind: table.kind,
                    property: property.name.clone(),
                    value_type: property.value_type,
                    nullable: property.nullable,
                })
            })
            .collect::<Vec<_>>();
        property_descriptors.sort();
        Self {
            labels,
            relationship_types,
            equality_indexes,
            range_indexes,
            full_text_indexes,
            composite_indexes,
            property_descriptors,
        }
    }
}

impl OptimizerEnvironmentKey {
    /// A prepared query may reuse a physical plan across data and statistics
    /// generations because those affect cost only. Schema changes can alter an
    /// operator's legality and therefore require re-planning.
    pub(super) fn is_execution_compatible(&self, catalog: &Catalog) -> bool {
        self.schema == OptimizerSchemaKey::from_catalog(catalog)
    }
}

impl OptimizerPlanningCache {
    pub(super) fn invalidate(&mut self) {
        self.statistics = None;
        self.statistics_schema = None;
        self.statistics_source_graph_commit_epoch = None;
        self.statistics_publication = None;
        // In-flight snapshots can still publish plans after invalidation. Keep
        // generations monotonic so a refreshed environment cannot reuse their key.
        self.catalog = None;
    }

    pub(super) fn environment_hint<R: hawdb_storage::graph_engine::GraphReadEngine>(
        &mut self,
        catalog: &Catalog,
        store: &R,
    ) -> OptimizerEnvironmentKey {
        // A cache lookup must retain the last published statistics generation:
        // ordinary data commits do not make a physical plan illegal. A cache
        // miss refreshes the snapshot before choosing a new plan instead.
        self.ensure_statistics(catalog, store, false, None);
        OptimizerEnvironmentKey {
            schema: OptimizerSchemaKey::from_catalog(catalog),
            statistics_generation: self.statistics_generation,
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
            statistics_generation: self.statistics_generation,
        };
        // The catalog is a pure function of the schema and the published
        // statistics, both captured by `environment` (statistics_generation
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

        let optimized = Arc::new(optimizer_catalog_from_graph_statistics(catalog, statistics));
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
    let environment_hint = (cache_mode == PlanCacheMode::Use).then(|| {
        context
            .planning_cache
            .borrow_mut()
            .environment_hint(context.catalog, context.store)
    });
    let parameterized = (cache_mode == PlanCacheMode::Use)
        .then(|| parameterize_logical_plan(statement_body(statement), parameters))
        .transpose()?;
    let mut key = parameterized.as_ref().map(|parameterized| PlanCacheKey {
        normalized_query: normalized_plan_cache_query,
        parameters: parameterized.cache_key().clone(),
        environment: environment_hint
            .clone()
            .expect("optimizer environment exists for a parameterized plan"),
        max_optimizer_groups: context.config.max_optimizer_groups,
        access_control: access_control_cache_key,
    });
    if cache_mode == PlanCacheMode::Use {
        let key = key.as_ref().expect("cache key exists in use mode");
        let cached = context.cache.borrow_mut().get(key);
        if let Some(cached) = cached {
            let physical_plan = bind_physical_plan_parameters(
                &cached.physical_template,
                execution_parameters.as_ref(),
                cached.has_parameter_slots,
            )?;
            let mut trace = cached.trace;
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
            record_access_control_plan_decision(&mut trace, context.access_control);
            return Ok(OptimizedQueryPlan {
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
    if let Some(access_control) = context.access_control {
        logical = apply_access_control_to_logical_plan(
            logical,
            access_control,
            cache_mode == PlanCacheMode::Use,
        );
    }
    let catalog_access = optimizer_catalog_access(&context);
    let logical_root = LogicalPlanRoot::new(logical);
    let physical_root = query_optimizer
        .optimize_root_with_catalog_and_directive(
            &logical_root,
            &catalog_access.catalog,
            context.optimizer_search,
        )
        .map_err(|error| {
            HawDBError::Execution(format!(
                "optimizer search directive could not be honored: {error}"
            ))
        })?;
    let (physical_template, mut trace) = physical_root.into_parts();
    trace.decisions.extend(catalog_access.decisions);
    let has_parameter_slots = parameterized
        .as_ref()
        .is_some_and(|parameterized| parameterized.slot_count() > 0)
        || (cache_mode == PlanCacheMode::Use && context.access_control.is_some());
    let physical_plan = bind_physical_plan_parameters(
        &physical_template,
        execution_parameters.as_ref(),
        has_parameter_slots,
    )?;
    if let Some(parameterized) = &parameterized {
        trace.decisions.push(format!(
            "parameterized plan template: slots={} exact_variants={}",
            parameterized.slot_count(),
            parameterized.exact_variant_count()
        ));
    }
    let mut cached_trace = trace.clone();
    refresh_materialized_plan_trace(&mut cached_trace, &physical_template);
    match trace_mode {
        PlanTraceMode::Template => refresh_materialized_plan_trace(&mut trace, &physical_plan),
        PlanTraceMode::Bound => query_optimizer.refresh_trace_for_physical_plan(
            &mut trace,
            &physical_plan,
            &catalog_access.catalog,
        ),
    }
    record_access_control_plan_decision(&mut trace, context.access_control);
    if cache_mode == PlanCacheMode::Use {
        let mut key = key.take().expect("cache key exists in use mode");
        key.environment = catalog_access.environment.clone();
        context.cache.borrow_mut().insert(
            key,
            CachedPlan {
                physical_template,
                trace: cached_trace,
                has_parameter_slots,
            },
        );
        trace
            .decisions
            .push("plan cache miss: optimized parameterized physical plan template".to_string());
        return Ok(OptimizedQueryPlan {
            physical_plan,
            trace,
            plan_cache_lookup: PlanCacheLookup::Miss,
            optimizer_environment: catalog_access.environment,
            configured_max_optimizer_groups: context.config.max_optimizer_groups,
            effective_max_optimizer_groups,
        });
    } else if let PlanCacheMode::Bypass(reason) = cache_mode {
        context.cache.borrow_mut().record_bypass();
        trace
            .decisions
            .push(format!("plan cache bypass: {}", reason.as_str()));
        return Ok(OptimizedQueryPlan {
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

fn refresh_materialized_plan_trace(trace: &mut OptimizerTrace, physical_plan: &PhysicalPlan) {
    trace.selected_plan = physical_plan.explain(0);
    trace.selected_plan_fingerprint = physical_plan.fingerprint();
}

fn optimizer_catalog_access<S: crate::executor::ExecutionStore>(
    context: &PlanCacheContext<'_, S>,
) -> OptimizerCatalogAccess {
    context.planning_cache.borrow_mut().optimizer_catalog(
        context.catalog,
        context.store,
        context.config.optimizer_statistics_inline_commit_lag,
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
            let catalog = optimizer_catalog_access(context).catalog;
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
        cypher::Statement::MatchReturn(query) if query.vector_seed.is_some() => {
            Some(hawdb_core::RuntimeCapability::VectorSearch)
        }
        cypher::Statement::ProjectGraph(_) | cypher::Statement::GraphAlgorithm(_) => {
            Some(hawdb_core::RuntimeCapability::GraphAnalytics)
        }
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
    hawdb_plan::apply_node_visibility_predicates(logical, &|variable| {
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
        cypher::Statement::MatchReturn(query) => query.vector_seed.is_none(),
        cypher::Statement::ShortestPathReturn(_)
        | cypher::Statement::MatchNodesReturn(_)
        | cypher::Statement::MatchOptionalRelationshipCountSum(_)
        | cypher::Statement::Pipeline(_)
        | cypher::Statement::GraphAlgorithm(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaObjectState;
    use std::collections::BTreeMap;

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
        store.create_node(&mut catalog, "T", node_props(10)).unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, None);
        assert!(refresh.snapshot_refreshed);

        // Some(0) is identical to None.
        store.create_node(&mut catalog, "T", node_props(11)).unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(0));
        assert!(refresh.snapshot_refreshed);

        // Some(1) tolerates exactly one commit: the first commit does not
        // refresh, the second does — distinguishable from None/Some(0).
        store.create_node(&mut catalog, "T", node_props(12)).unwrap();
        let refresh = cache.ensure_statistics(&catalog, &store, true, Some(1));
        assert!(!refresh.snapshot_refreshed);
        store.create_node(&mut catalog, "T", node_props(13)).unwrap();
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

        let access = cache.optimizer_catalog(&catalog, &store, Some(8));
        assert!(
            access
                .decisions
                .iter()
                .any(|decision| decision.contains("optimizer catalog cache refresh")),
            "{:?}",
            access.decisions
        );

        store.create_node(&mut catalog, "T", node_props(1)).unwrap();
        let access = cache.optimizer_catalog(&catalog, &store, Some(8));
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

    /// Online DDL lifecycle transitions (`DeleteOnly`→…→`Public`) must not
    /// churn the shared schema key: statistics eligibility depends on the
    /// declared type, not on how far the machinery has backfilled.
    #[test]
    fn schema_key_ignores_descriptor_lifecycle_states() {
        let mut catalog = Catalog::default();
        let table = catalog.get_or_create_table(TableKind::Node, "T");
        let property =
            catalog.get_or_create_property(table, "body", PropertyType::String, true);
        let baseline = OptimizerSchemaKey::from_catalog(&catalog);

        let mut churned = catalog.clone();
        churned.set_property_state(property, SchemaObjectState::Backfill);
        churned.set_table_state(table, SchemaObjectState::Validating);
        assert_eq!(baseline, OptimizerSchemaKey::from_catalog(&churned));

        let mut extended = catalog.clone();
        extended.get_or_create_property(table, "title", PropertyType::Text, false);
        assert_ne!(baseline, OptimizerSchemaKey::from_catalog(&extended));
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
