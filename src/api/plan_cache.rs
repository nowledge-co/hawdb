use super::{
    optimizer_catalog, optimizer_config_from_database_config, statement_body, DatabaseConfig,
    QueryAccessControlContext, SharedState,
};
use crate::cypher;
use crate::error::{Result, SkeinError};
use crate::optimizer::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerCatalog, OptimizerSearchDirective, OptimizerTrace,
    PhysicalPlan,
};
use crate::planner::{self, LogicalPlan, Predicate};
use crate::schema::{Catalog, GraphStatistics, IndexKind};
use crate::store::GraphStore;
use crate::value::Value;
pub use skein_plan_cache::PlanCacheStats;
use skein_plan_cache::{
    bind_physical_plan_parameters, parameterize_logical_plan, parameterize_value_list, LfuCache,
    PlanParameterCacheKey,
};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) const DEFAULT_PLAN_CACHE_MAX_ENTRIES: usize = 128;
const ACCESS_CONTROL_VALUES_PARAMETER: &str = "\0skein_access_control_visibility_values";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheLookup {
    Hit,
    Miss,
    Bypass(PlanCacheBypassReason),
}

impl PlanCacheLookup {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass(_) => "bypass",
        }
    }

    pub fn bypass_reason(self) -> Option<PlanCacheBypassReason> {
        match self {
            Self::Bypass(reason) => Some(reason),
            Self::Hit | Self::Miss => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheBypassReason {
    MutationPlanning,
    OptimizerDirective,
    StatementNotCacheable,
}

impl PlanCacheBypassReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MutationPlanning => "mutation_planning",
            Self::OptimizerDirective => "optimizer_directive",
            Self::StatementNotCacheable => "statement_not_cacheable",
        }
    }
}

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

pub(super) struct PlanCacheContext<'a> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a GraphStore,
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
}

#[derive(Debug, Clone)]
struct CachedOptimizerCatalog {
    environment: OptimizerEnvironmentKey,
    source_graph_commit_epoch: u64,
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
        Self {
            labels,
            relationship_types,
            equality_indexes,
            range_indexes,
            full_text_indexes,
            composite_indexes,
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
        self.statistics_generation = 0;
        self.catalog = None;
    }

    pub(super) fn environment_hint(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
    ) -> OptimizerEnvironmentKey {
        // A cache lookup must retain the last published statistics generation:
        // ordinary data commits do not make a physical plan illegal. A cache
        // miss refreshes the snapshot before choosing a new plan instead.
        self.ensure_statistics(catalog, store, false);
        OptimizerEnvironmentKey {
            schema: OptimizerSchemaKey::from_catalog(catalog),
            statistics_generation: self.statistics_generation,
        }
    }

    fn ensure_statistics(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
        refresh_for_data_change: bool,
    ) -> StatisticsCacheRefresh {
        let schema = OptimizerSchemaKey::from_catalog(catalog);
        let source_graph_commit_epoch = store.commit_epoch();
        let refresh_statistics = self.statistics.is_none()
            || self.statistics_schema.as_ref() != Some(&schema)
            || (refresh_for_data_change
                && self.statistics_source_graph_commit_epoch != Some(source_graph_commit_epoch));
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

    fn optimizer_catalog(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
    ) -> OptimizerCatalogAccess {
        let mut decisions = Vec::new();
        // This path is reached only after the physical-plan cache missed or
        // was intentionally bypassed, so cost-based planning must observe the
        // latest committed graph statistics.
        let refresh = self.ensure_statistics(catalog, store, true);
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
        if let Some(cached) = &self.catalog
            && cached.environment == environment
            && cached.source_graph_commit_epoch == store.commit_epoch()
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

        let optimized = Arc::new(optimizer_catalog(catalog, statistics));
        decisions.push(format!(
            "optimizer catalog cache refresh: statistics_epoch={} statistics_generation={} graph_commit_epoch={}",
            statistics.computed_at_commit_epoch,
            self.statistics_generation,
            store.commit_epoch()
        ));
        self.catalog = Some(CachedOptimizerCatalog {
            environment: environment.clone(),
            source_graph_commit_epoch: store.commit_epoch(),
            catalog: optimized.clone(),
        });
        OptimizerCatalogAccess {
            environment,
            catalog: optimized,
            decisions,
        }
    }
}

pub(super) fn optimized_query_plan_for(
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    cache_mode: PlanCacheMode,
    trace_mode: PlanTraceMode,
    context: PlanCacheContext<'_>,
) -> Result<OptimizedQueryPlan> {
    let query_identity = skein_query::QueryIdentity::new("cypher", cypher_text);
    let query_optimizer =
        CascadesOptimizer::with_context(context.optimizer.context().clone().with_query_identity(
            query_identity.normalized_query(),
            query_identity.query_digest(),
        ));
    if let Some(access_control) = context.access_control {
        context
            .config
            .runtime_capabilities
            .require(skein_core::RuntimeCapability::AccessControl)?;
        access_control.validate()?;
    }
    if let Some(capability) = required_runtime_capability(statement_body(statement)) {
        context.config.runtime_capabilities.require(capability)?;
    }
    let effective_max_optimizer_groups =
        optimizer_config_from_database_config(context.config).max_groups;
    let normalized_plan_cache_query =
        skein_query::normalize_query_for_plan_cache("cypher", cypher_text);
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
    let catalog_access = context
        .planning_cache
        .borrow_mut()
        .optimizer_catalog(context.catalog, context.store);
    let logical_root = LogicalPlanRoot::new(logical);
    let physical_root = query_optimizer
        .optimize_root_with_catalog_and_directive(
            &logical_root,
            &catalog_access.catalog,
            context.optimizer_search,
        )
        .map_err(|error| {
            SkeinError::Execution(format!(
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

fn refresh_plan_trace(
    optimizer: &CascadesOptimizer,
    trace: &mut OptimizerTrace,
    physical_plan: &PhysicalPlan,
    trace_mode: PlanTraceMode,
    context: &PlanCacheContext<'_>,
) {
    match trace_mode {
        PlanTraceMode::Template => refresh_materialized_plan_trace(trace, physical_plan),
        PlanTraceMode::Bound => {
            let catalog = context
                .planning_cache
                .borrow_mut()
                .optimizer_catalog(context.catalog, context.store)
                .catalog;
            optimizer.refresh_trace_for_physical_plan(trace, physical_plan, &catalog);
            trace.decisions.push(
                "selected physical plan estimates refreshed for bound parameters".to_string(),
            );
        }
    }
}

fn required_runtime_capability(
    statement: &cypher::Statement,
) -> Option<skein_core::RuntimeCapability> {
    match statement {
        cypher::Statement::CreateFullTextIndex(_) => {
            Some(skein_core::RuntimeCapability::FullTextSearch)
        }
        cypher::Statement::VectorSearch(_) => Some(skein_core::RuntimeCapability::VectorSearch),
        cypher::Statement::MatchReturn(query) if query.vector_seed.is_some() => {
            Some(skein_core::RuntimeCapability::VectorSearch)
        }
        cypher::Statement::ProjectGraph(_) | cypher::Statement::GraphAlgorithm(_) => {
            Some(skein_core::RuntimeCapability::GraphAnalytics)
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
    match logical {
        LogicalPlan::NodeScan { variable, label } => LogicalPlan::Filter {
            predicate: access_control_node_predicate(
                &variable,
                access_control,
                parameterized_values,
            ),
            input: Box::new(LogicalPlan::NodeScan { variable, label }),
        },
        LogicalPlan::NodeCartesianProduct { left, right } => LogicalPlan::NodeCartesianProduct {
            left: Box::new(apply_access_control_to_logical_plan(
                *left,
                access_control,
                parameterized_values,
            )),
            right: Box::new(apply_access_control_to_logical_plan(
                *right,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input,
        } => {
            let target_predicate = access_control_node_predicate(
                &target_variable,
                access_control,
                parameterized_values,
            );
            LogicalPlan::Filter {
                predicate: target_predicate,
                input: Box::new(LogicalPlan::Expand {
                    source_variable,
                    source_label,
                    rel_variable,
                    rel_type,
                    rel_properties,
                    direction,
                    target_variable,
                    target_label,
                    min_hops,
                    max_hops,
                    optional,
                    input: Box::new(apply_access_control_to_logical_plan(
                        *input,
                        access_control,
                        parameterized_values,
                    )),
                }),
            }
        }
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate: _,
        } => LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate: Some(access_control_node_predicate(
                "node",
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::ShortestPath {
            source_variable,
            source_label,
            source_id,
            source_visibility_predicate: _,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            target_visibility_predicate: _,
            min_hops,
            max_hops,
            returns,
        } => LogicalPlan::ShortestPath {
            source_visibility_predicate: Some(access_control_node_predicate(
                &source_variable,
                access_control,
                parameterized_values,
            )),
            target_visibility_predicate: Some(access_control_node_predicate(
                &target_variable,
                access_control,
                parameterized_values,
            )),
            source_variable,
            source_label,
            source_id,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            min_hops,
            max_hops,
            returns,
        },
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::Project { items, input } => LogicalPlan::Project {
            items,
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::Sort { items, input } => LogicalPlan::Sort {
            items,
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(apply_access_control_to_logical_plan(
                *input,
                access_control,
                parameterized_values,
            )),
        },
        other => other,
    }
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
        | cypher::Statement::MatchThreadRepairStats(_)
        | cypher::Statement::GraphAlgorithm(_) => true,
        _ => false,
    }
}
