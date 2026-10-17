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

//! Bounded, plan-derived source reads for immutable projection generations.

use super::*;
use hawdb_storage::projection_generation::{
    ProjectionGenerationError, ProjectionGenerationPublishReport, SealedProjectionGeneration,
};
use hawdb_storage::store::{GraphReadDependencies, GraphReadDependencyError};
use hawdb_storage::version::VersionKey;
use std::fmt;

#[path = "dependency_read/full_source.rs"]
mod full_source;
#[path = "dependency_read/staging_session.rs"]
mod staging_session;
pub use staging_session::{
    ProjectionBaselineComparisonConfig, ProjectionBaselineComparisonSession,
    ProjectionBaselineComparisonStep, ProjectionBaselineMappingContext,
    ProjectionBaselineMappingFunction, ProjectionBaselineMappingLimits,
    ProjectionFullRebuildConfig, ProjectionFullRebuildSession, ProjectionFullRebuildStep,
    ProjectionMappingContext, ProjectionMappingFunction, ProjectionMappingInput,
    ProjectionMappingLimits, ProjectionMappingStatus, ProjectionMappingUsage,
    ProjectionMatchedBaseline, ProjectionOutputVerificationContext,
    ProjectionOutputVerificationFunction, ProjectionOutputVerificationSession,
    ProjectionPreparedFullRebuild, ProjectionStagedInputs, ProjectionStagingSession,
    ProjectionStagingSessionConfig, ProjectionStagingSessionStep,
};
#[path = "dependency_read/graph_match.rs"]
mod graph_match;
#[path = "dependency_read/owner_feed.rs"]
mod owner_feed;
#[path = "dependency_read/read_scopes.rs"]
mod read_scopes;
pub use owner_feed::{
    PreparedProjectionOwnerFeed, ProjectionOwnerBefore, ProjectionOwnerFeedItem, ProjectionOwnerId,
};
pub use read_scopes::ProjectionReadScopes;
#[path = "dependency_read/projection_feed.rs"]
mod projection_feed;
pub use full_source::{
    PreparedProjectionFullSource, ProjectionFullSourceItem, ProjectionFullSourceLimits,
};
pub use projection_feed::{
    PreparedProjectionFeed, ProjectionFeedError, ProjectionFeedItem, ProjectionFeedLimits,
    ProjectionFeedRead, ProjectionFeedResource, ProjectionFeedStep, ProjectionFeedUsage,
};
#[path = "dependency_read/relational.rs"]
mod relational;
#[path = "dependency_read/search_projection.rs"]
mod search_projection;

/// Limits for one preparation. Output limits apply to each statement; callers
/// must additionally bound cross-statement work and the artifact builder.
#[derive(Debug, Clone, Copy)]
pub struct PreparedReadLimits {
    pub dependency_entries: NonZeroUsize,
    pub dependency_bytes: NonZeroUsize,
    pub statement_rows: NonZeroUsize,
    pub statement_payload_bytes: NonZeroUsize,
}

/// Source durability required by one projection publication. Synchronization
/// covers a complete WAL prefix and does not alter subsequent write policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProjectionSourceDurability {
    #[default]
    RequireKnownDurable,
    Synchronize,
}

/// A pinned query context with no untracked mutable read escape hatch.
#[derive(Debug)]
pub struct PreparedRead {
    read: DatabaseReadTransaction,
    dependencies: PreparedReadDependencies,
    failed: bool,
}

/// Source proof inputs retained until the publication decision. This is not
/// authorization to publish outside the database's exclusive ownership.
#[derive(Debug)]
pub struct PreparedReadDependencies {
    source: DatabaseInstanceIdentity,
    graph: GraphReadDependencies,
}

impl PreparedReadDependencies {
    pub fn relationship_domain_count(&self) -> usize {
        self.graph.relationship_domain_count()
    }
    /// Complete SQL table scopes, including rows outside a query's output page.
    pub fn relational_table_domain_count(&self) -> usize {
        self.graph.relational_table_domain_count()
    }

    pub fn source_epoch(&self) -> u64 {
        self.graph.source_epoch()
    }

    /// Reports a conservative whole-source fallback. Qualified graph/table
    /// domains and point/index dependencies admit commits outside their scope.
    pub fn requires_unchanged_database_epoch(&self) -> bool {
        self.graph.requires_unchanged_database_epoch()
    }

    pub fn exact_node_property_count(&self) -> usize {
        self.graph.exact_node_property_count()
    }

    pub fn exact_relational_row_count(&self) -> usize {
        self.graph.exact_relational_row_count()
    }

    pub fn relational_index_prefix_count(&self) -> usize {
        self.graph.relational_index_prefix_count()
    }

    pub fn node_domain_count(&self) -> usize {
        self.graph.node_domain_count()
    }
}

impl PreparedRead {
    pub fn source_epoch(&self) -> u64 {
        self.dependencies.source_epoch()
    }

    /// Executes parameterized Cypher with complete dependencies for qualified
    /// node and single-hop plans. Unsupported operators fail closed before execution.
    /// No returned row set is treated as a complete membership footprint.
    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<BoundedReadQueryOutput> {
        let result = self.query_inner(cypher_text, parameters);
        self.failed |= result.is_err();
        result
    }

    fn query_inner(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<BoundedReadQueryOutput> {
        let mut prepared = query_runtime::parse_runtime_execution(cypher_text)?;
        if matches!(prepared.statement, cypher::Statement::Explain(_)) {
            return Err(HawDBError::Execution(
                "prepared source reads do not support EXPLAIN".into(),
            ));
        }
        let optimized = self.read.optimized_query_plan_with_access_control(
            cypher_text,
            &prepared.statement,
            parameters,
            None,
        )?;
        collect_node_dependencies(
            &optimized.physical_plan,
            &self.read.catalog,
            &self.read.store,
            &mut self.dependencies.graph,
        )?;
        prepared.optimized = Some(optimized);
        self.read
            .query_with_params_bounded_profile_prepared_internal(
                cypher_text,
                prepared,
                parameters,
                self.read.config.max_read_result_rows,
                None,
                None,
            )
    }

    /// A failed or incomplete statement cannot be silently omitted from a
    /// source proof, even if the caller ignored its error and read again.
    pub fn finish(self) -> Result<PreparedReadDependencies> {
        if self.failed {
            return Err(HawDBError::Execution(
                "prepared source read contains a failed statement".into(),
            ));
        }
        Ok(self.dependencies)
    }
}

fn collect_node_dependencies(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &GraphStore,
    dependencies: &mut GraphReadDependencies,
) -> Result<()> {
    use PhysicalPlan as P;
    // A node scan's filter can perform adjacency reads without an explicit
    // traversal operator. Do not infer its footprint from the leaf alone.
    let predicate = match plan {
        P::FilterExec { predicate, .. } => Some(predicate),
        P::NodeProjectionScanExec { predicate, .. } => predicate.as_ref(),
        _ => None,
    };
    if predicate.is_some_and(predicate_reads_relationships) {
        return Err(HawDBError::Execution(
            "prepared source read requires an adjacency predicate collector".into(),
        ));
    }
    if let P::FilterExec { predicate, input } = plan {
        if let Some((variable, label)) = complete_node_source(input)
            && let Some(values) = graph_match::ids(predicate, variable)
        {
            return graph_match::record_ids(values, label, dependencies);
        }
        if let P::GraphMatchExec {
            program,
            input: None,
        } = input.as_ref()
        {
            return graph_match::collect(program, Some(predicate), catalog, store, dependencies);
        }
    }
    if let P::FilterExec { predicate, input } = plan
        && let Some((variable, label)) = complete_node_source(input)
        && let Some((property, values)) =
            necessary_scalar_bound(predicate, variable, label, catalog)
    {
        return collect_scalar_read_keys(catalog, label, property, values, dependencies);
    }
    let key = match plan {
        P::SeqNodeScan { label, .. }
        | P::NodeCountExec { label, .. } => VersionKey::ReadNodeDomain((!label.is_empty()).then(|| label.clone())),
        P::IndexNodeSeek { label, property, value, .. } => {
            return collect_scalar_read_keys(catalog, label, property, std::slice::from_ref(value), dependencies);
        }
        P::IndexNodeMultiSeek { label, property, values, .. } => {
            return collect_scalar_read_keys(catalog, label, property, values, dependencies);
        }
        P::NodeProjectionScanExec { variable, label, access, predicate, .. } => {
            if !matches!(access, hawdb_plan_cypher::NodeProjectionAccess::FullText { .. })
                && let Some(values) = predicate.as_ref().and_then(|predicate| graph_match::ids(predicate, variable)) {
                return graph_match::record_ids(values, label, dependencies);
            }
            if !matches!(access, hawdb_plan_cypher::NodeProjectionAccess::FullText { .. })
                && let Some((property, values)) = predicate.as_ref().and_then(|predicate| necessary_scalar_bound(predicate, variable, label, catalog)) {
                return collect_scalar_read_keys(catalog, label, property, values, dependencies);
            }
            if let hawdb_plan_cypher::NodeProjectionAccess::PropertyValues { property, values } = access {
                return collect_scalar_read_keys(catalog, label, property, values, dependencies);
            }
            VersionKey::ReadNodeDomain((!label.is_empty()).then(|| label.clone()))
        }
        P::IndexNodeUnionSeek { label, .. }
        | P::IndexNodeCompositeSeek { label, .. }
        | P::IndexNodeCompositeRangeSeek { label, .. }
        | P::IndexNodeRangeSeek { label, .. } => VersionKey::ReadNodeDomain((!label.is_empty()).then(|| label.clone())),
        P::GraphMatchExec { program, input: None } => return graph_match::collect(program, None, catalog, store, dependencies),
        P::AdjacencyExpandExec { .. } => return graph_match::collect_expand(plan, catalog, store, dependencies),
        P::FilterExec { input, .. }
        | P::ProjectExec { input, .. }
        | P::AggregateExec { input, .. }
        | P::DistinctExec { input }
        | P::SortExec { input, .. }
        | P::TopNExec { input, .. }
        | P::LimitExec { input, .. } => return collect_node_dependencies(input, catalog, store, dependencies),
        // An optimizer-folded empty result can depend on absent catalog/data.
        // Retain the conservative epoch dependency instead of losing it.
        P::EmptyExec => VersionKey::Database,
        _ => return Err(HawDBError::Execution("prepared source read has an unqualified operator; its dependency collector is not available".into())),
    };
    dependencies
        .record(key)
        .map_err(|error| HawDBError::Execution(error.to_string()))
}

// Bounds may move through local filters, but never through projections,
// LIMIT/TopN/aggregation or other selection boundaries. An outer predicate
// after a bounded candidate phase can depend on otherwise unrelated rows.
fn complete_node_source(plan: &PhysicalPlan) -> Option<(&str, &str)> {
    use PhysicalPlan as P;
    match plan {
        P::SeqNodeScan { variable, label }
        | P::IndexNodeSeek {
            variable, label, ..
        }
        | P::IndexNodeMultiSeek {
            variable, label, ..
        }
        | P::IndexNodeUnionSeek {
            variable, label, ..
        }
        | P::IndexNodeCompositeSeek {
            variable, label, ..
        }
        | P::IndexNodeCompositeRangeSeek {
            variable, label, ..
        }
        | P::IndexNodeRangeSeek {
            variable, label, ..
        } => Some((variable, label)),
        P::NodeProjectionScanExec {
            variable,
            label,
            access,
            predicate,
            items,
            ..
        } if items.is_empty()
            && !matches!(
                access,
                hawdb_plan_cypher::NodeProjectionAccess::FullText { .. }
            )
            && !predicate
                .as_ref()
                .is_some_and(predicate_reads_relationships) =>
        {
            Some((variable, label))
        }
        P::FilterExec { predicate, input } if !predicate_reads_relationships(predicate) => {
            complete_node_source(input)
        }
        _ => None,
    }
}

fn necessary_scalar_bound<'a>(
    predicate: &'a crate::planner::Predicate,
    variable: &str,
    label: &str,
    catalog: &Catalog,
) -> Option<(&'a str, &'a [Value])> {
    use crate::planner::Predicate as P;
    let (bound_variable, property, values) = match predicate {
        P::And(parts) => {
            return parts
                .iter()
                .find_map(|part| necessary_scalar_bound(part, variable, label, catalog))
        }
        P::PropertyEq {
            variable,
            property,
            value,
        } => (variable, property, std::slice::from_ref(value)),
        P::PropertyIn {
            variable,
            property,
            values,
        } => (variable, property, values.as_slice()),
        // Never extract a necessary key through OR, NOT or a different binding.
        _ => return None,
    };
    (bound_variable == variable
        && catalog
            .label_id(label)
            .is_some_and(|label| catalog.has_scalar_property_index(label, property)))
    .then_some((property, values))
}

fn collect_scalar_read_keys(
    catalog: &Catalog,
    label: &str,
    property: &str,
    values: &[Value],
    dependencies: &mut GraphReadDependencies,
) -> Result<()> {
    let qualified = catalog
        .label_id(label)
        .is_some_and(|id| catalog.has_scalar_property_index(id, property));
    if qualified {
        for value in values {
            if let Some(key) =
                hawdb_storage::version::node_property_read_key(label, property, value)
            {
                dependencies
                    .record(key)
                    .map_err(|error| HawDBError::Execution(error.to_string()))?;
            } else {
                // The writer uses the same representability bound, retaining
                // its label-domain summary for unsupported/oversized values.
                return record_node_domain(label, dependencies);
            }
        }
        Ok(())
    } else {
        record_node_domain(label, dependencies)
    }
}

fn record_node_domain(label: &str, dependencies: &mut GraphReadDependencies) -> Result<()> {
    dependencies
        .record(VersionKey::ReadNodeDomain(
            (!label.is_empty()).then(|| label.into()),
        ))
        .map_err(|error| HawDBError::Execution(error.to_string()))
}

fn predicate_reads_relationships(predicate: &crate::planner::Predicate) -> bool {
    use crate::planner::Predicate as P;
    match predicate {
        P::And(parts) | P::Or(parts) => parts.iter().any(predicate_reads_relationships),
        P::Not(inner) => predicate_reads_relationships(inner),
        P::RelationshipExists { .. } | P::BoundRelationshipExists { .. } => true,
        P::ConstantBool(_)
        | P::IdEq { .. }
        | P::IdNotEq { .. }
        | P::IdCompare { .. }
        | P::IdIn { .. }
        | P::PropertyEq { .. }
        | P::PropertyNotEq { .. }
        | P::PropertyCompare { .. }
        | P::ExpressionEq { .. }
        | P::ExpressionNotEq { .. }
        | P::ExpressionCompare { .. }
        | P::ExpressionContains { .. }
        | P::PropertyListContains { .. }
        | P::PropertyListContainsLower { .. }
        | P::PropertyContains { .. }
        | P::PropertyStartsWith { .. }
        | P::PropertyEndsWith { .. }
        | P::PropertyRegexMatch { .. }
        | P::PropertyIsNull { .. }
        | P::PropertyIsNotNull { .. }
        | P::PropertyIn { .. } => false,
    }
}

#[derive(Debug)]
pub enum ProjectionPublicationError {
    SourceChanged,
    SourceNotDurable,
    SourceWatermarkMissing,
    SourceWatermarkRegression { previous: u64, staged: u64 },
    SourceEpochMismatch { captured: u64, staged: u64 },
    Dependency(GraphReadDependencyError),
    Storage(HawDBError),
    Generation(ProjectionGenerationError),
}

impl fmt::Display for ProjectionPublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceChanged => f.write_str("projection source database instance changed"),
            Self::SourceWatermarkMissing => {
                f.write_str("prepared Search publication requires an explicit source watermark")
            }
            Self::SourceWatermarkRegression { previous, staged } => write!(
                f,
                "prepared Search watermark {staged} regresses the active watermark {previous}"
            ),
            Self::SourceNotDurable => {
                f.write_str("projection source requires a completed durability barrier")
            }
            Self::SourceEpochMismatch { captured, staged } => write!(
                f,
                "projection watermark {staged} does not match captured source epoch {captured}"
            ),
            Self::Dependency(error) => error.fmt(f),
            Self::Storage(error) => error.fmt(f),
            Self::Generation(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ProjectionPublicationError {}

impl Database {
    pub fn begin_prepared_read(&self, limits: PreparedReadLimits) -> Result<PreparedRead> {
        let graph = self.runtime.get()?.store.capture_read_dependencies(
            limits.dependency_entries.get(),
            limits.dependency_bytes.get(),
        )?;
        let mut read = self.begin_read_transaction()?;
        read.config.max_read_result_rows = restrictive_query_limit(
            read.config.max_read_result_rows,
            Some(limits.statement_rows.get()),
        );
        read.config.max_read_result_payload_bytes = restrictive_query_limit(
            read.config.max_read_result_payload_bytes,
            Some(limits.statement_payload_bytes.get()),
        );
        Ok(PreparedRead {
            read,
            dependencies: PreparedReadDependencies {
                source: self.instance_identity()?,
                graph,
            },
            failed: false,
        })
    }

    /// Validates and durably publishes a full generation under one exclusive
    /// database borrow. The catalog's expected-head CAS remains authoritative.
    /// Host bindings/cursors need their additional owner fence before calling.
    /// Requires an observed source durability barrier. Use the explicit policy
    /// variant to synchronize an uncovered relaxed/recovered source prefix.
    pub fn publish_prepared_projection(
        &mut self,
        dependencies: &PreparedReadDependencies,
        sealed: &SealedProjectionGeneration,
    ) -> std::result::Result<ProjectionGenerationPublishReport, ProjectionPublicationError> {
        self.publish_prepared_projection_with_source_durability(
            dependencies,
            sealed,
            ProjectionSourceDurability::RequireKnownDurable,
        )
    }

    pub fn publish_prepared_projection_with_source_durability(
        &mut self,
        dependencies: &PreparedReadDependencies,
        sealed: &SealedProjectionGeneration,
        source_durability: ProjectionSourceDurability,
    ) -> std::result::Result<ProjectionGenerationPublishReport, ProjectionPublicationError> {
        use ProjectionPublicationError as E;
        self.ensure_writable().map_err(E::Storage)?;
        let catalog = self.projection_generation_store().map_err(E::Storage)?;
        // Resolve an already committed generation before revalidating an old
        // source token. Subsequent source writes/reopen do not undo its receipt.
        if let Some(receipt) = catalog.published_receipt(sealed).map_err(E::Generation)? {
            return Ok(receipt);
        }
        self.validate_prepared_projection_source(
            dependencies,
            sealed.manifest().begin.source_watermark,
            source_durability,
        )?;
        catalog.publish(sealed).map_err(E::Generation)
    }

    /// Activates one closed Search candidate under the source database's
    /// exclusive borrow. The host must also retain its target/binding authority
    /// through this call. No arbitrary mutation callback can enter this fence.
    pub fn publish_prepared_search<P: crate::search::PreparedSearchPublication>(
        &mut self,
        dependencies: &PreparedReadDependencies,
        candidate: P,
    ) -> std::result::Result<P::Report, ProjectionPublicationError> {
        self.publish_prepared_search_with_source_durability(
            dependencies,
            candidate,
            ProjectionSourceDurability::RequireKnownDurable,
        )
    }

    /// Explicitly covers an uncovered source WAL prefix before Search activation.
    /// Requires the candidate watermark to equal its captured source epoch;
    /// bounded changefeed prefixes need their additional batch/receipt contract.
    pub fn publish_prepared_search_with_source_durability<
        P: crate::search::PreparedSearchPublication,
    >(
        &mut self,
        dependencies: &PreparedReadDependencies,
        candidate: P,
        source_durability: ProjectionSourceDurability,
    ) -> std::result::Result<P::Report, ProjectionPublicationError> {
        let watermark = candidate
            .source_graph_commit_epoch()
            .ok_or(ProjectionPublicationError::SourceWatermarkMissing)?;
        if let Some(previous) = candidate
            .source_watermark_floor()
            .map_err(ProjectionPublicationError::Storage)?
            && watermark < previous
        {
            return Err(ProjectionPublicationError::SourceWatermarkRegression {
                previous,
                staged: watermark,
            });
        }
        self.validate_prepared_projection_source(dependencies, watermark, source_durability)?;
        candidate
            .publish()
            .map_err(ProjectionPublicationError::Storage)
    }

    fn validate_prepared_projection_source(
        &mut self,
        dependencies: &PreparedReadDependencies,
        staged: u64,
        source_durability: ProjectionSourceDurability,
    ) -> std::result::Result<(), ProjectionPublicationError> {
        use ProjectionPublicationError as E;
        self.ensure_writable().map_err(E::Storage)?;
        if self.instance_identity().map_err(E::Storage)? != dependencies.source {
            return Err(E::SourceChanged);
        }
        let captured = dependencies.source_epoch();
        if captured != staged {
            return Err(E::SourceEpochMismatch { captured, staged });
        }
        let mut runtime = self.runtime.get_mut().map_err(E::Storage)?;
        runtime
            .store
            .validate_read_dependencies(&dependencies.graph)
            .map_err(E::Dependency)?;
        if runtime.store.wal_sync_group_active() {
            return Err(E::SourceNotDurable);
        }
        if runtime
            .store
            .known_source_durable_epoch()
            .is_none_or(|epoch| epoch < captured)
        {
            match source_durability {
                ProjectionSourceDurability::RequireKnownDurable => return Err(E::SourceNotDurable),
                ProjectionSourceDurability::Synchronize => {
                    let covered = runtime
                        .store
                        .synchronize_source_prefix()
                        .map_err(E::Storage)?;
                    if covered < captured {
                        return Err(E::SourceNotDurable);
                    }
                }
            }
        }
        Ok(())
    }
}
