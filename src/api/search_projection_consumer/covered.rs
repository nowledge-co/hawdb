// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Complete-E coverage handed directly to the registered publication protocol.

use super::*;
use crate::{
    PreparedReadDependencies, PreparedReadLimits, ProjectionBaselineMappingFunction,
    ProjectionCoverageConfig, ProjectionCoverageSession, ProjectionCoverageStep,
    ProjectionFeedError, ProjectionFeedLimits, ProjectionMappingFunction, ProjectionMappingLimits,
    ProjectionPublicationError, ProjectionSourceDurability, ProjectionStagingSession,
    ProjectionStagingSessionConfig, ProjectionStagingSessionStep,
};
use hawdb_search::{
    SearchComparedConsumerDocuments, SearchMutationSortStepLimits, SearchUnorderedMutationLimits,
};
use hawdb_storage::file_descriptors::{DescriptorReservation, FileOpenContext};
use std::fmt;

/// Cumulative private-work limits for one registered C..E catch-up.
/// The facade derives C from the completed receipt and captures E itself.
/// Registered generation/resident limits and checkpoint layout remain the
/// existing Consumer contract; standalone Search build options do not apply.
#[derive(Debug, Clone)]
pub struct ProjectionConsumerCatchUpConfig {
    pub reads: PreparedReadLimits,
    pub feed: ProjectionFeedLimits,
    pub mapping: ProjectionMappingLimits,
    pub sorting: SearchUnorderedMutationLimits,
    /// Cancellation/deadline state without pre-bound resource controllers.
    pub task: RuntimeTaskContext,
}

/// Retains the typed refusal from lifecycle, preparation, or final activation.
#[derive(Debug)]
pub enum ProjectionConsumerCatchUpError {
    Consumer(SearchProjectionConsumerError),
    Feed(ProjectionFeedError),
    Publication(ProjectionPublicationError),
    Stopped(hawdb_core::RuntimeCancellationReason),
    RecoveryRetention {
        failure: Box<ProjectionConsumerCatchUpError>,
        retention: HawDBError,
    },
}
impl fmt::Display for ProjectionConsumerCatchUpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Consumer(error) => error.fmt(formatter),
            Self::Feed(error) => error.fmt(formatter),
            Self::Publication(error) => error.fmt(formatter),
            Self::Stopped(error) => error.fmt(formatter),
            Self::RecoveryRetention { failure, retention } => write!(
                formatter,
                "{failure}; retaining the publication owner also failed: {retention}"
            ),
        }
    }
}
impl std::error::Error for ProjectionConsumerCatchUpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Consumer(error) => Some(error),
            Self::Feed(error) => Some(error),
            Self::Publication(error) => Some(error),
            Self::Stopped(error) => Some(error),
            Self::RecoveryRetention { failure, .. } => Some(failure.as_ref()),
        }
    }
}
impl From<SearchProjectionConsumerError> for ProjectionConsumerCatchUpError {
    fn from(error: SearchProjectionConsumerError) -> Self {
        Self::Consumer(error)
    }
}
impl From<HawDBError> for ProjectionConsumerCatchUpError {
    fn from(error: HawDBError) -> Self {
        Self::Consumer(error.into())
    }
}
impl From<ProjectionFeedError> for ProjectionConsumerCatchUpError {
    fn from(error: ProjectionFeedError) -> Self {
        Self::Feed(error)
    }
}
impl From<ProjectionPublicationError> for ProjectionConsumerCatchUpError {
    fn from(error: ProjectionPublicationError) -> Self {
        Self::Publication(error)
    }
}
type CatchUpResult<T> = std::result::Result<T, ProjectionConsumerCatchUpError>;

struct Base {
    receipt: CheckpointReceipt,
    _pin: SearchProjectionConsumerReadPin,
    _memory: hawdb_executor::QueryMemoryLease,
    task: RuntimeTaskContext,
    descriptor_context: Option<FileOpenContext>,
}

impl Base {
    fn descriptor_operation(&self) -> Result<Option<DescriptorReservation>> {
        self.descriptor_context
            .as_ref()
            .map(FileOpenContext::internal_operation)
            .transpose()
            .map(Option::flatten)
            .map_err(Into::into)
    }
}

/// Bounded invisible C..E work over this registration's exact selected base.
/// Source writers may continue between steps. The default full-database scope
/// rejects all post-E commits; declared domains retain only relevant invalidations.
pub struct ProjectionConsumerStagingSession<'consumer> {
    session: ProjectionStagingSession<'consumer>,
    base: Base,
    scope: crate::ProjectionSourceScope,
}
impl<'consumer> ProjectionConsumerStagingSession<'consumer> {
    pub fn source_epoch(&self) -> u64 {
        self.session.source_epoch()
    }
    pub fn advance(
        &mut self,
        feed: ProjectionFeedLimits,
        ordering: SearchMutationSortStepLimits,
    ) -> CatchUpResult<ProjectionStagingSessionStep> {
        let _descriptor_operation = self.base.descriptor_operation()?;
        Ok(self.session.advance(feed, ordering)?)
    }
    /// The fixed full-source mapper is the trusted projection definition. Keep
    /// its version bound to the stable Consumer ID through final publication.
    pub fn verify_complete_source(
        mut self,
        config: ProjectionCoverageConfig,
        mapper: ProjectionBaselineMappingFunction,
    ) -> CatchUpResult<ProjectionConsumerCoverageSession<'consumer>> {
        let scope = std::mem::take(&mut self.scope);
        self.verify_complete_source_in(config, mapper, scope)
    }

    /// Bind exhaustive verification to the projection's declared source domain.
    /// Writes outside that domain may advance while this candidate is prepared.
    pub fn verify_complete_source_in(
        self,
        config: ProjectionCoverageConfig,
        mapper: ProjectionBaselineMappingFunction,
        scope: crate::ProjectionSourceScope,
    ) -> CatchUpResult<ProjectionConsumerCoverageSession<'consumer>> {
        let _descriptor_operation = self.base.descriptor_operation()?;
        let operations = self.session.sorting_report().input_operations;
        Ok(ProjectionConsumerCoverageSession {
            session: self
                .session
                .verify_complete_source_in(config, mapper, scope)?,
            base: self.base,
            operations,
        })
    }
}

/// Independent full-E enumeration and exact comparison against the held base.
pub struct ProjectionConsumerCoverageSession<'consumer> {
    session: ProjectionCoverageSession<'consumer>,
    base: Base,
    operations: usize,
}
impl ProjectionConsumerCoverageSession<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.session.source_epoch()
    }
    /// Cumulative native source work, including filtered records and descriptor
    /// decisions. This is independent of mapped output and publication counts.
    pub fn feed_usage(&self) -> crate::ProjectionFeedUsage {
        self.session.feed_usage()
    }
    pub fn advance(
        &mut self,
        feed: ProjectionFeedLimits,
        ordering: SearchMutationSortStepLimits,
    ) -> CatchUpResult<ProjectionCoverageStep> {
        let _descriptor_operation = self.base.descriptor_operation()?;
        Ok(self.session.advance(feed, ordering)?)
    }
    /// Consume only a complete comparison. The resulting owned token retains
    /// the compared records and native base pin, and borrows no Consumer handle.
    pub fn finish(self) -> CatchUpResult<ProjectionCoveredConsumerUpdate> {
        let _descriptor_operation = self.base.descriptor_operation()?;
        let (dependencies, documents) = self.session.finish_consumer()?;
        Ok(ProjectionCoveredConsumerUpdate {
            dependencies,
            documents,
            base: self.base,
            operations: self.operations,
        })
    }
}

/// Opaque complete-E content, exact registration/base and source dependencies.
/// There is no mapper, document replacement, or intermediate watermark escape.
pub struct ProjectionCoveredConsumerUpdate {
    dependencies: PreparedReadDependencies,
    documents: SearchComparedConsumerDocuments,
    base: Base,
    operations: usize,
}
impl ProjectionCoveredConsumerUpdate {
    pub fn source_epoch(&self) -> u64 {
        self.dependencies.source_epoch()
    }
    pub fn base_epoch(&self) -> u64 {
        self.base.receipt.source_epoch
    }
}

mod preparation;
pub use preparation::{
    ProjectionConsumerPreparation, ProjectionConsumerPublication, ProjectionPreparedConsumerUpdate,
};

impl Database {
    /// Begin source-coherent catch-up for one persistent Consumer. Every step
    /// remains private until complete independent E verification and native
    /// selection succeed. Dropping work keeps the last completed receipt.
    ///
    /// The starting epoch comes from the completed Consumer receipt. `task` carries
    /// no resource controllers; this method supplies the original database
    /// governor's admission. Cancellation/deadline state may be supplied.
    pub fn begin_search_projection_consumer_catch_up<'consumer>(
        &mut self,
        consumer: &'consumer SearchProjectionConsumer,
        config: ProjectionConsumerCatchUpConfig,
        mapper: ProjectionMappingFunction,
    ) -> CatchUpResult<ProjectionConsumerStagingSession<'consumer>> {
        self.begin_search_projection_consumer_catch_up_in(
            consumer,
            config,
            mapper,
            crate::ProjectionSourceScope::Database,
        )
    }

    /// Declare enumeration domains before delta hydration so tracked queries
    /// can use their proven label/table scopes instead of unrelated physical IDs.
    pub fn begin_search_projection_consumer_catch_up_in<'consumer>(
        &mut self,
        consumer: &'consumer SearchProjectionConsumer,
        config: ProjectionConsumerCatchUpConfig,
        mapper: ProjectionMappingFunction,
        scope: crate::ProjectionSourceScope,
    ) -> CatchUpResult<ProjectionConsumerStagingSession<'consumer>> {
        let scope_bytes = scope.retained_bytes();
        if scope_bytes > config.reads.dependency_bytes.get() {
            return Err(
                ProjectionFeedError::BudgetExceeded(crate::ProjectionFeedResource::Bytes).into(),
            );
        }
        let _descriptor_operation = self.internal_descriptor_operation()?;
        if consumer.search_index()?.vector_index_descriptor().is_some() {
            return Err(HawDBError::Semantic(
                "schema-bound vector indexes require rebuild_vector_index maintenance".into(),
            )
            .into());
        }
        self.consumer_writable_root()?;
        self.reload_consumer_registry_if_unavailable()?;
        self.validate_consumer_receipt(consumer)?;
        let receipt = consumer.projection().receipt();
        if config.task.has_resource_controllers() {
            return Err(
                HawDBError::Semantic("Consumer staging requires an unbound task".into()).into(),
            );
        }
        let mut config = ProjectionStagingSessionConfig {
            after_epoch: receipt.source_epoch,
            reads: config.reads,
            feed: config.feed,
            mapping: config.mapping,
            sorting: config.sorting,
            task: config.task,
            build: Default::default(),
        };
        let mut source =
            self.begin_prepared_projection_feed(config.after_epoch, config.reads, config.feed)?;
        source.record_source_scope(&scope)?;
        config.build.source_graph_commit_epoch = Some(source.source_epoch());
        // Source mappings materialize query results as well as build state.
        // Preserve the separate result admission instead of treating the
        // publication-only zero result reservation as unlimited query output.
        let permit = self.consumer_publication_permit_with_results(
            config.reads.statement_payload_bytes.get() as u64,
        )?;
        let pin = consumer.pin(&permit)?;
        let task = config.task.clone();
        let stage = consumer.projection().prepare_source_coherent_update(
            Arc::clone(&permit),
            std::mem::take(&mut config.build),
            config.sorting,
            task.clone(),
        )?;
        let bytes = std::mem::size_of::<ProjectionConsumerStagingSession<'_>>()
            .max(std::mem::size_of::<ProjectionConsumerCoverageSession<'_>>())
            .max(std::mem::size_of::<ProjectionCoveredConsumerUpdate>())
            .max(std::mem::size_of::<ProjectionConsumerPreparation<'_>>())
            .max(std::mem::size_of::<ProjectionPreparedConsumerUpdate<'_>>())
            .max(std::mem::size_of::<ProjectionConsumerPublication>())
            + receipt.binding.consumer_id.len()
            + receipt.sha256.len()
            + scope_bytes;
        let memory = stage.reserve_session_metadata(bytes)?;
        let session = ProjectionStagingSession::from_source(source, stage, config, mapper)?;
        Ok(ProjectionConsumerStagingSession {
            session,
            scope,
            base: Base {
                receipt,
                _pin: pin,
                _memory: memory,
                task,
                descriptor_context: self.runtime.file_descriptor_context(),
            },
        })
    }
}
