// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Registered Search lifecycle bound to the embedded source owner.

use super::*;
use crate::{
    DefinedProjectionStagingSession, ProjectionConsumerCatchUpConfig,
    ProjectionConsumerCatchUpError, ProjectionConsumerStagingSession,
    ProjectionCoveredConsumerUpdate, ProjectionMappingFunction, ProjectionPreparedConsumerUpdate,
    ProjectionSourceDurability, ProjectionSourceScope, SearchProjectionConsumer,
    SearchProjectionConsumerCatchUpReport, SearchProjectionConsumerId,
    SearchProjectionConsumerOptions, SearchProjectionConsumerReadiness,
    SearchProjectionConsumerResult, SearchProjectionConsumerStatus, SearchProjectionDefinition,
};

type CatchUpResult<T> = std::result::Result<T, ProjectionConsumerCatchUpError>;

/// One application-owned registration and its original embedded source owner.
/// Drop closes the runtime lease, not the durable registration. The source stays
/// open while this owner or one of its borrowed preparation sessions is alive.
/// This is an explicit registered projection; it does not adopt or replace a
/// legacy/full-residency or incremental projection configured on the store.
pub struct NowledgeMemSearchProjectionConsumer {
    consumer: SearchProjectionConsumer,
    owner: NowledgeMemEmbeddedStoreHandle,
}

/// One versioned projection definition and the embedded owner that acquired it.
/// Mapping callbacks and source domains are selected once at acquisition.
pub struct NowledgeMemDefinedSearchProjectionConsumer {
    consumer: NowledgeMemSearchProjectionConsumer,
    definition: SearchProjectionDefinition,
}

/// Native construction is complete; only fenced publication remains. This value
/// can publish through its original embedded owner, never a caller-selected one.
#[must_use]
pub struct NowledgeMemPreparedConsumerUpdate<'consumer> {
    prepared: ProjectionPreparedConsumerUpdate<'consumer>,
    owner: NowledgeMemEmbeddedStoreHandle,
}

impl NowledgeMemEmbeddedStoreHandle {
    /// Acquire a version/scope-bound registration using its own initializer.
    /// A changed definition requires a fresh target and explicit query cutover.
    pub fn open_or_create_defined_search_projection_consumer(
        &self,
        definition: SearchProjectionDefinition,
        projection_directory: impl AsRef<Path>,
        options: SearchProjectionConsumerOptions,
    ) -> SearchProjectionConsumerResult<NowledgeMemDefinedSearchProjectionConsumer> {
        let consumer = self.open_or_create_search_projection_consumer(
            definition.consumer_id().clone(),
            projection_directory,
            options,
            |source, index| definition.initialize(source, index),
        )?;
        Ok(NowledgeMemDefinedSearchProjectionConsumer {
            consumer,
            definition,
        })
    }

    /// Explicitly remove a logical registration by ID, including idempotent
    /// retries. This is an operation on the source owner, not permission for an
    /// old Consumer handle to revoke a newer incarnation with the same ID.
    /// Projection files remain governed by native cleanup roots.
    pub fn unregister_search_projection_consumer(
        &self,
        id: &SearchProjectionConsumerId,
    ) -> SearchProjectionConsumerResult<()> {
        self.write_store()?
            .graph_mut()
            .database_mut()
            .unregister_search_projection_consumer(id)
    }

    /// Reuse a stable registration, or initialize a new one from a pinned source.
    /// Existing legacy projection paths are never adopted. Initialization is a
    /// blocking bootstrap operation; subsequent catch-up uses bounded private
    /// steps and construction outside this handle's publication lock.
    pub fn open_or_create_search_projection_consumer<F>(
        &self,
        id: SearchProjectionConsumerId,
        projection_directory: impl AsRef<Path>,
        options: SearchProjectionConsumerOptions,
        initialize: F,
    ) -> SearchProjectionConsumerResult<NowledgeMemSearchProjectionConsumer>
    where
        F: FnOnce(&mut DatabaseReadTransaction, &mut SearchIndex) -> Result<()>,
    {
        // The database protocol performs its own admission using this graph's
        // configured governor. A second maintenance admission would refuse a
        // host configured with one background slot.
        let consumer = self
            .write_store()?
            .graph_mut()
            .database_mut()
            .open_or_create_search_projection_consumer(
                id,
                projection_directory,
                options,
                initialize,
            )?;
        Ok(NowledgeMemSearchProjectionConsumer {
            consumer,
            owner: self.clone(),
        })
    }
}

impl NowledgeMemDefinedSearchProjectionConsumer {
    pub fn definition(&self) -> &SearchProjectionDefinition {
        &self.definition
    }

    pub fn consumer(&self) -> &SearchProjectionConsumer {
        self.consumer.consumer()
    }

    pub fn read_permit(&self, request: RuntimeWorkRequest) -> Result<RuntimePermit> {
        self.consumer.read_permit(request)
    }

    pub fn readiness(
        &self,
        max_operations: Option<usize>,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumerReadiness> {
        self.consumer.readiness(max_operations)
    }

    pub fn begin_catch_up(
        &self,
        config: ProjectionConsumerCatchUpConfig,
    ) -> CatchUpResult<DefinedProjectionStagingSession<'_>> {
        self.consumer
            .owner
            .write_store()?
            .graph_mut()
            .database_mut()
            .begin_defined_search_projection_consumer_catch_up(
                &self.consumer.consumer,
                &self.definition,
                config,
            )
    }

    pub fn prepare_covered_update(
        &mut self,
        covered: ProjectionCoveredConsumerUpdate,
        durability: ProjectionSourceDurability,
    ) -> CatchUpResult<NowledgeMemPreparedConsumerUpdate<'_>> {
        self.consumer.prepare_covered_update(covered, durability)
    }

    pub fn resolve_publication(&mut self) -> SearchProjectionConsumerResult<()> {
        self.consumer.resolve_publication()
    }

    pub fn renew(&mut self) -> SearchProjectionConsumerResult<SearchProjectionConsumerStatus> {
        self.consumer.renew()
    }

    pub fn cleanup(
        &mut self,
        options: &crate::search::consumer::ConsumerCleanupOptions,
    ) -> SearchProjectionConsumerResult<crate::search::consumer::ConsumerCleanupReport> {
        self.consumer.cleanup(options)
    }
}

impl NowledgeMemSearchProjectionConsumer {
    /// Immutable access to verified receipts and the registered query surface.
    /// Store-level legacy query methods keep their explicitly configured target.
    pub fn consumer(&self) -> &SearchProjectionConsumer {
        &self.consumer
    }

    pub fn read_permit(&self, request: RuntimeWorkRequest) -> Result<RuntimePermit> {
        self.owner
            .write_store()?
            .graph_mut()
            .database_mut()
            .search_projection_consumer_read_permit(request)
    }

    pub fn readiness(
        &self,
        max_operations: Option<usize>,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumerReadiness> {
        self.owner
            .read_store()?
            .graph()
            .database()
            .search_projection_consumer_readiness(&self.consumer, max_operations)
    }

    /// Capture one complete C..E interval, then release the embedded owner.
    /// Advance and verify the returned session with the existing bounded mapper
    /// APIs. Relational hydration remains host-supplied, tracked SQL against E.
    /// A partial session has no public watermark. Dropping it cancels private
    /// progress; reopen/retry starts from the last durable completed receipt.
    pub fn begin_catch_up(
        &self,
        config: ProjectionConsumerCatchUpConfig,
        mapper: ProjectionMappingFunction,
        scope: ProjectionSourceScope,
    ) -> CatchUpResult<ProjectionConsumerStagingSession<'_>> {
        self.owner
            .write_store()?
            .graph_mut()
            .database_mut()
            .begin_search_projection_consumer_catch_up_in(&self.consumer, config, mapper, scope)
    }

    /// Reserve under the original owner and build after releasing its guard.
    /// The completed token retains that owner for publication or recovery.
    pub fn prepare_covered_update(
        &mut self,
        covered: ProjectionCoveredConsumerUpdate,
        durability: ProjectionSourceDurability,
    ) -> CatchUpResult<NowledgeMemPreparedConsumerUpdate<'_>> {
        let reserved = {
            let mut store = self.owner.write_store()?;
            store
                .graph_mut()
                .database_mut()
                .prepare_search_projection_consumer_covered_update(
                    &mut self.consumer,
                    covered,
                    durability,
                )?
        };
        Ok(NowledgeMemPreparedConsumerUpdate {
            prepared: reserved.prepare(),
            owner: self.owner.clone(),
        })
    }

    /// Reconcile original native custody after cancellation or a lost reply.
    /// This does not infer rollback from an error or validate an old outcome
    /// against a newer source snapshot.
    pub fn resolve_publication(&mut self) -> SearchProjectionConsumerResult<()> {
        self.owner
            .write_store()?
            .graph_mut()
            .database_mut()
            .resolve_search_projection_consumer_publication(&mut self.consumer)
    }

    pub fn renew(&mut self) -> SearchProjectionConsumerResult<SearchProjectionConsumerStatus> {
        self.owner
            .write_store()?
            .graph_mut()
            .database_mut()
            .renew_search_projection_consumer(&self.consumer)
    }

    pub fn cleanup(
        &mut self,
        options: &crate::search::consumer::ConsumerCleanupOptions,
    ) -> SearchProjectionConsumerResult<crate::search::consumer::ConsumerCleanupReport> {
        self.owner
            .write_store()?
            .graph_mut()
            .database_mut()
            .cleanup_search_projection_consumer(&self.consumer, options)
    }
}

impl NowledgeMemPreparedConsumerUpdate<'_> {
    pub fn source_epoch(&self) -> u64 {
        self.prepared.source_epoch()
    }

    pub fn publish(self) -> CatchUpResult<SearchProjectionConsumerCatchUpReport> {
        let published = {
            let mut store = self.owner.write_store()?;
            store
                .graph_mut()
                .database_mut()
                .publish_search_projection_consumer_prepared_update(self.prepared)?
        };
        // Old resident indexes and source/base pins retire after the embedded
        // write guard has refreshed its canonical read publication and dropped.
        Ok(published.into_report())
    }
}
