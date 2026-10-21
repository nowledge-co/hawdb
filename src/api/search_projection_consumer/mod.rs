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

pub use hawdb_search::consumer::ConsumerReadPin as SearchProjectionConsumerReadPin;
pub(super) use hawdb_search::projection_consumer::ConsumerRegistry;
pub use hawdb_search::projection_consumer::{
    SearchProjectionConsumer, SearchProjectionConsumerCatchUpReport, SearchProjectionConsumerError,
    SearchProjectionConsumerId, SearchProjectionConsumerOptions, SearchProjectionConsumerReadiness,
    SearchProjectionConsumerRebuildReason, SearchProjectionConsumerResult,
    SearchProjectionConsumerState, SearchProjectionConsumerStatus,
};

use super::Database;
use crate::{
    DatabaseReadTransaction, HawDBError, Result, SearchIndex, SearchProjectionCatchUpReport,
    SearchProjectionChangeBatch, SearchProjectionRelationalDelta,
};
use hawdb_core::uuidv7::generate_uuidv7;
use hawdb_core::RuntimeTaskContext;
use hawdb_search::consumer::{
    CheckpointReceipt, ConsumerProjection, RegisteredConsumerLimits, RegisteredConsumerPreparation,
};
use hawdb_search::projection_consumer::{Record, MAX_CONSUMERS};
use hawdb_storage::file_io as fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use SearchProjectionConsumerError as Error;
use SearchProjectionConsumerRebuildReason as Reason;
use SearchProjectionConsumerState as State;

impl Database {
    /// Acquires one application-owned projection using a stable logical ID.
    ///
    /// Existing registrations are reopened and verified against this source and
    /// their exact projection/registration/checkpoint identity. Only an absent
    /// registration in a healthy registry permits initialization. An unavailable
    /// registry, invalid receipt, expiry, or lost replay window is never rebuilt
    /// implicitly. After an uncertain publication, reload the durable registry
    /// before deciding whether creation is necessary.
    ///
    /// The ID identifies the application's projection definition (for example,
    /// `knowledge-v1`); callers must change it when changing the mapping. There is
    /// no pathname alias or adoption of an existing unregistered projection.
    /// `options` and `initialize` apply only to first creation. Reopening does not
    /// renew the registration. Share the returned owner's immutable search index
    /// between query users; dropping it releases only its runtime lease.
    pub fn open_or_create_search_projection_consumer<F>(
        &mut self,
        id: SearchProjectionConsumerId,
        projection_directory: impl AsRef<Path>,
        options: SearchProjectionConsumerOptions,
        initialize: F,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumer>
    where
        F: FnOnce(&mut DatabaseReadTransaction, &mut SearchIndex) -> Result<()>,
    {
        self.open_or_create_consumer_with_capacity::<MAX_CONSUMERS, F>(
            id,
            projection_directory.as_ref(),
            options,
            initialize,
        )
    }

    fn open_or_create_consumer_with_capacity<const CAPACITY: usize, F>(
        &mut self,
        id: SearchProjectionConsumerId,
        projection_directory: &Path,
        options: SearchProjectionConsumerOptions,
        initialize: F,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumer>
    where
        F: FnOnce(&mut DatabaseReadTransaction, &mut SearchIndex) -> Result<()>,
    {
        self.consumer_writable_root()?;
        self.reload_consumer_registry_if_unavailable()?;
        {
            let runtime = self.runtime.get()?;
            let registry = &runtime.projection_consumers;
            if registry.unavailable {
                return Err(Error::RebuildRequired(Reason::RegistryUnavailable));
            }
            // A foreign registry must not become an empty local registry merely
            // because it has no record under the requested logical ID.
            if registry.database_uuid.is_some()
                && registry.database_uuid != runtime.store.search_projection_database_identity()
            {
                return Err(Error::RebuildRequired(Reason::DatabaseIdentityMismatch));
            }
        }
        if self
            .runtime
            .get()?
            .projection_consumers
            .pending
            .contains(id.as_str())
        {
            let destination = normalize_consumer_target(projection_directory)?;
            let permit = self.consumer_publication_permit()?;
            let (request, context) = {
                let runtime = self.runtime.get()?;
                (
                    runtime.store.prepare_search_consumer_reopen(id.as_str())?,
                    runtime
                        .store
                        .file_descriptor_context()
                        .ok_or(Error::SourceNotDurable)?,
                )
            };
            let parent = fs::SourceDirectory::capture(
                destination.parent().ok_or(Error::InvalidHandle)?,
                &context,
            )
            .map_err(HawDBError::from)?;
            let recovered = (|| {
                publication_failpoint(PublicationStage::BeforeReopen)?;
                ConsumerProjection::open_registered(
                    request,
                    parent,
                    &destination,
                    permit,
                    RuntimeTaskContext::default(),
                    RegisteredConsumerLimits::default(),
                )
            })();
            match recovered {
                Ok(projection) => drop(projection),
                Err(HawDBError::StorageIntegrity(_)) => {
                    return Err(Error::RebuildRequired(Reason::CheckpointMismatch))
                }
                Err(error) => return Err(error.into()),
            }
            self.reload_consumer_registry_if_unavailable()?;
        }
        match self.search_projection_consumer_status(&id) {
            Ok(_) => self.open_search_projection_consumer(&id, projection_directory),
            Err(Error::InvalidHandle) => self.create_consumer_with_capacity::<CAPACITY, F>(
                id,
                projection_directory,
                options,
                initialize,
            ),
            Err(error) => Err(error),
        }
    }

    /// Initializes a complete application projection from one pinned source
    /// snapshot. The initializer must propagate incomplete hydration and budget
    /// errors; it owns the mapping of graph and relational rows into documents.
    /// The initializer must retain the in-memory builder and its default analyzer;
    /// custom analyzer rules are not persisted by the current snapshot format.
    /// Unavailable publication returns `HawDBError::CapabilityUnavailable`
    /// with `RuntimeCapability::SearchProjectionPublication`
    /// before initialization, checkpoint, directory creation, or registration.
    pub fn create_search_projection_consumer<F>(
        &mut self,
        id: SearchProjectionConsumerId,
        projection_directory: impl AsRef<Path>,
        options: SearchProjectionConsumerOptions,
        initialize: F,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumer>
    where
        F: FnOnce(&mut DatabaseReadTransaction, &mut SearchIndex) -> Result<()>,
    {
        self.create_consumer_with_capacity::<MAX_CONSUMERS, F>(
            id,
            projection_directory.as_ref(),
            options,
            initialize,
        )
    }

    fn create_consumer_with_capacity<const CAPACITY: usize, F>(
        &mut self,
        id: SearchProjectionConsumerId,
        destination: &Path,
        options: SearchProjectionConsumerOptions,
        initialize: F,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumer>
    where
        F: FnOnce(&mut DatabaseReadTransaction, &mut SearchIndex) -> Result<()>,
    {
        let root = self.consumer_writable_root()?;
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::SearchProjectionPublication)?;
        self.reload_consumer_registry_if_unavailable()?;
        self.runtime
            .get()?
            .store
            .verify_search_consumer_allocation_evidence()?;
        let destination = normalize_consumer_target(destination)?;
        let destination = destination.as_path();
        let permit = self.consumer_publication_permit()?;
        if destination.try_exists().map_err(HawDBError::from)? {
            return Err(HawDBError::Storage(
                "consumer projection destination already exists".into(),
            )
            .into());
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.is_dir() {
            return Err(HawDBError::Storage("consumer projection parent must exist".into()).into());
        }
        let identity = self
            .runtime
            .get()?
            .store
            .search_projection_database_identity();
        let reinitialize_registry = self.runtime.get_mut()?.projection_consumers.unavailable
            || self
                .runtime
                .get_mut()?
                .projection_consumers
                .database_uuid
                .is_some_and(|registered_identity| Some(registered_identity) != identity);
        if !reinitialize_registry {
            if self
                .runtime
                .get_mut()?
                .projection_consumers
                .records
                .contains_key(id.as_str())
            {
                return Err(Error::AlreadyRegistered);
            }
            if self.runtime.get_mut()?.projection_consumers.records.len() >= CAPACITY {
                return Err(Error::RegistryFull);
            }
        }
        let expires_at_commit_epoch = self
            .runtime
            .get()?
            .store
            .commit_epoch()
            .checked_add(options.max_idle_commits().get())
            .ok_or_else(|| HawDBError::Semantic("consumer expiry epoch overflow".into()))?;
        let identity = self
            .runtime
            .get()?
            .store
            .search_projection_database_identity();
        let database_uuid = match identity {
            Some(identity) => identity,
            None => {
                let identity = generate_uuidv7()?;
                self.runtime
                    .get_mut()?
                    .store
                    .set_search_projection_database_identity(identity);
                identity
            }
        };
        // Also retry this checkpoint after an earlier identity publication error.
        self.checkpoint()?;
        if reinitialize_registry {
            self.runtime
                .get()?
                .store
                .forget_search_consumer_selection(id.as_str())?;
        }
        let allocation = self
            .runtime
            .get()?
            .store
            .reserve_registered_search_consumer(
                id.as_str(),
                destination,
                hawdb_storage::consumer_allocation::ConsumerLeasePolicy {
                    max_idle_commits: options.max_idle_commits().get(),
                    expires_at_commit_epoch,
                },
            )?;
        let registration_uuid = allocation.registration_uuid();
        let mut birth = allocation.prepare_initial_generation(parent)?;
        birth.create()?;
        let mut snapshot = self.begin_read_transaction()?;
        let epoch = snapshot.commit_epoch();
        let prepared = ConsumerProjection::prepare_registered(
            birth,
            RegisteredConsumerPreparation {
                locator: destination,
                epoch,
                permit: Arc::clone(&permit),
                task: RuntimeTaskContext::default(),
                limits: RegisteredConsumerLimits::default(),
                previous: None,
            },
            |index| initialize(&mut snapshot, index),
        );
        drop(snapshot);
        let mut prepared = prepared?;
        publication_failpoint(PublicationStage::AfterCheckpoint)?;
        self.runtime.get_mut()?.store.synchronize_source_prefix()?;
        let report = prepared.report();
        let receipt = {
            let runtime = self.runtime.get()?;
            super::require_search_projection_source_epoch(
                Some(report.source_epoch),
                runtime.store.commit_epoch(),
            )?;
            runtime.store.verify_search_consumer_candidate_source(
                prepared.reservation_binding()?,
                report.source_epoch,
                report.metadata_reference,
            )?;
            let mut attempt =
                prepared.admit_selection(Arc::clone(&permit), RuntimeTaskContext::default())?;
            publication_failpoint(PublicationStage::BeforeSelector)?;
            let pending = attempt.select()?;
            drop(runtime);
            publication_failpoint(PublicationStage::AfterSelector)?;
            let receipt = pending.finish()?;
            publication_failpoint(PublicationStage::AfterSelectorBarrier)?;
            receipt
        };
        let projection = prepared.complete(receipt)?;
        let receipt = projection.receipt();
        // Invalid foreign/corrupt records have no authority in this database.
        // Replace them only after explicit initialization actually succeeds.
        if reinitialize_registry {
            self.runtime.get_mut()?.projection_consumers = ConsumerRegistry::default();
        }
        self.runtime.get_mut()?.projection_consumers.database_uuid = Some(database_uuid);

        self.runtime.get_mut()?.projection_consumers.records.insert(
            id.as_str().into(),
            Record {
                checkpoint_uuid: receipt.binding.checkpoint_uuid.to_string(),
                durable_complete_through_epoch: receipt.source_epoch,
                expires_at_commit_epoch,
                id: id.as_str().into(),
                max_idle_commits: options.max_idle_commits().get(),
                projection_uuid: receipt.binding.projection_uuid.to_string(),
                registration_uuid: registration_uuid.to_string(),
                snapshot_encoded_len: receipt.encoded_len,
                snapshot_sha256: receipt.sha256,
            },
        );
        self.publish_consumer_registry(&root)?;
        self.runtime
            .get_mut()?
            .projection_consumers
            .verified
            .insert(id.as_str().into(), State::Active);
        Ok(SearchProjectionConsumer::from_projection(id, projection))
    }

    pub fn open_search_projection_consumer(
        &mut self,
        id: &SearchProjectionConsumerId,
        projection_directory: impl AsRef<Path>,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumer> {
        self.ensure_writable()?;
        self.reload_consumer_registry_if_unavailable()?;
        if !self
            .runtime
            .get()?
            .projection_consumers
            .pending
            .contains(id.as_str())
        {
            let status = self.search_projection_consumer_status(id)?;
            if let State::RebuildRequired(reason) = status.state {
                return Err(Error::RebuildRequired(reason));
            }
        }
        let destination = normalize_consumer_target(projection_directory.as_ref())?;
        if self
            .runtime
            .get()?
            .store
            .search_consumer_registry_snapshot()?
            .entries
            .iter()
            .any(|entry| {
                entry.receipt.binding().consumer_id == id.as_str()
                    && Path::new(entry.receipt.binding().target_locator) != destination
            })
        {
            return Err(Error::InvalidHandle);
        }
        let permit = self.consumer_publication_permit()?;
        let (request, context) = {
            let runtime = self.runtime.get()?;
            (
                runtime.store.prepare_search_consumer_reopen(id.as_str())?,
                runtime
                    .store
                    .file_descriptor_context()
                    .ok_or(Error::SourceNotDurable)?,
            )
        };
        let parent = fs::SourceDirectory::capture(
            destination.parent().ok_or(Error::InvalidHandle)?,
            &context,
        )
        .map_err(HawDBError::from)?;
        let projection = match (|| {
            publication_failpoint(PublicationStage::BeforeReopen)?;
            ConsumerProjection::open_registered(
                request,
                parent,
                &destination,
                permit,
                RuntimeTaskContext::default(),
                RegisteredConsumerLimits::default(),
            )
        })() {
            Ok(projection) => projection,
            Err(HawDBError::StorageIntegrity(_)) => {
                self.runtime
                    .get_mut()?
                    .projection_consumers
                    .verified
                    .insert(
                        id.as_str().into(),
                        State::RebuildRequired(Reason::CheckpointMismatch),
                    );
                return Err(Error::RebuildRequired(Reason::CheckpointMismatch));
            }
            Err(error) => return Err(error.into()),
        };
        let consumer = SearchProjectionConsumer::from_projection(id.clone(), projection);
        self.reload_consumer_registry_if_unavailable()?;
        self.reconcile_consumer_checkpoint(&consumer)?;
        if let Err(error) = self.validate_consumer_receipt(&consumer) {
            if let Error::RebuildRequired(reason) = &error {
                self.runtime
                    .get_mut()?
                    .projection_consumers
                    .verified
                    .insert(id.as_str().into(), State::RebuildRequired(reason.clone()));
            }
            return Err(error);
        }
        self.runtime
            .get_mut()?
            .projection_consumers
            .verified
            .insert(id.as_str().into(), State::Active);
        Ok(consumer)
    }

    pub fn catch_up_search_projection_consumer<F>(
        &mut self,
        consumer: &mut SearchProjectionConsumer,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        mut batch_hydrator: F,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumerCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        let root = self.consumer_writable_root()?;
        self.reload_consumer_registry_if_unavailable()?;
        self.validate_consumer_receipt(consumer)?;
        self.runtime
            .get_mut()?
            .projection_consumers
            .verified
            .insert(consumer.id().as_str().into(), State::Active);
        if max_change_operations_per_batch == 0
            || max_projection_operations_per_batch == 0
            || max_batches == 0
        {
            return Err(HawDBError::Semantic(
                "consumer catch-up budgets must be greater than zero".into(),
            )
            .into());
        }
        let start = consumer.search_index()?.projection_freshness();
        let graph_commit_epoch = self.runtime.get()?.store.commit_epoch();
        let mut applied_batch_count = 0usize;
        let mut applied_operation_count = 0usize;
        while applied_batch_count < max_batches {
            let Some(mut batch) = self.build_search_projection_change_batch_from_freshness(
                consumer.search_index()?,
                Some(max_change_operations_per_batch),
            )?
            else {
                break;
            };
            let operation_count = batch.operation_count();
            let mut snapshot = self.begin_read_transaction()?;
            super::require_search_projection_source_epoch(
                batch.complete_through_commit_epoch(),
                snapshot.commit_epoch(),
            )?;
            let hydrated = batch_hydrator(&mut snapshot, &batch)?;
            drop(snapshot);
            batch.graph_delta_mut().max_operations = Some(max_projection_operations_per_batch);
            publication_failpoint(PublicationStage::BeforeCheckpoint)?;
            let permit = self.consumer_publication_permit()?;
            let allocation = self
                .runtime
                .get()?
                .store
                .reserve_search_consumer_update(consumer.projection().stationary_receipt()?)?;
            let mut birth = consumer.projection().prepare_successor_birth(allocation)?;
            birth.create()?;
            let destination = consumer.projection().registered_locator()?.to_owned();
            let source_epoch = batch.complete_through_commit_epoch().ok_or_else(|| {
                HawDBError::StorageIntegrity("Consumer batch has no complete source epoch".into())
            })?;
            let mut prepared = ConsumerProjection::prepare_registered(
                birth,
                RegisteredConsumerPreparation {
                    locator: &destination,
                    epoch: source_epoch,
                    permit: Arc::clone(&permit),
                    task: RuntimeTaskContext::default(),
                    limits: RegisteredConsumerLimits::default(),
                    previous: Some(consumer.projection()),
                },
                |index| {
                    consumer.projection().copy_completed_documents_into(index)?;
                    self.apply_search_projection_change_batch(index, batch, hydrated)
                        .map(|_| ())
                },
            )?;
            let report = prepared.report();
            let selection = (|| -> Result<_> {
                publication_failpoint(PublicationStage::AfterCheckpoint)?;
                self.runtime.get_mut()?.store.synchronize_source_prefix()?;
                let runtime = self.runtime.get()?;
                super::require_search_projection_source_epoch(
                    Some(report.source_epoch),
                    runtime.store.commit_epoch(),
                )?;
                runtime.store.verify_search_consumer_candidate_source(
                    prepared.reservation_binding()?,
                    report.source_epoch,
                    report.metadata_reference,
                )?;
                let mut attempt =
                    prepared.admit_selection(Arc::clone(&permit), RuntimeTaskContext::default())?;
                publication_failpoint(PublicationStage::BeforeSelector)?;
                let pending = attempt.select()?;
                drop(runtime);
                publication_failpoint(PublicationStage::AfterSelector)?;
                let receipt = pending.finish()?;
                publication_failpoint(PublicationStage::AfterSelectorBarrier)?;
                Ok(receipt)
            })();
            let selected = match selection {
                Ok(receipt) => receipt,
                Err(error) => {
                    consumer
                        .projection_mut()
                        .retain_pending_publication(prepared)?;
                    return Err(error.into());
                }
            };
            *consumer.projection_mut() = prepared.complete(selected)?;
            let receipt = consumer.projection().receipt();
            self.acknowledge_consumer_checkpoint(consumer.id(), &receipt)?;
            self.publish_consumer_registry(&root)?;
            self.runtime
                .get_mut()?
                .projection_consumers
                .verified
                .insert(consumer.id().as_str().into(), State::Active);
            applied_batch_count = applied_batch_count.saturating_add(1);
            applied_operation_count = applied_operation_count.saturating_add(operation_count);
        }
        let end = consumer.search_index()?.projection_freshness();
        Ok(SearchProjectionConsumerCatchUpReport {
            catch_up: SearchProjectionCatchUpReport {
                graph_commit_epoch,
                start_applied_epoch: start.source_graph_commit_epoch,
                start_durable_epoch: start.durable_source_graph_commit_epoch,
                end_applied_epoch: end.source_graph_commit_epoch,
                end_durable_epoch: end.durable_source_graph_commit_epoch,
                applied_batch_count,
                applied_operation_count,
                complete: end.durable_source_graph_commit_epoch == Some(graph_commit_epoch),
            },
            consumer: self.search_projection_consumer_status(consumer.id())?,
        })
    }

    pub fn renew_search_projection_consumer(
        &mut self,
        consumer: &SearchProjectionConsumer,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumerStatus> {
        let root = self.consumer_writable_root()?;
        self.reload_consumer_registry_if_unavailable()?;
        self.validate_consumer_receipt(consumer)?;
        self.runtime
            .get_mut()?
            .projection_consumers
            .verified
            .insert(consumer.id().as_str().into(), State::Active);
        let epoch = self.runtime.get()?.store.commit_epoch();
        self.runtime
            .get()?
            .store
            .renew_search_consumer_lease(consumer.projection().stationary_receipt()?)?;
        {
            let mut runtime = self.runtime.get_mut()?;
            let record = runtime
                .projection_consumers
                .records
                .get_mut(consumer.id().as_str())
                .ok_or(Error::InvalidHandle)?;
            record.expires_at_commit_epoch = epoch
                .checked_add(record.max_idle_commits)
                .ok_or_else(|| HawDBError::Semantic("consumer expiry epoch overflow".into()))?;
        }
        self.publish_consumer_registry(&root)?;
        self.search_projection_consumer_status(consumer.id())
    }

    pub fn unregister_search_projection_consumer(
        &mut self,
        id: &SearchProjectionConsumerId,
    ) -> SearchProjectionConsumerResult<()> {
        self.ensure_writable()?;
        self.reload_consumer_registry_if_unavailable()?;
        let root = self
            .runtime
            .get()?
            .store
            .search_projection_registry_root()
            .ok_or(Error::SourceNotDurable)?
            .to_path_buf();
        if self.runtime.get_mut()?.projection_consumers.unavailable {
            return Err(Error::RebuildRequired(Reason::RegistryUnavailable));
        }
        self.runtime
            .get()?
            .store
            .forget_search_consumer_selection(id.as_str())?;
        if self
            .runtime
            .get_mut()?
            .projection_consumers
            .records
            .remove(id.as_str())
            .is_none()
        {
            return Ok(());
        }
        self.runtime
            .get_mut()?
            .projection_consumers
            .verified
            .remove(id.as_str());
        self.publish_consumer_registry(&root)
    }

    pub fn search_projection_consumer_status(
        &self,
        id: &SearchProjectionConsumerId,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumerStatus> {
        let native = self
            .runtime
            .get()?
            .store
            .search_consumer_registry_snapshot()?;
        if native.pending.iter().any(|pending| pending == id.as_str()) {
            return Err(HawDBError::ProjectionBusy.into());
        }
        if self.runtime.get()?.projection_consumers.unavailable {
            return Err(Error::RebuildRequired(Reason::RegistryUnavailable));
        }
        let runtime = self.runtime.get()?;
        let record = runtime
            .projection_consumers
            .records
            .get(id.as_str())
            .ok_or(Error::InvalidHandle)?;
        let changefeed = self
            .runtime
            .get()?
            .store
            .search_projection_changefeed_status();
        let identity = self
            .runtime
            .get()?
            .store
            .search_projection_database_identity();
        let state = self.runtime.get()?.projection_consumers.state(
            record,
            identity,
            changefeed.graph_commit_epoch,
            changefeed.resume_floor_commit_epoch,
        );
        let registry = &self.runtime.get()?.projection_consumers;
        let minimum = self
            .runtime
            .get()?
            .projection_consumers
            .records
            .values()
            .filter(|record| {
                registry.state(
                    record,
                    identity,
                    changefeed.graph_commit_epoch,
                    changefeed.resume_floor_commit_epoch,
                ) == State::Active
            })
            .map(|record| record.durable_complete_through_epoch)
            .min();
        Ok(SearchProjectionConsumerStatus {
            consumer_id: id.clone(),
            state,
            durable_complete_through_commit_epoch: Some(record.durable_complete_through_epoch),
            expires_at_commit_epoch: record.expires_at_commit_epoch,
            minimum_valid_consumer_commit_epoch: minimum,
            changefeed,
        })
    }

    pub fn search_projection_consumer_readiness(
        &self,
        consumer: &SearchProjectionConsumer,
        max_operations: Option<usize>,
    ) -> SearchProjectionConsumerResult<SearchProjectionConsumerReadiness> {
        let mut status = self.search_projection_consumer_status(consumer.id())?;
        match self.validate_consumer_receipt(consumer) {
            Ok(()) => {}
            Err(Error::RebuildRequired(reason)) => status.state = State::RebuildRequired(reason),
            Err(error) => return Err(error),
        }
        let freshness = consumer.search_index()?.projection_freshness();
        let changefeed = status.changefeed.readiness_after(
            freshness.source_graph_commit_epoch,
            freshness.durable_source_graph_commit_epoch,
            true,
            max_operations,
        );
        Ok(SearchProjectionConsumerReadiness {
            consumer: status,
            changefeed,
        })
    }

    /// Resolve a retained selection with fresh admission and mirror its actual
    /// completed receipt. Later graph writes do not invalidate the old outcome.
    pub fn resolve_search_projection_consumer_publication(
        &mut self,
        consumer: &mut SearchProjectionConsumer,
    ) -> SearchProjectionConsumerResult<()> {
        let root = self.consumer_writable_root()?;
        let permit = self.consumer_publication_permit()?;
        consumer
            .projection_mut()
            .resolve_registered_publication(permit, RuntimeTaskContext::default())?;
        self.reconcile_consumer_checkpoint(consumer)?;
        self.publish_consumer_registry(&root)
    }

    fn reconcile_consumer_checkpoint(
        &mut self,
        consumer: &SearchProjectionConsumer,
    ) -> SearchProjectionConsumerResult<()> {
        consumer.projection().stationary_receipt()?;
        self.reload_consumer_registry_if_unavailable()?;
        self.validate_consumer_receipt(consumer)?;
        if self.runtime.get()?.projection_consumers.needs_publication {
            let root = self.consumer_writable_root()?;
            self.publish_consumer_registry(&root)?;
        }
        Ok(())
    }

    /// Hosts can obtain query admission from the same database governor used
    /// for Consumer construction; a configured host governor is always reused.
    pub fn search_projection_consumer_read_permit(
        &mut self,
        request: hawdb_qos::RuntimeWorkRequest,
    ) -> Result<hawdb_qos::RuntimePermit> {
        self.consumer_governor()?
            .try_admit(request)
            .map_err(|error| HawDBError::Execution(error.to_string()))
    }

    fn consumer_governor(&mut self) -> Result<hawdb_qos::RuntimeGovernor> {
        let governor = self
            .retained_runtime
            .governor(2)
            .map_err(|error| HawDBError::Execution(error.to_string()))?
            .clone();
        if self.runtime_governor.is_none() {
            self.set_runtime_governor(governor.clone());
        }
        Ok(governor)
    }

    fn consumer_publication_permit(&mut self) -> Result<Arc<hawdb_qos::RuntimePermit>> {
        self.consumer_governor()?
            .try_admit(
                hawdb_qos::RuntimeWorkRequest::background_maintenance(64 * 1024 * 1024)
                    .with_io_wave_slots(1),
            )
            .map(Arc::new)
            .map_err(|error| HawDBError::Execution(error.to_string()))
    }

    fn reload_consumer_registry_if_unavailable(&mut self) -> SearchProjectionConsumerResult<()> {
        let mut runtime = self.runtime.get_mut()?;
        let identity = runtime.store.search_projection_database_identity();
        let snapshot = runtime.store.search_consumer_registry_snapshot()?;
        let mut recovered = ConsumerRegistry::from_native(snapshot, identity)?;
        let previous = &runtime.projection_consumers;
        recovered.needs_publication = previous.needs_publication
            || previous.unavailable
            || previous.database_uuid != identity
            || previous.records != recovered.records;
        for (id, state) in &previous.verified {
            if previous.records.get(id) == recovered.records.get(id)
                && recovered.records.contains_key(id)
            {
                recovered.verified.insert(id.clone(), state.clone());
            }
        }
        runtime.projection_consumers = recovered;
        Ok(())
    }

    fn consumer_writable_root(&self) -> SearchProjectionConsumerResult<PathBuf> {
        self.ensure_writable()?;
        if self.runtime.get()?.store.wal_sync_group_active() {
            return Err(Error::SourceNotDurable);
        }
        self.runtime
            .get()?
            .store
            .search_projection_registry_root()
            .map(Path::to_path_buf)
            .ok_or(Error::SourceNotDurable)
    }

    fn validate_consumer_receipt(
        &self,
        consumer: &SearchProjectionConsumer,
    ) -> SearchProjectionConsumerResult<()> {
        consumer.projection().require_completed_view()?;
        let status = self.search_projection_consumer_status(consumer.id())?;
        let runtime = self.runtime.get()?;
        let record = runtime
            .projection_consumers
            .records
            .get(consumer.id().as_str())
            .ok_or(Error::InvalidHandle)?;
        let binding = consumer.projection().binding();
        if Some(binding.database_uuid)
            != self
                .runtime
                .get()?
                .store
                .search_projection_database_identity()
        {
            return Err(Error::RebuildRequired(Reason::DatabaseIdentityMismatch));
        }
        if binding.consumer_id != consumer.id().as_str()
            || binding.registration_uuid.to_string() != record.registration_uuid
        {
            return Err(Error::InvalidHandle);
        }
        if binding.projection_uuid.to_string() != record.projection_uuid {
            return Err(Error::RebuildRequired(Reason::ProjectionIdentityMismatch));
        }
        if let State::RebuildRequired(reason) = status.state {
            return Err(Error::RebuildRequired(reason));
        }
        let receipt = consumer.projection().receipt();
        if receipt.binding.checkpoint_uuid.to_string() != record.checkpoint_uuid
            || receipt.encoded_len != record.snapshot_encoded_len
            || receipt.sha256 != record.snapshot_sha256
            || receipt.source_epoch != record.durable_complete_through_epoch
        {
            return Err(Error::RebuildRequired(Reason::CheckpointMismatch));
        }
        if !consumer.projection().binding_is_valid() {
            return Err(Error::RebuildRequired(Reason::UntrackedProjectionMutation));
        }
        let freshness = consumer.search_index()?.projection_freshness();
        if freshness.source_graph_commit_epoch != Some(receipt.source_epoch)
            || freshness.has_uncheckpointed_changes
        {
            return Err(Error::RebuildRequired(Reason::UntrackedProjectionMutation));
        }
        Ok(())
    }

    fn acknowledge_consumer_checkpoint(
        &mut self,
        id: &SearchProjectionConsumerId,
        receipt: &CheckpointReceipt,
    ) -> SearchProjectionConsumerResult<()> {
        let native = self
            .runtime
            .get()?
            .store
            .search_consumer_registry_snapshot()?;
        let actual = native
            .entries
            .iter()
            .find(|entry| entry.receipt.binding().consumer_id == id.as_str())
            .ok_or(Error::RebuildRequired(Reason::CheckpointMismatch))?;
        let binding = actual.receipt.binding();
        let metadata = actual.receipt.metadata_reference();
        if receipt.binding.database_uuid != binding.database_uuid
            || receipt.binding.registration_uuid != binding.registration_uuid
            || receipt.binding.projection_uuid != binding.projection_uuid
            || receipt.binding.checkpoint_uuid != binding.checkpoint_uuid
            || receipt.source_epoch != actual.receipt.source_epoch()
            || receipt.encoded_len != metadata.byte_length
            || receipt.sha256 != metadata.sha256.to_string()
        {
            return Err(Error::RebuildRequired(Reason::CheckpointMismatch));
        }
        let identity = self
            .runtime
            .get()?
            .store
            .search_projection_database_identity();
        let epoch = self.runtime.get()?.store.commit_epoch();
        let mut runtime = self.runtime.get_mut()?;
        let record = runtime
            .projection_consumers
            .records
            .get_mut(id.as_str())
            .ok_or(Error::InvalidHandle)?;
        if Some(receipt.binding.database_uuid) != identity
            || receipt.binding.consumer_id != id.as_str()
            || receipt.binding.registration_uuid.to_string() != record.registration_uuid
            || receipt.binding.projection_uuid.to_string() != record.projection_uuid
            || receipt.source_epoch > epoch
            || receipt.source_epoch < record.durable_complete_through_epoch
            || (receipt.source_epoch == record.durable_complete_through_epoch
                && (receipt.binding.checkpoint_uuid.to_string() != record.checkpoint_uuid
                    || receipt.encoded_len != record.snapshot_encoded_len
                    || receipt.sha256 != record.snapshot_sha256))
        {
            return Err(Error::RebuildRequired(Reason::CheckpointMismatch));
        }
        record.checkpoint_uuid = receipt.binding.checkpoint_uuid.to_string();
        record.durable_complete_through_epoch = receipt.source_epoch;
        record.snapshot_encoded_len = receipt.encoded_len;
        record.snapshot_sha256 = receipt.sha256.clone();
        Ok(())
    }

    fn publish_consumer_registry(&mut self, _root: &Path) -> SearchProjectionConsumerResult<()> {
        let mut runtime = self.runtime.get_mut()?;
        let directory = runtime
            .store
            .captured_search_consumer_registry_directory()?;
        if let Err(error) = runtime.projection_consumers.publish_captured(
            &directory,
            || publication_failpoint(PublicationStage::BeforeRegistry),
            || publication_failpoint(PublicationStage::AfterRegistry),
        ) {
            runtime.projection_consumers.unavailable = true;
            return Err(error.into());
        }
        runtime.projection_consumers.needs_publication = false;
        Ok(())
    }
}

fn normalize_consumer_target(destination: &Path) -> Result<PathBuf> {
    let name = destination.file_name().ok_or_else(|| {
        HawDBError::Semantic("Consumer target requires a final child name".into())
    })?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(parent.canonicalize()?.join(name))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationStage {
    BeforeReopen,
    BeforeCheckpoint,
    AfterCheckpoint,
    BeforeSelector,
    AfterSelector,
    AfterSelectorBarrier,
    BeforeRegistry,
    AfterRegistry,
}

#[cfg(test)]
thread_local! {
    static PUBLICATION_FAILURE: std::cell::Cell<Option<(PublicationStage, bool)>> = const { std::cell::Cell::new(None) };
}

fn publication_failpoint(_stage: PublicationStage) -> Result<()> {
    #[cfg(test)]
    if let Some((stage, crash)) = PUBLICATION_FAILURE.get()
        && stage == _stage
    {
        PUBLICATION_FAILURE.set(None);
        if crash {
            std::process::exit(86);
        }
        return Err(HawDBError::Storage(format!(
            "injected consumer publication failure at {stage:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
