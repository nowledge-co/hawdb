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
use hawdb_search::consumer::{CheckpointReceipt, ConsumerBinding, ConsumerProjection};
use hawdb_search::projection_consumer::{Record, MAX_CONSUMERS};
use hawdb_storage::file_io as fs;
use std::path::{Path, PathBuf};
use SearchProjectionConsumerError as Error;
use SearchProjectionConsumerRebuildReason as Reason;
use SearchProjectionConsumerState as State;

impl Database {
    /// Initializes a complete application projection from one pinned source
    /// snapshot. The initializer must propagate incomplete hydration and budget
    /// errors; it owns the mapping of graph and relational rows into documents.
    /// The initializer must retain the staging index and its default analyzer;
    /// custom analyzer rules are not persisted by the current snapshot format.
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
        let root = self.consumer_writable_root()?;
        if self.runtime.get_mut()?.projection_consumers.unavailable {
            self.runtime.get_mut()?.projection_consumers = ConsumerRegistry::load(
                Some(&root),
                self.runtime
                    .get()?
                    .store
                    .search_projection_database_identity(),
            );
        }
        let destination = projection_directory.as_ref();
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
            if self.runtime.get_mut()?.projection_consumers.records.len() >= MAX_CONSUMERS {
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
        let database_uuid = match self
            .runtime
            .get()?
            .store
            .search_projection_database_identity()
        {
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
        let registration_uuid = generate_uuidv7()?;
        let binding = ConsumerBinding {
            database_uuid,
            projection_uuid: generate_uuidv7()?,
            consumer_id: id.as_str().into(),
            registration_uuid,
            checkpoint_uuid: generate_uuidv7()?,
        };
        let stage = parent.join(format!(".hawdb-consumer-{registration_uuid}.stage"));
        fs::create_dir(&stage).map_err(HawDBError::from)?;
        let stage_guard = StageDirectory(stage.clone());
        let mut snapshot = self.begin_read_transaction()?;
        let epoch = snapshot.commit_epoch();
        let initialized = (|| -> Result<ConsumerProjection> {
            let mut projection = ConsumerProjection::initialize(&stage, binding, epoch, |index| {
                initialize(&mut snapshot, index)
            })?;
            publication_failpoint(PublicationStage::AfterCheckpoint)?;
            projection.publish_directory(destination)?;
            Ok(projection)
        })();
        drop(snapshot);
        let projection = match initialized {
            Ok(projection) => {
                stage_guard.cleanup()?;
                projection
            }
            Err(error) => {
                if let Err(cleanup) = stage_guard.cleanup() {
                    // An exhausted cleanup must retain the stage and remain
                    // retryable even if initialization failed for another reason.
                    if let Some(resource) = hawdb_core::error::file_descriptor_error(&error)
                        .or_else(|| hawdb_core::error::file_descriptor_error(&cleanup))
                    {
                        return Err(HawDBError::FileDescriptors(resource).into());
                    }
                    return Err(HawDBError::Storage(format!("consumer initialization failed: {error}; staging cleanup failed: {cleanup}")).into());
                }
                return Err(error.into());
            }
        };
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
        let status = self.search_projection_consumer_status(id)?;
        if let State::RebuildRequired(reason) = status.state {
            return Err(Error::RebuildRequired(reason));
        }
        let projection = match ConsumerProjection::open(projection_directory.as_ref()) {
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
        let start = consumer.search_index().projection_freshness();
        let graph_commit_epoch = self.runtime.get()?.store.commit_epoch();
        let mut applied_batch_count = 0usize;
        let mut applied_operation_count = 0usize;
        while applied_batch_count < max_batches {
            let Some(mut batch) = self.build_search_projection_change_batch_from_freshness(
                consumer.search_index(),
                Some(max_change_operations_per_batch),
            )?
            else {
                break;
            };
            let operation_count = batch.operation_count();
            let mut snapshot = self.begin_read_transaction()?;
            let hydrated = batch_hydrator(&mut snapshot, &batch)?;
            drop(snapshot);
            batch.graph_delta_mut().max_operations = Some(max_projection_operations_per_batch);
            consumer
                .projection_mut()
                .apply(|index| self.apply_search_projection_change_batch(index, batch, hydrated))?;
            // Any error from this point leaves an applied or durable projection
            // that must not be silently paired with the previous cursor receipt.
            self.runtime
                .get_mut()?
                .projection_consumers
                .verified
                .insert(
                    consumer.id().as_str().into(),
                    State::RebuildRequired(Reason::CheckpointMismatch),
                );
            publication_failpoint(PublicationStage::BeforeCheckpoint)?;
            let receipt = consumer.projection_mut().checkpoint()?;
            publication_failpoint(PublicationStage::AfterCheckpoint)?;
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
        let end = consumer.search_index().projection_freshness();
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
        self.validate_consumer_receipt(consumer)?;
        self.runtime
            .get_mut()?
            .projection_consumers
            .verified
            .insert(consumer.id().as_str().into(), State::Active);
        let epoch = self.runtime.get()?.store.commit_epoch();
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
        if self.runtime.get()?.projection_consumers.unavailable {
            return Err(Error::RebuildRequired(Reason::RegistryUnavailable));
        }
        let record = self
            .runtime
            .get()?
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
        let freshness = consumer.search_index().projection_freshness();
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
        let status = self.search_projection_consumer_status(consumer.id())?;
        let record = self
            .runtime
            .get()?
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
        let freshness = consumer.search_index().projection_freshness();
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

    fn publish_consumer_registry(&mut self, root: &Path) -> SearchProjectionConsumerResult<()> {
        let mut runtime = self.runtime.get_mut()?;
        if let Err(error) = runtime.projection_consumers.publish(
            root,
            || publication_failpoint(PublicationStage::BeforeRegistry),
            || publication_failpoint(PublicationStage::AfterRegistry),
        ) {
            runtime.projection_consumers.unavailable = true;
            return Err(error.into());
        }
        Ok(())
    }
}

struct StageDirectory(PathBuf);
impl StageDirectory {
    fn cleanup(self) -> Result<()> {
        match fs::remove_dir_all(&self.0) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
impl Drop for StageDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationStage {
    BeforeCheckpoint,
    AfterCheckpoint,
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
