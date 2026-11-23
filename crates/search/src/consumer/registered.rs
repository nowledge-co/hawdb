// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Registered stationary generations. Construction and resident compatibility
//! loading finish before selection; the selector never moves a live directory.

use super::*;
use hawdb_qos::{RuntimePermit, RuntimeRetainedMemory};
use hawdb_storage::consumer_allocation::{ConsumerSelectionReceipt, ReservedConsumerAllocation};
use hawdb_storage::file_io::{CapturedDirectoryCandidate, SourceDirectory};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod recovery;
use recovery::{ProducerScratch, UnsealedRegisteredConsumer};
pub use recovery::{RegisteredConsumerError, RegisteredConsumerRecovery};

#[derive(Debug, Clone, Copy)]
pub struct RegisteredConsumerLimits {
    pub generation_bytes: NonZeroU64,
    pub resident_bytes: NonZeroU64,
    pub metadata: generation_metadata::MetadataLimits,
}
impl Default for RegisteredConsumerLimits {
    fn default() -> Self {
        Self {
            generation_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            resident_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            metadata: generation_metadata::MetadataLimits {
                encoded_bytes: 1024 * 1024,
                artifacts: 4096,
                artifact_bytes: 256 * 1024 * 1024,
            },
        }
    }
}

#[derive(Debug)]
/// Inputs retained while one immutable generation is prepared.
#[doc(hidden)]
pub struct RegisteredConsumerPreparation<'a> {
    pub locator: &'a Path,
    pub epoch: u64,
    pub permit: Arc<RuntimePermit>,
    pub task: RuntimeTaskContext,
    pub limits: RegisteredConsumerLimits,
    pub recovery_memory: RuntimeRetainedMemory,
    pub previous: Option<&'a ConsumerProjection>,
    pub projector: RegisteredConsumerProjector,
}

/// Chosen by the facade before invoking any host initializer. A builder's
/// completion marker alone cannot grant fixed-projector publication authority.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisteredConsumerProjector {
    HostMapping,
    CanonicalVectorField,
}

#[derive(Debug)]
pub(super) struct StationaryConsumer {
    pub(super) slot: ConsumerReadSlot,
    view: crate::out_of_core::CandidateView,
    pub(super) selected: ConsumerSelectionReceipt,
    pending: Option<Box<PreparedRegisteredConsumer>>,
    preparation: Option<RegisteredConsumerRecovery>,
    // The first directory continues to exclude a second logical owner after
    // selection advances to a sibling. No reader or publication lock is dropped
    // to make a namespace move possible.
    logical_owner: Arc<operation::Authority>,
    pub(super) parent: SourceDirectory,
    locator: PathBuf,
    _resident_memory: RuntimeRetainedMemory,
}

/// Complete immutable candidate plus its already loaded compatibility snapshot.
/// Dropping an unsuccessful candidate retains its durable birth/intent evidence.
#[doc(hidden)]
#[derive(Debug)]
pub struct PreparedRegisteredConsumer {
    candidate: ConsumerPreparedCandidate,
    index: SearchIndex,
    resident_memory: RuntimeRetainedMemory,
    parent: SourceDirectory,
    locator: PathBuf,
    slot: Option<ConsumerReadSlot>,
    scratch: Option<ProducerScratch>,
    ready: bool,
    recovery_memory: Option<RuntimeRetainedMemory>,
    logical_owner: Option<Arc<operation::Authority>>,
}

fn binding(
    value: hawdb_storage::consumer_allocation::ConsumerReservationBinding<'_>,
) -> ConsumerBinding {
    ConsumerBinding {
        database_uuid: value.database_uuid,
        projection_uuid: value.projection_uuid,
        consumer_id: value.consumer_id.to_owned(),
        registration_uuid: value.registration_uuid,
        checkpoint_uuid: value.checkpoint_uuid,
    }
}

impl ConsumerProjection {
    /// Create private bounded work from this exact completed native version.
    /// No ordinary reader, refresh, or serving capability escapes its admission.
    #[doc(hidden)]
    pub fn prepare_source_coherent_update(
        &self,
        permit: Arc<RuntimePermit>,
        options: crate::SearchOutOfCoreGenerationBuildOptions,
        sorting: crate::SearchUnorderedMutationLimits,
        task: RuntimeTaskContext,
    ) -> Result<crate::SearchOutOfCoreUnorderedMutationWriter<'_>> {
        self.stationary_receipt()?;
        self.stationary
            .as_ref()
            .expect("stationary owner")
            .view
            .prepare_consumer_update(permit, options, sorting, task)
    }

    pub fn registered_locator(&self) -> Result<&Path> {
        Ok(&self
            .stationary
            .as_ref()
            .ok_or_else(|| invalid("Consumer is not stationary"))?
            .locator)
    }

    pub fn stationary_receipt(&self) -> Result<&ConsumerSelectionReceipt> {
        let native = self
            .stationary
            .as_ref()
            .ok_or_else(|| invalid("Consumer is not stationary"))?;
        if native.pending.is_some() || native.preparation.is_some() {
            return Err(HawDBError::ProjectionBusy);
        }
        let report = native.slot.report()?;
        if report.source_epoch != native.selected.source_epoch()
            || report.metadata_reference != native.selected.metadata_reference()
        {
            return Err(HawDBError::ProjectionBusy);
        }
        Ok(&native.selected)
    }

    pub fn pin(&self, permit: &RuntimePermit) -> Result<ConsumerReadPin> {
        self.stationary_receipt()?;
        self.stationary
            .as_ref()
            .expect("stationary owner")
            .slot
            .pin(permit)
    }

    pub fn require_completed_view(&self) -> Result<()> {
        if self.stationary.is_some() {
            self.stationary_receipt()?;
        }
        self.authority.require_available()
    }

    /// The successor uses the actual original parent, even if its diagnostic
    /// label has been replaced. Its fresh reservation must share that FD domain.
    pub fn prepare_successor_birth(
        &self,
        allocation: ReservedConsumerAllocation,
    ) -> Result<PendingConsumerGeneration> {
        self.stationary_receipt()?;
        let native = self.stationary.as_ref().expect("stationary owner");
        allocation.begin_stationary_generation_captured(
            &native.parent,
            native
                .locator
                .parent()
                .ok_or_else(|| invalid("Consumer parent missing"))?,
        )
    }

    pub fn prepare_registered<F>(
        birth: PendingConsumerGeneration,
        preparation: RegisteredConsumerPreparation<'_>,
        initialize: F,
    ) -> std::result::Result<PreparedRegisteredConsumer, Box<RegisteredConsumerError>>
    where
        F: FnOnce(&mut SearchIndex) -> Result<()>,
    {
        let RegisteredConsumerPreparation {
            locator,
            epoch,
            permit,
            task,
            limits,
            recovery_memory,
            previous,
            projector,
        } = preparation;
        let mut cleanup = UnsealedRegisteredConsumer {
            birth: Some(birth),
            scratch: None,
            locator: locator.to_owned(),
            target_children: Vec::new(),
            cleaned: false,
            _memory: Some(recovery_memory),
        };
        let built = (|| -> Result<PreparedRegisteredConsumer> {
            if let Some(previous) = previous {
                previous.stationary_receipt()?;
            }
            let birth = cleanup.birth.as_mut().expect("preparing birth");
            birth.create()?;
            let reserved_binding = binding(birth.reservation_binding()?);
            let parent = birth
                .original_parent()?
                .duplicate(&birth.original_parent()?.operation_context())?;
            let target_label = locator
                .parent()
                .ok_or_else(|| invalid("Consumer target has no parent"))?
                .join(birth.generation_name());
            let born = birth.born_directory()?;
            let context = born.operation_context();
            cleanup.scratch = Some(ProducerScratch {
                candidate: born.prepare_directory_candidate(".consumer-producer")?,
                directory: None,
            });
            let scratch = cleanup.scratch.as_mut().expect("scratch owner");
            born.create_directory_candidate(&context, &mut scratch.candidate)?;
            scratch.directory = Some(born.open_created_directory(&context, &scratch.candidate)?);
            let scratch_label = target_label.join(scratch.candidate.name());
            let scratch_directory = scratch.directory.as_ref().expect("open scratch");
            let producer = initialize_producer(
                &scratch_label,
                scratch_directory,
                reserved_binding.clone(),
                epoch,
                ProducerControls {
                    permit: Arc::clone(&permit),
                    task: task.clone(),
                    generation_bytes: limits.generation_bytes,
                    projector,
                },
                previous,
                initialize,
            )?;
            if !producer
                .authority
                .publication()
                .with_captured_directory(|directory| directory.same_directory(scratch_directory))
            {
                return Err(invalid(
                    "Consumer producer no longer names its original native directory",
                ));
            }
            let crate::out_of_core::ConsumerProducerReferences {
                manifest,
                artifacts: references,
            } = crate::out_of_core::consumer_producer_references(producer.authority.publication())?;
            let artifacts = references
                .iter()
                .map(|(name, reference)| generation_metadata::ArtifactReference {
                    name,
                    reference: *reference,
                })
                .collect::<Vec<_>>();
            let identity = birth.reservation_binding()?;
            let input = generation_metadata::MetadataInput {
                binding: &reserved_binding,
                source_epoch: epoch,
                target_incarnation: identity.target_incarnation,
                generation: identity.generation,
                operation: identity.operation,
                component_uuid: reserved_binding.projection_uuid,
                search_manifest: manifest,
                artifacts: &artifacts,
            };
            let total = artifacts
                .iter()
                .try_fold(manifest.byte_length, |total, artifact| {
                    total
                        .checked_add(artifact.reference.byte_length)
                        .ok_or_else(|| invalid("Consumer generation length overflow"))
                })?
                .checked_add(input.encoded_len(limits.metadata, &task)? as u64)
                .ok_or_else(|| invalid("Consumer metadata length overflow"))?;
            if total > limits.generation_bytes.get() {
                return Err(invalid("Consumer generation exceeds configured byte bound"));
            }
            cleanup
                .target_children
                .extend(artifacts.iter().map(|artifact| artifact.name.to_owned()));
            cleanup.target_children.extend([
                crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE.to_owned(),
                GENERATION_METADATA_FILE.to_owned(),
                ".search_projection.publish.lock".to_owned(),
                ".search-source-process.lock".to_owned(),
            ]);
            let mut target = ConsumerStationaryTargetLease::acquire_governed(
                birth,
                Arc::clone(&permit),
                task.clone(),
            )?;
            {
                let mut output = target.begin_candidate_output(total)?;
                output.install_sealed_artifact_link(
                    scratch_directory,
                    crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE,
                    crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE,
                    manifest,
                )?;
                for artifact in &artifacts {
                    output.install_sealed_artifact_link(
                        scratch_directory,
                        artifact.name,
                        artifact.name,
                        artifact.reference,
                    )?;
                }
                output.seal_generation_metadata(&input, limits.metadata)?;
            }
            target.prepare_candidate_view(limits.generation_bytes)?;
            let detached = target.detach_candidate()?;
            let index_memory = resident_admission(&detached.lease, &permit, limits)?;
            let index = load_compatibility_snapshot(&detached.lease)?;
            // Runtime queries use the sealed native view. The compatibility index
            // comes from the same captured snapshot and cannot be independently mutated.
            if index.consumer_binding.as_ref() != Some(&reserved_binding)
                || index.source_graph_commit_epoch != Some(epoch)
            {
                return Err(invalid(
                    "Consumer resident compatibility snapshot differs from sealed candidate",
                ));
            }
            drop(producer);
            let birth = cleanup.birth.take().expect("attached birth");
            let candidate = match detached.attach_birth(birth) {
                Ok(candidate) => candidate,
                Err(error) => {
                    let (error, detached, birth) = error.into_parts();
                    drop(detached);
                    cleanup.birth = Some(birth);
                    return Err(error);
                }
            };
            Ok(PreparedRegisteredConsumer {
                candidate,
                index,
                resident_memory: index_memory,
                parent,
                locator: locator.to_owned(),
                slot: previous.and_then(|previous| {
                    previous
                        .stationary
                        .as_ref()
                        .map(|native| native.slot.clone())
                }),
                scratch: cleanup.scratch.take(),
                ready: false,
                recovery_memory: cleanup._memory.take(),
                logical_owner: previous.and_then(|previous| {
                    previous
                        .stationary
                        .as_ref()
                        .map(|native| Arc::clone(&native.logical_owner))
                }),
            })
        })();
        let mut prepared = match built {
            Ok(prepared) => prepared,
            Err(error) => {
                let recovery = match cleanup.retire() {
                    Ok(()) => None,
                    Err(_) => Some(RegisteredConsumerRecovery::Unsealed(Box::new(cleanup))),
                };
                return Err(Box::new(RegisteredConsumerError { error, recovery }));
            }
        };
        if let Err(error) = prepared.finish_preparation(&permit, task) {
            return Err(Box::new(RegisteredConsumerError {
                error,
                recovery: Some(RegisteredConsumerRecovery::Prepared(Box::new(prepared))),
            }));
        }
        Ok(prepared)
    }

    pub fn retain_pending_publication(
        &mut self,
        pending: PreparedRegisteredConsumer,
    ) -> Result<()> {
        let native = self
            .stationary
            .as_mut()
            .ok_or_else(|| invalid("Consumer is not stationary"))?;
        if native.pending.is_some() {
            return Err(HawDBError::ProjectionBusy);
        }
        native.pending = Some(Box::new(pending));
        Ok(())
    }

    pub fn retain_failed_preparation(
        &mut self,
        recovery: RegisteredConsumerRecovery,
    ) -> Result<()> {
        let native = self
            .stationary
            .as_mut()
            .ok_or_else(|| invalid("Consumer is not stationary"))?;
        if native.preparation.is_some() || native.pending.is_some() {
            return Err(HawDBError::ProjectionBusy);
        }
        native.preparation = Some(recovery);
        Ok(())
    }

    /// Reconcile the retained operation with fresh admission. Do not validate
    /// its old E against today's graph head: the original selector is authority.
    pub fn resolve_registered_publication(
        &mut self,
        permit: Arc<RuntimePermit>,
        task: RuntimeTaskContext,
    ) -> Result<()> {
        let native = self
            .stationary
            .as_mut()
            .ok_or_else(|| invalid("Consumer is not stationary"))?;
        if let Some(recovery) = native.preparation.take() {
            match recovery.retry(&permit, task.clone()) {
                Ok(Some(prepared)) => native.pending = Some(Box::new(prepared)),
                Ok(None) => {}
                Err(error) => {
                    let (error, recovery) = error.into_parts();
                    native.preparation = recovery;
                    return Err(error);
                }
            }
        }
        let Some(mut pending) = native.pending.take() else {
            return self.require_completed_view();
        };
        let outcome = (|| {
            pending
                .admit_selection(permit, task)?
                .resolve_or_cancel_unattempted()
        })();
        match outcome {
            Ok(hawdb_storage::consumer_allocation::ConsumerSelectionResolution::Published(
                receipt,
            )) => {
                if let Err(error) = pending.validate_completion(&receipt) {
                    self.stationary.as_mut().expect("stationary owner").pending = Some(pending);
                    return Err(error);
                }
                *self = pending.into_completed(*receipt);
                Ok(())
            }
            Ok(hawdb_storage::consumer_allocation::ConsumerSelectionResolution::NotSelected) => {
                Ok(())
            }
            Err(error) => {
                self.stationary.as_mut().expect("stationary owner").pending = Some(pending);
                Err(error)
            }
        }
    }

    pub fn open_registered(
        request: hawdb_storage::consumer_allocation::ConsumerReopenRequest,
        parent: SourceDirectory,
        locator: &Path,
        permit: Arc<RuntimePermit>,
        task: RuntimeTaskContext,
        limits: RegisteredConsumerLimits,
    ) -> Result<Self> {
        if task.has_resource_controllers() {
            return Err(invalid("Consumer open requires an unbound task"));
        }
        let controlled = permit.bind_task_context(task.clone());
        let memory = BuildMemory::new(&controlled)?;
        let control = memory
            .spool
            .reserve(hawdb_storage::consumer_allocation::CONSUMER_REOPEN_MEMORY_BOUND as usize)?;
        let io = controlled
            .acquire_io_wave(std::num::NonZeroUsize::MIN)
            .map_err(HawDBError::from_execution_error)?
            .ok_or_else(|| invalid("Consumer recovery requires I/O admission"))?;
        let target = request.capture_registered_target(&parent, locator, &controlled)?;
        let logical = SearchProjectionPublishLease::acquire_captured(target, &memory, &controlled)?;
        request.resolve_publication(
            &parent,
            locator,
            &controlled,
            |directory, bytes, receipt| {
                let metadata =
                    generation_metadata::MetadataView::decode(bytes, limits.metadata, &controlled)?;
                let reserved = receipt.binding();
                if !metadata.matches_binding(&binding(reserved))
                    || metadata.source_epoch() != receipt.source_epoch()
                    || metadata.target_incarnation() != reserved.target_incarnation
                    || metadata.generation() != reserved.generation
                    || metadata.operation() != reserved.operation
                {
                    return Err(HawDBError::StorageIntegrity(
                        "Consumer recovery metadata binding differs from original operation".into(),
                    ));
                }
                let artifacts = metadata.artifacts().collect::<Vec<_>>();
                let mut scratch = [0; crate::build_memory::SPOOL_BUFFER_BYTES];
                reopen::verify_generation_contents(
                    directory,
                    metadata.search_manifest(),
                    receipt.source_epoch(),
                    &artifacts,
                    &memory,
                    &controlled,
                    &mut scratch,
                )
            },
        )?;
        let original_parent = parent.duplicate(&parent.operation_context())?;
        drop(io);
        drop(control);
        let reopen = ConsumerReopenLimits {
            metadata: limits.metadata,
            artifact_read_bytes: limits.generation_bytes,
            generation_bytes: limits.generation_bytes,
        };
        let super::reopen::ReopenedConsumer {
            slot,
            publication,
            selected,
            logical,
        } = ConsumerReadSlot::reopen_retained(
            request,
            parent,
            Arc::clone(&permit),
            task,
            reopen,
            Some(logical),
        )?;
        let resident_memory = resident_admission(&publication, &permit, limits)?;
        let mut index = load_compatibility_snapshot(&publication)?;
        if index.consumer_binding.as_ref() != Some(&binding(selected.binding()))
            || index.source_graph_commit_epoch != Some(selected.source_epoch())
        {
            return Err(integrity(
                "reopened Consumer resident snapshot differs from selected generation",
            ));
        }
        let authority = operation::Authority::new(publication);
        index.consumer_owner = Arc::downgrade(&authority);
        let logical_owner = logical
            .map(operation::Authority::new)
            .unwrap_or_else(|| Arc::clone(&authority));
        let view = slot.completed_view()?;
        let native = StationaryConsumer {
            slot,
            view,
            selected,
            pending: None,
            preparation: None,
            logical_owner,
            parent: original_parent,
            locator: locator.to_owned(),
            _resident_memory: resident_memory,
        };
        let owner = ConsumerProjection {
            index,
            authority,
            pending_mutation: false,
            stationary: Some(native),
        };
        let receipt = owner.receipt();
        *owner
            .index
            .consumer_receipt
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(receipt);
        Ok(owner)
    }

    /// Populate a private candidate without changing the selected version or
    /// its completed cursor. The host's hydrator still runs against one pinned E.
    pub fn copy_completed_documents_into(&self, target: &mut SearchIndex) -> Result<()> {
        self.index()?;
        target.documents = self.index.documents.clone();
        target.embedding_manifest = self.index.embedding_manifest.clone();
        target.embedding_dimension = self.index.embedding_dimension;
        target.import_source_graph_commit_epoch = self.index.import_source_graph_commit_epoch;
        target.source_graph_commit_epoch = self.index.source_graph_commit_epoch;
        Ok(())
    }

    pub(crate) fn canonical_vector_summary(
        &self,
    ) -> Result<crate::vector_index::VectorDocumentSummary> {
        self.require_completed_view()?;
        let summary = self
            .index
            .vector_document_summary
            .ok_or_else(|| invalid("vector baseline has no logical summary"))?;
        if self.index.canonical_vector_source_epoch != self.index.source_graph_commit_epoch
            || self.index.canonical_vector_source_epoch.is_none()
            || summary.document_count != summary.vector_document_count
        {
            return Err(invalid(
                "vector baseline was not built by the fixed canonical projector",
            ));
        }
        Ok(summary)
    }

    fn write_vector_successor(
        &self,
        index: &SearchIndex,
        target: &SearchProjectionPublishLease,
        task: RuntimeTaskContext,
        generation_bytes: NonZeroU64,
        permit: Arc<RuntimePermit>,
    ) -> Result<crate::snapshot_writer::SearchSnapshotWriteReport> {
        self.canonical_vector_summary()?;
        let delta = index
            .canonical_vector_delta
            .as_ref()
            .ok_or_else(|| invalid("vector successor has no changes"))?;
        let binding = index
            .consumer_binding
            .as_ref()
            .ok_or_else(|| invalid("vector successor has no binding"))?;
        if self.index.canonical_vector_source_epoch != Some(delta.base_epoch)
            || self.index.vector_index_descriptor != index.vector_index_descriptor
            || binding.database_uuid != self.binding().database_uuid
            || binding.projection_uuid != self.binding().projection_uuid
            || binding.consumer_id != self.binding().consumer_id
        {
            return Err(invalid(
                "vector successor does not match its completed base",
            ));
        }
        self.stationary
            .as_ref()
            .ok_or_else(|| invalid("vector baseline is not stationary"))?
            .view
            .write_vector_successor(index, self, target, task, generation_bytes, permit)
    }

    pub(crate) fn seed_vector_producer(
        &self,
        target: &SearchProjectionPublishLease,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
        generation_bytes: u64,
    ) -> Result<()> {
        self.canonical_vector_summary()?;
        // Includes bounded decode, sorted reference collection and verification scratch.
        let _metadata = memory.spool.reserve(
            crate::out_of_core::MAX_OUT_OF_CORE_MANIFEST_BYTES as usize * 16
                + crate::build_memory::SPOOL_BUFFER_BYTES,
        )?;
        let references =
            crate::out_of_core::consumer_producer_references(self.authority.publication())?;
        let total = references.artifacts.iter().try_fold(
            references.manifest.byte_length,
            |total, (_, reference)| {
                total
                    .checked_add(reference.byte_length)
                    .ok_or_else(|| invalid("vector baseline byte length overflow"))
            },
        )?;
        if total > generation_bytes {
            return Err(invalid(
                "vector baseline exceeds successor generation admission",
            ));
        }
        let mut buffer = [0; crate::build_memory::SPOOL_BUFFER_BYTES];
        self.authority
            .publication()
            .with_captured_directory(|source| {
                target.with_captured_directory(|destination| {
                    let context = source.operation_context();
                    for (name, reference) in std::iter::once((
                        crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE,
                        references.manifest,
                    ))
                    .chain(
                        references
                            .artifacts
                            .iter()
                            .map(|(name, reference)| (name.as_str(), *reference)),
                    ) {
                        let file = open_verified_candidate_child(
                            source,
                            &context,
                            name,
                            reference,
                            task,
                            &mut buffer,
                            None,
                        )?;
                        source.link_child_new(&context, name, &file, destination, name)?;
                    }
                    destination.sync(&destination.operation_context())?;
                    Ok(())
                })
            })
    }
}

impl PreparedRegisteredConsumer {
    fn finish_preparation(
        &mut self,
        permit: &RuntimePermit,
        task: RuntimeTaskContext,
    ) -> Result<()> {
        if self.ready {
            return Ok(());
        }
        if self.slot.is_none() {
            self.slot = Some(self.candidate.create_read_slot(permit)?);
        }
        self.candidate.record_durable_seal()?;
        self.candidate.record_publication_intent()?;
        if self.candidate.selection.is_none() {
            self.candidate.prepare_selection(permit, task)?;
        }
        if self
            .candidate
            .selection
            .as_ref()
            .is_some_and(|selection| selection.read.is_none())
        {
            self.candidate
                .bind_read_slot(self.slot.as_ref().expect("read slot"), permit)?;
        }
        if let Some(scratch) = self.scratch.as_mut() {
            scratch.remove(self.candidate.birth.born_directory()?)?;
        }
        self.scratch = None;
        self.ready = true;
        // The prepared selector now funds retained recovery metadata. Never
        // retain a construction execution permit merely to keep cleanup alive.
        self.recovery_memory = None;
        Ok(())
    }

    pub fn selection_attempted(&self) -> bool {
        self.candidate
            .selection
            .as_ref()
            .is_some_and(|selection| selection.selection.native_selection_attempted())
    }

    pub fn report(&self) -> ConsumerCandidateViewReport {
        self.candidate.report()
    }
    pub fn reservation_binding(
        &self,
    ) -> Result<hawdb_storage::consumer_allocation::ConsumerReservationBinding<'_>> {
        self.candidate.reservation_binding()
    }
    pub fn admit_selection(
        &mut self,
        permit: Arc<RuntimePermit>,
        task: RuntimeTaskContext,
    ) -> Result<ConsumerSelectionAttempt<'_>> {
        if !self.ready {
            return Err(HawDBError::ProjectionBusy);
        }
        self.candidate.admit_selection(permit, task)
    }
    fn validate_completion(&self, receipt: &ConsumerSelectionReceipt) -> Result<()> {
        if !self.ready {
            return Err(HawDBError::ProjectionBusy);
        }
        if self
            .slot
            .as_ref()
            .ok_or(HawDBError::ProjectionBusy)?
            .report()?
            .metadata_reference
            != receipt.metadata_reference()
            || self.candidate.report().metadata_reference != receipt.metadata_reference()
        {
            return Err(invalid("registered Consumer completion differs"));
        }
        Ok(())
    }

    pub fn complete(
        self,
        receipt: ConsumerSelectionReceipt,
    ) -> std::result::Result<ConsumerProjection, Box<RegisteredConsumerError>> {
        if let Err(error) = self.validate_completion(&receipt) {
            return Err(Box::new(RegisteredConsumerError {
                error,
                recovery: Some(RegisteredConsumerRecovery::Prepared(Box::new(self))),
            }));
        }
        Ok(self.into_completed(receipt))
    }

    // Consume custody only after all fallible read-slot checks. A transient
    // report refusal must retain the same pending owner for explicit retry.
    fn into_completed(self, receipt: ConsumerSelectionReceipt) -> ConsumerProjection {
        let Self {
            candidate,
            mut index,
            resident_memory,
            parent,
            locator,
            slot,
            logical_owner,
            scratch: _,
            ready: _,
            recovery_memory: _,
        } = self;
        let ConsumerPreparedCandidate {
            detached,
            birth,
            selection,
        } = candidate;
        drop(selection);
        let authority = operation::Authority::new(detached.lease);
        index.consumer_owner = Arc::downgrade(&authority);
        let logical_owner = logical_owner.unwrap_or_else(|| Arc::clone(&authority));
        let native = StationaryConsumer {
            slot: slot.expect("completed read slot"),
            view: detached.view,
            selected: receipt,
            pending: None,
            preparation: None,
            parent,
            locator,
            logical_owner,
            _resident_memory: resident_memory,
        };
        drop(birth);
        let owner = ConsumerProjection {
            index,
            authority,
            pending_mutation: false,
            stationary: Some(native),
        };
        let receipt = owner.receipt();
        *owner
            .index
            .consumer_receipt
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(receipt);
        owner
    }
}

fn resident_admission(
    publication: &SearchProjectionPublishLease,
    permit: &RuntimePermit,
    limits: RegisteredConsumerLimits,
) -> Result<RuntimeRetainedMemory> {
    let mut file = publication.open_captured_child(SEARCH_SNAPSHOT_FILE)?;
    let mut header = [0u8; 4096];
    let count = file.read(&mut header)?;
    let end = header[..count]
        .windows(2)
        .position(|bytes| bytes == b"\n\n")
        .ok_or_else(|| invalid("Consumer snapshot header exceeds bound"))?;
    let header = std::str::from_utf8(&header[..end])
        .map_err(|_| invalid("invalid Consumer snapshot header"))?;
    let length = crate::parse_snapshot_header(header)?
        .uncompressed_len
        .ok_or_else(|| invalid("missing Consumer snapshot decode bound"))? as u64;
    // Covers parsed resident strings/map nodes and simultaneous decode buffers.
    // This is a conservative retained envelope, not a measured RSS value.
    let bound = length
        .checked_mul(32)
        .and_then(|bytes| bytes.checked_add(file.metadata().ok()?.len()))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .filter(|bytes| *bytes <= limits.resident_bytes.get())
        .ok_or_else(|| invalid("Consumer compatibility snapshot exceeds retained byte bound"))?;
    permit
        .reserve_retained_memory(bound)
        .map_err(HawDBError::from_execution_error)
}

fn remove_producer(
    parent: &SourceDirectory,
    directory: SourceDirectory,
    candidate: &mut CapturedDirectoryCandidate,
) -> Result<()> {
    let context = directory.operation_context();
    let _operation = context.internal_operation()?;
    directory.retire_cached_children();
    let mut names = Vec::new();
    directory.visit_children(&context, 4096, |name| {
        names.push(name.to_owned());
        Ok(())
    })?;
    for name in &names {
        directory.remove_child(&context, name)?;
    }
    drop(directory);
    parent.remove_created_directory(&parent.operation_context(), candidate)?;
    Ok(())
}

fn load_compatibility_snapshot(publication: &SearchProjectionPublishLease) -> Result<SearchIndex> {
    let mut index = SearchIndex::in_memory();
    index.load_snapshot_from_publication(publication)?;
    crate::out_of_core::validate_captured_resident_snapshot_identity(&index, publication)?;
    Ok(index)
}

struct ProducerControls {
    permit: Arc<RuntimePermit>,
    task: RuntimeTaskContext,
    generation_bytes: NonZeroU64,
    projector: RegisteredConsumerProjector,
}

fn initialize_producer<F>(
    label: &Path,
    directory: &SourceDirectory,
    binding: ConsumerBinding,
    epoch: u64,
    controls: ProducerControls,
    previous: Option<&ConsumerProjection>,
    initialize: F,
) -> Result<ConsumerProjection>
where
    F: FnOnce(&mut SearchIndex) -> Result<()>,
{
    let context = directory.operation_context();
    let task = controls.permit.bind_task_context(controls.task);
    let memory = BuildMemory::new(&task)?;
    let lease = SearchProjectionPublishLease::acquire_captured(
        directory.duplicate(&context)?,
        &memory,
        &task,
    )?;
    let authority = operation::Authority::new(lease);
    let operation = authority.acquire()?;
    let mut index = SearchIndex {
        consumer_owner: Arc::downgrade(&authority),
        consumer_mutation_generation: Some(operation.open_mutations()),
        ..SearchIndex::default()
    };
    {
        let scope = MutationScope {
            index: &mut index,
            operation,
        };
        // The initializer owns only a private builder and a read snapshot. Do
        // not unwind past its native cleanup owner after partial construction.
        // An interrupted initializer is never a publication acknowledgement.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| initialize(scope.index)))
            .map_err(|_| {
                HawDBError::Execution("Consumer initializer panicked before publication".into())
            })??;
    }
    if index.canonical_vector_source_epoch.is_some()
        != (controls.projector == RegisteredConsumerProjector::CanonicalVectorField)
    {
        return Err(invalid(
            "canonical vector completion differs from the admitted projector",
        ));
    }
    if !authority.owns(&index)
        || !index.consumer_binding_valid
        || index
            .canonical_vector_source_epoch
            .is_some_and(|source| source != epoch)
        || index.path.is_some()
        || index.consumer_binding.is_some()
        || index
            .import_source_graph_commit_epoch
            .is_some_and(|source| source > epoch)
        || *index.analyzer_lexicon != crate::SearchAnalyzerLexicon::default()
    {
        return Err(invalid("initializer replaced the private Consumer builder or supplied an invalid binding/analyzer"));
    }
    if !authority.publication().matches_source_directory(label)? {
        return Err(HawDBError::StorageIntegrity(
            "Consumer producer label no longer names its original native root".into(),
        ));
    }
    index.path = Some(label.to_owned());
    index.source_graph_commit_epoch = Some(epoch);
    index.consumer_binding = Some(binding);
    index._project_files =
        Some(hawdb_storage::file_descriptors::ProjectFileDescriptors::from_open_context(&context));
    let mut producer = ConsumerProjection {
        index,
        authority,
        pending_mutation: true,
        stationary: None,
    };
    context.with_component_context(label, || {
        if producer.index.vector_index_descriptor.is_some() {
            let snapshot = match producer.index.canonical_vector_delta.as_ref() {
                Some(_) => previous
                    .ok_or_else(|| invalid("vector delta has no selected base"))?
                    .write_vector_successor(
                        &producer.index,
                        producer.authority.publication(),
                        task,
                        controls.generation_bytes,
                        controls.permit,
                    )?,
                None => crate::SearchOutOfCoreGenerationWriter::write_consumer(
                    &producer.index,
                    producer.authority.publication(),
                    task,
                    controls.generation_bytes,
                    controls.permit,
                )?,
            };
            *producer
                .index
                .consumer_receipt
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(CheckpointReceipt {
                binding: producer
                    .index
                    .consumer_binding
                    .as_ref()
                    .expect("bound producer")
                    .clone(),
                source_epoch: epoch,
                encoded_len: snapshot.encoded_len,
                sha256: snapshot.encoded_sha256.to_string(),
            });
            *producer
                .index
                .durable_source_graph_commit_epoch
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(epoch);
            producer.pending_mutation = false;
            Ok(producer.receipt())
        } else {
            producer.checkpoint_inner_with_task(false, &task)
        }
    })??;
    if !producer
        .authority
        .publication()
        .matches_source_directory(label)?
    {
        return Err(HawDBError::StorageIntegrity(
            "Consumer producer namespace changed during construction".into(),
        ));
    }
    Ok(producer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    #[test]
    fn stationary_producer_cleanup_retries_native_failure_and_progresses_at_strict_capacity() {
        let root = std::env::temp_dir().join(format!(
            "hawdb-native-cleanup-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let project = ProjectFileDescriptors::acquire_with_capacity_limit(&root, 6, true).unwrap();
        let context = project.io_context();
        let parent = SourceDirectory::capture(&root, &context).unwrap();
        let mut candidate = parent.prepare_directory_candidate("producer").unwrap();
        parent
            .create_directory_candidate(&context, &mut candidate)
            .unwrap();
        let directory = parent.open_created_directory(&context, &candidate).unwrap();
        let mut file = directory.create_child_new(&context, "evidence").unwrap();
        file.write_all(b"unpublished bytes").unwrap();
        drop(file);
        let blocker = root.join("producer/unexpected-directory");
        std::fs::create_dir(&blocker).unwrap();
        std::fs::write(
            blocker.join("foreign-evidence"),
            b"retain on native failure",
        )
        .unwrap();
        assert!(remove_producer(&parent, directory, &mut candidate).is_err());
        assert_eq!(
            std::fs::read(blocker.join("foreign-evidence")).unwrap(),
            b"retain on native failure"
        );
        std::fs::remove_file(blocker.join("foreign-evidence")).unwrap();
        std::fs::remove_dir(blocker).unwrap();
        let directory = parent.open_created_directory(&context, &candidate).unwrap();
        match directory.remove_child(&context, "evidence") {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot reset cleanup evidence: {error}"),
        }
        let mut file = directory.create_child_new(&context, "evidence").unwrap();
        file.write_all(b"unpublished bytes").unwrap();
        drop(file);
        let mut held = Vec::new();
        for attempt in 0..=6 {
            assert!(
                attempt < 6,
                "strict capacity did not reject foreground opens"
            );
            match directory.open_child(&context, "evidence") {
                Ok(file) => held.push(file),
                Err(error) => {
                    assert!(matches!(
                        HawDBError::from(error),
                        HawDBError::FileDescriptors(_)
                    ));
                    break;
                }
            }
        }
        assert_eq!(project.metrics().open, 6);
        let rejections = project.metrics().budget_rejections;
        remove_producer(&parent, directory, &mut candidate).unwrap();
        assert!(!root.join("producer").exists());
        for file in &mut held {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, b"unpublished bytes");
        }
        assert_eq!(project.metrics().reserved, 0);
        assert_eq!(project.metrics().budget_rejections, rejections);
        assert!(project.metrics().high_water > 6);
        drop(held);
        drop(parent);
        drop(project);
        std::fs::remove_dir(root).unwrap();
    }
}
