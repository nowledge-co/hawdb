// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Original-handle birth evidence. This does not select a serving generation.

use super::*;
use crate::file_io::CapturedDirectoryCandidate;
use crate::immutable_object::{ObjectKind, ObjectReference};
use std::sync::atomic::{AtomicBool, Ordering};

mod retirement;
use retirement::RetirementStep;

const BIRTH_FILE: &str = "consumer-birth.hawdb";
const BIRTH_HEADER: &[u8] = b"HAWDB_CONSUMER_BIRTH_V1\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepared,
    Attempted,
    Recorded,
    Settling,
    Settled,
    Retiring,
    Retired,
}

/// A single native attempt and its durable debt. Drop never removes a name or
/// refunds identities. The caller must exclude mutation of its private name;
/// serving publication additionally requires the target publication lease.
/// Neither an installed marker nor decoded ledger bytes can construct this.
#[doc(hidden)]
#[derive(Debug)]
pub struct PendingConsumerGeneration {
    allocation: Option<ReservedConsumerAllocation>,
    parent: Option<SourceDirectory>,
    candidate: CapturedDirectoryCandidate,
    directory: Option<SourceDirectory>,
    marker: Option<File>,
    marker_bytes: Vec<u8>,
    parent_locator: String,
    phase: Phase,
    settlement_publication_attempted: bool,
    candidate_seal_attempted: bool,
    publication_intent_attempted: bool,
    publication_intent: Option<PublicationIntent>,
    selection_preparation_attempted: bool,
    producer_exposed: AtomicBool,
    retirement_step: RetirementStep,
}

impl ReservedConsumerAllocation {
    /// Capture the original target parent once, using the existing source FD
    /// domain, and persist birth intent before any native directory effect.
    /// This is for stationary producers; the legacy relocating stage is not
    /// promoted to a stationary generation by this method.
    pub fn prepare_stationary_generation(
        self,
        original_parent: &Path,
    ) -> Result<PendingConsumerGeneration> {
        self.prepare_generation(original_parent, None, None)
    }

    /// Create the initial immutable generation at the configured target itself.
    /// The recorded birth, rather than a directory move, establishes ownership.
    pub fn prepare_initial_generation(
        self,
        original_parent: &Path,
    ) -> Result<PendingConsumerGeneration> {
        let name = Path::new(&self.target_locator)
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| invalid("initial target requires a child name"))?
            .to_owned();
        self.prepare_generation(original_parent, Some(name), None)
    }

    pub fn prepare_stationary_generation_captured(
        self,
        parent: &SourceDirectory,
        label: &Path,
    ) -> Result<PendingConsumerGeneration> {
        self.prepare_generation(label, None, Some(parent))
    }

    fn prepare_generation(
        self,
        original_parent: &Path,
        initial_name: Option<String>,
        captured: Option<&SourceDirectory>,
    ) -> Result<PendingConsumerGeneration> {
        let parent_locator = original_parent
            .to_str()
            .filter(|value| !value.is_empty() && value.len() <= 16 * 1024)
            .ok_or_else(|| invalid("birth parent locator must be bounded UTF-8"))?
            .to_owned();
        let context = self._source.captured_directory().operation_context();
        let parent = match captured {
            Some(parent) => parent.duplicate(&context)?,
            None => SourceDirectory::capture(original_parent, &context)?,
        };
        let name = initial_name.unwrap_or_else(|| {
            generation_name_for(
                self.domain,
                self.incarnation.0,
                self.generation.0,
                self.operation.0,
            )
        });
        let candidate = parent.prepare_directory_candidate(&name)?;
        // Prepare all bytes before installing intent. An uncertain intent
        // publication must not be followed by mkdir through a returned token.
        let marker_bytes = self.with_row(|row| {
            if row.native_birth != NativeBirth::Reserved {
                return Err(invalid("reservation already has a native attempt"));
            }
            marker_bytes(self.domain, row, &name, &parent_locator)
        })?;
        self.transition_birth(
            NativeBirth::Reserved,
            NativeBirth::Pending {
                generation_name: name,
                parent_locator: parent_locator.clone(),
            },
        )?;
        Ok(PendingConsumerGeneration {
            allocation: Some(self),
            parent: Some(parent),
            candidate,
            directory: None,
            marker: None,
            marker_bytes,
            parent_locator,
            phase: Phase::Prepared,
            settlement_publication_attempted: false,
            candidate_seal_attempted: false,
            publication_intent_attempted: false,
            publication_intent: None,
            selection_preparation_attempted: false,
            producer_exposed: AtomicBool::new(false),
            retirement_step: RetirementStep::Intent,
        })
    }

    pub(super) fn require_row(&self, row: &Reservation) -> Result<()> {
        if row.operation != self.operation.0
            || row.target_incarnation != self.incarnation.0
            || row.generation != self.generation.0
            || row.branch_id != self.source_branch.as_uuid().to_string()
            || row.database_uuid != self.database.to_string()
            || row.consumer_id != self.consumer_id
            || row.target_locator != self.target_locator
            || row.registration_uuid != self.registration.to_string()
            || row.projection_uuid != self.projection.to_string()
            || row.checkpoint_uuid != self.checkpoint.to_string()
        {
            return Err(invalid("native attempt reservation binding differs"));
        }
        Ok(())
    }

    fn with_row<T>(&self, action: impl FnOnce(&Reservation) -> Result<T>) -> Result<T> {
        self._project.with_locked_catalog(|root, context, catalog| {
            let ledger = check_source(
                &self._project,
                &self._source,
                self.source_branch,
                root,
                context,
                &catalog,
            )?;
            let row = ledger
                .pending
                .iter()
                .find(|row| row.operation == self.operation.0)
                .ok_or_else(|| invalid("native attempt reservation is missing"))?;
            self.require_row(row)?;
            action(row)
        })
    }

    fn transition_birth(&self, expected: NativeBirth, next: NativeBirth) -> Result<()> {
        self.transition_birth_with_retry(expected, next, false)
    }

    fn transition_birth_with_retry(
        &self,
        expected: NativeBirth,
        next: NativeBirth,
        retry_selected: bool,
    ) -> Result<()> {
        self._project.with_locked_catalog(|root, context, catalog| {
            let mut ledger = check_source(
                &self._project,
                &self._source,
                self.source_branch,
                root,
                context,
                &catalog,
            )?;
            let row = ledger
                .pending
                .iter_mut()
                .find(|row| row.operation == self.operation.0)
                .ok_or_else(|| invalid("native attempt reservation is missing"))?;
            self.require_row(row)?;
            if retry_selected && row.native_birth == next {
                // A prior rename may have completed without its final barrier.
                let file = root.open_mutable_child(context, LEDGER_FILE, false)?;
                file.sync_all()?;
                root.sync(context)?;
                return Ok(());
            }
            if row.native_birth != expected {
                return Err(invalid(
                    "native attempt state differs; retain original debt",
                ));
            }
            row.native_birth = next;
            validate(&ledger)?;
            let bytes = encode(&ledger)?;
            let name = format!(
                "consumer-birth-ledger-{}.tmp",
                hawdb_core::generate_uuidv7()?
            );
            let mut candidate = root.create_control_candidate(context, &name)?;
            candidate.write_all(&bytes)?;
            candidate.seal()?;
            candidate.replace_head(context, LEDGER_FILE)?;
            Ok(())
        })
    }

    fn settle_closed_record(
        &self,
        expected: NativeBirth,
        publication_attempted: &mut bool,
    ) -> Result<()> {
        self._project.with_locked_catalog(|root, context, catalog| {
            let mut ledger = check_source(
                &self._project,
                &self._source,
                self.source_branch,
                root,
                context,
                &catalog,
            )?;
            let index = ledger
                .pending
                .iter()
                .position(|row| row.operation == self.operation.0);
            let Some(index) = index else {
                if !*publication_attempted || ledger.high_water < self.operation.0 {
                    return Err(invalid("unattempted settlement record is missing"));
                }
                // A previous response can be lost after the row disappeared.
                // Repeat actual file and original-parent barriers before ACK;
                // reading absent metadata alone cannot establish durability.
                let file = root.open_mutable_child(context, LEDGER_FILE, false)?;
                file.sync_all()?;
                root.sync(context)?;
                return Ok(());
            };
            let row = &ledger.pending[index];
            self.require_row(row)?;
            if row.native_birth != expected {
                return Err(invalid("closed settlement birth state differs"));
            }
            ledger.pending.remove(index);
            // high_water deliberately survives removal. A settled operation
            // remains issued, including after close/reopen or lost response.
            validate(&ledger)?;
            let bytes = encode(&ledger)?;
            let name = format!("consumer-settlement-{}.tmp", hawdb_core::generate_uuidv7()?);
            let mut candidate = root.create_control_candidate(context, &name)?;
            candidate.write_all(&bytes)?;
            candidate.seal()?;
            *publication_attempted = true;
            candidate.replace_head(context, LEDGER_FILE)?;
            Ok(())
        })
    }
}

pub(super) fn marker_bytes(
    domain: BranchId,
    row: &Reservation,
    name: &str,
    parent_locator: &str,
) -> Result<Vec<u8>> {
    let mut bytes = BoundedBytes(Vec::new());
    bytes.write_all(BIRTH_HEADER)?;
    let mut marker_row = row.clone();
    marker_row.native_birth = NativeBirth::Pending {
        generation_name: name.to_owned(),
        parent_locator: parent_locator.to_owned(),
    };
    // The immutable birth marker predates sealing and publication intent.
    marker_row.candidate_seal = None;
    marker_row.publication_intent = None;
    serde_json::to_writer(&mut bytes, &(domain.as_uuid().to_string(), marker_row))
        .map_err(|_| invalid("birth marker exceeds byte limit"))?;
    let checksum = hawdb_integrity::crc32c(&bytes.0);
    bytes.0.extend_from_slice(&checksum.get().to_le_bytes());
    Ok(bytes.0)
}

pub(super) fn marker_file_name() -> &'static str {
    BIRTH_FILE
}

impl PendingConsumerGeneration {
    /// Precompute a Consumer ledger selector outside the source validation
    /// fence. The caller retains this original birth and target exclusion.
    pub fn prepare_selection(&mut self) -> Result<PreparedConsumerSelection> {
        self.born_directory()?;
        let allocation = self
            .allocation
            .as_ref()
            .ok_or_else(|| invalid("selection lost its reservation"))?;
        let intent = self
            .publication_intent
            .as_ref()
            .filter(|_| self.publication_intent_attempted)
            .ok_or_else(|| invalid("selection requires its original publication intent"))?;
        super::selection::prepare(
            allocation,
            intent,
            &mut self.selection_preparation_attempted,
        )
    }

    pub fn original_parent(&self) -> Result<&SourceDirectory> {
        self.parent
            .as_ref()
            .ok_or_else(|| invalid("Consumer birth parent is closed"))
    }

    pub fn generation_name(&self) -> &str {
        self.candidate.name()
    }

    /// Return the original reservation binding only after fresh recorded-birth
    /// validation. The borrowed strings remain tied to this live native token;
    /// a decoded marker or generation name cannot supply them independently.
    pub fn reservation_binding(&self) -> Result<ConsumerReservationBinding<'_>> {
        self.born_directory()?;
        let allocation = self
            .allocation
            .as_ref()
            .ok_or_else(|| invalid("recorded birth lost its reservation"))?;
        Ok(ConsumerReservationBinding {
            source_branch: allocation.source_branch,
            database_uuid: allocation.database,
            consumer_id: &allocation.consumer_id,
            target_locator: &allocation.target_locator,
            target_incarnation: NonZeroU64::new(allocation.incarnation.0)
                .ok_or_else(|| invalid("target incarnation is zero"))?,
            generation: NonZeroU64::new(allocation.generation.0)
                .ok_or_else(|| invalid("generation is zero"))?,
            operation: NonZeroU64::new(allocation.operation.0)
                .ok_or_else(|| invalid("operation is zero"))?,
            registration_uuid: allocation.registration,
            projection_uuid: allocation.projection,
            checkpoint_uuid: allocation.checkpoint,
        })
    }

    /// Persist the sealed candidate's reference against its original birth.
    /// This retains candidate debt across a lost response, but does not select
    /// a serving generation or authorize source freshness. A repeated call
    /// only completes barriers for the same attempted seal.
    pub fn record_candidate_seal(
        &mut self,
        source_epoch: u64,
        metadata: ObjectReference,
    ) -> Result<()> {
        self.born_directory()?;
        if metadata.kind != ObjectKind::CheckpointArtifact
            || metadata.format_version != 1
            || metadata.byte_length == 0
            || metadata.byte_length > MAX_BYTES as u64
        {
            return Err(invalid("unsupported candidate metadata reference"));
        }
        let seal = CandidateSeal {
            source_epoch,
            metadata_length: metadata.byte_length,
            metadata_sha256: metadata.sha256.to_string(),
        };
        let expected_birth = NativeBirth::Recorded {
            generation_name: self.generation_name().to_owned(),
            parent_locator: self.parent_locator.clone(),
        };
        let attempted = &mut self.candidate_seal_attempted;
        let allocation = self
            .allocation
            .as_ref()
            .ok_or_else(|| invalid("candidate seal lost its reservation"))?;
        allocation
            ._project
            .with_locked_catalog(|root, context, catalog| {
                let mut ledger = check_source(
                    &allocation._project,
                    &allocation._source,
                    allocation.source_branch,
                    root,
                    context,
                    &catalog,
                )?;
                let row = ledger
                    .pending
                    .iter_mut()
                    .find(|row| row.operation == allocation.operation.0)
                    .ok_or_else(|| invalid("candidate seal reservation is missing"))?;
                allocation.require_row(row)?;
                if row.native_birth != expected_birth {
                    return Err(invalid("candidate seal birth state differs"));
                }
                if *attempted {
                    if row.candidate_seal.as_ref() != Some(&seal) {
                        return Err(invalid("candidate seal outcome is unresolved"));
                    }
                    let file = root.open_mutable_child(context, LEDGER_FILE, false)?;
                    file.sync_all()?;
                    root.sync(context)?;
                    return Ok(());
                }
                if row.candidate_seal.is_some() {
                    return Err(invalid("candidate seal already belongs to an operation"));
                }
                row.candidate_seal = Some(seal.clone());
                validate(&ledger)?;
                let bytes = encode(&ledger)?;
                let name = format!(
                    "consumer-seal-ledger-{}.tmp",
                    hawdb_core::generate_uuidv7()?
                );
                // A failed or lost native response never starts a fresh seal
                // attempt for this owner. Resolve the original ledger effect.
                *attempted = true;
                let mut candidate = root.create_control_candidate(context, &name)?;
                candidate.write_all(&bytes)?;
                candidate.seal()?;
                candidate.replace_head(context, LEDGER_FILE)?;
                Ok(())
            })
    }

    /// Persist old/new recovery roots before any Consumer selector attempt.
    /// The new root is this pending row and its seal; the previous selected
    /// root is copied into the intent. No selector or receipt is published.
    pub fn record_publication_intent(&mut self) -> Result<()> {
        self.born_directory()?;
        let attempted = &mut self.publication_intent_attempted;
        let remembered = &mut self.publication_intent;
        let allocation = self
            .allocation
            .as_ref()
            .ok_or_else(|| invalid("publication intent lost its reservation"))?;
        allocation
            ._project
            .with_locked_catalog(|root, context, catalog| {
                let mut ledger = check_source(
                    &allocation._project,
                    &allocation._source,
                    allocation.source_branch,
                    root,
                    context,
                    &catalog,
                )?;
                let index = ledger
                    .pending
                    .iter()
                    .position(|row| row.operation == allocation.operation.0)
                    .ok_or_else(|| invalid("publication intent reservation is missing"))?;
                let row = &ledger.pending[index];
                allocation.require_row(row)?;
                SelectedRoot::from_candidate(row)?;
                if *attempted {
                    if row.publication_intent.as_ref() != remembered.as_ref() {
                        return Err(invalid("publication intent outcome is unresolved"));
                    }
                    let file = root.open_mutable_child(context, LEDGER_FILE, false)?;
                    file.sync_all()?;
                    root.sync(context)?;
                    return Ok(());
                }
                if row.publication_intent.is_some() {
                    return Err(invalid("publication intent already exists"));
                }
                let previous = ledger
                    .selected
                    .iter()
                    .find(|selected| selected.matches_consumer(row))
                    .cloned();
                let intent = PublicationIntent {
                    unregistered: false,
                    previous,
                    receipt_complete: false,
                    not_selected: false,
                };
                ledger.pending[index].publication_intent = Some(intent.clone());
                validate(&ledger)?;
                let bytes = encode(&ledger)?;
                let name = format!(
                    "consumer-intent-ledger-{}.tmp",
                    hawdb_core::generate_uuidv7()?
                );
                *remembered = Some(intent);
                // Reentry after an uncertain native effect only resolves this
                // exact intent; it never selects a replacement root by name.
                *attempted = true;
                let mut candidate = root.create_control_candidate(context, &name)?;
                candidate.write_all(&bytes)?;
                candidate.seal()?;
                candidate.replace_head(context, LEDGER_FILE)?;
                Ok(())
            })
    }

    /// One exclusive native attempt. Any error retains intent and all acquired
    /// handles in this owner; retry/recovery must resolve the original attempt.
    /// No producer receives the directory until both target and ledger barriers
    /// have completed. A birth receipt is not source freshness or activation.
    pub fn create(&mut self) -> Result<()> {
        if self.phase != Phase::Prepared {
            return Err(invalid(
                "native birth already attempted; resolve original debt",
            ));
        }
        self.phase = Phase::Attempted;
        let parent = self.parent.as_ref().expect("prepared birth retains parent");
        let context = parent.operation_context();
        parent.create_directory_candidate(&context, &mut self.candidate)?;
        self.directory = Some(parent.open_created_directory(&context, &self.candidate)?);
        let directory = self.directory.as_ref().expect("opened created directory");
        self.marker = Some(directory.create_child_new(&context, BIRTH_FILE)?);
        let marker = self.marker.as_mut().expect("created marker");
        marker.write_all(&self.marker_bytes)?;
        marker.sync_all()?;
        directory.sync(&context)?;
        parent.sync(&context)?;
        let generation_name = self.generation_name().to_owned();
        self.allocation
            .as_ref()
            .expect("unsettled allocation")
            .transition_birth(
                NativeBirth::Pending {
                    generation_name: generation_name.clone(),
                    parent_locator: self.parent_locator.clone(),
                },
                NativeBirth::Recorded {
                    generation_name,
                    parent_locator: self.parent_locator.clone(),
                },
            )?;
        self.phase = Phase::Recorded;
        Ok(())
    }

    /// Original captured directory for a producer, never reconstructed by name.
    /// Preparing/selecting a complete serving view remains a separate protocol.
    pub fn born_directory(&self) -> Result<&SourceDirectory> {
        if self.phase != Phase::Recorded {
            return Err(invalid("native birth has no completed durable receipt"));
        }
        let directory = self
            .directory
            .as_ref()
            .expect("recorded birth retains directory");
        let marker = self.marker.as_ref().expect("recorded birth retains marker");
        let context = directory.operation_context();
        if !directory.matches_child_file(&context, BIRTH_FILE, marker)? {
            return Err(HawDBError::StorageIntegrity(
                "consumer birth marker identity changed".into(),
            ));
        }
        let mut observed = directory.open_child(&context, BIRTH_FILE)?;
        let mut buffer = [0u8; 8192];
        for expected in self.marker_bytes.chunks(buffer.len()) {
            observed.read_exact(&mut buffer[..expected.len()])?;
            if buffer[..expected.len()] != *expected {
                return Err(HawDBError::StorageIntegrity(
                    "consumer birth marker bytes changed".into(),
                ));
            }
        }
        if observed.read(&mut buffer[..1])? != 0 {
            return Err(HawDBError::StorageIntegrity(
                "consumer birth marker bytes changed".into(),
            ));
        }
        let expected = NativeBirth::Recorded {
            generation_name: self.generation_name().to_owned(),
            parent_locator: self.parent_locator.clone(),
        };
        self.allocation
            .as_ref()
            .expect("recorded birth retains allocation")
            .with_row(|row| {
                if row.native_birth != expected {
                    return Err(HawDBError::StorageIntegrity(
                        "consumer birth ledger state changed".into(),
                    ));
                }
                Ok(())
            })?;
        // Returning even a borrowed directory can let a producer retain native
        // children or duplicate custody. Never later treat it as an unused birth.
        self.producer_exposed.store(true, Ordering::Relaxed);
        Ok(directory)
    }

    /// Settle only native evidence that proves this operation owns no entry.
    /// Fresh admission refusal and exclusive-create collision qualify; a
    /// possible creation, successful birth or path absence never does.
    /// Errors retain this token for fresh admission and lost-response retry.
    pub fn settle_no_effect(&mut self) -> Result<()> {
        if self.phase == Phase::Settled {
            return Ok(());
        }
        if !matches!(
            self.phase,
            Phase::Prepared | Phase::Attempted | Phase::Settling
        ) {
            return Err(HawDBError::Execution(
                "consumer birth requires its original retirement protocol".into(),
            ));
        }
        if self.candidate.may_own_entry() || self.directory.is_some() || self.marker.is_some() {
            return Err(HawDBError::Execution(
                "consumer generation may own a native entry; retain its debt".into(),
            ));
        }
        // Revoke creation before closing native target capacity. Even a
        // pre-publication settlement error must not allow a later mkdir.
        self.phase = Phase::Settling;
        drop(self.parent.take());
        drop(std::mem::take(&mut self.marker_bytes));
        let expected = NativeBirth::Pending {
            generation_name: self.generation_name().to_owned(),
            parent_locator: self.parent_locator.clone(),
        };
        self.allocation
            .as_ref()
            .expect("unsettled allocation")
            .settle_closed_record(expected, &mut self.settlement_publication_attempted)?;
        drop(self.allocation.take());
        self.phase = Phase::Settled;
        Ok(())
    }
}
