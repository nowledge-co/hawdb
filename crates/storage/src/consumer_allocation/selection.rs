// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Precomputed Consumer selection and out-of-fence durability custody.

use super::*;
use crate::file_descriptors::DescriptorReservation;
use crate::file_io::{CapturedControlCandidate, PendingControlPublication};
use crate::immutable_object::ObjectKind;

/// Conservative admission for the bounded ledger decode, cloned recovery roots,
/// encoded replacement and retained selection metadata.
pub const CONSUMER_SELECTION_MEMORY_BOUND: u64 = 8 * MAX_BYTES as u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerSelectionReceipt {
    pub(super) selected: SelectedRoot,
}

impl ConsumerSelectionReceipt {
    pub fn generation_name(&self) -> &str {
        &self.selected.generation_name
    }

    pub fn source_epoch(&self) -> u64 {
        self.selected.seal.source_epoch
    }

    pub fn metadata_reference(&self) -> ObjectReference {
        ObjectReference {
            kind: ObjectKind::CheckpointArtifact,
            format_version: 1,
            byte_length: self.selected.seal.metadata_length,
            sha256: self
                .selected
                .seal
                .metadata_sha256
                .parse()
                .expect("validated candidate digest"),
        }
    }

    pub fn binding(&self) -> ConsumerReservationBinding<'_> {
        let root = &self.selected;
        ConsumerReservationBinding {
            source_branch: BranchId::parse(&root.branch_id).expect("validated source branch"),
            database_uuid: Uuid::parse_str(&root.database_uuid).expect("validated database UUID"),
            consumer_id: &root.consumer_id,
            target_locator: &root.target_locator,
            target_incarnation: NonZeroU64::new(root.target_incarnation)
                .expect("nonzero incarnation"),
            generation: NonZeroU64::new(root.generation).expect("nonzero generation"),
            operation: NonZeroU64::new(root.operation).expect("nonzero operation"),
            registration_uuid: Uuid::parse_str(&root.registration_uuid)
                .expect("validated registration UUID"),
            projection_uuid: Uuid::parse_str(&root.projection_uuid)
                .expect("validated projection UUID"),
            checkpoint_uuid: Uuid::parse_str(&root.checkpoint_uuid)
                .expect("validated checkpoint UUID"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerSelectionResolution {
    Published(Box<ConsumerSelectionReceipt>),
    NotSelected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepared,
    Rejected,
    Attempted,
    Published,
    NotSelected,
}

#[derive(Clone, Copy)]
pub(super) enum ExpectedOutcome {
    Either,
    Published,
    NotSelected,
}

/// Retains the original ledger file to prevent native identity reuse, a sealed
/// replacement. Fresh descriptor admission precedes each attempt. The host retains the actual born
/// target, reader and publication exclusion and validates source E before select.
#[doc(hidden)]
#[derive(Debug)]
#[must_use]
pub struct PreparedConsumerSelection {
    control: CapturedControlCandidate<'static>,
    expected: File,
    project: CapturedProject,
    _source: Arc<DatabaseDirectoryLease>,
    next: SelectedRoot,
    previous: Option<SelectedRoot>,
    phase: Phase,
}

/// The native selection has been attempted. Finish after releasing SourceOwner;
/// dropping this borrow neither acknowledges nor rolls back the operation.
#[doc(hidden)]
#[derive(Debug)]
#[must_use]
pub struct PendingConsumerSelection<'a> {
    control: PendingControlPublication<'a, 'static>,
    project: &'a CapturedProject,
    next: &'a SelectedRoot,
    previous: &'a Option<SelectedRoot>,
    phase: &'a mut Phase,
}

pub(super) fn prepare(
    allocation: &ReservedConsumerAllocation,
    remembered: &PublicationIntent,
    preparation_attempted: &mut bool,
) -> Result<PreparedConsumerSelection> {
    if *preparation_attempted {
        return Err(invalid(
            "Consumer selector preparation was already attempted",
        ));
    }
    // Covers the catalog lock/captures, original ledger and control candidate,
    // plus bounded transient native checks. It is acquired before target effects.
    let _descriptors = allocation
        ._project
        .file_descriptors()
        .reserve_admission(12)?;
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
                .iter()
                .find(|row| row.operation == allocation.operation.0)
                .ok_or_else(|| invalid("selection reservation is missing"))?;
            allocation.require_row(row)?;
            if row.publication_intent.as_ref() != Some(remembered) || !remembered.pending() {
                return Err(invalid("selection requires its unresolved durable intent"));
            }
            let next = SelectedRoot::from_candidate(row)?;
            let lease = row.lease;
            let active = ledger
                .selected
                .iter()
                .find(|selected| selected.matches_consumer(row));
            if active != remembered.previous.as_ref() {
                return Err(invalid("selection base differs from publication intent"));
            }
            // Stabilize the intent before exposing a selectable replacement.
            let intent = root.open_mutable_child(context, LEDGER_FILE, false)?;
            intent.sync_all()?;
            root.sync(context)?;
            drop(intent);
            let expected = root.open_child(context, LEDGER_FILE)?;
            ledger
                .selected
                .retain(|selected| selected.consumer_key() != next.consumer_key());
            ledger.selected.push(next.clone());
            ledger
                .policies
                .retain(|policy| policy.key() != next.consumer_key());
            if let Some(lease) = lease {
                ledger.policies.push(RegistrationPolicy {
                    branch_id: next.branch_id.clone(),
                    database_uuid: next.database_uuid.clone(),
                    consumer_id: next.consumer_id.clone(),
                    registration_uuid: next.registration_uuid.clone(),
                    lease,
                });
                ledger.policies.sort_by(|a, b| a.key().cmp(&b.key()));
            }
            ledger
                .selected
                .sort_by(|a, b| a.consumer_key().cmp(&b.consumer_key()));
            validate(&ledger)?;
            let bytes = encode(&ledger)?;
            let name = format!("consumer-selector-{}.tmp", hawdb_core::generate_uuidv7()?);
            *preparation_attempted = true;
            let mut control = allocation._project.create_owned_control_candidate(&name)?;
            control.write_all(&bytes)?;
            control.seal()?;
            Ok(PreparedConsumerSelection {
                control,
                expected,
                project: allocation._project.clone(),
                _source: Arc::clone(&allocation._source),
                next,
                previous: remembered.previous.clone(),
                phase: Phase::Prepared,
            })
        })
}

impl PreparedConsumerSelection {
    pub fn native_selection_attempted(&self) -> bool {
        self.phase != Phase::Prepared
    }

    pub fn previous_receipt(&self) -> Option<ConsumerSelectionReceipt> {
        self.previous
            .as_ref()
            .map(|selected| ConsumerSelectionReceipt {
                selected: selected.clone(),
            })
    }

    /// Settle a prepared selector that never entered native selection. This
    /// cannot turn a visible selected operation into a cancellation.
    pub fn cancel_unattempted(&mut self) -> Result<()> {
        if self.phase != Phase::Prepared {
            return Err(invalid("Consumer selector is no longer unattempted"));
        }
        let outcome = complete_outcome(
            &self.project,
            &self.next,
            &self.previous,
            ExpectedOutcome::NotSelected,
        )?;
        if !matches!(outcome, ConsumerSelectionResolution::NotSelected) {
            return Err(invalid("unattempted Consumer selector became selected"));
        }
        self.phase = Phase::NotSelected;
        Ok(())
    }

    pub fn reserve_attempt_descriptors(&self) -> Result<DescriptorReservation> {
        self.project
            .file_descriptors()
            .reserve_admission(12)
            .map_err(Into::into)
    }

    /// Native atomic selection only. The caller must hold its source-E fence
    /// and actual target authority. No serialization or synchronization occurs.
    pub fn select(&mut self) -> Result<PendingConsumerSelection<'_>> {
        if self.phase != Phase::Prepared {
            return Err(invalid("Consumer selector is no longer selectable"));
        }
        let Self {
            control,
            expected,
            project,
            next,
            previous,
            phase,
            ..
        } = self;
        let control = project.with_locked_catalog(|root, context, _catalog| {
            if !root.matches_child_file(context, LEDGER_FILE, expected)? {
                *phase = Phase::Rejected;
                return Err(invalid("Consumer selector preparation became stale"));
            }
            // Revoke before native code, including errors and unwinding.
            *phase = Phase::Attempted;
            control
                .select_head(context, LEDGER_FILE)
                .map_err(Into::into)
        })?;
        Ok(PendingConsumerSelection {
            control,
            project,
            next,
            previous,
            phase,
        })
    }

    /// Resolve the original operation without rechecking a later source epoch.
    /// Both possible roots remain in the durable intent through uncertainty.
    pub fn resolve(&mut self) -> Result<ConsumerSelectionResolution> {
        match self.phase {
            Phase::Prepared => return Err(invalid("Consumer selection was never attempted")),
            Phase::NotSelected => return Ok(ConsumerSelectionResolution::NotSelected),
            Phase::Published => {
                return Ok(ConsumerSelectionResolution::Published(Box::new(
                    ConsumerSelectionReceipt {
                        selected: self.next.clone(),
                    },
                )))
            }
            Phase::Attempted | Phase::Rejected => {}
        }
        let outcome = complete_outcome(
            &self.project,
            &self.next,
            &self.previous,
            ExpectedOutcome::Either,
        )?;
        self.phase = match outcome {
            ConsumerSelectionResolution::Published(_) => Phase::Published,
            ConsumerSelectionResolution::NotSelected => Phase::NotSelected,
        };
        Ok(outcome)
    }
}

impl PendingConsumerSelection<'_> {
    pub fn finish(self) -> Result<ConsumerSelectionReceipt> {
        self.control
            .finish(&self.project.file_descriptors().io_context())?;
        let outcome = complete_outcome(
            self.project,
            self.next,
            self.previous,
            ExpectedOutcome::Published,
        )?;
        match outcome {
            ConsumerSelectionResolution::Published(receipt) => {
                *self.phase = Phase::Published;
                Ok(*receipt)
            }
            ConsumerSelectionResolution::NotSelected => {
                Err(invalid("completed selector disappeared"))
            }
        }
    }
}

pub(super) fn complete_outcome(
    project: &CapturedProject,
    next: &SelectedRoot,
    previous: &Option<SelectedRoot>,
    expected: ExpectedOutcome,
) -> Result<ConsumerSelectionResolution> {
    project.with_locked_catalog(|root, context, _catalog| {
        let mut ledger = read(root, context)?;
        if selector(&ledger)? != project.allocation_identity() {
            return Err(invalid("selection recovery project differs"));
        }
        let index = ledger
            .pending
            .iter()
            .position(|row| row.operation == next.operation)
            .ok_or_else(|| invalid("selection recovery reservation is missing"))?;
        let row = &ledger.pending[index];
        if SelectedRoot::from_candidate(row)? != *next {
            return Err(invalid("selection recovery candidate differs"));
        }
        let intent = row
            .publication_intent
            .as_ref()
            .ok_or_else(|| invalid("selection recovery intent is missing"))?;
        if intent.previous != *previous {
            return Err(invalid("selection recovery previous root differs"));
        }
        let active = ledger
            .selected
            .iter()
            .find(|selected| selected.matches_consumer(row));
        let published = if intent.receipt_complete {
            true
        } else if intent.not_selected {
            false
        } else if active == Some(next) {
            true
        } else if !matches!(expected, ExpectedOutcome::Published) && active == previous.as_ref() {
            false
        } else {
            return Err(invalid("Consumer selector outcome is uncertain"));
        };
        if matches!(expected, ExpectedOutcome::Published) && !published {
            return Err(invalid("Consumer selector was not selected"));
        }
        if matches!(expected, ExpectedOutcome::NotSelected) && published {
            return Err(invalid("unattempted Consumer selector became selected"));
        }
        if intent.pending() {
            let intent = ledger.pending[index]
                .publication_intent
                .as_mut()
                .expect("checked intent");
            intent.receipt_complete = published;
            intent.not_selected = !published;
            compact_completed(&mut ledger, Some(next.operation));
            validate(&ledger)?;
            let bytes = encode(&ledger)?;
            let name = format!("consumer-receipt-{}.tmp", hawdb_core::generate_uuidv7()?);
            let mut candidate = root.create_control_candidate(context, &name)?;
            candidate.write_all(&bytes)?;
            candidate.seal()?;
            candidate.replace_head(context, LEDGER_FILE)?;
        } else {
            // A complete image with a lost barrier still needs real barriers.
            let file = root.open_mutable_child(context, LEDGER_FILE, false)?;
            file.sync_all()?;
            root.sync(context)?;
        }
        Ok(if published {
            ConsumerSelectionResolution::Published(Box::new(ConsumerSelectionReceipt {
                selected: next.clone(),
            }))
        } else {
            ConsumerSelectionResolution::NotSelected
        })
    })
}
