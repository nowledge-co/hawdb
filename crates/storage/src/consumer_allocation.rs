// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Durable, bounded reservations independent of the rebuildable Consumer cache.
//! Reservations are not directory birth or publication receipts. Unresolved
//! records remain until native custody settles their actual effects; completed
//! predecessors can be compacted without deleting their physical generations.

use crate::branch_catalog::BranchId;
use crate::branch_project::{CapturedProject, ProjectSelector};
use crate::file_descriptors::FileOpenContext;
use crate::file_io::{File, SourceDirectory};
use crate::immutable_object::ObjectReference;
use crate::ownership::DatabaseDirectoryLease;
use hawdb_core::{HawDBError, Result, Uuid};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

pub(crate) const LEDGER_FILE: &str = "consumer-allocations.hawdb";
const MAX_BYTES: usize = 1024 * 1024;
const MAX_PENDING: usize = 256;
const HEADER: &[u8] = b"HAWDB_CONSUMER_ALLOCATIONS_V2\n";

mod native_birth;
pub use native_birth::PendingConsumerGeneration;
mod selection;
pub use selection::{
    ConsumerSelectionReceipt, ConsumerSelectionResolution, PendingConsumerSelection,
    PreparedConsumerSelection, CONSUMER_SELECTION_MEMORY_BOUND,
};
mod reopen;
pub(crate) use reopen::prepare as prepare_reopen;
pub use reopen::{ConsumerReopenRequest, ReopenedConsumerGeneration, CONSUMER_REOPEN_MEMORY_BOUND};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    project_id: String,
    main_branch_id: String,
    high_water: u64,
    pending: Vec<Reservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    selected: Vec<SelectedRoot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    policies: Vec<RegistrationPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reservation {
    branch_id: String,
    database_uuid: String,
    consumer_id: String,
    target_locator: String,
    target_incarnation: u64,
    generation: u64,
    operation: u64,
    registration_uuid: String,
    projection_uuid: String,
    checkpoint_uuid: String,
    native_birth: NativeBirth,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease: Option<ConsumerLeasePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    candidate_seal: Option<CandidateSeal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publication_intent: Option<PublicationIntent>,
}

/// Native registration policy, independent of the disposable registry cache.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerLeasePolicy {
    pub max_idle_commits: u64,
    pub expires_at_commit_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrationPolicy {
    branch_id: String,
    database_uuid: String,
    consumer_id: String,
    registration_uuid: String,
    lease: ConsumerLeasePolicy,
}
impl RegistrationPolicy {
    fn key(&self) -> (&str, &str, &str) {
        (&self.branch_id, &self.database_uuid, &self.consumer_id)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct ConsumerRegistryEntry {
    pub receipt: ConsumerSelectionReceipt,
    pub policy: ConsumerLeasePolicy,
}

#[doc(hidden)]
#[derive(Debug, Default)]
pub struct ConsumerRegistrySnapshot {
    pub entries: Vec<ConsumerRegistryEntry>,
    pub pending: Vec<String>,
}

/// Durable candidate debt, not a selector or a completed publication receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateSeal {
    source_epoch: u64,
    metadata_length: u64,
    metadata_sha256: String,
}

/// A logical selected root. Native directory and file custody must be
/// reconstructed and verified before it can serve after reopen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedRoot {
    branch_id: String,
    database_uuid: String,
    consumer_id: String,
    target_locator: String,
    target_incarnation: u64,
    generation: u64,
    operation: u64,
    registration_uuid: String,
    projection_uuid: String,
    checkpoint_uuid: String,
    generation_name: String,
    parent_locator: String,
    seal: CandidateSeal,
}

/// Retains the old root before any attempt to select the candidate in this row.
/// The candidate itself remains rooted by the pending row and its seal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationIntent {
    previous: Option<SelectedRoot>,
    // Encode every flag before selection so completion/revocation only shrinks
    // the image, including when the prepared ledger is at its byte limit.
    #[serde(default)]
    receipt_complete: bool,
    #[serde(default)]
    unregistered: bool,
    #[serde(default)]
    not_selected: bool,
}

impl PublicationIntent {
    fn pending(&self) -> bool {
        !self.receipt_complete && !self.not_selected
    }
}

// Completed predecessor receipts do not authorize deletion of their physical
// generations. Native read pins retain those independently. Keep every current
// selected birth, the initial birth that owns its logical target lease,
// unresolved operation and the operation whose completion may still lose its
// reply. Issued identities remain protected by high_water.
fn compact_completed(ledger: &mut Ledger, completing: Option<u64>) {
    ledger.pending.retain(|row| {
        completing == Some(row.operation)
            || ledger.selected.iter().any(|root| {
                root.operation == row.operation
                    || (root.matches_consumer(row)
                        && root.registration_uuid == row.registration_uuid
                        && root.target_incarnation == row.target_incarnation
                        && row.operation.checked_sub(2) == Some(row.target_incarnation))
            })
            || row
                .publication_intent
                .as_ref()
                .is_none_or(|intent| !intent.receipt_complete && !intent.not_selected)
    });
}

fn pending_capacity_exhausted(used: usize) -> HawDBError {
    hawdb_core::ResourceBudgetError {
        resource: hawdb_core::ResourceBudgetKind::MetadataEntries,
        scope: hawdb_core::ResourceBudgetScope::SharedPool,
        owner: "Consumer allocation ledger".into(),
        memory_class: None,
        requested: 1,
        used: used as u64,
        budget: MAX_PENDING as u64,
        budget_option: "consumer_allocation::MAX_PENDING",
        message: "Consumer allocation ledger has no free pending reservation slots".into(),
    }
    .into()
}

impl SelectedRoot {
    fn consumer_key(&self) -> (&str, &str, &str) {
        (&self.branch_id, &self.database_uuid, &self.consumer_id)
    }

    fn matches_consumer(&self, row: &Reservation) -> bool {
        self.consumer_key()
            == (
                row.branch_id.as_str(),
                row.database_uuid.as_str(),
                row.consumer_id.as_str(),
            )
    }

    fn from_candidate(row: &Reservation) -> Result<Self> {
        let NativeBirth::Recorded {
            generation_name,
            parent_locator,
        } = &row.native_birth
        else {
            return Err(invalid("selected candidate has no recorded birth"));
        };
        let seal = row
            .candidate_seal
            .clone()
            .ok_or_else(|| invalid("selected candidate has no durable seal"))?;
        Ok(Self {
            branch_id: row.branch_id.clone(),
            database_uuid: row.database_uuid.clone(),
            consumer_id: row.consumer_id.clone(),
            target_locator: row.target_locator.clone(),
            target_incarnation: row.target_incarnation,
            generation: row.generation,
            operation: row.operation,
            registration_uuid: row.registration_uuid.clone(),
            projection_uuid: row.projection_uuid.clone(),
            checkpoint_uuid: row.checkpoint_uuid.clone(),
            generation_name: generation_name.clone(),
            parent_locator: parent_locator.clone(),
            seal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", deny_unknown_fields)]
enum NativeBirth {
    Reserved,
    Pending {
        generation_name: String,
        parent_locator: String,
    },
    Recorded {
        generation_name: String,
        parent_locator: String,
    },
    Retiring {
        generation_name: String,
        parent_locator: String,
    },
    Retired {
        generation_name: String,
        parent_locator: String,
    },
}

#[derive(Debug)]
struct TargetIncarnation(u64);
#[derive(Debug)]
struct ImmutableGeneration(u64);
#[derive(Debug)]
struct OperationIdentity(u64);

/// Kernel-owned source admission. It checks initialized allocation evidence
/// before identity/checkpoint changes, and retains original custody for reserve.
#[doc(hidden)]
#[derive(Debug)]
pub(crate) struct ConsumerAllocationSource {
    project: CapturedProject,
    owner: Arc<DatabaseDirectoryLease>,
    branch: BranchId,
}
impl ConsumerAllocationSource {
    pub(crate) fn matches_branch(&self, branch: BranchId) -> bool {
        self.branch == branch
    }

    pub(crate) fn verify_candidate_seal(
        &self,
        binding: ConsumerReservationBinding<'_>,
        source_epoch: u64,
        metadata: ObjectReference,
    ) -> Result<()> {
        if !self.matches_branch(binding.source_branch) {
            return Err(invalid("candidate source branch differs"));
        }
        self.project.with_locked_catalog(|root, context, catalog| {
            let ledger = check_source(
                &self.project,
                &self.owner,
                self.branch,
                root,
                context,
                &catalog,
            )?;
            let row = ledger
                .pending
                .iter()
                .find(|row| row.operation == binding.operation.get())
                .ok_or_else(|| invalid("candidate reservation is missing"))?;
            if row.branch_id != binding.source_branch.as_uuid().to_string()
                || row.database_uuid != binding.database_uuid.to_string()
                || row.consumer_id != binding.consumer_id
                || row.target_locator != binding.target_locator
                || row.target_incarnation != binding.target_incarnation.get()
                || row.generation != binding.generation.get()
                || row.registration_uuid != binding.registration_uuid.to_string()
                || row.projection_uuid != binding.projection_uuid.to_string()
                || row.checkpoint_uuid != binding.checkpoint_uuid.to_string()
                || row.candidate_seal
                    != Some(CandidateSeal {
                        source_epoch,
                        metadata_length: metadata.byte_length,
                        metadata_sha256: metadata.sha256.to_string(),
                    })
            {
                return Err(invalid("candidate seal differs from durable reservation"));
            }
            Ok(())
        })
    }

    pub(crate) fn reserve(
        &self,
        database: Uuid,
        consumer: &str,
        destination: &Path,
    ) -> Result<ReservedConsumerAllocation> {
        reserve(self, database, consumer, destination, None, None)
    }

    pub(crate) fn reserve_registered(
        &self,
        database: Uuid,
        consumer: &str,
        destination: &Path,
        policy: ConsumerLeasePolicy,
    ) -> Result<ReservedConsumerAllocation> {
        reserve(self, database, consumer, destination, None, Some(policy))
    }

    pub(crate) fn registry(&self, database: Uuid) -> Result<ConsumerRegistrySnapshot> {
        self.project.with_locked_catalog(|root, context, catalog| {
            let ledger = check_source(
                &self.project,
                &self.owner,
                self.branch,
                root,
                context,
                &catalog,
            )?;
            let branch = self.branch.as_uuid().to_string();
            let database = database.to_string();
            let mut output = ConsumerRegistrySnapshot::default();
            for row in &ledger.pending {
                if row.branch_id == branch
                    && row.database_uuid == database
                    && row
                        .publication_intent
                        .as_ref()
                        .is_some_and(PublicationIntent::pending)
                {
                    output.pending.push(row.consumer_id.clone());
                }
            }
            for selected in &ledger.selected {
                if selected.branch_id != branch
                    || selected.database_uuid != database
                    || output.pending.contains(&selected.consumer_id)
                {
                    continue;
                }
                let row = ledger
                    .pending
                    .iter()
                    .find(|row| row.operation == selected.operation)
                    .ok_or_else(|| invalid("registered selection lost its birth"))?;
                if row
                    .publication_intent
                    .as_ref()
                    .is_none_or(|intent| !intent.receipt_complete)
                {
                    return Err(invalid("registered selection has no completed receipt"));
                }
                let policy = ledger
                    .policies
                    .iter()
                    .find(|policy| {
                        policy.key() == selected.consumer_key()
                            && policy.registration_uuid == selected.registration_uuid
                    })
                    .ok_or_else(|| invalid("registered selection lost its lease policy"))?;
                output.entries.push(ConsumerRegistryEntry {
                    receipt: ConsumerSelectionReceipt {
                        selected: selected.clone(),
                    },
                    policy: policy.lease,
                });
            }
            Ok(output)
        })
    }

    pub(crate) fn renew(&self, receipt: &ConsumerSelectionReceipt, epoch: u64) -> Result<()> {
        self.project.with_locked_catalog(|root, context, catalog| {
            let mut ledger = check_source(
                &self.project,
                &self.owner,
                self.branch,
                root,
                context,
                &catalog,
            )?;
            if !ledger
                .selected
                .iter()
                .any(|selected| selected == &receipt.selected)
            {
                return Err(HawDBError::ProjectionBusy);
            }
            if ledger.pending.iter().any(|row| {
                receipt.selected.matches_consumer(row)
                    && row
                        .publication_intent
                        .as_ref()
                        .is_some_and(PublicationIntent::pending)
            }) {
                return Err(HawDBError::ProjectionBusy);
            }
            let policy = ledger
                .policies
                .iter_mut()
                .find(|policy| {
                    policy.key() == receipt.selected.consumer_key()
                        && policy.registration_uuid == receipt.selected.registration_uuid
                })
                .ok_or_else(|| invalid("Consumer renewal policy missing"))?;
            policy.lease.expires_at_commit_epoch = epoch
                .checked_add(policy.lease.max_idle_commits)
                .ok_or_else(|| invalid("Consumer expiry overflow"))?;
            let bytes = encode(&ledger)?;
            let name = format!("consumer-renew-{}.tmp", hawdb_core::generate_uuidv7()?);
            let mut candidate = root.create_control_candidate(context, &name)?;
            candidate.write_all(&bytes)?;
            candidate.seal()?;
            candidate.replace_head(context, LEDGER_FILE)?;
            Ok(())
        })
    }

    pub(crate) fn forget(&self, database: Uuid, consumer: &str) -> Result<()> {
        self.project.with_locked_catalog(|root, context, catalog| {
            let mut ledger = check_source(
                &self.project,
                &self.owner,
                self.branch,
                root,
                context,
                &catalog,
            )?;
            let branch = self.branch.as_uuid().to_string();
            let database = database.to_string();
            if ledger.pending.iter().any(|row| {
                row.branch_id == branch
                    && row.database_uuid == database
                    && row.consumer_id == consumer
                    && row
                        .publication_intent
                        .as_ref()
                        .is_some_and(PublicationIntent::pending)
            }) {
                return Err(HawDBError::ProjectionBusy);
            }
            // Revoke the completed logical registration without erasing its
            // immutable birth or reusing any allocation identity.
            for row in &mut ledger.pending {
                if row.branch_id == branch
                    && row.database_uuid == database
                    && row.consumer_id == consumer
                    && let Some(intent) = &mut row.publication_intent
                    && intent.receipt_complete
                {
                    intent.unregistered = true;
                }
            }
            ledger.selected.retain(|selected| {
                selected.consumer_key() != (branch.as_str(), database.as_str(), consumer)
            });
            ledger
                .policies
                .retain(|policy| policy.key() != (branch.as_str(), database.as_str(), consumer));
            compact_completed(&mut ledger, None);
            validate(&ledger)?;
            let bytes = encode(&ledger)?;
            let name = format!("consumer-unregister-{}.tmp", hawdb_core::generate_uuidv7()?);
            let mut candidate = root.create_control_candidate(context, &name)?;
            candidate.write_all(&bytes)?;
            candidate.seal()?;
            candidate.replace_head(context, LEDGER_FILE)?;
            Ok(())
        })
    }

    pub(crate) fn reserve_update(
        &self,
        previous: &ConsumerSelectionReceipt,
    ) -> Result<ReservedConsumerAllocation> {
        let binding = previous.binding();
        reserve(
            self,
            binding.database_uuid,
            binding.consumer_id,
            Path::new(binding.target_locator),
            Some(previous),
            None,
        )
    }
}

/// Captures existing custody only; it performs no I/O or ledger admission.
/// Consumers must verify the captured source before exposing any authority.
pub(crate) fn captured_source(
    project: &CapturedProject,
    owner: Arc<DatabaseDirectoryLease>,
    branch: BranchId,
) -> ConsumerAllocationSource {
    ConsumerAllocationSource {
        project: project.clone(),
        owner,
        branch,
    }
}

impl ConsumerAllocationSource {
    pub(crate) fn verify(&self) -> Result<()> {
        self.project.with_locked_catalog(|root, context, catalog| {
            check_source(
                &self.project,
                &self.owner,
                self.branch,
                root,
                context,
                &catalog,
            )
            .map(|_| ())
        })
    }
}

fn check_source(
    project: &CapturedProject,
    owner: &DatabaseDirectoryLease,
    branch: BranchId,
    root: &SourceDirectory,
    context: &FileOpenContext,
    catalog: &crate::branch_catalog::Catalog,
) -> Result<Ledger> {
    check_source_custody(project, owner, branch, catalog)?;
    let ledger = read(root, context)?;
    if selector(&ledger)? != project.allocation_identity() {
        return Err(invalid("project binding differs"));
    }
    Ok(ledger)
}

fn check_source_custody(
    project: &CapturedProject,
    owner: &DatabaseDirectoryLease,
    branch: BranchId,
    catalog: &crate::branch_catalog::Catalog,
) -> Result<()> {
    let record = catalog
        .branches
        .iter()
        .find(|r| r.id == branch && r.state == crate::branch_catalog::BranchState::Ready)
        .ok_or_else(|| invalid("source branch is not ready"))?;
    let actual = project.capture_branch_directory(record.id)?;
    if !actual.same_directory(owner.captured_directory()) {
        return Err(invalid("source owner differs from original project"));
    }
    if catalog.project_id != project.allocation_identity().project_id() {
        return Err(invalid("project binding differs"));
    }
    Ok(())
}

/// Kernel-created reservation, never a native birth/adoption/deletion token.
#[doc(hidden)]
#[derive(Debug)]
pub struct ReservedConsumerAllocation {
    domain: BranchId,
    incarnation: TargetIncarnation,
    generation: ImmutableGeneration,
    operation: OperationIdentity,
    registration: Uuid,
    projection: Uuid,
    checkpoint: Uuid,
    source_branch: BranchId,
    database: Uuid,
    consumer_id: String,
    target_locator: String,
    _project: CapturedProject,
    _source: Arc<DatabaseDirectoryLease>,
}

/// Original reservation identities retained by a recorded native birth.
/// This description cannot mint a new reservation or authorize publication.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerReservationBinding<'a> {
    pub source_branch: BranchId,
    pub database_uuid: Uuid,
    pub consumer_id: &'a str,
    pub target_locator: &'a str,
    pub target_incarnation: NonZeroU64,
    pub generation: NonZeroU64,
    pub operation: NonZeroU64,
    pub registration_uuid: Uuid,
    pub projection_uuid: Uuid,
    pub checkpoint_uuid: Uuid,
}

impl ReservedConsumerAllocation {
    pub fn registration_uuid(&self) -> Uuid {
        self.registration
    }
    pub fn projection_uuid(&self) -> Uuid {
        self.projection
    }
    pub fn checkpoint_uuid(&self) -> Uuid {
        self.checkpoint
    }
    pub fn stage_name(&self) -> String {
        format!(
            ".hawdb-consumer-{}-{}-{}-{}.stage",
            self.domain.as_uuid(),
            self.incarnation.0,
            self.generation.0,
            self.operation.0
        )
    }
}

fn invalid(message: &str) -> HawDBError {
    HawDBError::StorageIntegrity(format!("consumer allocation ledger: {message}"))
}

fn read(root: &SourceDirectory, context: &FileOpenContext) -> Result<Ledger> {
    let file = match root.open_child(context, LEDGER_FILE) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(invalid("initialized domain ledger is missing"))
        }
        Err(error) => return Err(error.into()),
    };
    decode(file)
}

fn decode(mut file: File) -> Result<Ledger> {
    decode_held(&mut file)
}

fn decode_held(file: &mut File) -> Result<Ledger> {
    if file.metadata()?.len() > MAX_BYTES as u64 {
        return Err(invalid("byte limit exceeded"));
    }
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BYTES || !bytes.starts_with(HEADER) || bytes.len() < HEADER.len() + 4 {
        return Err(invalid("missing or incomplete header"));
    }
    let checksum_at = bytes.len() - 4;
    let checksum = u32::from_le_bytes(
        bytes[checksum_at..]
            .try_into()
            .expect("four checksum bytes"),
    );
    if hawdb_integrity::crc32c(&bytes[..checksum_at]).get() != checksum {
        return Err(invalid("checksum mismatch"));
    }
    let ledger: Ledger = serde_json::from_slice(&bytes[HEADER.len()..checksum_at])
        .map_err(|_| invalid("invalid payload"))?;
    validate(&ledger)?;
    if encode(&ledger)? != bytes {
        return Err(invalid("noncanonical payload"));
    }
    Ok(ledger)
}

fn validate(ledger: &Ledger) -> Result<()> {
    let project =
        BranchId::parse(&ledger.project_id).map_err(|_| invalid("invalid project identity"))?;
    let main =
        BranchId::parse(&ledger.main_branch_id).map_err(|_| invalid("invalid main identity"))?;
    ProjectSelector::new(project, main)?;
    if ledger.pending.len() > MAX_PENDING
        || ledger.selected.len() > MAX_PENDING
        || !ledger.high_water.is_multiple_of(3)
    {
        return Err(invalid("invalid allocator boundary"));
    }
    let mut previous = 0;
    for (index, row) in ledger.pending.iter().enumerate() {
        if row.operation > ledger.high_water
            || row.operation <= previous
            || !row.operation.is_multiple_of(3)
            || row.operation.saturating_sub(2) < row.target_incarnation
            || row.target_incarnation % 3 != 1
            || row.operation.checked_sub(1) != Some(row.generation)
            || row.target_incarnation == 0
        {
            return Err(invalid("invalid reservation identities"));
        }
        let branch =
            BranchId::parse(&row.branch_id).map_err(|_| invalid("invalid source branch"))?;
        if branch.as_uuid().to_string() != row.branch_id {
            return Err(invalid("noncanonical source branch"));
        }
        for id in [
            &row.database_uuid,
            &row.registration_uuid,
            &row.projection_uuid,
            &row.checkpoint_uuid,
        ] {
            let parsed = Uuid::parse_str(id).map_err(|_| invalid("invalid binding UUID"))?;
            if parsed.is_nil() {
                return Err(invalid("nil binding UUID"));
            }
            if parsed.to_string() != *id {
                return Err(invalid("noncanonical binding UUID"));
            }
        }
        if row.consumer_id.is_empty()
            || row.consumer_id.len() > 128
            || !row
                .consumer_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            || row.target_locator.len() > 16 * 1024
        {
            return Err(invalid("invalid consumer or target locator"));
        }
        if let NativeBirth::Pending {
            generation_name,
            parent_locator,
        }
        | NativeBirth::Recorded {
            generation_name,
            parent_locator,
        }
        | NativeBirth::Retiring {
            generation_name,
            parent_locator,
        }
        | NativeBirth::Retired {
            generation_name,
            parent_locator,
        } = &row.native_birth
            && (parent_locator.is_empty()
                || parent_locator.len() > 16 * 1024
                || !valid_generation_name(
                    project,
                    row.target_incarnation,
                    row.generation,
                    row.operation,
                    &row.target_locator,
                    generation_name,
                ))
        {
            return Err(invalid("birth generation differs from reservation"));
        }
        if let Some(seal) = &row.candidate_seal
            && (!matches!(row.native_birth, NativeBirth::Recorded { .. }) || !valid_seal(seal))
        {
            return Err(invalid("invalid candidate seal"));
        }
        if let Some(intent) = &row.publication_intent {
            let next = SelectedRoot::from_candidate(row)?;
            let active = ledger
                .selected
                .iter()
                .find(|selected| selected.matches_consumer(row));
            if intent.receipt_complete && intent.not_selected {
                return Err(invalid("publication intent has conflicting outcomes"));
            }
            if intent.unregistered
                && (!intent.receipt_complete
                    || active
                        .is_some_and(|active| active.registration_uuid == row.registration_uuid))
            {
                return Err(invalid("revoked registration remains active"));
            }
            if intent.receipt_complete
                && !intent.unregistered
                && active.is_none_or(|active| active.operation < next.operation)
            {
                return Err(invalid("completed receipt lost its selected successor"));
            }
            if intent.not_selected && active == Some(&next) {
                return Err(invalid("unselected operation became an active selector"));
            }
            if intent.pending() && intent.previous.as_ref() != active && active != Some(&next) {
                return Err(invalid("publication intent lost its previous root"));
            }
            if let Some(previous) = &intent.previous {
                if !previous.matches_consumer(row) {
                    return Err(invalid("publication intent has a foreign previous root"));
                }
                if previous.operation >= next.operation
                    || previous.seal.source_epoch > next.seal.source_epoch
                {
                    return Err(invalid("publication intent regresses its previous root"));
                }
                validate_selected_root(project, ledger.high_water, previous)?;
            }
            if ledger.pending[..index].iter().any(|candidate| {
                candidate.branch_id == row.branch_id
                    && candidate.database_uuid == row.database_uuid
                    && candidate.consumer_id == row.consumer_id
                    && intent.pending()
                    && candidate
                        .publication_intent
                        .as_ref()
                        .is_some_and(PublicationIntent::pending)
            }) {
                return Err(invalid("multiple publication intents for one consumer"));
            }
        }
        previous = row.operation;
    }
    let mut previous_consumer = None;
    for selected in &ledger.selected {
        validate_selected_root(project, ledger.high_water, selected)?;
        let row = ledger
            .pending
            .iter()
            .find(|row| row.operation == selected.operation)
            .ok_or_else(|| invalid("selected root lost its reservation"))?;
        if SelectedRoot::from_candidate(row)? != *selected
            || row
                .publication_intent
                .as_ref()
                .is_none_or(|intent| intent.not_selected)
        {
            return Err(invalid("selected root differs from its publication intent"));
        }
        if previous_consumer.is_some_and(|previous| selected.consumer_key() <= previous) {
            return Err(invalid("selected consumers are not unique and ordered"));
        }
        previous_consumer = Some(selected.consumer_key());
    }
    if ledger.policies.len() > MAX_PENDING {
        return Err(invalid("Consumer policy budget exceeded"));
    }
    let mut previous_policy = None;
    for policy in &ledger.policies {
        if policy.lease.max_idle_commits == 0
            || previous_policy.is_some_and(|previous| policy.key() <= previous)
        {
            return Err(invalid("Consumer policies are not ordered and unique"));
        }
        let selected = ledger
            .selected
            .iter()
            .find(|selected| {
                selected.consumer_key() == policy.key()
                    && selected.registration_uuid == policy.registration_uuid
            })
            .ok_or_else(|| invalid("orphan Consumer lease policy"))?;
        let row = ledger
            .pending
            .iter()
            .find(|row| row.operation == selected.operation)
            .ok_or_else(|| invalid("Consumer policy lost its reservation"))?;
        if row.lease.is_none_or(|lease| {
            lease.max_idle_commits != policy.lease.max_idle_commits
                || lease.expires_at_commit_epoch > policy.lease.expires_at_commit_epoch
        }) {
            return Err(invalid(
                "Consumer policy differs from selected registration",
            ));
        }
        previous_policy = Some(policy.key());
    }
    Ok(())
}

fn valid_seal(seal: &CandidateSeal) -> bool {
    seal.metadata_length != 0
        && seal.metadata_length <= MAX_BYTES as u64
        && seal
            .metadata_sha256
            .parse::<hawdb_integrity::Sha256Digest>()
            .is_ok()
}

fn validate_selected_root(project: BranchId, high_water: u64, root: &SelectedRoot) -> Result<()> {
    if root.operation > high_water
        || !root.operation.is_multiple_of(3)
        || root.operation.saturating_sub(2) < root.target_incarnation
        || root.target_incarnation % 3 != 1
        || root.operation.checked_sub(1) != Some(root.generation)
        || root.target_incarnation == 0
        || root.consumer_id.is_empty()
        || root.consumer_id.len() > 128
        || !root
            .consumer_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        || root.target_locator.len() > 16 * 1024
        || root.parent_locator.is_empty()
        || root.parent_locator.len() > 16 * 1024
        || !valid_generation_name(
            project,
            root.target_incarnation,
            root.generation,
            root.operation,
            &root.target_locator,
            &root.generation_name,
        )
        || !valid_seal(&root.seal)
    {
        return Err(invalid("invalid selected Consumer root"));
    }
    let branch =
        BranchId::parse(&root.branch_id).map_err(|_| invalid("invalid selected source branch"))?;
    if branch.as_uuid().to_string() != root.branch_id {
        return Err(invalid("noncanonical selected source branch"));
    }
    for id in [
        &root.database_uuid,
        &root.registration_uuid,
        &root.projection_uuid,
        &root.checkpoint_uuid,
    ] {
        let parsed = Uuid::parse_str(id).map_err(|_| invalid("invalid selected binding UUID"))?;
        if parsed.is_nil() {
            return Err(invalid("nil selected binding UUID"));
        }
        if parsed.to_string() != *id {
            return Err(invalid("noncanonical selected binding UUID"));
        }
    }
    Ok(())
}

struct BoundedBytes(Vec<u8>);
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_BYTES - 4)
            .ok_or_else(|| io::Error::other("consumer allocation ledger byte limit exceeded"))?;
        self.0
            .try_reserve_exact(next - self.0.len())
            .map_err(io::Error::other)?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode(ledger: &Ledger) -> Result<Vec<u8>> {
    let mut bytes = BoundedBytes(Vec::new());
    bytes.write_all(HEADER)?;
    serde_json::to_writer(&mut bytes, ledger)
        .map_err(|_| invalid("encoding byte limit exceeded"))?;
    let checksum = hawdb_integrity::crc32c(&bytes.0);
    bytes.0.extend_from_slice(&checksum.get().to_le_bytes());
    Ok(bytes.0)
}
fn selector(ledger: &Ledger) -> Result<ProjectSelector> {
    ProjectSelector::new(
        BranchId::parse(&ledger.project_id).map_err(|_| invalid("invalid project"))?,
        BranchId::parse(&ledger.main_branch_id).map_err(|_| invalid("invalid main"))?,
    )
}

/// Caller owns original-root bootstrap and original catalog exclusion. No source
/// identity may be published, reservation exposed or target effect run before
/// this complete file and its parent namespace have synchronized.
pub(crate) fn bootstrap(
    root: &SourceDirectory,
    context: &FileOpenContext,
    proposed: ProjectSelector,
    virgin: bool,
) -> Result<ProjectSelector> {
    match root.open_mutable_child(context, LEDGER_FILE, false) {
        Ok(file) => {
            // A prior attempt may have written complete bytes but lost its
            // file barrier. Retry that barrier through this exact native file
            // before publishing any initialized source identity.
            file.sync_all()?;
            let ledger = decode(file)?;
            if virgin && (ledger.high_water != 0 || !ledger.pending.is_empty()) {
                return Err(invalid(
                    "uninitialized source has nonempty allocation evidence",
                ));
            }
            let selected = selector(&ledger)?;
            if !virgin && selected != proposed {
                return Err(invalid("source identity differs"));
            }
            // Retry the namespace barrier even after a lost bootstrap response.
            root.sync(context)?;
            Ok(selected)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && virgin => {
            let ledger = Ledger {
                project_id: proposed.project_id().as_uuid().to_string(),
                main_branch_id: proposed.main_branch_id().as_uuid().to_string(),
                high_water: 0,
                pending: Vec::new(),
                selected: Vec::new(),
                policies: Vec::new(),
            };
            let bytes = encode(&ledger)?;
            let mut file = root.create_child_new(context, LEDGER_FILE)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            root.sync(context)?;
            Ok(proposed)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(invalid("initialized domain ledger is missing"))
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn reserve(
    source: &ConsumerAllocationSource,
    database: Uuid,
    consumer: &str,
    destination: &Path,
    previous: Option<&ConsumerSelectionReceipt>,
    policy: Option<ConsumerLeasePolicy>,
) -> Result<ReservedConsumerAllocation> {
    let project = &source.project;
    let owner = Arc::clone(&source.owner);
    let branch = source.branch;
    let locator = destination
        .to_str()
        .filter(|s| s.len() <= 16 * 1024)
        .ok_or_else(|| invalid("target locator must be bounded UTF-8"))?;
    let registration = match previous {
        Some(receipt) => receipt.binding().registration_uuid,
        None => hawdb_core::generate_uuidv7()?,
    };
    let projection = match previous {
        Some(receipt) => receipt.binding().projection_uuid,
        None => hawdb_core::generate_uuidv7()?,
    };
    let checkpoint = hawdb_core::generate_uuidv7()?;
    let operation = project.with_locked_catalog(|root, context, catalog| {
        let mut ledger = check_source(project, &owner, branch, root, context, &catalog)?;
        compact_completed(&mut ledger, None);
        let key = (branch.as_uuid().to_string(), database.to_string(), consumer);
        let active = ledger
            .selected
            .iter()
            .find(|selected| selected.consumer_key() == (key.0.as_str(), key.1.as_str(), key.2));
        if active != previous.map(|receipt| &receipt.selected) {
            return Err(HawDBError::ProjectionBusy);
        }
        if ledger.pending.iter().any(|row| {
            row.branch_id == key.0
                && row.database_uuid == key.1
                && row.consumer_id == consumer
                && row
                    .publication_intent
                    .as_ref()
                    .is_some_and(PublicationIntent::pending)
        }) {
            return Err(HawDBError::ProjectionBusy);
        }
        if ledger.pending.len() >= MAX_PENDING {
            return Err(pending_capacity_exhausted(ledger.pending.len()));
        }
        let lease = match previous {
            Some(receipt) => ledger
                .policies
                .iter()
                .find(|policy| {
                    policy.key() == receipt.selected.consumer_key()
                        && policy.registration_uuid == receipt.selected.registration_uuid
                })
                .map(|policy| policy.lease),
            None => policy,
        };
        if lease.is_some_and(|policy| policy.max_idle_commits == 0) {
            return Err(invalid("Consumer lease must be positive"));
        }
        let operation = ledger
            .high_water
            .checked_add(3)
            .ok_or_else(|| invalid("allocation identities exhausted"))?;
        ledger.pending.push(Reservation {
            branch_id: branch.as_uuid().to_string(),
            database_uuid: database.to_string(),
            consumer_id: consumer.into(),
            target_locator: locator.into(),
            target_incarnation: previous
                .map_or(operation - 2, |receipt| receipt.selected.target_incarnation),
            generation: operation - 1,
            operation,
            registration_uuid: registration.to_string(),
            projection_uuid: projection.to_string(),
            checkpoint_uuid: checkpoint.to_string(),
            native_birth: NativeBirth::Reserved,
            lease,
            candidate_seal: None,
            publication_intent: None,
        });
        ledger.high_water = operation;
        validate(&ledger)?;
        let bytes = encode(&ledger)?;
        let name = format!("consumer-allocation-{}.tmp", hawdb_core::generate_uuidv7()?);
        let mut candidate = root.create_control_candidate(context, &name)?;
        candidate.write_all(&bytes)?;
        candidate.seal()?;
        candidate.replace_head(context, LEDGER_FILE)?;
        Ok(operation)
    })?;
    Ok(ReservedConsumerAllocation {
        domain: project.allocation_identity().project_id(),
        incarnation: TargetIncarnation(
            previous.map_or(operation - 2, |receipt| receipt.selected.target_incarnation),
        ),
        generation: ImmutableGeneration(operation - 1),
        operation: OperationIdentity(operation),
        registration,
        projection,
        checkpoint,
        source_branch: branch,
        database,
        consumer_id: consumer.into(),
        target_locator: locator.into(),
        _project: project.clone(),
        _source: owner,
    })
}

fn valid_generation_name(
    project: BranchId,
    incarnation: u64,
    generation: u64,
    operation: u64,
    locator: &str,
    name: &str,
) -> bool {
    name == generation_name_for(project, incarnation, generation, operation)
        || (generation == incarnation + 1
            && Path::new(locator)
                .file_name()
                .and_then(|value| value.to_str())
                == Some(name)
            && !matches!(name, "" | "." | "..")
            && !name.contains(['/', '\\']))
}

fn generation_name_for(
    project: BranchId,
    incarnation: u64,
    generation: u64,
    operation: u64,
) -> String {
    format!(
        "hawdb-generation-{}-{incarnation}-{generation}-{operation}",
        project.as_uuid()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_next_cannot_hide_a_foreign_previous_root() {
        let uuid = |byte| Uuid::from_bytes([byte; 16]).to_string();
        let project = BranchId::new(Uuid::from_bytes([1; 16])).unwrap();
        let branch = BranchId::new(Uuid::from_bytes([2; 16])).unwrap();
        let mut row = Reservation {
            branch_id: branch.as_uuid().to_string(),
            database_uuid: uuid(3),
            consumer_id: "memory".into(),
            target_locator: "target".into(),
            target_incarnation: 1,
            generation: 2,
            operation: 3,
            registration_uuid: uuid(4),
            projection_uuid: uuid(5),
            checkpoint_uuid: uuid(6),
            native_birth: NativeBirth::Recorded {
                generation_name: generation_name_for(project, 1, 2, 3),
                parent_locator: "parent".into(),
            },
            lease: None,
            candidate_seal: Some(CandidateSeal {
                source_epoch: 7,
                metadata_length: 1,
                metadata_sha256: "0".repeat(64),
            }),
            publication_intent: None,
        };
        let next = SelectedRoot::from_candidate(&row).unwrap();
        let mut foreign_previous = next.clone();
        foreign_previous.branch_id = uuid(9);
        row.publication_intent = Some(PublicationIntent {
            unregistered: false,
            previous: Some(foreign_previous),
            receipt_complete: false,
            not_selected: false,
        });
        let ledger = Ledger {
            project_id: project.as_uuid().to_string(),
            main_branch_id: branch.as_uuid().to_string(),
            high_water: 3,
            pending: vec![row],
            selected: vec![next],
            policies: Vec::new(),
        };
        assert!(validate(&ledger)
            .unwrap_err()
            .to_string()
            .contains("foreign previous root"));
    }
}
