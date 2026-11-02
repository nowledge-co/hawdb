// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded physical retirement, rooted in the complete allocation ledger.
//! A parent-side copy of the immutable birth marker survives partial removal.

use super::*;
use hawdb_core::RuntimeTaskContext;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::PathBuf;

pub const CONSUMER_CLEANUP_MEMORY_BOUND: u64 = 16 * MAX_BYTES as u64;
const MAX_SCAN: usize = 65_536;
const MAX_BATCH: usize = 256;
const MAX_CHILDREN: usize = 4096;
const LOCK_FILES: [&str; 2] = [
    ".search_projection.publish.lock",
    ".search-source-process.lock",
];

#[derive(Debug, Clone)]
pub struct ConsumerCleanupOptions {
    pub max_generations: NonZeroUsize,
    pub max_delete_attempts: NonZeroUsize,
    /// Continue a bounded sweep after this opaque native child name. Start a
    /// fresh sweep with None to retry previously pinned or failed generations.
    pub after: Option<String>,
}
impl Default for ConsumerCleanupOptions {
    fn default() -> Self {
        Self {
            max_generations: NonZeroUsize::new(32).unwrap(),
            max_delete_attempts: NonZeroUsize::new(256).unwrap(),
            after: None,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConsumerCleanupReport {
    pub scanned_entries: usize,
    pub examined_generations: usize,
    pub retained_generations: usize,
    pub busy_generations: usize,
    pub failed_generations: usize,
    pub reclaimed_generations: usize,
    pub delete_attempts: usize,
    pub deleted_files: usize,
    pub progress_limited: bool,
    pub next_after: Option<String>,
    pub retry_required: bool,
}

/// Original source custody, with no I/O or active work admission of its own.
/// The search layer supplies publication/process exclusion and reader checks.
#[doc(hidden)]
#[derive(Debug)]
pub struct ConsumerCleanupRequest {
    pub(crate) source: ConsumerAllocationSource,
    pub(crate) database: Uuid,
    pub(crate) consumer: String,
    pub(crate) locator: PathBuf,
}

struct CleanupPass<'a, F, K> {
    parent: &'a SourceDirectory,
    journal_prefix: &'a str,
    options: &'a ConsumerCleanupOptions,
    task: &'a RuntimeTaskContext,
    report: &'a mut ConsumerCleanupReport,
    exclude: &'a mut F,
    known_file: &'a K,
}

impl ConsumerCleanupRequest {
    pub fn capture_parent(&self) -> Result<SourceDirectory> {
        let context = self.source.owner.captured_directory().operation_context();
        Ok(SourceDirectory::capture(self.parent_label()?, &context)?)
    }

    fn parent_label(&self) -> Result<&Path> {
        self.locator
            .parent()
            .ok_or_else(|| invalid("cleanup target has no parent"))
    }

    /// The caller prepays CONSUMER_CLEANUP_MEMORY_BOUND and I/O on its existing
    /// governor. `exclude` must keep native publication/process custody live
    /// and reject every pending capture or reader pin until its guard drops.
    pub fn run<G>(
        &self,
        parent: &SourceDirectory,
        options: &ConsumerCleanupOptions,
        task: &RuntimeTaskContext,
        mut exclude: impl FnMut(&SourceDirectory) -> Result<G>,
        known_file: impl Fn(&str) -> bool,
    ) -> Result<ConsumerCleanupReport> {
        if options.max_generations.get() > MAX_BATCH || options.max_delete_attempts.get() < 5 {
            return Err(invalid(
                "cleanup requires at most 256 generations and at least five delete attempts",
            ));
        }
        if !task.has_resource_controllers()
            || task.memory_reservation().is_none_or(|reservation| {
                reservation.memory_bytes() < CONSUMER_CLEANUP_MEMORY_BOUND
            })
        {
            return Err(HawDBError::Execution(
                "Consumer cleanup requires prepaid runtime admission".into(),
            ));
        }
        if !parent
            .shares_project_context(&self.source.owner.captured_directory().operation_context())
        {
            return Err(invalid(
                "Consumer cleanup parent belongs to another descriptor domain",
            ));
        }
        let context = parent.operation_context();
        let initial = self
            .locator
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| invalid("invalid cleanup target name"))?;
        let domain = self.source.project.allocation_identity().project_id();
        let prefix = format!("{GENERATION_DIRECTORY_PREFIX}{}-", domain.as_uuid());
        let journal_prefix = format!(".consumer-reclaim-{}-", domain.as_uuid());
        let mut names = BTreeSet::new();
        let mut report = ConsumerCleanupReport::default();
        // Select the lexicographically earliest bounded batch. Enumeration is
        // capped independently and completes before any deletion is authorized.
        parent.visit_children(&context, MAX_SCAN, |name| {
            task.checkpoint().map_err(io::Error::other)?;
            report.scanned_entries += 1;
            if (name == initial || name.starts_with(&prefix) || name.starts_with(&journal_prefix))
                && options.after.as_deref().is_none_or(|after| name > after)
            {
                names.insert(name.to_owned());
                if names.len() > options.max_generations.get() {
                    names.pop_last();
                    report.progress_limited = true;
                }
            }
            Ok(())
        })?;
        if report.progress_limited {
            report.next_after = names.last().cloned();
        }
        for name in names {
            task.checkpoint()
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
            if report.delete_attempts >= options.max_delete_attempts.get() {
                report.progress_limited = true;
                // Restart to revisit this unattempted entry; never skip work.
                report.next_after = None;
                break;
            }
            report.examined_generations += 1;
            let result =
                self.source
                    .project
                    .with_locked_catalog(|root, source_context, catalog| {
                        let ledger = check_source(
                            &self.source.project,
                            &self.source.owner,
                            self.source.branch,
                            root,
                            source_context,
                            &catalog,
                        )?;
                        // A prior selector/compaction may have lost its final
                        // barrier or reply. Persist this exact locked image
                        // before absence in it can authorize irreversible GC.
                        root.open_mutable_child(source_context, LEDGER_FILE, false)?
                            .sync_all()?;
                        root.sync(source_context)?;
                        self.reclaim_one(
                            &name,
                            &ledger,
                            &mut CleanupPass {
                                parent,
                                journal_prefix: &journal_prefix,
                                options,
                                task,
                                report: &mut report,
                                exclude: &mut exclude,
                                known_file: &known_file,
                            },
                        )
                    });
            match result {
                Ok(()) => {}
                Err(HawDBError::ProjectionBusy) => report.busy_generations += 1,
                Err(_) => report.failed_generations += 1,
            }
        }
        report.retry_required = report.progress_limited
            || report.failed_generations != 0
            || report.busy_generations != 0;
        Ok(report)
    }

    fn reclaim_one<G>(
        &self,
        name: &str,
        ledger: &Ledger,
        pass: &mut CleanupPass<
            '_,
            impl FnMut(&SourceDirectory) -> Result<G>,
            impl Fn(&str) -> bool,
        >,
    ) -> Result<()> {
        let CleanupPass {
            parent,
            journal_prefix,
            options,
            task,
            report,
            exclude,
            known_file,
        } = pass;
        let context = parent.operation_context();
        let from_journal = name.starts_with(*journal_prefix);
        let directory = if from_journal {
            None
        } else {
            match parent.open_directory(&context, name) {
                Ok(directory) => Some(directory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        };
        let marker_name = native_birth::marker_file_name();
        let bytes = if let Some(directory) = &directory {
            read_bytes(directory, marker_name)?
        } else {
            match read_bytes(parent, name) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        };
        let row = decode_marker(&bytes, ledger)?;
        let NativeBirth::Pending {
            generation_name,
            parent_locator,
        } = &row.native_birth
        else {
            return Err(invalid("cleanup marker has no birth identity"));
        };
        let journal = format!("{journal_prefix}{}.hawdb", row.operation);
        if (from_journal && name != journal) || (!from_journal && name != generation_name) {
            return Err(invalid("cleanup name differs from birth"));
        }
        if row.branch_id != self.source.branch.as_uuid().to_string()
            || row.database_uuid != self.database.to_string()
            || row.consumer_id != self.consumer
            || Path::new(&row.target_locator) != self.locator
            || Path::new(parent_locator) != self.parent_label()?
        {
            // A shared parent can contain other databases, branches or consumers.
            return Ok(());
        }
        if retained(ledger, &row) {
            report.retained_generations += 1;
            return Ok(());
        }
        let directory = match directory {
            Some(directory) => Some(directory),
            None => match parent.open_directory(&context, generation_name) {
                Ok(directory) => Some(directory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            },
        };
        let Some(directory) = directory else {
            // A prior attempt may have removed the directory but lost its reply
            // or parent barrier. Complete that barrier before forgetting intent.
            parent.sync(&context)?;
            report.delete_attempts += 1;
            parent.remove_child(&context, &journal)?;
            parent.sync(&context)?;
            report.reclaimed_generations += 1;
            return Ok(());
        };
        let _exclusion = exclude(&directory)?;
        let marker_present = match read_bytes(&directory, marker_name) {
            Ok(observed) if observed == bytes => true,
            Ok(_) => return Err(invalid("cleanup birth marker changed")),
            Err(error) if from_journal && error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        let mut children = Vec::new();
        directory.visit_children(&context, MAX_CHILDREN, |child| {
            if child != marker_name && !LOCK_FILES.contains(&child) {
                if !marker_present || !known_file(child) {
                    return Err(io::Error::other("cleanup retains an unknown child"));
                }
                // Reject directories and symlinks before removing any evidence.
                let file = directory.open_child(&context, child)?;
                if !file.metadata()?.is_file() {
                    return Err(io::Error::other("cleanup child is not a regular file"));
                }
                children.push(child.to_owned());
            }
            Ok(())
        })?;
        if !from_journal {
            match read_bytes(parent, &journal) {
                Ok(observed) if observed == bytes => {}
                Ok(_) => return Err(invalid("cleanup intent differs from birth")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let temporary =
                        format!("consumer-reclaim-{}.tmp", hawdb_core::generate_uuidv7()?);
                    let mut candidate = parent.create_control_candidate(&context, &temporary)?;
                    candidate.write_all(&bytes)?;
                    candidate.seal()?;
                    candidate.replace_head(&context, &journal)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        // Refresh the durable intent barrier, including a previous lost reply.
        parent
            .open_mutable_child(&context, &journal, false)?
            .sync_all()?;
        parent.sync(&context)?;
        for child in children {
            task.checkpoint()
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
            if report.delete_attempts >= options.max_delete_attempts.get() {
                directory.sync(&context)?;
                report.progress_limited = true;
                return Ok(());
            }
            report.delete_attempts += 1;
            directory.remove_child(&context, &child)?;
            report.deleted_files += 1;
        }
        // Payload absence is durable before the last in-directory witness goes.
        directory.sync(&context)?;
        // Finalization is one fixed five-unlink unit. Never exceed the caller's
        // budget or strand an unjournaled partial retirement.
        if options
            .max_delete_attempts
            .get()
            .saturating_sub(report.delete_attempts)
            < 5
        {
            report.progress_limited = true;
            return Ok(());
        }
        for child in LOCK_FILES.into_iter().chain([marker_name]) {
            report.delete_attempts += 1;
            match directory.remove_child(&context, child) {
                Ok(()) => report.deleted_files += 1,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        directory.sync(&context)?;
        report.delete_attempts += 1;
        parent.remove_reclaimed_directory(&context, generation_name, &directory)?;
        drop(directory);
        drop(_exclusion);
        parent.sync(&context)?;
        report.delete_attempts += 1;
        parent.remove_child(&context, &journal)?;
        parent.sync(&context)?;
        report.reclaimed_generations += 1;
        Ok(())
    }
}

fn read_bytes(directory: &SourceDirectory, name: &str) -> io::Result<Vec<u8>> {
    let file = directory.open_child(&directory.operation_context(), name)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > MAX_BYTES as u64 {
        return Err(io::Error::other(
            "cleanup marker exceeds byte limit or is not a file",
        ));
    }
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BYTES {
        return Err(io::Error::other("cleanup marker exceeds byte limit"));
    }
    Ok(bytes)
}

fn decode_marker(bytes: &[u8], ledger: &Ledger) -> Result<Reservation> {
    const HEADER: &[u8] = b"HAWDB_CONSUMER_BIRTH_V2\n";
    if !bytes.starts_with(HEADER) || bytes.len() < HEADER.len() + 4 {
        return Err(invalid("cleanup birth header is incomplete"));
    }
    let end = bytes.len() - 4;
    if hawdb_integrity::crc32c(&bytes[..end]).get()
        != u32::from_le_bytes(bytes[end..].try_into().unwrap())
    {
        return Err(invalid("cleanup birth checksum differs"));
    }
    let (domain, row): (String, Reservation) = serde_json::from_slice(&bytes[HEADER.len()..end])
        .map_err(|_| invalid("cleanup birth payload is invalid"))?;
    if domain != ledger.project_id
        || row.candidate_seal.is_some()
        || row.publication_intent.is_some()
    {
        return Err(invalid(
            "cleanup birth belongs to another allocation domain",
        ));
    }
    let NativeBirth::Pending {
        generation_name,
        parent_locator,
    } = &row.native_birth
    else {
        return Err(invalid("cleanup birth state is invalid"));
    };
    let isolated = Ledger {
        project_id: ledger.project_id.clone(),
        main_branch_id: ledger.main_branch_id.clone(),
        high_water: ledger.high_water,
        pending: vec![row.clone()],
        selected: Vec::new(),
        policies: Vec::new(),
    };
    validate(&isolated)?;
    if native_birth::marker_bytes(
        BranchId::parse(&domain).map_err(|_| invalid("invalid domain"))?,
        &row,
        generation_name,
        parent_locator,
    )? != bytes
    {
        return Err(invalid("cleanup birth is noncanonical"));
    }
    Ok(row)
}

fn retained(ledger: &Ledger, row: &Reservation) -> bool {
    // Every branch participates, even with no open handle. Expiry and process
    // liveness do not revoke a durable selection or an unresolved operation.
    ledger.pending.iter().any(|pending| {
        pending.operation == row.operation
            || pending.publication_intent.as_ref().is_some_and(|intent| {
                intent.pending()
                    && intent
                        .previous
                        .as_ref()
                        .is_some_and(|previous| previous.operation == row.operation)
            })
    }) || ledger.selected.iter().any(|selected| {
        selected.operation == row.operation
            || (selected.target_incarnation == row.target_incarnation
                && selected.registration_uuid == row.registration_uuid
                && row.operation.checked_sub(2) == Some(row.target_incarnation))
    })
}
