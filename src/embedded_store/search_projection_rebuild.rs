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

//! Bounded-memory rebuild of an incremental-maintenance search projection
//! from one pinned graph snapshot into a new directory.

use super::*;
use crate::error::SearchProjectionRebuildRefusal;
use crate::search::{
    SearchDocumentSetDigest, SearchMutationSortStepLimits, SearchOutOfCoreGenerationBuildReport,
    SearchOutOfCoreGenerationWriter, SearchProjectionScanBudget, SearchUnorderedMutationLimits,
    SearchUnorderedMutationReport,
};
use crate::SearchProjectionRow;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::AtomicU64;

/// Resource and identity settings for [`EmbeddedStoreHandle::rebuild_out_of_core_search_projection`].
#[derive(Debug, Clone)]
pub struct SearchProjectionRebuildOptions {
    /// The incremental-maintenance options the host will open the rebuilt
    /// directory with. Build options supply the embedding manifest, vector
    /// binding, analyzer and admission limits; their epochs must be unset
    /// because the rebuild stamps the pinned graph snapshot epoch.
    /// `working_memory_bytes` is the hard memory reservation of the rebuild.
    pub maintenance: IncrementalSearchMaintenanceOptions,
    /// Runtime lane. Background competes with other maintenance; an explicit
    /// operator command may select Foreground.
    pub priority: RuntimeWorkPriority,
    /// Graph rows handed to the host hydrator at once. Bounds the only
    /// rebuild-owned row buffer outside the external sort.
    pub hydration_batch_rows: NonZeroUsize,
    /// External-sort admission: buffer size, spill and read totals.
    pub sorting: SearchUnorderedMutationLimits,
    /// Logical work per sort step between cancellation checks.
    pub sort_step: SearchMutationSortStepLimits,
}

impl Default for SearchProjectionRebuildOptions {
    fn default() -> Self {
        Self {
            maintenance: IncrementalSearchMaintenanceOptions::default(),
            priority: RuntimeWorkPriority::Background,
            hydration_batch_rows: NonZeroUsize::new(1024).unwrap(),
            sorting: SearchUnorderedMutationLimits {
                max_buffer_records: NonZeroUsize::new(16_384).unwrap(),
                max_buffer_payload_bytes: NonZeroUsize::new(32 * 1024 * 1024).unwrap(),
                max_input_operations: NonZeroUsize::new(100_000_000).unwrap(),
                max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024).unwrap(),
                max_read_bytes: NonZeroU64::new(16 * 1024 * 1024 * 1024 * 1024).unwrap(),
                max_merge_visits: NonZeroUsize::new(usize::MAX >> 1).unwrap(),
                max_sort_work: NonZeroUsize::new(usize::MAX >> 1).unwrap(),
            },
            sort_step: SearchMutationSortStepLimits::default(),
        }
    }
}

/// Host-owned parts of a projection that the canonical graph mapping cannot
/// derive, read from the same pinned snapshot as the graph rows.
///
/// Every method has a no-op default, so [`GraphOnlySearchProjectionRebuild`]
/// rebuilds exactly the canonical graph rows. A host that attaches external
/// embeddings or owns relational documents implements the same mapping its
/// catch-up hydrator applies. The rebuild calls the source twice, once to
/// build and once to verify, against the same snapshot; both passes must
/// produce the same documents.
///
/// Rows of kinds the store declares externally owned (#1814) never reach
/// `hydrate_graph_rows`: the rebuild carries the current projection's
/// document for each live node over instead. `visit_host_rows` output is
/// unaffected.
pub trait SearchProjectionRebuildSource {
    /// Adjusts one bounded batch of canonical graph rows, for example by
    /// attaching embeddings. Rows may be changed or removed; each document ID
    /// may appear once in the complete rebuild.
    fn hydrate_graph_rows(
        &mut self,
        _snapshot: &mut DatabaseReadTransaction,
        _rows: &mut Vec<SearchProjectionRow>,
    ) -> Result<()> {
        Ok(())
    }

    /// Emits host-owned rows that no graph node maps to, after the graph scan.
    fn visit_host_rows(
        &mut self,
        _snapshot: &mut DatabaseReadTransaction,
        _emit: &mut dyn FnMut(SearchProjectionRow) -> Result<()>,
    ) -> Result<()> {
        Ok(())
    }

    /// Observes progress at batch and phase boundaries. Must not reenter the
    /// handle's write paths for long; the rebuild holds no store lock here.
    fn progress(&mut self, _progress: &SearchProjectionRebuildProgress) {}
}

/// Rebuilds only the canonical graph-derived documents.
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphOnlySearchProjectionRebuild;

impl SearchProjectionRebuildSource for GraphOnlySearchProjectionRebuild {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchProjectionRebuildPhase {
    /// Scanning the graph and feeding the external sort.
    Scan,
    /// Merging sorted runs and building the generation in the private stage.
    Build,
    /// Re-reading the new generation and the graph for comparison.
    Verify,
    /// Moving the verified stage onto the target name.
    Install,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionRebuildProgress {
    pub phase: SearchProjectionRebuildPhase,
    pub source_graph_commit_epoch: u64,
    pub scanned_nodes: u64,
    pub graph_documents: u64,
    pub host_documents: u64,
    pub sorting: SearchUnorderedMutationReport,
}

/// The two independent reads compared before the target is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionRebuildVerification {
    /// Fresh scan of the pinned graph snapshot through the same source.
    pub graph: SearchDocumentSetDigest,
    /// Complete read of the published generation through its reader.
    pub generation: SearchDocumentSetDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionRebuildReport {
    /// The installed, verified projection directory. Open it with
    /// `options.maintenance` to resume incremental maintenance.
    pub target_path: PathBuf,
    /// The pinned graph snapshot stamped as the generation's source epoch.
    /// Catch-up after opening the target resumes from this epoch.
    pub source_graph_commit_epoch: u64,
    pub scanned_nodes: u64,
    pub graph_documents: u64,
    pub host_documents: u64,
    pub sorting: SearchUnorderedMutationReport,
    pub build: SearchOutOfCoreGenerationBuildReport,
    pub verification: SearchProjectionRebuildVerification,
    /// The memory reservation every phase ran under.
    pub working_memory_bytes: u64,
    pub elapsed_micros: u64,
}

/// A verified generation in the stage, ready to install.
struct BuiltStage {
    sorting: SearchUnorderedMutationReport,
    build: SearchOutOfCoreGenerationBuildReport,
    verification: SearchProjectionRebuildVerification,
    counts: StreamCounts,
    epoch: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct StreamCounts {
    scanned_nodes: u64,
    graph_documents: u64,
    host_documents: u64,
}

impl EmbeddedStoreHandle {
    /// Rebuilds an incremental-maintenance search projection from the graph
    /// into `target`, a directory name that must not exist yet.
    ///
    /// The rebuild pins one graph read snapshot at epoch E and streams its
    /// projection rows, in bounded hydration batches, through an external
    /// sort into a new out-of-core generation stamped with E. No step holds
    /// the corpus in memory: rows, sort buffers and artifact construction run
    /// under `options.maintenance.working_memory_bytes`, admitted as
    /// maintenance in `options.priority` and bound to `task` for
    /// cancellation and deadlines. Writers are not blocked; their commits
    /// after E are left to incremental catch-up.
    ///
    /// The generation is built in a private sibling stage
    /// (`.<target name>.hawdb-search-rebuild-*`), opened with the same
    /// incremental options the host will use, and verified: a complete read
    /// of the generation and a second scan of the pinned graph must have the
    /// same document count and content digest. Only then is the stage moved
    /// onto `target` with one no-replace rename followed by directory
    /// barriers. A failure or cancellation removes the stage this call
    /// created; a crash can leave it for the host to remove. Nothing outside
    /// the stage and `target` is written, so the current projection directory
    /// is never modified or deleted and remains evidence.
    ///
    /// Switching is the host's step: reopen the store with
    /// `search_projection_path = target` and `options.maintenance`, then run
    /// catch-up, which replays commits after E. The change log must still
    /// retain them; otherwise catch-up refuses and the rebuild is repeated.
    /// [`HawDBError::PublishOutcomeUnknown`] means the move may have
    /// completed without its durability barrier.
    ///
    /// An existing target, a missing parent, a target that appears before
    /// installation and a verification mismatch return
    /// [`HawDBError::SearchProjectionRebuildRefused`]. That is not a storage
    /// failure: nothing was installed and the graph and current projection
    /// are unchanged. Cancellation keeps the `Execution` cancellation error.
    ///
    /// The graph snapshot stays pinned for the whole rebuild, so versions it
    /// needs are not reclaimed until the call returns.
    pub fn rebuild_out_of_core_search_projection(
        &self,
        target: impl AsRef<Path>,
        options: SearchProjectionRebuildOptions,
        source: &mut dyn SearchProjectionRebuildSource,
        task: RuntimeTaskContext,
    ) -> Result<SearchProjectionRebuildReport> {
        let started = Instant::now();
        options.maintenance.validate()?;
        let target = target.as_ref();
        let stage = RebuildStage::create(target)?;
        let working = options.maintenance.working_memory_bytes.get();
        let permit = {
            let store = self.read_store()?;
            store.graph.check_runtime_context(&task)?;
            store.graph.try_admit_runtime(
                RuntimeWorkRequest::new(options.priority, RuntimeWorkKind::Maintenance)
                    .with_cpu_slots(1)
                    .with_memory_bytes(working)
                    .with_io_slots(1)
                    .with_blocking(true),
            )?
        };
        let task = permit
            .bind_task_context(task)
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(working, 0));
        let (owned, previous) = {
            let store = self.read_store()?;
            (
                Arc::clone(&store.externally_owned_search_kinds),
                store.search_projection_path.clone(),
            )
        };
        let carry_over_available =
            !owned.is_empty() && self.carry_over_unavailable(&owned).is_none();
        let ownership = Ownership {
            handle: self,
            owned: &owned,
            carry_over: carry_over_available,
        };
        let result = self
            .rebuild_into_stage(&stage, &options, source, &ownership, &task)
            .and_then(|built| {
                if !owned.is_empty() {
                    record_ownership(&stage.path, &owned, previous.as_deref())?;
                }
                Ok(built)
            });
        let BuiltStage {
            sorting,
            build,
            verification,
            counts,
            epoch,
        } = match result {
            Ok(value) => value,
            Err(error) => {
                drop(permit);
                return Err(error);
            }
        };
        source.progress(&SearchProjectionRebuildProgress {
            phase: SearchProjectionRebuildPhase::Install,
            source_graph_commit_epoch: epoch,
            scanned_nodes: counts.scanned_nodes,
            graph_documents: counts.graph_documents,
            host_documents: counts.host_documents,
            sorting,
        });
        let installed = stage.install(&task);
        permit.release();
        installed?;
        Ok(SearchProjectionRebuildReport {
            target_path: target.to_path_buf(),
            source_graph_commit_epoch: epoch,
            scanned_nodes: counts.scanned_nodes,
            graph_documents: counts.graph_documents,
            host_documents: counts.host_documents,
            sorting,
            build,
            verification,
            working_memory_bytes: working,
            elapsed_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        })
    }

    fn rebuild_into_stage(
        &self,
        stage: &RebuildStage,
        options: &SearchProjectionRebuildOptions,
        source: &mut dyn SearchProjectionRebuildSource,
        ownership: &Ownership<'_>,
        task: &RuntimeTaskContext,
    ) -> Result<BuiltStage> {
        // One pin serves the build scan, the verification scan and both host
        // views, so every read observes epoch E exactly.
        let scan = {
            let store = self.read_store()?;
            store
                .graph
                .database()
                .begin_read_transaction_with_context(task)?
        };
        let mut host = scan.fork_at_same_snapshot(Some(task.clone()))?;
        let epoch = scan.commit_epoch();
        let maintenance = &options.maintenance;
        let mut build_options = maintenance.build_options.clone();
        build_options.source_graph_commit_epoch = Some(epoch);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create_unordered_replacement_with_context(
                &stage.path,
                build_options,
                &maintenance.reader_config,
                options.sorting,
                task.clone(),
            )?;
        let sort_step = options.sort_step;
        let mut progress = SearchProjectionRebuildProgress {
            phase: SearchProjectionRebuildPhase::Scan,
            source_graph_commit_epoch: epoch,
            scanned_nodes: 0,
            graph_documents: 0,
            host_documents: 0,
            sorting: writer.report(),
        };
        let counts = stream_documents(
            &scan,
            &mut host,
            source,
            ownership,
            Pass::Build,
            options.hydration_batch_rows,
            task,
            &mut progress,
            &mut |document, progress| {
                writer.queue_upsert(document)?;
                while !writer.ready_for_input() {
                    writer.advance_ordering(sort_step)?;
                }
                progress.sorting = writer.report();
                Ok(())
            },
        )?;
        writer.finish_input()?;
        progress.phase = SearchProjectionRebuildPhase::Build;
        while !writer.ordering_complete() {
            writer.advance_ordering(sort_step)?;
            progress.sorting = writer.report();
            source.progress(&progress);
        }
        let (sorting, candidate) = writer.prepare_publication()?;
        let build = candidate.publish()?;

        progress.phase = SearchProjectionRebuildPhase::Verify;
        progress.sorting = sorting;
        source.progress(&progress);
        let projection =
            OutOfCoreSearchProjection::open_incremental(&stage.path, maintenance.clone())?;
        if projection.reader.source_graph_commit_epoch() != Some(epoch) {
            return Err(refused(
                SearchProjectionRebuildRefusal::VerificationMismatch,
                &stage.target,
                format!(
                    "rebuilt search projection is stamped at {:?}, expected graph snapshot epoch {epoch}; nothing was installed",
                    projection.reader.source_graph_commit_epoch()
                ),
            ));
        }
        let build_options = &maintenance.build_options;
        let budget = SearchProjectionScanBudget {
            max_rows: build_options.max_documents.get(),
            max_payload_bytes: usize::try_from(build_options.max_logical_document_bytes.get())
                .unwrap_or(usize::MAX),
            max_encoded_record_bytes: usize::try_from(build_options.max_record_bytes.get())
                .unwrap_or(usize::MAX),
        };
        // Owned documents are carried from a live projection the host may
        // write concurrently, so verification compares only derived kinds;
        // the total still has to match the build.
        let mut generation = SearchDocumentSetDigest::default();
        let mut generation_total = 0usize;
        projection
            .reader
            .visit_projection_documents(budget, task, &mut |document| {
                generation_total += 1;
                if !ownership.owns_document(document) {
                    generation.add(document);
                }
                Ok(())
            })?;
        drop(projection);
        let mut graph = SearchDocumentSetDigest::default();
        let mut verify_progress = progress;
        stream_documents(
            &scan,
            &mut host,
            source,
            ownership,
            Pass::Verify,
            options.hydration_batch_rows,
            task,
            &mut verify_progress,
            &mut |document, _| {
                graph.add(&document);
                Ok(())
            },
        )?;
        if graph != generation || generation_total != build.document_count {
            return Err(refused(
                SearchProjectionRebuildRefusal::VerificationMismatch,
                &stage.target,
                format!(
                    "rebuilt search projection differs from graph snapshot {epoch}: graph {} documents ({}), generation {} documents ({}), build reported {}; nothing was installed",
                    graph.document_count(),
                    graph.content_digest_hex(),
                    generation.document_count(),
                    generation.content_digest_hex(),
                    build.document_count,
                ),
            ));
        }
        Ok(BuiltStage {
            sorting,
            build,
            verification: SearchProjectionRebuildVerification { graph, generation },
            counts,
            epoch,
        })
    }
}

/// Upper bound of one carry-over lookup's payload admission.
const CARRY_OVER_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

#[cfg(test)]
thread_local! {
    /// Makes the previous projection unavailable to carry-over in tests.
    pub(super) static FAIL_CARRY_OVER_PROBE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Externally owned kinds of the store being rebuilt (#1814).
///
/// Rows of owned kinds never reach the host's `hydrate_graph_rows`. In the
/// build pass each live node's document is carried over from the store's
/// current projection when it is readable, and dropped otherwise; host rows
/// pass through. The verification pass excludes owned kinds entirely.
struct Ownership<'a> {
    handle: &'a EmbeddedStoreHandle,
    owned: &'a BTreeSet<SearchProjectionKind>,
    carry_over: bool,
}

impl Ownership<'_> {
    fn owns(&self, kind: SearchProjectionKind) -> bool {
        self.owned.contains(&kind)
    }

    fn owns_document(&self, document: &SearchDocument) -> bool {
        document
            .id
            .split_once(':')
            .and_then(|(kind, _)| SearchProjectionKind::from_name(kind))
            .is_some_and(|kind| self.owns(kind))
    }

    /// The carried rows for `owned`, in order, skipping nodes without a
    /// current document.
    fn carry(&self, owned: Vec<SearchProjectionRow>) -> Result<Vec<SearchProjectionRow>> {
        if owned.is_empty() || !self.carry_over {
            return Ok(Vec::new());
        }
        let ids = owned
            .iter()
            .map(|row| format!("{}:{}", row.kind.as_str(), row.external_id))
            .collect::<Vec<_>>();
        let mut by_id = self
            .handle
            .lookup_previous(&ids)?
            .into_iter()
            .map(|document| (document.id.clone(), document))
            .collect::<BTreeMap<_, _>>();
        Ok(owned
            .into_iter()
            .zip(ids)
            .filter_map(|(row, id)| {
                by_id
                    .remove(&id)
                    .map(|document| row_from_carried(row, document))
            })
            .collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pass {
    Build,
    Verify,
}

/// Records the set and a raised rows-required state in the verified stage,
/// so both install atomically with the generation. Host writes made to the
/// current projection during the rebuild, or after installation until the
/// host switches, are not in the target, so the epoch is always raised and
/// continues past the previous directory's.
fn record_ownership(
    stage: &Path,
    owned: &BTreeSet<SearchProjectionKind>,
    previous: Option<&Path>,
) -> Result<()> {
    use crate::search::projection_ownership::{
        raised_after, read_rows_state, write_record, write_rows_state, ExternallyOwnedRowsState,
    };
    let previous = match previous {
        Some(path) => read_rows_state(path),
        None => Ok(ExternallyOwnedRowsState::default()),
    };
    write_rows_state(stage, raised_after(previous))?;
    write_record(stage, owned)
}

/// Turns a carried document back into the row that reproduces it exactly:
/// `into_document` re-inserts the identity keys removed here.
fn row_from_carried(row: SearchProjectionRow, document: SearchDocument) -> SearchProjectionRow {
    let SearchDocument {
        title,
        content,
        embedding,
        mut metadata,
        ..
    } = document;
    metadata.remove("kind");
    metadata.remove("external_id");
    let source_id = metadata.remove("source_id");
    SearchProjectionRow {
        kind: row.kind,
        external_id: row.external_id,
        title,
        body: content,
        embedding,
        source_id,
        metadata,
    }
}

impl EmbeddedStoreHandle {
    /// Why the current projection cannot serve carry-over, if it cannot.
    fn carry_over_unavailable(&self, owned: &BTreeSet<SearchProjectionKind>) -> Option<String> {
        #[cfg(test)]
        if FAIL_CARRY_OVER_PROBE.with(std::cell::Cell::get) {
            return Some("injected carry-over failure".into());
        }
        let kind = owned.first()?.as_str();
        self.lookup_previous(&[format!("{kind}:")])
            .err()
            .map(|error| error.to_string())
    }

    /// Documents the current projection holds for `ids`, missing ones
    /// omitted, under one bounded read admission.
    fn lookup_previous(&self, ids: &[String]) -> Result<Vec<SearchDocument>> {
        let budget = crate::EmbeddedReadSnapshotBudget {
            max_rows: ids.len(),
            max_payload_bytes: CARRY_OVER_MAX_BATCH_BYTES,
        };
        self.with_bounded_read_snapshot(budget, |snapshot| {
            snapshot
                .lookup_search_projection_documents(ids)
                .map(|lookup| lookup.documents)
        })
    }
}

/// Feeds the pinned graph rows, hydrated in bounded batches, then host rows.
/// Owned kinds are carried over in the build pass and excluded from the
/// verification pass.
#[allow(clippy::too_many_arguments)]
fn stream_documents(
    scan: &DatabaseReadTransaction,
    host: &mut DatabaseReadTransaction,
    source: &mut dyn SearchProjectionRebuildSource,
    ownership: &Ownership<'_>,
    pass: Pass,
    batch_rows: NonZeroUsize,
    task: &RuntimeTaskContext,
    progress: &mut SearchProjectionRebuildProgress,
    sink: &mut dyn FnMut(SearchDocument, &mut SearchProjectionRebuildProgress) -> Result<()>,
) -> Result<StreamCounts> {
    progress.scanned_nodes = 0;
    progress.graph_documents = 0;
    progress.host_documents = 0;
    let mut batch = Vec::with_capacity(batch_rows.get());
    let mut flush = |batch: &mut Vec<SearchProjectionRow>,
                     source: &mut dyn SearchProjectionRebuildSource,
                     host: &mut DatabaseReadTransaction,
                     progress: &mut SearchProjectionRebuildProgress|
     -> Result<()> {
        let (owned, mut rows): (Vec<_>, Vec<_>) =
            batch.drain(..).partition(|row| ownership.owns(row.kind));
        source.hydrate_graph_rows(host, &mut rows)?;
        if pass == Pass::Build {
            rows.extend(ownership.carry(owned)?);
        }
        for row in rows {
            if pass == Pass::Verify && ownership.owns(row.kind) {
                continue;
            }
            sink(row.into_document(), progress)?;
            progress.graph_documents += 1;
        }
        source.progress(progress);
        Ok(())
    };
    let scanned = scan.visit_search_projection_rows(task, &mut |row| {
        batch.push(row);
        if batch.len() == batch_rows.get() {
            flush(&mut batch, source, host, progress)?;
        }
        Ok(())
    })?;
    progress.scanned_nodes = scanned;
    if !batch.is_empty() {
        flush(&mut batch, source, host, progress)?;
    }
    let mut host_documents = 0u64;
    source.visit_host_rows(host, &mut |row| {
        task.checkpoint().map_err(|reason| {
            HawDBError::Execution(format!("search projection rebuild {reason}"))
        })?;
        if pass == Pass::Verify && ownership.owns(row.kind) {
            return Ok(());
        }
        sink(row.into_document(), progress)?;
        host_documents += 1;
        Ok(())
    })?;
    progress.host_documents = host_documents;
    source.progress(progress);
    Ok(StreamCounts {
        scanned_nodes: scanned,
        graph_documents: progress.graph_documents,
        host_documents,
    })
}

fn refused(reason: SearchProjectionRebuildRefusal, target: &Path, message: String) -> HawDBError {
    HawDBError::SearchProjectionRebuildRefused {
        reason,
        target: target.to_path_buf(),
        message,
    }
}

/// A sibling directory created by this call. Dropping an uninstalled stage
/// removes exactly that directory; installation is a no-replace rename.
struct RebuildStage {
    path: PathBuf,
    target: PathBuf,
    installed: bool,
}

impl RebuildStage {
    fn create(target: &Path) -> Result<Self> {
        if std::fs::symlink_metadata(target).is_ok() {
            return Err(refused(
                SearchProjectionRebuildRefusal::TargetExists,
                target,
                format!(
                    "search projection rebuild target {} already exists; the rebuild writes only to a new directory",
                    target.display()
                ),
            ));
        }
        let name = target.file_name().ok_or_else(|| {
            refused(
                SearchProjectionRebuildRefusal::TargetInvalid,
                target,
                format!(
                    "search projection rebuild target {} has no final path component",
                    target.display()
                ),
            )
        })?;
        let parent = match target.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        if !parent.is_dir() {
            return Err(refused(
                SearchProjectionRebuildRefusal::TargetParentMissing,
                target,
                format!(
                    "search projection rebuild parent directory {} does not exist",
                    parent.display()
                ),
            ));
        }
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let path = parent.join(format!(
            ".{}.hawdb-search-rebuild-{}-{nonce}-{}",
            name.to_string_lossy(),
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        // `create_dir` refuses an existing name, so the stage is always ours.
        hawdb_storage::file_io::create_dir(&path).map_err(|error| {
            HawDBError::Storage(format!(
                "create search projection rebuild stage {}: {error}",
                path.display()
            ))
        })?;
        Ok(Self {
            path,
            target: target.to_path_buf(),
            installed: false,
        })
    }

    fn install(mut self, task: &RuntimeTaskContext) -> Result<()> {
        task.checkpoint().map_err(|reason| {
            HawDBError::Execution(format!("search projection rebuild {reason}"))
        })?;
        crate::sync_bootstrap_directory(&self.path)?;
        #[cfg(test)]
        hooks::before_install(&self.path, &self.target);
        if let Err(error) = hawdb_storage::file_io::rename_directory_new(&self.path, &self.target) {
            if std::fs::symlink_metadata(&self.path).is_ok() {
                let message = format!(
                    "search projection rebuild target {} could not be installed ({error}); nothing was installed",
                    self.target.display()
                );
                // Another entry took the name: a refused attempt, not a
                // storage failure. Other I/O errors keep their storage kind.
                return Err(if error.kind() == std::io::ErrorKind::AlreadyExists {
                    refused(
                        SearchProjectionRebuildRefusal::TargetAppeared,
                        &self.target,
                        message,
                    )
                } else {
                    HawDBError::Storage(message)
                });
            }
            self.installed = true;
            return Err(self.outcome_unknown(format!("the move reported {error}")));
        }
        self.installed = true;
        let parent = match self.target.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        crate::sync_bootstrap_directory(&self.target)
            .and_then(|()| crate::sync_bootstrap_directory(parent))
            .map_err(|error| {
                self.outcome_unknown(format!(
                    "the directory barrier after the move failed: {error}"
                ))
            })
    }

    fn outcome_unknown(&self, message: String) -> HawDBError {
        HawDBError::PublishOutcomeUnknown {
            destination: self.target.clone(),
            message: format!("search projection rebuild: {message}"),
        }
    }
}

impl Drop for RebuildStage {
    fn drop(&mut self) {
        if !self.installed {
            let _ = hawdb_storage::file_io::remove_dir_all(&self.path);
        }
    }
}

/// Test-only fault point immediately before the no-replace move. Thread
/// local, so a test only affects rebuilds on its own thread.
#[cfg(test)]
pub(super) mod hooks {
    use std::cell::RefCell;
    use std::path::Path;

    type BeforeInstall = Box<dyn FnOnce(&Path, &Path)>;

    thread_local! {
        static BEFORE_INSTALL: RefCell<Option<BeforeInstall>> = const { RefCell::new(None) };
    }

    /// Runs `hook(stage, target)` before the next install on this thread.
    pub(crate) fn set_before_install(hook: impl FnOnce(&Path, &Path) + 'static) {
        BEFORE_INSTALL.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    }

    pub(super) fn before_install(stage: &Path, target: &Path) {
        let hook = BEFORE_INSTALL.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(stage, target);
        }
    }
}
