//! GraphStore integration for immutable-root publication and WAL handoff.

use super::{GraphStore, MANIFEST_FILE};
use crate::checkpoint_closure::CheckpointArtifactInput;
use crate::config::{DurabilityPolicy, RecoveryMode, WalReplayConfig};
use crate::durable_manifest::DurableManifest;
use crate::error::{HawDBError, Result};
use crate::file_io as fs;
use crate::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
use crate::ownership::{DatabaseDirectoryLease, DatabaseDirectoryLeaseError};
use crate::relational::RelationalRecoverySourceBuilder;
use crate::sealed_wal::{
    prepare_wal_rotation, seal_wal_file, PreparedWalRotation, SealedWalPublication,
};
use crate::{
    branch_catalog, branch_head,
    sealed_root::{CheckpointArtifactBinding, SealedRoot},
};
use std::collections::{btree_map::Entry, BTreeMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Three retained target handles (UUID/runtime locks and mutable WAL), the
// bounded segment-read wave, and four catalog/publication temporary handles.
// Additional retained recovery artifacts still obey the same project cap.
const MIN_BRANCH_ADMISSION_DESCRIPTORS: usize =
    3 + crate::scan::SHARED_SEGMENT_READ_WORKER_LIMIT + 4;

/// A sealed source with ownership retained until child publication completes.
/// No graph/schema runtime or checkpoint data is materialized by this path.
#[derive(Debug)]
#[doc(hidden)]
pub struct SealedBranchSource {
    _lease: DatabaseDirectoryLease,
    catalog_path: PathBuf,
    head_path: PathBuf,
    head: branch_head::BranchHead,
    max_wal_bytes: u64,
}

impl SealedBranchSource {
    pub fn head(&self) -> &branch_head::BranchHead {
        &self.head
    }

    pub fn create_child(
        &self,
        child_head_path: &Path,
        child_wal_path: &Path,
        request: branch_catalog::CreateRequest,
    ) -> Result<branch_catalog::BranchCreateResult> {
        create_child_from_sealed_head(
            &self.catalog_path,
            &self.head_path,
            child_head_path,
            child_wal_path,
            self.head,
            self.max_wal_bytes,
            request,
        )
    }
}

/// The selected mutable branch binding, separate from its immutable lineage.
#[derive(Debug, Clone)]
pub(super) struct BranchRuntimeBinding {
    catalog_path: PathBuf,
    immutable_store_root: PathBuf,
    head_path: PathBuf,
    metadata_revision: u64,
    pub(super) head: branch_head::BranchHead,
    root: Arc<SealedRoot>,
    checkpoint_generation: u64,
    max_sealed_wal_intervals: usize,
}

/// A prepared immutable-root handoff. The successor WAL exists and is durable,
/// but the branch selector has not been switched yet.
#[derive(Debug)]
pub struct PreparedImmutableRootHandoff {
    pub root: SealedRoot,
    pub rotation: PreparedWalRotation,
    pub immutable_store_root: PathBuf,
}

/// Inputs to storage-owned branch admission or closed-source sealing.
///
/// The caller resolves a branch name before constructing this request, then
/// supplies the observed UUID and metadata revision.  The admission kernel
/// revalidates both around recovery or WAL validation, so name reuse
/// or a concurrent lifecycle transition cannot expose the wrong runtime.
#[doc(hidden)]
pub struct BranchAdmissionRequest<'a> {
    pub catalog_path: &'a Path,
    pub branch_id: branch_catalog::BranchId,
    pub expected_metadata_revision: u64,
    pub head_path: &'a Path,
    pub immutable_store_root: &'a Path,
    pub durability: DurabilityPolicy,
    pub replay_config: WalReplayConfig,
}

/// A recovered branch runtime that retains the target branch writer lease.
///
/// Immutable bindings are materialized below the target branch; WAL recovery
/// dependencies remain in its persistent data directory. The runtime selector
/// is disposable, but the branch head, private WAL, immutable objects, and
/// mutable recovery dependencies survive closure of the handle.
#[derive(Debug)]
#[doc(hidden)]
pub struct AdmittedBranchStore {
    store: GraphStore,
    catalog: crate::schema::Catalog,
}

impl AdmittedBranchStore {
    pub fn store(&self) -> &GraphStore {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut GraphStore {
        &mut self.store
    }

    pub fn catalog(&self) -> &crate::schema::Catalog {
        &self.catalog
    }

    pub fn store_and_catalog_mut(&mut self) -> (&mut GraphStore, &mut crate::schema::Catalog) {
        (&mut self.store, &mut self.catalog)
    }

    pub fn head(&self) -> &branch_head::BranchHead {
        self.store
            .admitted_branch_head()
            .expect("admitted store retains its branch binding")
    }

    /// Transfers the recovered runtime and schema while preserving ownership.
    pub fn into_parts(self) -> (GraphStore, crate::schema::Catalog) {
        (self.store, self.catalog)
    }
}

/// Typed rejection classes for direct branch admission.
#[derive(Debug)]
#[doc(hidden)]
pub enum BranchAdmissionError {
    Busy(&'static str),
    Lease(DatabaseDirectoryLeaseError),
    Catalog(io::Error),
    UnknownBranch,
    InvalidState(branch_catalog::BranchState),
    StaleMetadataRevision { expected: u64, actual: u64 },
    IdentityMismatch(&'static str),
    SealedWalLimit { intervals: usize, limit: usize },
    Recovery(HawDBError),
}

impl std::fmt::Display for BranchAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(resource) => write!(formatter, "branch admission is busy: {resource}"),
            Self::Lease(error) => write!(formatter, "branch admission lease failed: {error}"),
            Self::Catalog(error) => write!(formatter, "branch admission catalog failed: {error}"),
            Self::UnknownBranch => formatter.write_str("branch admission target is unknown"),
            Self::InvalidState(state) => {
                write!(formatter, "branch admission target is not ready: {state:?}")
            }
            Self::StaleMetadataRevision { expected, actual } => write!(
                formatter,
                "branch admission metadata revision is stale: expected {expected}, found {actual}"
            ),
            Self::IdentityMismatch(reason) => {
                write!(formatter, "branch admission identity mismatch: {reason}")
            }
            Self::SealedWalLimit { intervals, limit } => write!(
                formatter,
                "branch admission sealed WAL interval limit exceeded: {intervals} > {limit}"
            ),
            Self::Recovery(error) => write!(formatter, "branch admission recovery failed: {error}"),
        }
    }
}

impl std::error::Error for BranchAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Lease(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Recovery(error) => Some(error),
            _ => None,
        }
    }
}

impl GraphStore {
    #[doc(hidden)]
    pub fn reserve_branch_project_identity(
        &self,
        proposed: crate::branch_project::ProjectSelector,
    ) -> Result<crate::branch_project::ProjectSelector> {
        self.ensure_usable()?;
        self.durable
            .as_ref()
            .ok_or_else(|| {
                HawDBError::Storage("project bootstrap requires durable storage".into())
            })?
            .reserve_project_bootstrap_identity(proposed)
    }

    #[doc(hidden)]
    pub fn reserve_branch_admission_resources(
        &self,
    ) -> Result<crate::file_descriptors::DescriptorReservation> {
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage("branch admission requires a durable project".into())
        })?;
        durable.reserve_branch_admission_resources(MIN_BRANCH_ADMISSION_DESCRIPTORS)
    }
    pub fn file_descriptor_metrics(
        &self,
    ) -> Option<crate::file_descriptors::FileDescriptorMetrics> {
        self.durable
            .as_ref()
            .map(|durable| durable.file_descriptor_metrics())
            .or_else(|| {
                self.branch_runtime_owner
                    .as_ref()
                    .map(|owner| owner.metrics())
            })
    }

    #[doc(hidden)]
    pub fn file_descriptor_context(&self) -> Option<crate::file_descriptors::FileOpenContext> {
        self.durable
            .as_ref()
            .map(|durable| durable.file_descriptor_context())
            .or_else(|| {
                self.branch_runtime_owner
                    .as_ref()
                    .map(|owner| owner.io_context())
            })
            .or_else(|| self.snapshot_file_context.clone())
    }
    /// Seals a closed source from its selector and complete private WAL only.
    /// Existing immutable checkpoint references are reused without recovery.
    /// The lease also pins immutable candidates against catalog-backed GC.
    #[doc(hidden)]
    pub fn seal_branch_from_head(
        request: BranchAdmissionRequest<'_>,
        expected_commit_epoch: u64,
    ) -> std::result::Result<SealedBranchSource, BranchAdmissionError> {
        let project_path =
            request
                .catalog_path
                .parent()
                .ok_or(BranchAdmissionError::IdentityMismatch(
                    "catalog has no project directory",
                ))?;
        let project_files = crate::file_descriptors::ProjectFileDescriptors::acquire_containing(
            project_path,
            request.replay_config.max_open_files,
        )
        .map_err(BranchAdmissionError::Recovery)?;
        let _admission_resources = project_files
            .reserve_admission(MIN_BRANCH_ADMISSION_DESCRIPTORS)
            .map_err(|error| {
                BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
            })?;
        if matches!(
            request.replay_config.recovery_mode,
            RecoveryMode::AutoRepairTornTail | RecoveryMode::DoctorRepairTornTail
        ) {
            return Err(BranchAdmissionError::Recovery(HawDBError::Storage(
                "automatic repair of a branch private WAL requires branch-aware repair publication"
                    .into(),
            )));
        }
        let before = ready_branch_record(
            request.catalog_path,
            request.branch_id,
            request.expected_metadata_revision,
        )?;
        let directory =
            request
                .head_path
                .parent()
                .ok_or(BranchAdmissionError::IdentityMismatch(
                    "branch head has no branch directory",
                ))?;
        let lease =
            DatabaseDirectoryLease::acquire(directory).map_err(BranchAdmissionError::Lease)?;
        let (head, mut root, mut objects) = (|| -> Result<_> {
            let head = branch_head::read_branch_head(request.head_path)
                .map_err(HawDBError::from_storage_error)?;
            let objects = ImmutableObjectStore::open(request.immutable_store_root)
                .map_err(HawDBError::from_storage_error)?;
            let bytes = objects
                .read(head.sealed_root)
                .map_err(HawDBError::from_storage_error)?;
            let root = SealedRoot::decode(&bytes).map_err(HawDBError::from_storage_error)?;
            Ok((head, root, objects))
        })()
        .map_err(BranchAdmissionError::Recovery)?;
        let reference = root.object_reference().map_err(|error| {
            BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
        })?;
        validate_admission_binding(&before, &head, &root, reference)?;
        if root.sealed_wals.len() > request.replay_config.max_branch_sealed_wal_intervals {
            return Err(BranchAdmissionError::SealedWalLimit {
                intervals: root.sealed_wals.len(),
                limit: request.replay_config.max_branch_sealed_wal_intervals,
            });
        }
        let path = directory.join(crate::artifact_files::wal_generation_file(
            head.active_wal.generation,
        ));
        let max_wal_bytes = request.replay_config.max_bytes.unwrap_or(u64::MAX);
        let (commit_epoch, end_lsn) =
            validate_source_wal(&objects, &root, &head, &path, request.replay_config)
                .map_err(BranchAdmissionError::Recovery)?;
        if commit_epoch != expected_commit_epoch {
            return Err(BranchAdmissionError::Recovery(HawDBError::Semantic(format!(
                "branch source revision is stale: expected {expected_commit_epoch}, found {commit_epoch}"
            ))));
        }
        ready_branch_record(
            request.catalog_path,
            request.branch_id,
            request.expected_metadata_revision,
        )?;
        let head = if end_lsn == head.active_wal.replay_start_lsn {
            head
        } else {
            let intervals = root.sealed_wals.len().checked_add(1).ok_or_else(|| {
                BranchAdmissionError::Recovery(HawDBError::Storage(
                    "sealed WAL count overflow".into(),
                ))
            })?;
            if intervals > request.replay_config.max_branch_sealed_wal_intervals {
                return Err(BranchAdmissionError::SealedWalLimit {
                    intervals,
                    limit: request.replay_config.max_branch_sealed_wal_intervals,
                });
            }
            (|| -> Result<_> {
                let prepared = prepare_fresh_branch_wal_rotation(
                    &path,
                    directory,
                    head.active_wal.generation,
                    head.active_wal.replay_start_lsn,
                    max_wal_bytes,
                    &mut objects,
                )?;
                if prepared.sealed.end_lsn != end_lsn {
                    return Err(HawDBError::StorageIntegrity(
                        "source WAL changed while leased".into(),
                    ));
                }
                root.commit_epoch = commit_epoch;
                root.sealed_wals
                    .push(crate::sealed_root::SealedWalReference {
                        start_lsn: prepared.sealed.start_lsn,
                        end_lsn,
                        object: prepared.sealed.object,
                    });
                branch_head::publish_prepared_wal_rotation_with_root(
                    request.head_path,
                    branch_head::PreparedWalRootPublicationRequest {
                        expected_current_generation: head.physical_generation,
                        project_id: head.project_id,
                        branch_id: head.branch_id,
                        logical_commit_epoch: commit_epoch,
                        prepared: &prepared,
                        max_active_wal_bytes: max_wal_bytes,
                    },
                    &root,
                    &mut objects,
                )
                .map_err(wal_rotation_publication_error)
            })()
            .map_err(BranchAdmissionError::Recovery)?
        };
        Ok(SealedBranchSource {
            _lease: lease,
            catalog_path: request.catalog_path.to_path_buf(),
            head_path: request.head_path.to_path_buf(),
            head,
            max_wal_bytes,
        })
    }

    /// Creates an isolated child branch from the selected sealed parent head.
    /// The catalog/head primitives own the durable state machine; this method
    /// only derives the child request from the live parent selector and keeps
    /// the parent data files untouched.
    #[doc(hidden)]
    pub fn create_isolated_branch_from_parent_head(
        &self,
        catalog_path: impl AsRef<Path>,
        parent_head_path: impl AsRef<Path>,
        child_head_path: impl AsRef<Path>,
        child_wal_path: impl AsRef<Path>,
        request: branch_catalog::CreateRequest,
    ) -> Result<branch_catalog::BranchCreateResult> {
        self.ensure_usable()?;
        let parent = branch_head::read_branch_head(parent_head_path.as_ref())
            .map_err(HawDBError::from_storage_error)?;
        if let Some(selected) = self.admitted_branch_head()
            && (selected != &parent || self.commit_epoch != request.source_commit_epoch)
        {
            return Err(HawDBError::Semantic(
                "child source must be the selected branch sealed at its current revision"
                    .to_string(),
            ));
        }
        create_child_from_sealed_head(
            catalog_path.as_ref(),
            parent_head_path.as_ref(),
            child_head_path.as_ref(),
            child_wal_path.as_ref(),
            parent,
            self.durable
                .as_ref()
                .and_then(|durable| durable.max_wal_bytes)
                .unwrap_or(u64::MAX),
            request,
        )
    }

    /// Admits a ready branch directly from its own selector, immutable root,
    /// and private active WAL.
    ///
    /// This is deliberately separate from [`Self::open_from_branch_head`]. The
    /// latter remains a recovery-qualification helper because it copies a
    /// source directory; this admission path never reads that directory. The
    /// target branch directory is leased before recovery and the catalog is
    /// checked again after recovery, so a busy or remapped target cannot
    /// replace an already-active caller context.
    #[doc(hidden)]
    pub fn admit_branch_from_head(
        request: BranchAdmissionRequest<'_>,
    ) -> std::result::Result<AdmittedBranchStore, BranchAdmissionError> {
        Self::admit_branch_from_head_with_cleanup(request, |runtime_directory| {
            let _ = fs::remove_dir_all(runtime_directory);
        })
    }

    fn admit_branch_from_head_with_cleanup(
        request: BranchAdmissionRequest<'_>,
        cleanup: impl FnOnce(&Path),
    ) -> std::result::Result<AdmittedBranchStore, BranchAdmissionError> {
        let project_path =
            request
                .catalog_path
                .parent()
                .ok_or(BranchAdmissionError::IdentityMismatch(
                    "catalog has no project directory",
                ))?;
        let project_files = crate::file_descriptors::ProjectFileDescriptors::acquire_containing(
            project_path,
            request.replay_config.max_open_files,
        )
        .map_err(BranchAdmissionError::Recovery)?;
        let _admission_resources = project_files
            .reserve_admission(MIN_BRANCH_ADMISSION_DESCRIPTORS)
            .map_err(|error| {
                BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
            })?;
        let total_open_started = std::time::Instant::now();
        if matches!(
            request.replay_config.recovery_mode,
            RecoveryMode::AutoRepairTornTail | RecoveryMode::DoctorRepairTornTail
        ) {
            return Err(BranchAdmissionError::Recovery(HawDBError::Storage(
                "automatic repair of a branch private WAL requires branch-aware repair publication"
                    .to_string(),
            )));
        }
        let before = ready_branch_record(
            request.catalog_path,
            request.branch_id,
            request.expected_metadata_revision,
        )?;
        let branch_directory =
            request
                .head_path
                .parent()
                .ok_or(BranchAdmissionError::IdentityMismatch(
                    "branch head has no branch directory",
                ))?;
        let branch_lease = DatabaseDirectoryLease::acquire(branch_directory)
            .map_err(BranchAdmissionError::Lease)?;
        let head = branch_head::read_branch_head(request.head_path).map_err(|error| {
            BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
        })?;
        let objects =
            ImmutableObjectStore::open(request.immutable_store_root).map_err(|error| {
                BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
            })?;
        let root_bytes = objects.read(head.sealed_root).map_err(|error| {
            BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
        })?;
        let root = SealedRoot::decode(&root_bytes).map_err(|error| {
            BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
        })?;
        let root_reference = root.object_reference().map_err(|error| {
            BranchAdmissionError::Recovery(HawDBError::from_storage_error(error))
        })?;
        validate_admission_binding(&before, &head, &root, root_reference)?;
        if root.sealed_wals.len() > request.replay_config.max_branch_sealed_wal_intervals {
            return Err(BranchAdmissionError::SealedWalLimit {
                intervals: root.sealed_wals.len(),
                limit: request.replay_config.max_branch_sealed_wal_intervals,
            });
        }

        let active_wal_path = branch_directory.join(crate::artifact_files::wal_generation_file(
            head.active_wal.generation,
        ));
        let max_wal_bytes = request.replay_config.max_bytes.unwrap_or(u64::MAX);
        branch_head::validate_active_wal_prefix_from_file(
            &active_wal_path,
            head.active_wal,
            max_wal_bytes,
        )
        .map_err(|error| BranchAdmissionError::Recovery(HawDBError::from_storage_error(error)))?;

        let runtime_directory = branch_directory.join("runtime");
        // Mutable WAL dependencies survive disposable-runtime cleanup. They
        // belong to this UUID, never to its parent or to the materialization.
        let artifact_directory = branch_directory.join("data");
        let result = (|| {
            materialize_branch_runtime(&objects, &root, &runtime_directory, &artifact_directory)
                .map_err(BranchAdmissionError::Recovery)?;
            let mut catalog = crate::schema::Catalog::default();
            let manifest_open_started = std::time::Instant::now();
            let durable = super::durable::DurableStore::open_branch_runtime(
                &runtime_directory,
                &artifact_directory,
                request.durability,
                request.replay_config,
            )
            .map_err(BranchAdmissionError::Recovery)?;
            let (mut store, _) = GraphStore::finish_open_with_replay(
                durable,
                &mut catalog,
                request.replay_config,
                super::elapsed_micros(manifest_open_started),
                |store, catalog, replay_config| {
                    let durable = store.durable.as_ref().expect("opened durable store");
                    // Anchor the recovery identity at the manifest generation
                    // and hash every validated record in contiguous LSN order,
                    // across sealed generations and the private suffix.
                    let mut source = RelationalRecoverySourceBuilder::new(
                        durable.wal_generation,
                        durable.wal_replay_start_lsn,
                    );
                    store.storage_recovery_report =
                        store.replay_wal_interval(catalog, replay_config, &mut source)?;
                    store.replay_sealed_wals(
                        catalog,
                        &root,
                        &objects,
                        &runtime_directory,
                        replay_config,
                        &mut source,
                    )?;
                    store.replay_branch_active_wal(
                        catalog,
                        &head,
                        &active_wal_path,
                        replay_config,
                        &mut source,
                    )?;
                    store.finish_wal_recovery(
                        source,
                        store.storage_recovery_report.replayed_wal_entries,
                    )?;
                    Ok(std::mem::take(&mut store.storage_recovery_report))
                },
            )
            .map_err(BranchAdmissionError::Recovery)?;
            let activation_started = std::time::Instant::now();
            store
                .activate_out_of_core_relational_rows()
                .map_err(BranchAdmissionError::Recovery)?;
            store
                .storage_recovery_report
                .open_timings
                .post_replay_open_micros = store
                .storage_recovery_report
                .open_timings
                .post_replay_open_micros
                .saturating_add(super::elapsed_micros(activation_started));

            let after = ready_branch_record(
                request.catalog_path,
                request.branch_id,
                request.expected_metadata_revision,
            )?;
            validate_admission_binding(&after, &head, &root, root_reference)?;
            store.storage_recovery_report.open_timings.total_open_micros =
                super::elapsed_micros(total_open_started);
            Ok((store, catalog))
        })();
        // Keep the target lease outside recovery: a failure must finish runtime
        // cleanup before another opener can acquire ownership of this UUID.
        match result {
            Ok((mut store, catalog)) => {
                store.branch_lease = Some(Arc::new(branch_lease));
                let durable = store
                    .durable
                    .as_mut()
                    .expect("admission opened a durable store");
                let runtime_owner = durable.retain_admitted_runtime();
                durable.branch_runtime = Some(BranchRuntimeBinding {
                    catalog_path: request.catalog_path.to_path_buf(),
                    immutable_store_root: request.immutable_store_root.to_path_buf(),
                    head_path: request.head_path.to_path_buf(),
                    metadata_revision: request.expected_metadata_revision,
                    head,
                    root: Arc::new(root),
                    checkpoint_generation: durable.checkpoint_epoch,
                    max_sealed_wal_intervals: request.replay_config.max_branch_sealed_wal_intervals,
                });
                store.branch_runtime_owner = Some(runtime_owner);
                Ok(AdmittedBranchStore { store, catalog })
            }
            Err(error) => {
                cleanup(&runtime_directory);
                Err(error)
            }
        }
    }

    /// Reconstructs a read/write GraphStore from immutable root objects for
    /// recovery qualification. The source directory is copied only as a
    /// container for non-checkpoint metadata; the root's manifest,
    /// checkpoint artifacts, and sealed WAL are all read from immutable
    /// objects. The current source manifest is never consulted.
    #[doc(hidden)]
    pub fn open_from_immutable_root(
        source: &GraphStore,
        root: &SealedRoot,
        immutable_store_root: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        catalog: &mut crate::schema::Catalog,
    ) -> Result<GraphStore> {
        root.validate().map_err(HawDBError::from_storage_error)?;
        let source_durable = source.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage("immutable root replay requires durable storage".to_string())
        })?;
        let destination = destination.as_ref();
        let _project_files =
            match crate::file_descriptors::ProjectFileDescriptors::containing(destination)? {
                Some(project) => project,
                None => crate::file_descriptors::ProjectFileDescriptors::acquire(
                    destination,
                    crate::file_descriptors::DEFAULT_MAX_OPEN_FILES,
                )?,
            };
        copy_recovery_container(&source_durable.root_path, destination)?;
        let objects = ImmutableObjectStore::open(immutable_store_root.as_ref())
            .map_err(HawDBError::from_storage_error)?;
        let manifest_bytes = objects
            .read(root.durable_manifest)
            .map_err(HawDBError::from_storage_error)?;
        fs::write(destination.join(MANIFEST_FILE), manifest_bytes)?;
        let manifest = DurableManifest::load(&destination.join(MANIFEST_FILE))?;
        materialize_checkpoint_bindings(&objects, destination, &root.checkpoint_bindings)?;
        if root.sealed_wals.len() != 1 {
            return Err(HawDBError::Storage(
                "immutable root replay currently requires one sealed WAL interval".to_string(),
            ));
        }
        let wal = objects
            .read(root.sealed_wals[0].object)
            .map_err(HawDBError::from_storage_error)?;
        let wal_path = manifest.wal_path(destination);
        if let Some(parent) = wal_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(wal_path, wal)?;
        GraphStore::open(destination, catalog)
    }

    /// Opens an immutable root selected by a durable branch-head selector.
    /// This is the recovery-facing bridge used while the public branch facade
    /// remains in its separate lifecycle issue.
    #[doc(hidden)]
    pub fn open_from_branch_head(
        source: &GraphStore,
        head_path: impl AsRef<Path>,
        immutable_store_root: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        catalog: &mut crate::schema::Catalog,
    ) -> Result<(GraphStore, branch_head::BranchHead)> {
        let head = branch_head::read_branch_head(head_path.as_ref())
            .map_err(HawDBError::from_storage_error)?;
        let objects = ImmutableObjectStore::open(immutable_store_root.as_ref())
            .map_err(HawDBError::from_storage_error)?;
        let root_bytes = objects
            .read(head.sealed_root)
            .map_err(HawDBError::from_storage_error)?;
        let root = SealedRoot::decode(&root_bytes).map_err(HawDBError::from_storage_error)?;
        let store = Self::open_from_immutable_root(
            source,
            &root,
            immutable_store_root,
            destination,
            catalog,
        )?;
        Ok((store, head))
    }

    /// Seals the current active WAL, publishes the validated manifest-bound
    /// checkpoint closure and root, and creates the next private WAL. The
    /// caller must keep the branch publication barrier until
    /// [`GraphStore::complete_immutable_root_handoff`] succeeds.
    ///
    /// This method intentionally publishes only artifacts enumerated by the
    /// validated durable manifest. Physical descendants still require their
    /// family readers to contribute closure inputs before #779 is complete.
    pub fn prepare_immutable_root_handoff(
        &mut self,
        immutable_store_root: impl AsRef<Path>,
    ) -> Result<PreparedImmutableRootHandoff> {
        self.ensure_usable()?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage("immutable root handoff requires durable storage".to_string())
        })?;
        if durable.read_only {
            return Err(HawDBError::Storage(
                "read-only storage cannot prepare an immutable root handoff".to_string(),
            ));
        }
        if let Some(branch) = &durable.branch_runtime
            && branch.checkpoint_generation == durable.checkpoint_epoch
            && branch.root.sealed_wals.len() >= branch.max_sealed_wal_intervals
            && durable.next_lsn > durable.wal_replay_start_lsn
        {
            return Err(HawDBError::Storage(format!(
                "branch sealed WAL interval limit exceeded: {}; checkpoint before sealing again",
                branch.max_sealed_wal_intervals
            )));
        }
        let max_active_wal_bytes = durable.max_wal_bytes.unwrap_or(u64::MAX);
        let immutable_store_root = immutable_store_root.as_ref().to_path_buf();
        let mut objects = ImmutableObjectStore::open(&immutable_store_root)
            .map_err(HawDBError::from_storage_error)?;
        let rotation = match &durable.branch_runtime {
            Some(branch) => {
                let directory = branch.head_path.parent().ok_or_else(|| {
                    HawDBError::Storage("admitted head has no branch directory".into())
                })?;
                prepare_fresh_branch_wal_rotation(
                    &durable.wal_path,
                    directory,
                    durable.wal_generation,
                    durable.wal_replay_start_lsn,
                    max_active_wal_bytes,
                    &mut objects,
                )?
            }
            None => {
                let generation = durable
                    .wal_generation
                    .checked_add(1)
                    .ok_or_else(|| HawDBError::Storage("WAL generation overflow".into()))?;
                let path = durable
                    .root_path
                    .join(crate::artifact_files::wal_generation_file(generation));
                prepare_wal_rotation(
                    &durable.wal_path,
                    &path,
                    durable.wal_generation,
                    generation,
                    durable.wal_replay_start_lsn,
                    max_active_wal_bytes,
                    &mut objects,
                )
                .map_err(HawDBError::from_storage_error)?
            }
        };

        let root = publish_sealed_root(durable, self.commit_epoch, rotation.sealed, &mut objects)?;
        Ok(PreparedImmutableRootHandoff {
            root,
            rotation,
            immutable_store_root,
        })
    }

    /// Switches the branch head to a previously prepared immutable root and
    /// successor WAL, then updates this GraphStore's durable WAL identity.
    pub fn complete_immutable_root_handoff(
        &mut self,
        prepared: PreparedImmutableRootHandoff,
        head_path: impl AsRef<Path>,
        project_id: [u8; 16],
        branch_id: [u8; 16],
        expected_head_generation: u64,
    ) -> Result<branch_head::BranchHead> {
        let result = self.complete_immutable_root_handoff_inner(
            prepared,
            head_path.as_ref(),
            project_id,
            branch_id,
            expected_head_generation,
        );
        self.poison_on_storage_error(&result);
        result
    }

    fn complete_immutable_root_handoff_inner(
        &mut self,
        prepared: PreparedImmutableRootHandoff,
        head_path: &Path,
        project_id: [u8; 16],
        branch_id: [u8; 16],
        expected_head_generation: u64,
    ) -> Result<branch_head::BranchHead> {
        self.ensure_usable()?;
        let durable = self.durable.as_mut().ok_or_else(|| {
            HawDBError::Storage("immutable root handoff requires durable storage".to_string())
        })?;
        let max_active_wal_bytes = durable.max_wal_bytes.unwrap_or(u64::MAX);
        let root_reference = prepared
            .root
            .object_reference()
            .map_err(HawDBError::from_storage_error)?;
        let mut objects = ImmutableObjectStore::open(&prepared.immutable_store_root)
            .map_err(HawDBError::from_storage_error)?;
        let head = branch_head::publish_prepared_wal_rotation_with_root(
            head_path,
            branch_head::PreparedWalRootPublicationRequest {
                expected_current_generation: expected_head_generation,
                project_id,
                branch_id,
                logical_commit_epoch: self.commit_epoch,
                prepared: &prepared.rotation,
                max_active_wal_bytes,
            },
            &prepared.root,
            &mut objects,
        )
        .map_err(wal_rotation_publication_error)?;
        if head.sealed_root != root_reference {
            return Err(HawDBError::StorageIntegrity(
                "published branch head selected an unexpected immutable root".to_string(),
            ));
        }
        durable.wal_append_file = None;
        durable.wal_path = prepared.rotation.next_wal_path;
        durable.wal_generation = prepared.rotation.next_generation;
        durable.wal_replay_start_lsn = prepared.rotation.next_start_lsn;
        // The publication primitive already validated this exact successor.
        // No fallible IO may separate a durable selector switch from adopting
        // its complete in-memory WAL/head identity.
        durable.wal_bytes = head.active_wal.byte_length;
        durable.wal_commit_epoch = self.commit_epoch;
        durable.wal_tail_repair = None;
        if let Some(branch) = &mut durable.branch_runtime {
            branch.head = head;
            branch.root = Arc::new(prepared.root);
            branch.checkpoint_generation = durable.checkpoint_epoch;
        }
        Ok(head)
    }

    /// The current seal/rotation selector of an admitted branch.
    #[doc(hidden)]
    pub fn admitted_branch_head(&self) -> Option<&branch_head::BranchHead> {
        self.durable
            .as_ref()
            .and_then(|durable| durable.branch_runtime.as_ref())
            .map(|branch| &branch.head)
    }

    /// Recovery may reconstruct derived runtime files, but a read-only
    /// execution context cannot append WAL, checkpoint, or publish a head.
    #[doc(hidden)]
    pub fn make_admitted_branch_read_only(&mut self) -> Result<()> {
        let durable = self.durable.as_mut().ok_or_else(|| {
            HawDBError::Storage("read-only branch requires an admitted store".into())
        })?;
        if durable.branch_runtime.is_none() {
            return Err(HawDBError::Storage("store has no admitted branch".into()));
        }
        durable.read_only = true;
        Ok(())
    }

    /// Seals the exact committed revision while exclusive runtime access
    /// prevents concurrent writes. Ordinary commits continue to append WAL.
    #[doc(hidden)]
    pub fn seal_admitted_branch(
        &mut self,
        expected_commit_epoch: u64,
    ) -> Result<branch_head::BranchHead> {
        self.ensure_usable()?;
        if self.commit_epoch != expected_commit_epoch {
            return Err(HawDBError::Semantic(format!(
                "branch source revision is stale: expected {expected_commit_epoch}, found {}",
                self.commit_epoch
            )));
        }
        if let Some(head) = self.admitted_branch_head()
            && head.logical_commit_epoch == self.commit_epoch
        {
            return Ok(*head);
        }
        let result = self.publish_admitted_branch_root();
        self.poison_on_storage_error(&result);
        result
    }

    /// Refresh administrative metadata without replacing a live branch runtime.
    /// Its admitted head/root identity remains authoritative for the data plane.
    #[doc(hidden)]
    pub fn refresh_admitted_branch_metadata(
        &mut self,
        expected_metadata_revision: u64,
    ) -> std::result::Result<(), BranchAdmissionError> {
        self.ensure_usable()
            .map_err(BranchAdmissionError::Recovery)?;
        let branch = self
            .durable
            .as_mut()
            .and_then(|durable| durable.branch_runtime.as_mut())
            .ok_or(BranchAdmissionError::IdentityMismatch(
                "store has no admitted branch",
            ))?;
        let id = branch_catalog::BranchId::new(hawdb_core::Uuid::from_bytes(branch.head.branch_id))
            .map_err(|_| BranchAdmissionError::IdentityMismatch("invalid admitted branch UUID"))?;
        let record = ready_branch_record(&branch.catalog_path, id, expected_metadata_revision)?;
        validate_admission_binding(&record, &branch.head, &branch.root, branch.head.sealed_root)?;
        branch.metadata_revision = expected_metadata_revision;
        Ok(())
    }

    pub(super) fn publish_admitted_branch_root(&mut self) -> Result<branch_head::BranchHead> {
        let branch = self
            .durable
            .as_ref()
            .and_then(|durable| durable.branch_runtime.clone())
            .ok_or_else(|| HawDBError::Storage("store has no admitted branch".to_string()))?;
        ready_branch_record(
            &branch.catalog_path,
            branch_catalog::BranchId::new(hawdb_core::Uuid::from_bytes(branch.head.branch_id))
                .map_err(HawDBError::from_storage_error)?,
            branch.metadata_revision,
        )
        .map_err(HawDBError::from_storage_error)?;
        let prepared = self.prepare_immutable_root_handoff(&branch.immutable_store_root)?;
        self.complete_immutable_root_handoff(
            prepared,
            &branch.head_path,
            branch.head.project_id,
            branch.head.branch_id,
            branch.head.physical_generation,
        )
    }

    /// Replays the target branch's active WAL after opening the sealed root
    /// materialization. The root's sealed WAL has already been replayed by the
    /// ordinary durable opener; the private WAL must start at exactly that
    /// replay boundary and is then adopted as the mutable WAL for this runtime.
    fn replay_branch_active_wal(
        &mut self,
        catalog: &mut crate::schema::Catalog,
        head: &branch_head::BranchHead,
        active_wal_path: &Path,
        replay_config: WalReplayConfig,
        source: &mut RelationalRecoverySourceBuilder,
    ) -> Result<()> {
        self.replay_branch_wal_interval(
            catalog,
            active_wal_path,
            head.active_wal.generation,
            head.active_wal.replay_start_lsn,
            replay_config,
            source,
        )?;
        if self.commit_epoch < head.logical_commit_epoch {
            return Err(HawDBError::Storage(format!(
                "branch head commit epoch {} exceeds recovered epoch {}",
                head.logical_commit_epoch, self.commit_epoch
            )));
        }
        Ok(())
    }

    fn replay_sealed_wals(
        &mut self,
        catalog: &mut crate::schema::Catalog,
        root: &SealedRoot,
        objects: &ImmutableObjectStore,
        runtime_directory: &Path,
        replay_config: WalReplayConfig,
        source: &mut RelationalRecoverySourceBuilder,
    ) -> Result<()> {
        for interval in &root.sealed_wals {
            let bytes = objects
                .read(interval.object)
                .map_err(HawDBError::from_storage_error)?;
            let (generation, start_lsn) = crate::wal::frame::decode_binary_wal_header(&bytes)?;
            if start_lsn != interval.start_lsn {
                return Err(HawDBError::Storage(
                    "sealed WAL header differs from its declared interval".to_string(),
                ));
            }
            let path = runtime_directory.join("replay.hawdb");
            fs::write(&path, bytes)?;
            self.replay_branch_wal_interval(
                catalog,
                &path,
                generation,
                start_lsn,
                replay_config,
                source,
            )?;
            if self.durable.as_ref().map(|durable| durable.next_lsn) != Some(interval.end_lsn) {
                return Err(HawDBError::Storage(
                    "sealed WAL replay does not reach the declared interval boundary".to_string(),
                ));
            }
        }
        if self.commit_epoch != root.commit_epoch {
            return Err(HawDBError::Storage(
                "sealed WAL replay does not recover the declared root epoch".to_string(),
            ));
        }
        Ok(())
    }

    fn replay_branch_wal_interval(
        &mut self,
        catalog: &mut crate::schema::Catalog,
        path: &Path,
        generation: u64,
        start_lsn: u64,
        mut replay_config: WalReplayConfig,
        source: &mut RelationalRecoverySourceBuilder,
    ) -> Result<()> {
        let before = self.storage_recovery_report.clone();
        replay_config.max_entries = replay_config
            .max_entries
            .map(|limit| {
                limit
                    .checked_sub(before.replayed_wal_entries)
                    .ok_or_else(|| {
                        HawDBError::Storage("branch WAL replay entry budget exceeded".to_string())
                    })
            })
            .transpose()?;
        replay_config.max_bytes = replay_config
            .max_bytes
            .map(|limit| {
                limit.checked_sub(before.replayed_wal_bytes).ok_or_else(|| {
                    HawDBError::Storage("branch WAL replay byte budget exceeded".to_string())
                })
            })
            .transpose()?;
        let durable = self.durable.as_mut().ok_or_else(|| {
            HawDBError::Storage("branch admission requires durable storage".to_string())
        })?;
        if durable.next_lsn != start_lsn {
            return Err(HawDBError::Storage(format!(
                "branch WAL starts at {start_lsn}, but preceding replay ended at {}",
                durable.next_lsn
            )));
        }
        durable.wal_path = path.to_path_buf();
        durable.wal_append_file = None;
        durable.wal_generation = generation;
        durable.wal_replay_start_lsn = start_lsn;
        durable.wal_bytes = fs::metadata(path)?.len();
        durable.wal_tail_repair = None;
        self.storage_recovery_report = combine_recovery_reports(
            before,
            self.replay_wal_interval(catalog, replay_config, source)?,
        );
        Ok(())
    }

    /// Publishes an initial sealed snapshot with a private empty WAL.
    /// The ordinary database remains manifest-owned: changing its writer here
    /// would lose subsequent writes when ordinary open replays the manifest WAL.
    #[doc(hidden)]
    pub fn initialize_immutable_root_head(
        &mut self,
        immutable_store_root: impl AsRef<Path>,
        head_path: impl AsRef<Path>,
        project_id: [u8; 16],
        branch_id: [u8; 16],
    ) -> Result<branch_head::BranchHead> {
        self.ensure_usable()?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            HawDBError::Storage(
                "immutable root initialization requires durable storage".to_string(),
            )
        })?;
        if durable.read_only {
            return Err(HawDBError::Storage(
                "read-only storage cannot initialize a branch".to_string(),
            ));
        }
        let max_active_wal_bytes = durable.max_wal_bytes.unwrap_or(u64::MAX);
        let mut objects = ImmutableObjectStore::open(immutable_store_root.as_ref())
            .map_err(HawDBError::from_storage_error)?;
        let sealed = seal_wal_file(
            &durable.wal_path,
            durable.wal_generation,
            durable.wal_replay_start_lsn,
            max_active_wal_bytes,
            &mut objects,
        )
        .map_err(HawDBError::from_storage_error)?;
        let root = publish_sealed_root(durable, self.commit_epoch, sealed, &mut objects)?;
        let root_reference = root
            .object_reference()
            .map_err(HawDBError::from_storage_error)?;
        let generation = durable.wal_generation.checked_add(1).ok_or_else(|| {
            HawDBError::Storage("initial branch WAL generation overflow".to_string())
        })?;
        let head_path = head_path.as_ref();
        let wal_path =
            head_path.with_file_name(crate::artifact_files::wal_generation_file(generation));
        if wal_path.try_exists()? {
            // A crash may leave the private WAL before publishing the selector.
            // Only an exact empty successor can be reused; never adopt data.
            let header = crate::wal::frame::encode_binary_wal_header(generation, sealed.end_lsn);
            if fs::metadata(&wal_path)?.len() != header.len() as u64
                || fs::read(&wal_path)? != header
            {
                return Err(HawDBError::Storage(
                    "initial branch WAL is not the expected empty successor".to_string(),
                ));
            }
            return branch_head::create_initial_branch_head(
                branch_head::InitialBranchHeadRequest {
                    path: head_path,
                    project_id,
                    branch_id,
                    sealed_root: root_reference,
                    logical_commit_epoch: self.commit_epoch,
                    wal_path: &wal_path,
                    wal_generation: generation,
                    replay_start_lsn: sealed.end_lsn,
                    max_active_wal_bytes,
                },
            )
            .map_err(HawDBError::from_storage_error);
        }
        branch_head::create_child_branch_head(
            head_path,
            branch_head::ChildBranchHeadRequest {
                project_id,
                branch_id,
                sealed_root: root_reference,
                logical_commit_epoch: self.commit_epoch,
                active_wal_generation: generation,
                replay_start_lsn: sealed.end_lsn,
                head_path: head_path.to_path_buf(),
                wal_path,
            },
            max_active_wal_bytes,
        )
        .map_err(HawDBError::from_storage_error)
    }
}

fn wal_rotation_publication_error(error: branch_head::WalRotationPublicationError) -> HawDBError {
    match error {
        branch_head::WalRotationPublicationError::Head(
            branch_head::BranchHeadError::CandidatePublicationUncertain { .. },
        ) => HawDBError::StorageIntegrity(format!(
            "branch head publication is uncertain; close and reopen the branch: {error}"
        )),
        _ => HawDBError::from_storage_error(error),
    }
}

fn prepare_fresh_branch_wal_rotation(
    old_path: &Path,
    directory: &Path,
    old_generation: u64,
    start_lsn: u64,
    max_bytes: u64,
    objects: &mut ImmutableObjectStore,
) -> Result<PreparedWalRotation> {
    let mut generation = old_generation
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage("WAL generation overflow".into()))?;
    loop {
        let next_path = directory.join(crate::artifact_files::wal_generation_file(generation));
        match prepare_wal_rotation(
            old_path,
            &next_path,
            old_generation,
            generation,
            start_lsn,
            max_bytes,
            objects,
        ) {
            Ok(rotation) => return Ok(rotation),
            Err(crate::sealed_wal::SealedWalError::SuccessorAlreadyExists { .. }) => {
                // An interrupted publication can leave a durable candidate.
                // Keep it as evidence and use atomic create_new for retry.
                generation = generation
                    .checked_add(1)
                    .ok_or_else(|| HawDBError::Storage("WAL generation overflow".into()))?;
            }
            Err(error) => return Err(HawDBError::from_storage_error(error)),
        }
    }
}

/// Count only complete, contiguous transactions. A framed record is one commit,
/// including mixed schema/data batches; inner operations never advance epoch.
fn validate_source_wal(
    objects: &ImmutableObjectStore,
    root: &SealedRoot,
    head: &branch_head::BranchHead,
    path: &Path,
    config: WalReplayConfig,
) -> Result<(u64, u64)> {
    let bytes = objects
        .read(root.durable_manifest)
        .map_err(HawDBError::from_storage_error)?;
    let manifest = DurableManifest::decode(
        std::str::from_utf8(&bytes).map_err(HawDBError::from_storage_error)?,
    )?;
    if manifest.checkpoint_commit_epoch != root.checkpoint_epoch
        || manifest.wal_replay_start_lsn != root.wal_replay_start_lsn
        || root.replay_end_lsn().checked_sub(root.wal_replay_start_lsn)
            != root.commit_epoch.checked_sub(root.checkpoint_epoch)
    {
        return Err(HawDBError::StorageIntegrity(
            "sealed source recovery boundaries differ".into(),
        ));
    }
    let mut entries = root.commit_epoch - root.checkpoint_epoch;
    let mut bytes = 0u64;
    for interval in &root.sealed_wals {
        let length = interval
            .object
            .byte_length
            .checked_sub(crate::wal::frame::WAL_BINARY_FILE_HEADER_BYTES as u64)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity("sealed WAL is shorter than its header".into())
            })?;
        bytes = bytes
            .checked_add(length)
            .ok_or_else(|| HawDBError::Storage("branch WAL byte count overflow".into()))?;
    }
    ensure_source_wal_budget(entries, bytes, config)?;
    branch_head::validate_active_wal_prefix_from_file(
        path,
        head.active_wal,
        config.max_bytes.unwrap_or(u64::MAX),
    )
    .map_err(HawDBError::from_storage_error)?;
    let mut cursor = match crate::wal::WalRecordCursor::open(path, config.max_record_bytes)? {
        crate::wal::WalOpenOutcome::Cursor(cursor) => cursor,
        _ => {
            return Err(HawDBError::StorageIntegrity(
                "source private WAL has an invalid header".into(),
            ))
        }
    };
    if cursor.generation() != head.active_wal.generation
        || cursor.start_lsn() != head.active_wal.replay_start_lsn
    {
        return Err(HawDBError::StorageIntegrity(
            "source private WAL identity differs".into(),
        ));
    }
    let mut end_lsn = cursor.start_lsn();
    let mut epoch = root.commit_epoch;
    loop {
        match cursor.next()? {
            crate::wal::WalCursorEvent::Entry {
                entry, encoded_len, ..
            } => {
                if entry.lsn != end_lsn {
                    return Err(HawDBError::StorageIntegrity(
                        "source private WAL has an LSN gap".into(),
                    ));
                }
                entries = entries
                    .checked_add(1)
                    .ok_or_else(|| HawDBError::Storage("branch WAL entry count overflow".into()))?;
                bytes = bytes
                    .checked_add(encoded_len)
                    .ok_or_else(|| HawDBError::Storage("branch WAL byte count overflow".into()))?;
                ensure_source_wal_budget(entries, bytes, config)?;
                epoch = epoch
                    .checked_add(1)
                    .ok_or_else(|| HawDBError::Storage("branch commit epoch overflow".into()))?;
                crate::wal::validate_wal_op_values(std::slice::from_ref(&entry.op))?;
                validate_source_wal_op(&entry.op, epoch, config.max_batch_operations)?;
                end_lsn = end_lsn
                    .checked_add(1)
                    .ok_or_else(|| HawDBError::Storage("branch WAL LSN overflow".into()))?;
            }
            crate::wal::WalCursorEvent::TornTail { reason, .. }
            | crate::wal::WalCursorEvent::Corrupt { reason, .. } => {
                // Sealing never truncates/quarantines evidence or guesses a
                // committed prefix, even under a relaxed durability policy.
                return Err(HawDBError::StorageIntegrity(format!(
                    "source private WAL is damaged: {reason}"
                )));
            }
            crate::wal::WalCursorEvent::Eof => return Ok((epoch, end_lsn)),
        }
    }
}

fn ensure_source_wal_budget(entries: u64, bytes: u64, config: WalReplayConfig) -> Result<()> {
    if config
        .max_entries
        .is_some_and(|limit| entries > limit as u64)
    {
        return Err(HawDBError::Storage(
            "branch WAL replay entry budget exceeded".into(),
        ));
    }
    if config.max_bytes.is_some_and(|limit| bytes > limit) {
        return Err(HawDBError::Storage(
            "branch WAL replay byte budget exceeded".into(),
        ));
    }
    Ok(())
}

fn validate_source_wal_op(
    op: &crate::wal::WalOp,
    epoch: u64,
    max_batch_ops: Option<usize>,
) -> Result<()> {
    use crate::wal::WalOp;
    let embedded_epoch = match op {
        WalOp::Relational { record } => Some(
            crate::relational::decode_relational_wal_batch(
                record,
                crate::relational::RelationalDecodeLimits::wal(),
            )
            .map_err(HawDBError::from_storage_error)?
            .epoch,
        ),
        WalOp::RelationalSnapshot { record } => Some(
            crate::relational::decode_relational_checkpoint(
                record,
                crate::relational::RelationalDecodeLimits::checkpoint(),
            )
            .map_err(HawDBError::from_storage_error)?
            .epoch,
        ),
        WalOp::Append { record } => Some(
            crate::append_table::decode_append_wal_batch(
                record,
                crate::append_table::AppendDecodeLimits::wal(),
            )
            .map_err(HawDBError::from_storage_error)?
            .epoch,
        ),
        WalOp::Batch(ops) => {
            if max_batch_ops.is_some_and(|limit| ops.len() > limit) {
                return Err(HawDBError::Storage(
                    "branch WAL batch operation budget exceeded".into(),
                ));
            }
            for op in ops {
                validate_source_wal_op(op, epoch, max_batch_ops)?;
            }
            None
        }
        _ => None,
    };
    if embedded_epoch.is_some_and(|actual| actual != epoch) {
        return Err(HawDBError::StorageIntegrity(format!(
            "source WAL transaction epoch mismatch: expected {epoch}, found {}",
            embedded_epoch.unwrap_or_default(),
        )));
    }
    Ok(())
}

fn create_child_from_sealed_head(
    catalog_path: &Path,
    parent_head_path: &Path,
    child_head_path: &Path,
    child_wal_path: &Path,
    parent: branch_head::BranchHead,
    max_active_wal_bytes: u64,
    request: branch_catalog::CreateRequest,
) -> Result<branch_catalog::BranchCreateResult> {
    if request.base_root_digest != *parent.sealed_root.sha256.as_bytes()
        || request.source_commit_epoch != parent.logical_commit_epoch
    {
        return Err(HawDBError::Storage(
            "child branch request does not match the selected parent head".to_string(),
        ));
    }
    let active_wal_generation = parent
        .active_wal
        .generation
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage("child WAL generation overflow".to_string()))?;
    let child_request = branch_head::ChildBranchHeadRequest {
        project_id: parent.project_id,
        branch_id: *request.id.as_uuid().as_bytes(),
        sealed_root: parent.sealed_root,
        logical_commit_epoch: parent.logical_commit_epoch,
        active_wal_generation,
        replay_start_lsn: parent.active_wal.replay_start_lsn,
        head_path: child_head_path.to_path_buf(),
        wal_path: child_wal_path.to_path_buf(),
    };
    let expected_parent = branch_head::ChildBranchSourceExpectation {
        branch_id: parent.branch_id,
        physical_generation: parent.physical_generation,
        logical_commit_epoch: parent.logical_commit_epoch,
        sealed_root: parent.sealed_root,
    };
    branch_catalog::create_branch_from_parent(
        catalog_path,
        parent_head_path,
        child_request,
        expected_parent,
        max_active_wal_bytes,
        request,
    )
    .map_err(HawDBError::from_storage_error)
}

fn publish_sealed_root(
    durable: &super::durable::DurableStore,
    commit_epoch: u64,
    sealed: SealedWalPublication,
    objects: &mut ImmutableObjectStore,
) -> Result<SealedRoot> {
    let manifest_bytes = fs::read(durable.manifest_path())?;
    let manifest =
        DurableManifest::decode(std::str::from_utf8(&manifest_bytes).map_err(|error| {
            HawDBError::Storage(format!("decode durable manifest bytes: {error}"))
        })?)?;
    let durable_manifest =
        ObjectReference::for_bytes(ObjectKind::DurableManifest, 1, &manifest_bytes);
    objects
        .publish(durable_manifest, &manifest_bytes)
        .map_err(HawDBError::from_storage_error)?;
    // Admission/publication already validated this root, and the runtime
    // lease pins its immutable closure against reclamation.
    let previous = durable.branch_runtime.as_ref().map(|branch| &branch.root);
    let same_checkpoint = previous.as_ref().is_some_and(|root| {
        root.durable_manifest == durable_manifest
            && root.checkpoint_epoch == manifest.checkpoint_commit_epoch
            && root.wal_replay_start_lsn == manifest.wal_replay_start_lsn
    });
    let (checkpoint_references, checkpoint_bindings, mut sealed_wals) = if same_checkpoint {
        // An unchanged, already-published checkpoint is shared by immutable
        // reference. Sealing must not reread/hash the mutable materialization
        // or scale with the checkpoint's data size.
        let previous = previous.expect("same checkpoint has a previous root");
        (
            previous.checkpoint_references.clone(),
            previous.checkpoint_bindings.clone(),
            previous.sealed_wals.clone(),
        )
    } else {
        let plan = durable.checkpoint_closure_plan(manifest)?;
        let bindings = checkpoint_artifact_bindings(&durable.root_path, plan.inputs())?;
        let closure = plan
            .publish(objects)
            .map_err(HawDBError::from_storage_error)?;
        // A new manifest includes its checkpoint replay boundary. Previously
        // sealed intervals are covered and must not be replayed a second time.
        (closure.references, bindings, Vec::new())
    };
    if sealed.start_lsn < sealed.end_lsn {
        sealed_wals.push(crate::sealed_root::SealedWalReference {
            start_lsn: sealed.start_lsn,
            end_lsn: sealed.end_lsn,
            object: sealed.object,
        });
    }
    if let Some(branch) = &durable.branch_runtime
        && sealed_wals.len() > branch.max_sealed_wal_intervals
    {
        return Err(HawDBError::Storage(format!(
            "branch sealed WAL interval limit exceeded: {} > {}; checkpoint before sealing again",
            sealed_wals.len(),
            branch.max_sealed_wal_intervals
        )));
    }
    let root = SealedRoot {
        checkpoint_epoch: manifest.checkpoint_commit_epoch,
        commit_epoch,
        wal_replay_start_lsn: manifest.wal_replay_start_lsn,
        durable_manifest,
        checkpoint_references,
        checkpoint_bindings,
        sealed_wals,
    };
    let root_reference = root
        .object_reference()
        .map_err(HawDBError::from_storage_error)?;
    let encoded = root.encode().map_err(HawDBError::from_storage_error)?;
    objects
        .publish(root_reference, &encoded)
        .map_err(HawDBError::from_storage_error)?;
    Ok(root)
}

fn checkpoint_artifact_bindings(
    database_root: &Path,
    inputs: &[CheckpointArtifactInput],
) -> Result<Vec<CheckpointArtifactBinding>> {
    let mut bindings = Vec::with_capacity(inputs.len());
    for input in inputs {
        let relative_path = input.path.strip_prefix(database_root).map_err(|_| {
            HawDBError::Storage(format!(
                "checkpoint closure artifact {} is outside the database directory",
                input.path.display()
            ))
        })?;
        let relative_path = relative_path.to_str().ok_or_else(|| {
            HawDBError::Storage(format!(
                "checkpoint closure artifact {} has a non-UTF-8 path",
                input.path.display()
            ))
        })?;
        bindings.push(CheckpointArtifactBinding {
            relative_path: relative_path.to_string(),
            reference: input.reference,
        });
    }
    bindings.sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(bindings)
}

#[derive(Clone)]
struct ReadyBranchRecord {
    project_id: branch_catalog::BranchId,
    branch: branch_catalog::BranchRecord,
}

fn ready_branch_record(
    catalog_path: &Path,
    branch_id: branch_catalog::BranchId,
    expected_metadata_revision: u64,
) -> std::result::Result<ReadyBranchRecord, BranchAdmissionError> {
    let project_directory = catalog_path
        .parent()
        .ok_or(BranchAdmissionError::IdentityMismatch(
            "branch catalog has no project directory",
        ))?;
    let _metadata_lease = match branch_catalog::CatalogMetadataLease::acquire(project_directory) {
        Ok(lease) => lease,
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            return Err(BranchAdmissionError::Busy("branch catalog metadata"));
        }
        Err(error) => return Err(BranchAdmissionError::Catalog(error)),
    };
    let catalog =
        branch_catalog::read_catalog(catalog_path).map_err(BranchAdmissionError::Catalog)?;
    let branch = catalog
        .branches
        .iter()
        .find(|branch| branch.id == branch_id)
        .cloned()
        .ok_or(BranchAdmissionError::UnknownBranch)?;
    if branch.metadata_revision != expected_metadata_revision {
        return Err(BranchAdmissionError::StaleMetadataRevision {
            expected: expected_metadata_revision,
            actual: branch.metadata_revision,
        });
    }
    if branch.state != branch_catalog::BranchState::Ready {
        return Err(BranchAdmissionError::InvalidState(branch.state));
    }
    Ok(ReadyBranchRecord {
        project_id: catalog.project_id,
        branch,
    })
}

fn validate_admission_binding(
    record: &ReadyBranchRecord,
    head: &branch_head::BranchHead,
    root: &SealedRoot,
    root_reference: ObjectReference,
) -> std::result::Result<(), BranchAdmissionError> {
    if head.project_id != *record.project_id.as_uuid().as_bytes() {
        return Err(BranchAdmissionError::IdentityMismatch(
            "catalog project UUID does not match branch head",
        ));
    }
    if head.branch_id != *record.branch.id.as_uuid().as_bytes() {
        return Err(BranchAdmissionError::IdentityMismatch(
            "catalog branch UUID does not match branch head",
        ));
    }
    if head.sealed_root != root_reference {
        return Err(BranchAdmissionError::IdentityMismatch(
            "branch head root reference does not match immutable root",
        ));
    }
    if head.physical_generation == 1 {
        if record.branch.base_root_digest != Some(*head.sealed_root.sha256.as_bytes()) {
            return Err(BranchAdmissionError::IdentityMismatch(
                "catalog root digest does not match branch head",
            ));
        }
        if record.branch.source_commit_epoch != root.commit_epoch {
            return Err(BranchAdmissionError::IdentityMismatch(
                "catalog source epoch does not match the initial sealed root",
            ));
        }
    }
    if root.commit_epoch < record.branch.source_commit_epoch {
        return Err(BranchAdmissionError::IdentityMismatch(
            "catalog source epoch does not match the sealed root",
        ));
    }
    if head.logical_commit_epoch != root.commit_epoch {
        return Err(BranchAdmissionError::IdentityMismatch(
            "branch head commit epoch differs from its sealed root",
        ));
    }
    if root.replay_end_lsn() != head.active_wal.replay_start_lsn {
        return Err(BranchAdmissionError::IdentityMismatch(
            "branch private WAL does not begin after the sealed root",
        ));
    }
    Ok(())
}

fn materialize_branch_runtime(
    objects: &ImmutableObjectStore,
    root: &SealedRoot,
    runtime_directory: &Path,
    artifact_directory: &Path,
) -> Result<()> {
    root.validate().map_err(HawDBError::from_storage_error)?;
    if runtime_directory.exists() {
        fs::remove_dir_all(runtime_directory)?;
    }
    fs::create_dir_all(runtime_directory)?;
    fs::create_dir_all(artifact_directory)?;
    // Future synchronous commits may depend on files in this directory. Make
    // its parent entry durable before exposing a writable branch runtime.
    crate::durability::sync_parent_directory(artifact_directory)?;
    let manifest_bytes = objects
        .read(root.durable_manifest)
        .map_err(HawDBError::from_storage_error)?;
    let manifest_path = runtime_directory.join(MANIFEST_FILE);
    fs::write(&manifest_path, manifest_bytes)?;
    let manifest = DurableManifest::load(&manifest_path)?;
    if manifest.checkpoint_commit_epoch != root.checkpoint_epoch
        || manifest.wal_replay_start_lsn != root.wal_replay_start_lsn
    {
        return Err(HawDBError::Storage(
            "sealed root differs from its checkpoint manifest recovery boundary".to_string(),
        ));
    }
    materialize_checkpoint_bindings(objects, artifact_directory, &root.checkpoint_bindings)?;
    // The manifest names the checkpoint's original WAL generation. A seal
    // can omit that empty generation and begin at a newer private generation.
    // Mount an empty anchor, then replay each root interval with its own header.
    let sealed = crate::wal::frame::encode_binary_wal_header(
        manifest.wal_generation,
        root.wal_replay_start_lsn,
    );
    let sealed_wal_path = manifest.wal_path(runtime_directory);
    if let Some(parent) = sealed_wal_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(sealed_wal_path, sealed)?;
    Ok(())
}

fn materialize_checkpoint_bindings(
    objects: &ImmutableObjectStore,
    destination: &Path,
    bindings: &[CheckpointArtifactBinding],
) -> Result<()> {
    let _project_files =
        match crate::file_descriptors::ProjectFileDescriptors::containing(destination)? {
            Some(project) => project,
            None => crate::file_descriptors::ProjectFileDescriptors::acquire(
                destination,
                crate::file_descriptors::DEFAULT_MAX_OPEN_FILES,
            )?,
        };
    let mut objects_by_reference = BTreeMap::new();
    for binding in bindings {
        let bytes = match objects_by_reference.entry(binding.reference) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(
                objects
                    .read(binding.reference)
                    .map_err(HawDBError::from_storage_error)?,
            ),
        };
        let path = destination.join(&binding.relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, bytes)?;
        crate::file_descriptors::ProjectFileDescriptors::registered(&path)?
            .immutable_handles
            .bind(
                &path,
                crate::immutable_files::ImmutableFileBinding {
                    reference: binding.reference,
                    object_path: objects.object_path(binding.reference),
                },
            )?;
    }
    Ok(())
}

fn combine_recovery_reports(
    mut sealed_root: crate::projection::StorageRecoveryReport,
    private_wal: crate::projection::StorageRecoveryReport,
) -> crate::projection::StorageRecoveryReport {
    sealed_root.wal_present |= private_wal.wal_present;
    sealed_root.wal_generation = private_wal.wal_generation.or(sealed_root.wal_generation);
    sealed_root.wal_replay_start_lsn = private_wal
        .wal_replay_start_lsn
        .or(sealed_root.wal_replay_start_lsn);
    sealed_root.next_lsn_after_replay = private_wal
        .next_lsn_after_replay
        .or(sealed_root.next_lsn_after_replay);
    sealed_root.replayed_wal_entries = sealed_root
        .replayed_wal_entries
        .saturating_add(private_wal.replayed_wal_entries);
    sealed_root.replayed_wal_bytes = sealed_root
        .replayed_wal_bytes
        .saturating_add(private_wal.replayed_wal_bytes);
    sealed_root.torn_tail_ignored |= private_wal.torn_tail_ignored;
    sealed_root.torn_tail_repaired |= private_wal.torn_tail_repaired;
    sealed_root.discarded_wal_tail_bytes = sealed_root
        .discarded_wal_tail_bytes
        .saturating_add(private_wal.discarded_wal_tail_bytes);
    sealed_root.torn_tail_reason =
        match (sealed_root.torn_tail_reason, private_wal.torn_tail_reason) {
            (Some(sealed), Some(private)) => {
                Some(format!("sealed root WAL: {sealed}; private WAL: {private}"))
            }
            (Some(reason), None) | (None, Some(reason)) => Some(reason),
            (None, None) => None,
        };
    sealed_root.recovered_commit_epoch = sealed_root
        .recovered_commit_epoch
        .max(private_wal.recovered_commit_epoch);
    sealed_root
}

fn copy_recovery_container(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        fs::remove_dir_all(destination)?;
    }
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let name = entry.file_name();
        if name.to_string_lossy().contains(".lock") {
            continue;
        }
        let destination_path = destination.join(name);
        if source_path.is_dir() {
            copy_recovery_container(&source_path, &destination_path)?;
        } else {
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint_closure::build_sealed_root;
    use crate::relational::{
        RelationalColumnSchema, RelationalInsertMode, RelationalRow, RelationalScalarType,
        RelationalTableSchema, RelationalTransaction, RelationalValue, RelationalWrite,
    };
    use crate::schema::Catalog;
    use crate::store::{GraphMutation, MutationLimits};
    use crate::value::Value;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-{label}-{}-{suffix}-{sequence}",
            std::process::id(),
        ));
        fs::create_dir_all(&path).expect("create test directory");
        path
    }

    struct BranchFixture {
        project: PathBuf,
        objects: PathBuf,
        catalog_path: PathBuf,
        main: branch_catalog::BranchId,
    }

    impl BranchFixture {
        fn new() -> Self {
            let project = temp_dir("branch-runtime");
            let fixture = Self {
                objects: project.join("immutable"),
                catalog_path: project.join("catalog.hawdb"),
                main: branch_id(1),
                project,
            };
            let source_directory = fixture.project.join("former-parent");
            let mut catalog = Catalog::default();
            let mut source = GraphStore::open(&source_directory, &mut catalog).unwrap();
            source
                .create_node(&mut catalog, "Seed", BTreeMap::new())
                .unwrap();
            source.checkpoint(&catalog).unwrap();
            source
                .create_node(&mut catalog, "Seed", BTreeMap::new())
                .unwrap();
            let head_path = fixture.head_path(fixture.main);
            fs::create_dir_all(head_path.parent().unwrap()).unwrap();
            let project_id = branch_id(90);
            let head = source
                .initialize_immutable_root_head(
                    &fixture.objects,
                    &head_path,
                    *project_id.as_uuid().as_bytes(),
                    *fixture.main.as_uuid().as_bytes(),
                )
                .unwrap();
            branch_catalog::initialize_catalog_file(
                &fixture.catalog_path,
                project_id,
                fixture.main,
            )
            .unwrap();
            branch_catalog::bind_main_head_file(
                &fixture.catalog_path,
                project_id,
                fixture.main,
                *head.sealed_root.sha256.as_bytes(),
                head.logical_commit_epoch,
            )
            .unwrap();
            drop(source);
            fs::remove_dir_all(source_directory).unwrap();
            fixture
        }

        fn head_path(&self, id: branch_catalog::BranchId) -> PathBuf {
            self.project
                .join("branches")
                .join(id.as_uuid().to_string())
                .join("branch.head")
        }

        fn admit(
            &self,
            id: branch_catalog::BranchId,
            durability: DurabilityPolicy,
        ) -> AdmittedBranchStore {
            self.try_admit(id, durability, WalReplayConfig::default())
                .unwrap()
        }

        fn try_admit(
            &self,
            id: branch_catalog::BranchId,
            durability: DurabilityPolicy,
            replay_config: WalReplayConfig,
        ) -> std::result::Result<AdmittedBranchStore, BranchAdmissionError> {
            let catalog = branch_catalog::read_catalog(&self.catalog_path).unwrap();
            let record = catalog
                .branches
                .iter()
                .find(|branch| branch.id == id)
                .unwrap();
            GraphStore::admit_branch_from_head(BranchAdmissionRequest {
                catalog_path: &self.catalog_path,
                branch_id: id,
                expected_metadata_revision: record.metadata_revision,
                head_path: &self.head_path(id),
                immutable_store_root: &self.objects,
                durability,
                replay_config,
            })
        }

        fn try_seal(
            &self,
            id: branch_catalog::BranchId,
            epoch: u64,
            durability: DurabilityPolicy,
            replay_config: WalReplayConfig,
        ) -> std::result::Result<SealedBranchSource, BranchAdmissionError> {
            let catalog = branch_catalog::read_catalog(&self.catalog_path).unwrap();
            let record = catalog
                .branches
                .iter()
                .find(|record| record.id == id)
                .unwrap();
            GraphStore::seal_branch_from_head(
                BranchAdmissionRequest {
                    catalog_path: &self.catalog_path,
                    branch_id: id,
                    expected_metadata_revision: record.metadata_revision,
                    head_path: &self.head_path(id),
                    immutable_store_root: &self.objects,
                    durability,
                    replay_config,
                },
                epoch,
            )
        }

        fn fork(&self, source: &AdmittedBranchStore, child: branch_catalog::BranchId, name: &str) {
            self.try_fork(source, child, name).unwrap();
        }

        fn try_fork(
            &self,
            source: &AdmittedBranchStore,
            child: branch_catalog::BranchId,
            name: &str,
        ) -> Result<branch_catalog::BranchCreateResult> {
            let head = source.head();
            let child_head = self.head_path(child);
            let child_wal = child_head.with_file_name(crate::artifact_files::wal_generation_file(
                head.active_wal.generation + 1,
            ));
            let parent =
                branch_catalog::BranchId::new(hawdb_core::Uuid::from_bytes(head.branch_id))
                    .unwrap();
            source.store().create_isolated_branch_from_parent_head(
                &self.catalog_path,
                self.head_path(parent),
                &child_head,
                &child_wal,
                branch_catalog::CreateRequest {
                    id: child,
                    name: branch_catalog::BranchName::new(name).unwrap(),
                    parent_id: parent,
                    source_commit_epoch: source.store().commit_epoch(),
                    base_root_digest: *head.sealed_root.sha256.as_bytes(),
                    owner: None,
                    request_key: format!("create-{name}"),
                    request_fingerprint: *hawdb_integrity::sha256(name.as_bytes()).as_bytes(),
                },
            )
        }
    }

    impl Drop for BranchFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.project);
        }
    }

    fn branch_id(value: u128) -> branch_catalog::BranchId {
        branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(value)).unwrap()
    }

    fn write_schema_and_graph(branch: &mut AdmittedBranchStore) {
        let table = RelationalTableSchema {
            name: "messages".into(),
            columns: vec![RelationalColumnSchema {
                name: "body".into(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            }],
            primary_key: vec!["body".into()],
            unique_constraints: vec![],
            foreign_keys: vec![],
            indexes: vec![],
        };
        let (store, catalog) = branch.store_and_catalog_mut();
        store
            .commit_mutations_and_relational(
                catalog,
                vec![GraphMutation::CreateNode {
                    label: "Marker".into(),
                    properties: BTreeMap::new(),
                }],
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(table),
                        RelationalWrite::Insert {
                            table: "messages".into(),
                            rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                                "payload".repeat(1_024),
                            )])],
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
                MutationLimits::default(),
            )
            .unwrap();
    }

    fn write_row(branch: &mut AdmittedBranchStore, body: &str) {
        let (store, catalog) = branch.store_and_catalog_mut();
        store
            .commit_relational_transaction(
                catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "messages".into(),
                        rows: vec![RelationalRow::new(vec![RelationalValue::Text(body.into())])],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .unwrap();
    }

    #[test]
    fn closed_source_seal_reuses_checkpoint_without_materialization_and_pins_child_creation() {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let fixture = BranchFixture::new();
            let mut main = fixture.admit(fixture.main, durability);
            write_schema_and_graph(&mut main);
            let (store, catalog) = main.store_and_catalog_mut();
            store.checkpoint(catalog).unwrap();
            let checkpoint_head = *main.head();
            write_row(&mut main, &"private-body".repeat(4096));
            let epoch = main.store().commit_epoch();
            drop(main);
            let directory = fixture
                .head_path(fixture.main)
                .parent()
                .unwrap()
                .to_path_buf();
            fs::remove_dir_all(directory.join("data")).unwrap();
            fs::remove_dir_all(directory.join("runtime")).unwrap();
            fs::write(directory.join("runtime"), b"must not materialize").unwrap();
            let candidate = directory.join(crate::artifact_files::wal_generation_file(
                checkpoint_head.active_wal.generation + 1,
            ));
            fs::write(&candidate, b"interrupted publication evidence").unwrap();
            let source = fixture
                .try_seal(fixture.main, epoch, durability, WalReplayConfig::default())
                .unwrap();
            assert_eq!(source.head().logical_commit_epoch, epoch);
            assert_eq!(
                source.head().active_wal.generation,
                checkpoint_head.active_wal.generation + 2
            );
            assert!(!directory.join("data").exists());
            assert_eq!(
                fs::read(directory.join("runtime")).unwrap(),
                b"must not materialize"
            );
            assert_eq!(
                fs::read(&candidate).unwrap(),
                b"interrupted publication evidence"
            );
            assert!(matches!(
                fixture.try_admit(fixture.main, durability, WalReplayConfig::default()),
                Err(BranchAdmissionError::Lease(
                    DatabaseDirectoryLeaseError::AlreadyOpen
                ))
            ));
            let objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
            let before =
                SealedRoot::decode(&objects.read(checkpoint_head.sealed_root).unwrap()).unwrap();
            let after =
                SealedRoot::decode(&objects.read(source.head().sealed_root).unwrap()).unwrap();
            assert_eq!(after.checkpoint_references, before.checkpoint_references);
            assert_eq!(after.checkpoint_bindings, before.checkpoint_bindings);
            assert_eq!(after.durable_manifest, before.durable_manifest);
            let child = branch_id(2);
            source
                .create_child(
                    &fixture.head_path(child),
                    &fixture.head_path(child).with_file_name(
                        crate::artifact_files::wal_generation_file(
                            source.head().active_wal.generation + 1,
                        ),
                    ),
                    branch_catalog::CreateRequest {
                        id: child,
                        name: branch_catalog::BranchName::new("cheap-child").unwrap(),
                        parent_id: fixture.main,
                        source_commit_epoch: epoch,
                        base_root_digest: *source.head().sealed_root.sha256.as_bytes(),
                        owner: None,
                        request_key: "cheap-child-request".into(),
                        request_fingerprint: [7; 32],
                    },
                )
                .unwrap();
            drop(source);
            fs::remove_dir_all(&directory).unwrap();
            let child = fixture.admit(child, durability);
            assert_eq!(child.store().commit_epoch(), epoch);
            assert_eq!(child.store().relational_state().row_count("messages"), 2);
            assert_eq!(child.store().node_count_for_label(None), 3);
        }
    }

    #[test]
    fn closed_source_seal_rejections_preserve_head_catalog_and_wal_evidence() {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let fixture = BranchFixture::new();
            let mut main = fixture.admit(fixture.main, durability);
            write_schema_and_graph(&mut main);
            let epoch = main.store().commit_epoch();
            let head = *main.head();
            assert!(matches!(
                fixture.try_seal(fixture.main, epoch, durability, WalReplayConfig::default()),
                Err(BranchAdmissionError::Lease(
                    DatabaseDirectoryLeaseError::AlreadyOpen
                ))
            ));
            drop(main);
            let head_path = fixture.head_path(fixture.main);
            let directory = head_path.parent().unwrap();
            let wal_path = directory.join(crate::artifact_files::wal_generation_file(
                head.active_wal.generation,
            ));
            let original_head = fs::read(&head_path).unwrap();
            let original_catalog = fs::read(&fixture.catalog_path).unwrap();
            let original_wal = fs::read(&wal_path).unwrap();
            assert!(matches!(
                fixture.try_seal(
                    fixture.main,
                    epoch - 1,
                    durability,
                    WalReplayConfig::default()
                ),
                Err(BranchAdmissionError::Recovery(HawDBError::Semantic(_)))
            ));
            assert!(fixture
                .try_seal(
                    fixture.main,
                    epoch,
                    durability,
                    WalReplayConfig {
                        max_entries: Some(0),
                        ..WalReplayConfig::default()
                    }
                )
                .is_err());
            assert!(fixture
                .try_seal(
                    fixture.main,
                    epoch,
                    durability,
                    WalReplayConfig {
                        max_bytes: Some(1),
                        ..WalReplayConfig::default()
                    }
                )
                .is_err());
            let objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
            let root = SealedRoot::decode(&objects.read(head.sealed_root).unwrap()).unwrap();
            assert!(matches!(
                fixture.try_seal(
                    fixture.main,
                    epoch,
                    durability,
                    WalReplayConfig {
                        max_branch_sealed_wal_intervals: root.sealed_wals.len(),
                        ..WalReplayConfig::default()
                    }
                ),
                Err(BranchAdmissionError::SealedWalLimit { .. })
            ));
            assert_eq!(fs::read(&wal_path).unwrap(), original_wal);
            let successor = directory.join(crate::artifact_files::wal_generation_file(
                head.active_wal.generation + 1,
            ));
            assert!(!successor.exists());
            let mut torn = original_wal;
            torn.push(0xff);
            fs::write(&wal_path, &torn).unwrap();
            assert!(matches!(
                fixture.try_seal(fixture.main, epoch, durability, WalReplayConfig::default()),
                Err(BranchAdmissionError::Recovery(_))
            ));
            assert_eq!(fs::read(&wal_path).unwrap(), torn);
            assert_eq!(fs::read(&head_path).unwrap(), original_head);
            assert_eq!(fs::read(&fixture.catalog_path).unwrap(), original_catalog);
            assert!(!successor.exists());
        }
    }

    #[test]
    fn branch_checkpoint_preserves_schema_data_and_private_wal_after_reopen() {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let fixture = BranchFixture::new();
            let main = fixture.admit(fixture.main, durability);
            fixture.fork(&main, branch_id(2), "child");
            fixture.fork(&main, branch_id(3), "sibling");
            let mut child = fixture.admit(branch_id(2), durability);
            let initial_head = *child.head();
            let initial_catalog = fs::read(&fixture.catalog_path).unwrap();
            write_schema_and_graph(&mut child);
            assert_eq!(
                *child.head(),
                initial_head,
                "ordinary commits append the private WAL"
            );
            drop(child);
            let mut child = fixture.admit(branch_id(2), durability);
            assert_eq!(child.store().node_count_for_label(None), 3);
            assert_eq!(child.store().relational_state().row_count("messages"), 1);
            let (store, catalog) = child.store_and_catalog_mut();
            store.checkpoint(catalog).unwrap();
            assert!(child.head().physical_generation > initial_head.physical_generation);
            assert_eq!(
                child.head().logical_commit_epoch,
                child.store().commit_epoch()
            );
            let branch_directory = fixture
                .head_path(branch_id(2))
                .parent()
                .unwrap()
                .to_path_buf();
            assert_eq!(
                child.store().durable.as_ref().unwrap().wal_path.parent(),
                Some(branch_directory.as_path())
            );
            assert_eq!(
                fs::read(&fixture.catalog_path).unwrap(),
                initial_catalog,
                "lineage stays immutable"
            );
            write_row(&mut child, "after-checkpoint");
            let expected_epoch = child.store().commit_epoch();
            drop(child);
            let child = fixture.admit(branch_id(2), durability);
            assert_eq!(child.store().commit_epoch(), expected_epoch);
            assert_eq!(child.store().relational_state().row_count("messages"), 2);
            assert_eq!(child.store().node_count_for_label(None), 3);
            assert!(main
                .store()
                .relational_state()
                .table_schema("messages")
                .is_none());
            let sibling = fixture.admit(branch_id(3), durability);
            assert_eq!(sibling.store().node_count_for_label(None), 2);
            assert!(sibling
                .store()
                .relational_state()
                .table_schema("messages")
                .is_none());
        }
    }

    #[test]
    fn seal_reuses_immutable_checkpoint_without_reading_mutable_materialization() {
        let fixture = BranchFixture::new();
        let mut branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        write_schema_and_graph(&mut branch);
        let (store, catalog) = branch.store_and_catalog_mut();
        store.checkpoint(catalog).unwrap();
        let before = Arc::clone(
            &branch
                .store()
                .durable
                .as_ref()
                .unwrap()
                .branch_runtime
                .as_ref()
                .unwrap()
                .root,
        );
        write_row(&mut branch, "new-wal-suffix");
        let epoch = branch.store().commit_epoch();
        // Make mutable checkpoint paths unavailable without deleting files
        // retained by Windows readers. Sealing still has its live WAL and
        // manifest, and must use the published immutable dependency closure.
        let durable = branch.store_mut().durable.as_mut().unwrap();
        let data_path = std::mem::replace(
            &mut durable.root_path,
            fixture.project.join("unavailable-materialization"),
        );
        let sealed = branch.store_mut().seal_admitted_branch(epoch);
        branch.store_mut().durable.as_mut().unwrap().root_path = data_path;
        sealed.unwrap();
        let root = &branch
            .store()
            .durable
            .as_ref()
            .unwrap()
            .branch_runtime
            .as_ref()
            .unwrap()
            .root;
        assert_eq!(root.checkpoint_references, before.checkpoint_references);
        assert_eq!(root.checkpoint_bindings, before.checkpoint_bindings);
        assert_eq!(root.durable_manifest, before.durable_manifest);
        assert_eq!(root.sealed_wals.len(), 1);
        fixture.fork(&branch, branch_id(2), "child");

        // A changed checkpoint must publish a fresh closure, reset covered
        // intervals, and preserve the child's earlier immutable revision.
        write_row(&mut branch, "later-parent-only");
        let (store, catalog) = branch.store_and_catalog_mut();
        store.checkpoint(catalog).unwrap();
        let root = &branch
            .store()
            .durable
            .as_ref()
            .unwrap()
            .branch_runtime
            .as_ref()
            .unwrap()
            .root;
        assert_ne!(root.durable_manifest, before.durable_manifest);
        assert_ne!(root.checkpoint_references, before.checkpoint_references);
        assert!(root.sealed_wals.is_empty());
        drop(branch);
        fs::remove_dir_all(fixture.head_path(fixture.main).parent().unwrap()).unwrap();
        let child = fixture.admit(branch_id(2), DurabilityPolicy::default());
        assert_eq!(child.store().commit_epoch(), epoch);
        assert_eq!(child.store().node_count_for_label(None), 3);
        assert_eq!(child.store().relational_state().row_count("messages"), 2);
    }

    #[test]
    fn branch_interval_admission_and_sealing_limits_require_checkpoint_compaction() {
        let fixture = BranchFixture::new();
        let mut branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        write_schema_and_graph(&mut branch);
        let (store, catalog) = branch.store_and_catalog_mut();
        store.checkpoint(catalog).unwrap();
        drop(branch);
        let config = WalReplayConfig {
            max_branch_sealed_wal_intervals: 2,
            ..WalReplayConfig::default()
        };
        let mut branch = fixture
            .try_admit(fixture.main, DurabilityPolicy::default(), config)
            .unwrap();
        for body in ["sealed-one", "sealed-two"] {
            write_row(&mut branch, body);
            let epoch = branch.store().commit_epoch();
            branch.store_mut().seal_admitted_branch(epoch).unwrap();
        }
        write_row(&mut branch, "private-third");
        let epoch = branch.store().commit_epoch();
        let head_path = fixture.head_path(fixture.main);
        let before = fs::read(&head_path).unwrap();
        let candidates_before = fs::read_dir(head_path.parent().unwrap()).unwrap().count();
        let error = branch.store_mut().seal_admitted_branch(epoch).unwrap_err();
        assert!(
            error.to_string().contains("interval limit exceeded"),
            "{error}"
        );
        assert_eq!(fs::read(&head_path).unwrap(), before);
        assert_eq!(
            fs::read_dir(head_path.parent().unwrap()).unwrap().count(),
            candidates_before
        );
        assert!(!branch.store().storage_handle_poisoned());
        drop(branch);
        let runtime = head_path.parent().unwrap().join("runtime");
        let evidence = runtime.join("admission-evidence");
        fs::write(&evidence, b"must not materialize before admission limits").unwrap();
        assert!(matches!(
            fixture.try_admit(
                fixture.main,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    max_branch_sealed_wal_intervals: 1,
                    ..config
                }
            ),
            Err(BranchAdmissionError::SealedWalLimit {
                intervals: 2,
                limit: 1
            })
        ));
        assert!(evidence.exists());
        assert_eq!(fs::read(&head_path).unwrap(), before);
        let mut branch = fixture
            .try_admit(fixture.main, DurabilityPolicy::default(), config)
            .unwrap();
        assert_eq!(branch.store().commit_epoch(), epoch);
        assert_eq!(branch.store().relational_state().row_count("messages"), 4);
        let (store, catalog) = branch.store_and_catalog_mut();
        store.checkpoint(catalog).unwrap();
        write_row(&mut branch, "after-compaction");
        let epoch = branch.store().commit_epoch();
        branch.store_mut().seal_admitted_branch(epoch).unwrap();
        drop(branch);
        let branch = fixture
            .try_admit(fixture.main, DurabilityPolicy::default(), config)
            .unwrap();
        assert_eq!(branch.store().commit_epoch(), epoch);
        assert_eq!(branch.store().relational_state().row_count("messages"), 5);
    }

    #[test]
    fn failed_local_manifest_publication_poisons_branch_before_head_handoff() {
        let fixture = BranchFixture::new();
        let mut branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        write_schema_and_graph(&mut branch);
        let epoch = branch.store().commit_epoch();
        let head = *branch.head();
        super::super::set_checkpoint_failpoint(Some(
            super::super::CheckpointPublishStage::ManifestPublished,
        ));
        let (store, catalog) = branch.store_and_catalog_mut();
        let result = store.checkpoint(catalog);
        super::super::set_checkpoint_failpoint(None);
        assert!(matches!(result, Err(HawDBError::StorageIntegrity(_))));
        assert!(branch.store().storage_handle_poisoned());
        assert_eq!(
            branch_head::read_branch_head(&fixture.head_path(fixture.main)).unwrap(),
            head
        );
        assert!(branch.store_mut().seal_admitted_branch(epoch).is_err());
        drop(branch);
        let branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        assert_eq!(branch.store().commit_epoch(), epoch);
        assert_eq!(branch.store().node_count_for_label(None), 3);
        assert_eq!(branch.store().relational_state().row_count("messages"), 1);
    }

    #[test]
    fn stale_catalog_after_checkpoint_installation_poisons_before_head_handoff() {
        let fixture = BranchFixture::new();
        let mut branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        write_schema_and_graph(&mut branch);
        let epoch = branch.store().commit_epoch();
        let head = *branch.head();
        let mut catalog = branch_catalog::read_catalog(&fixture.catalog_path).unwrap();
        let record = catalog
            .branches
            .iter_mut()
            .find(|record| record.id == fixture.main)
            .unwrap();
        record.metadata_revision += 1;
        branch_catalog::write_catalog(&fixture.catalog_path, &catalog).unwrap();

        let (store, schema) = branch.store_and_catalog_mut();
        let error = store.checkpoint(schema).unwrap_err();
        assert!(matches!(error, HawDBError::Storage(_)), "{error}");
        assert!(store.storage_handle_poisoned());
        assert_ne!(
            store.durable.as_ref().unwrap().wal_generation,
            head.active_wal.generation
        );
        assert_eq!(
            branch_head::read_branch_head(&fixture.head_path(fixture.main)).unwrap(),
            head
        );
        assert!(store.seal_admitted_branch(epoch).is_err());
        drop(branch);
        let branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        assert_eq!(branch.store().commit_epoch(), epoch);
        assert_eq!(branch.store().node_count_for_label(None), 3);
        assert_eq!(branch.store().relational_state().row_count("messages"), 1);
    }

    #[test]
    fn modified_child_can_fork_grandchild_after_multiple_wal_seals() {
        let fixture = BranchFixture::new();
        let main = fixture.admit(fixture.main, DurabilityPolicy::default());
        fixture.fork(&main, branch_id(2), "child");
        let mut child = fixture.admit(branch_id(2), DurabilityPolicy::default());
        write_schema_and_graph(&mut child);
        let (store, catalog) = child.store_and_catalog_mut();
        store.checkpoint(catalog).unwrap();
        write_row(&mut child, "first-sealed-row");
        let epoch = child.store().commit_epoch();
        child.store_mut().seal_admitted_branch(epoch).unwrap();
        write_row(&mut child, "sealed-row");
        let epoch = child.store().commit_epoch();
        child.store_mut().seal_admitted_branch(epoch).unwrap();
        let head_bytes = fs::read(fixture.head_path(branch_id(2))).unwrap();
        assert!(child.store_mut().seal_admitted_branch(epoch - 1).is_err());
        assert_eq!(
            fs::read(fixture.head_path(branch_id(2))).unwrap(),
            head_bytes
        );
        let objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
        let root = SealedRoot::decode(&objects.read(child.head().sealed_root).unwrap()).unwrap();
        assert_eq!(root.sealed_wals.len(), 2);
        fixture.fork(&child, branch_id(4), "grandchild");
        let grandchild = fixture.admit(branch_id(4), DurabilityPolicy::default());
        assert_eq!(grandchild.store().commit_epoch(), epoch);
        assert_eq!(grandchild.store().node_count_for_label(None), 3);
        assert_eq!(
            grandchild.store().relational_state().row_count("messages"),
            3
        );
        write_row(&mut child, "child-only");
        assert_eq!(
            grandchild.store().relational_state().row_count("messages"),
            3
        );
        drop(child);
        let child = fixture.admit(branch_id(2), DurabilityPolicy::default());
        assert_eq!(child.store().relational_state().row_count("messages"), 4);
        assert!(main
            .store()
            .relational_state()
            .table_schema("messages")
            .is_none());
    }

    #[test]
    fn failed_checkpoint_head_publication_recovers_the_acknowledged_transaction() {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let fixture = BranchFixture::new();
            let mut branch = fixture.admit(fixture.main, durability);
            write_schema_and_graph(&mut branch);
            let expected_epoch = branch.store().commit_epoch();
            let head_path = fixture.head_path(fixture.main);
            let old_head = fs::read(&head_path).unwrap();
            {
                let _failure = crate::durability::fail_durable_replace_for_destination(
                    head_path.file_name().unwrap(),
                );
                let (store, catalog) = branch.store_and_catalog_mut();
                assert!(store.checkpoint(catalog).is_err());
            }
            assert_eq!(fs::read(&head_path).unwrap(), old_head);
            assert!(
                branch
                    .store_mut()
                    .seal_admitted_branch(expected_epoch)
                    .is_err(),
                "uncertain handle fails closed"
            );
            let error = fixture
                .try_fork(&branch, branch_id(2), "poisoned-source")
                .unwrap_err();
            assert!(error.to_string().contains("poisoned"), "{error}");
            drop(branch);
            let mut branch = fixture.admit(fixture.main, durability);
            assert_eq!(branch.store().commit_epoch(), expected_epoch);
            assert_eq!(branch.store().node_count_for_label(None), 3);
            assert_eq!(branch.store().relational_state().row_count("messages"), 1);
            let (store, catalog) = branch.store_and_catalog_mut();
            store.checkpoint(catalog).unwrap();
            drop(branch);
            let branch = fixture.admit(fixture.main, durability);
            assert_eq!(branch.store().commit_epoch(), expected_epoch);
            assert_eq!(branch.store().relational_state().row_count("messages"), 1);
        }
    }

    #[test]
    fn branch_snapshot_and_transferred_store_retain_the_writer_lease() {
        let fixture = BranchFixture::new();
        let branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        let (store, _catalog) = branch.into_parts();
        let snapshot = store.snapshot_for_read();
        drop(store);
        assert!(matches!(
            fixture.try_admit(
                fixture.main,
                DurabilityPolicy::default(),
                WalReplayConfig::default()
            ),
            Err(BranchAdmissionError::Lease(
                DatabaseDirectoryLeaseError::AlreadyOpen
            ))
        ));
        drop(snapshot);
        fixture.admit(fixture.main, DurabilityPolicy::default());
    }

    #[test]
    fn branch_admission_requires_a_durable_data_directory_and_preserves_its_source() {
        let fixture = BranchFixture::new();
        let mut main = fixture.admit(fixture.main, DurabilityPolicy::default());
        fixture.fork(&main, branch_id(2), "child");
        let child_head = fixture.head_path(branch_id(2));
        let child_directory = child_head.parent().unwrap();
        let head_bytes = fs::read(&child_head).unwrap();
        let catalog_bytes = fs::read(&fixture.catalog_path).unwrap();
        {
            let _failure = crate::durability::fail_sync_directory_for(child_directory);
            let error = fixture
                .try_admit(
                    branch_id(2),
                    DurabilityPolicy::default(),
                    WalReplayConfig::default(),
                )
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected directory sync failure"),
                "{error}"
            );
        }
        assert_eq!(fs::read(&child_head).unwrap(), head_bytes);
        assert_eq!(fs::read(&fixture.catalog_path).unwrap(), catalog_bytes);
        assert!(!child_directory.join("runtime").exists());
        let (store, catalog) = main.store_and_catalog_mut();
        store
            .create_node(catalog, "SourceStillWritable", BTreeMap::new())
            .unwrap();
        assert_eq!(main.store().node_count_for_label(None), 3);
        let child = fixture.admit(branch_id(2), DurabilityPolicy::default());
        assert_eq!(child.store().node_count_for_label(None), 2);
    }

    #[test]
    fn branch_replay_budget_covers_every_sealed_and_private_interval() {
        let fixture = BranchFixture::new();
        let mut branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        write_schema_and_graph(&mut branch);
        let (store, catalog) = branch.store_and_catalog_mut();
        store.checkpoint(catalog).unwrap();
        for body in ["sealed-one", "sealed-two"] {
            write_row(&mut branch, body);
            let epoch = branch.store().commit_epoch();
            branch.store_mut().seal_admitted_branch(epoch).unwrap();
        }
        write_row(&mut branch, "private-three");
        let expected_epoch = branch.store().commit_epoch();
        let head_path = fixture.head_path(fixture.main);
        let old_head = fs::read(&head_path).unwrap();
        let wal_path = branch.store().durable.as_ref().unwrap().wal_path.clone();
        let old_wal = fs::read(&wal_path).unwrap();
        let evidence_path = head_path.parent().unwrap().join("data/recovery-evidence");
        fs::write(&evidence_path, b"persistent dependency").unwrap();
        drop(branch);
        let error = fixture
            .try_admit(
                fixture.main,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    max_entries: Some(2),
                    ..WalReplayConfig::default()
                },
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("entry limit exceeded"),
            "{error}"
        );
        assert_eq!(fs::read(&head_path).unwrap(), old_head);
        assert_eq!(fs::read(&wal_path).unwrap(), old_wal);
        assert_eq!(fs::read(&evidence_path).unwrap(), b"persistent dependency");
        assert!(!head_path.parent().unwrap().join("runtime").exists());
        let branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        assert_eq!(branch.store().commit_epoch(), expected_epoch);
        assert_eq!(branch.store().relational_state().row_count("messages"), 4);
        assert_eq!(
            branch.store().storage_recovery_report.replayed_wal_entries,
            3
        );
    }

    #[test]
    fn catalog_sweep_defers_for_publication_candidates_and_reader_leases() {
        let fixture = BranchFixture::new();
        let mut branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        let old_head = *branch.head();
        let snapshot = branch.store().snapshot_for_read();
        write_schema_and_graph(&mut branch);
        let prepared = branch
            .store_mut()
            .prepare_immutable_root_handoff(&fixture.objects)
            .unwrap();
        let new_root = prepared.root.object_reference().unwrap();
        let mut objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
        let orphan = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"orphan");
        objects.publish(orphan, b"orphan").unwrap();
        let mut inventory = vec![
            old_head.sealed_root,
            new_root,
            orphan,
            prepared.root.durable_manifest,
        ];
        inventory.extend_from_slice(&prepared.root.checkpoint_references);
        inventory.extend(prepared.root.sealed_wals.iter().map(|wal| wal.object));
        let paths = vec![branch_catalog::BranchReclamationPath {
            id: fixture.main,
            directory: fixture
                .head_path(fixture.main)
                .parent()
                .unwrap()
                .to_path_buf(),
            head_path: fixture.head_path(fixture.main),
        }];
        let report = branch_catalog::reclaim_catalog_branches(
            &fixture.catalog_path,
            &mut objects,
            &inventory,
            &paths,
        )
        .unwrap();
        assert!(report.deferred_for_active_leases);
        assert_eq!(report.reclaimed_objects, 0);
        assert!(
            objects.read(new_root).is_ok(),
            "unpublished candidate stays protected"
        );
        branch
            .store_mut()
            .complete_immutable_root_handoff(
                prepared,
                fixture.head_path(fixture.main),
                old_head.project_id,
                old_head.branch_id,
                old_head.physical_generation,
            )
            .unwrap();
        drop(branch);
        let report = branch_catalog::reclaim_catalog_branches(
            &fixture.catalog_path,
            &mut objects,
            &inventory,
            &paths,
        )
        .unwrap();
        assert!(
            report.deferred_for_active_leases,
            "old snapshot keeps its lease"
        );
        assert!(objects.read(old_head.sealed_root).is_ok());
        drop(snapshot);
        let report = branch_catalog::reclaim_catalog_branches(
            &fixture.catalog_path,
            &mut objects,
            &inventory,
            &paths,
        )
        .unwrap();
        assert!(!report.deferred_for_active_leases);
        assert!(report.reclaimed_objects > 0);
        assert!(objects.read(new_root).is_ok());
        assert!(objects.read(orphan).is_err());
        let branch = fixture.admit(fixture.main, DurabilityPolicy::default());
        assert_eq!(branch.store().node_count_for_label(None), 3);
        assert_eq!(branch.store().relational_state().row_count("messages"), 1);
    }

    #[test]
    fn sealed_root_retains_every_path_for_duplicate_content_artifacts() {
        let database = temp_dir("duplicate-content-bindings");
        let immutable = database.join("immutable");
        let checkpoint = database.join("checkpoint.hawdb");
        let first = database.join("generations/overflow-11.hawdb");
        let second = database.join("generations/overflow-12.hawdb");
        let shared = b"identical empty overflow generation";
        fs::create_dir_all(first.parent().expect("first parent")).expect("create artifacts");
        fs::write(&checkpoint, b"checkpoint").expect("write checkpoint");
        fs::write(&first, shared).expect("write first artifact");
        fs::write(&second, shared).expect("write second artifact");

        let mut plan =
            crate::checkpoint_closure::CheckpointClosurePlan::new(vec![CheckpointArtifactInput {
                path: checkpoint,
                reference: ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"checkpoint"),
            }]);
        let shared_reference =
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, shared);
        plan.add_family_artifacts(
            crate::checkpoint_closure::CheckpointArtifactFamily::RelationalOverflow,
            vec![
                CheckpointArtifactInput {
                    path: first.clone(),
                    reference: shared_reference,
                },
                CheckpointArtifactInput {
                    path: second.clone(),
                    reference: shared_reference,
                },
            ],
        )
        .expect("bind overflow artifacts");
        for family in [
            crate::checkpoint_closure::CheckpointArtifactFamily::Canonical,
            crate::checkpoint_closure::CheckpointArtifactFamily::Adjacency,
            crate::checkpoint_closure::CheckpointArtifactFamily::PropertySpill,
            crate::checkpoint_closure::CheckpointArtifactFamily::PropertyProjection,
            crate::checkpoint_closure::CheckpointArtifactFamily::RelationalRow,
            crate::checkpoint_closure::CheckpointArtifactFamily::RelationalIndex,
            crate::checkpoint_closure::CheckpointArtifactFamily::Append,
        ] {
            plan.mark_family_empty(family).expect("complete family");
        }

        let bindings = checkpoint_artifact_bindings(&database, plan.inputs())
            .expect("derive every physical path binding");
        assert_eq!(bindings.len(), 3);
        assert_eq!(
            bindings
                .iter()
                .filter(|binding| binding.reference == shared_reference)
                .count(),
            2
        );
        let mut objects = ImmutableObjectStore::open(&immutable).expect("open immutable objects");
        let closure = plan
            .publish(&mut objects)
            .expect("publish deduplicated objects");
        assert_eq!(closure.references.len(), 2);
        let root = build_sealed_root(
            &closure,
            1,
            1,
            1,
            ObjectReference::for_bytes(ObjectKind::DurableManifest, 1, b"manifest"),
            bindings,
            Vec::new(),
        )
        .expect("build sealed root");
        root.validate().expect("validate sealed root");

        let destination = temp_dir("duplicate-content-bindings-recovery");
        materialize_checkpoint_bindings(&objects, &destination, &root.checkpoint_bindings)
            .expect("materialize every checkpoint binding");
        assert_eq!(
            fs::read(destination.join("generations/overflow-11.hawdb")).unwrap(),
            shared
        );
        assert_eq!(
            fs::read(destination.join("generations/overflow-12.hawdb")).unwrap(),
            shared
        );

        let _ = fs::remove_dir_all(database);
        let _ = fs::remove_dir_all(destination);
    }

    #[test]
    fn graph_store_prepares_root_and_private_successor_wal() {
        let database = temp_dir("immutable-root");
        let objects = database.join("immutable");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&database, &mut catalog).expect("open database");
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(1))]),
            )
            .expect("create node");
        store.checkpoint(&catalog).expect("publish checkpoint");
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(2))]),
            )
            .expect("create post-checkpoint node");

        let prepared = store
            .prepare_immutable_root_handoff(&objects)
            .expect("prepare immutable root");
        prepared.root.validate().expect("valid root");
        assert!(!prepared.root.checkpoint_references.is_empty());
        assert!(
            prepared.root.checkpoint_bindings.len() >= prepared.root.checkpoint_references.len()
        );
        let root_manifest = ImmutableObjectStore::open(&objects)
            .expect("open immutable objects")
            .read(prepared.root.durable_manifest)
            .expect("read root-bound manifest");
        assert_eq!(
            root_manifest,
            fs::read(database.join(MANIFEST_FILE)).unwrap()
        );
        assert!(prepared.rotation.next_generation > prepared.rotation.sealed.generation);
        assert!(prepared.rotation.next_wal_path.exists());
        assert!(objects.join("objects").exists());

        let replay_database = temp_dir("immutable-root-replay");
        let mut replay_catalog = Catalog::default();
        let replayed = GraphStore::open_from_immutable_root(
            &store,
            &prepared.root,
            &objects,
            &replay_database,
            &mut replay_catalog,
        )
        .expect("replay immutable root");
        assert_eq!(replayed.node_count_for_label(None), 2);
        drop(replayed);

        let head_path = database.join("branch.head");
        let seed_wal_path = database.join("seed-wal.99");
        let root_reference = prepared.root.object_reference().expect("root reference");
        crate::branch_head::create_child_branch_head(
            &head_path,
            crate::branch_head::ChildBranchHeadRequest {
                project_id: [1; 16],
                branch_id: [2; 16],
                sealed_root: root_reference,
                logical_commit_epoch: store.commit_epoch(),
                active_wal_generation: 99,
                replay_start_lsn: 1,
                head_path: head_path.clone(),
                wal_path: seed_wal_path,
            },
            u64::MAX,
        )
        .expect("seed branch selector");
        store
            .complete_immutable_root_handoff(prepared, &head_path, [1; 16], [2; 16], 1)
            .expect("complete immutable root handoff");

        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(3))]),
            )
            .expect("write unrelated branch state");
        let active_wal_path = store.durable.as_ref().unwrap().wal_path.clone();
        let active_wal_bytes = fs::read(&active_wal_path).unwrap();
        let previous_checkpoint = store.durable.as_ref().unwrap().checkpoint_epoch;
        let checkpoint = store
            .prepare_checkpoint(&catalog)
            .expect("prepare source checkpoint after root sealing")
            .expect("persistent source has a checkpoint candidate");
        assert!(checkpoint.generation > previous_checkpoint + 1);
        assert_eq!(
            fs::read(&active_wal_path).unwrap(),
            active_wal_bytes,
            "checkpoint preparation must preserve the active successor WAL"
        );
        store
            .publish_prepared_checkpoint(checkpoint, None)
            .expect("advance source checkpoint after root sealing");
        assert!(
            database
                .join(format!("canonical.{previous_checkpoint}.hawdb"))
                .exists(),
            "retain the actual preceding checkpoint across a generation gap"
        );
        drop(store);
        assert_ne!(
            root_manifest,
            fs::read(database.join(MANIFEST_FILE)).unwrap()
        );
        let mut reopened_catalog = Catalog::default();
        let mut reopened_source =
            GraphStore::open(&database, &mut reopened_catalog).expect("reopen source database");
        assert_eq!(reopened_source.node_count_for_label(None), 3);
        let replay_after_write_database = temp_dir("immutable-root-replay-after-write");
        let mut replay_after_write_catalog = Catalog::default();
        let (replay_after_write, reopened_head) = GraphStore::open_from_branch_head(
            &reopened_source,
            &head_path,
            &objects,
            &replay_after_write_database,
            &mut replay_after_write_catalog,
        )
        .expect("replay immutable root after selector reopen");
        assert_eq!(reopened_head.sealed_root, root_reference);
        assert_eq!(replay_after_write.node_count_for_label(None), 2);

        // A failed selector publication must leave the previously published
        // root authoritative even though the successor WAL and root were
        // prepared durably.
        reopened_source
            .create_node(
                &mut reopened_catalog,
                "Memory",
                BTreeMap::from([("value".into(), Value::Int(4))]),
            )
            .expect("create post-reopen node");
        let next_prepared = reopened_source
            .prepare_immutable_root_handoff(&objects)
            .expect("prepare second immutable root");
        let previous_head_bytes = fs::read(&head_path).expect("read published head");
        let _failure = crate::durability::fail_durable_replace_for_destination(
            head_path.file_name().expect("head file name"),
        );
        let error = reopened_source
            .complete_immutable_root_handoff(
                next_prepared,
                &head_path,
                [1; 16],
                [2; 16],
                reopened_head.physical_generation,
            )
            .expect_err("injected selector publication failure");
        assert!(error.to_string().contains("uncertain"));
        assert!(reopened_source.storage_handle_poisoned());
        assert!(reopened_source
            .prepare_immutable_root_handoff(&objects)
            .is_err());
        assert_eq!(
            fs::read(&head_path).expect("read unchanged published head"),
            previous_head_bytes
        );

        let _ = fs::remove_dir_all(database);
        let _ = fs::remove_dir_all(replay_database);
        let _ = fs::remove_dir_all(replay_after_write_database);
    }

    #[test]
    fn graph_store_creates_isolated_child_from_selected_parent_head() {
        let database = temp_dir("isolated-child");
        let catalog_path = database.join("catalog");
        let parent_head_path = database.join("parent.head");
        let child_directory = database.join("child");
        let child_head_path = child_directory.join("branch.head");
        let child_wal_path = child_directory.join("wal.8");
        fs::create_dir_all(&child_directory).expect("create child directory");

        let project_id = crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(1))
            .expect("project id");
        let parent_id = crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(2))
            .expect("parent id");
        let child_id =
            crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(3)).expect("child id");
        let parent_root = crate::immutable_object::ObjectReference::for_bytes(
            crate::immutable_object::ObjectKind::SealedRoot,
            1,
            b"parent-root",
        );
        let parent_head = crate::branch_head::BranchHead {
            project_id: *project_id.as_uuid().as_bytes(),
            branch_id: *parent_id.as_uuid().as_bytes(),
            physical_generation: 4,
            sealed_root: parent_root,
            logical_commit_epoch: 7,
            active_wal: crate::branch_head::ActiveWalIdentity {
                generation: 7,
                replay_start_lsn: 20,
                byte_length: 1,
                sha256: hawdb_integrity::sha256(b"parent-wal"),
            },
        };
        fs::write(
            &parent_head_path,
            parent_head.encode().expect("encode parent head"),
        )
        .expect("write parent head");
        let catalog = crate::branch_catalog::Catalog {
            project_id,
            revision: 1,
            branches: vec![crate::branch_catalog::BranchRecord {
                id: parent_id,
                name: crate::branch_catalog::BranchName::new("parent").expect("parent name"),
                parent_id: None,
                source_commit_epoch: 7,
                base_root_digest: Some(*parent_root.sha256.as_bytes()),
                metadata_revision: 1,
                state: crate::branch_catalog::BranchState::Ready,
                owner: None,
                create_request_key: "parent-create".to_string(),
                request_fingerprint: [1; 32],
                create_outcome: crate::branch_catalog::CreateOutcome::Succeeded,
            }],
        };
        crate::branch_catalog::write_catalog(&catalog_path, &catalog).expect("write catalog");

        let request = crate::branch_catalog::CreateRequest {
            id: child_id,
            name: crate::branch_catalog::BranchName::new("child").expect("child name"),
            parent_id,
            source_commit_epoch: 7,
            base_root_digest: *parent_root.sha256.as_bytes(),
            owner: None,
            request_key: "child-create".to_string(),
            request_fingerprint: [2; 32],
        };
        let parent_bytes = fs::read(&parent_head_path).expect("read parent head");
        let result = GraphStore::default()
            .create_isolated_branch_from_parent_head(
                &catalog_path,
                &parent_head_path,
                &child_head_path,
                &child_wal_path,
                request,
            )
            .expect("create isolated child");

        assert_eq!(result.head.sealed_root, parent_root);
        assert_eq!(result.head.branch_id, *child_id.as_uuid().as_bytes());
        assert_ne!(
            result.head.active_wal.generation,
            parent_head.active_wal.generation
        );
        assert!(child_head_path.is_file());
        assert!(child_wal_path.is_file());
        assert_eq!(
            fs::read(&parent_head_path).expect("read parent head"),
            parent_bytes
        );
        let child_record = crate::branch_catalog::read_catalog(&catalog_path)
            .expect("read catalog")
            .branches
            .into_iter()
            .find(|branch| branch.id == child_id)
            .expect("child catalog record");
        assert_eq!(
            child_record.state,
            crate::branch_catalog::BranchState::Ready
        );
        assert_eq!(child_record.parent_id, Some(parent_id));

        let _ = fs::remove_dir_all(database);
    }

    #[test]
    fn combines_sealed_and_private_wal_recovery_evidence() {
        let sealed = crate::projection::StorageRecoveryReport {
            wal_present: true,
            wal_generation: Some(7),
            wal_replay_start_lsn: Some(3),
            next_lsn_after_replay: Some(9),
            replayed_wal_entries: 4,
            replayed_wal_bytes: 80,
            torn_tail_ignored: true,
            discarded_wal_tail_bytes: 6,
            torn_tail_reason: Some("sealed tail".to_string()),
            recovered_commit_epoch: 8,
            ..Default::default()
        };
        let private = crate::projection::StorageRecoveryReport {
            wal_present: true,
            wal_generation: Some(8),
            wal_replay_start_lsn: Some(9),
            next_lsn_after_replay: Some(11),
            replayed_wal_entries: 2,
            replayed_wal_bytes: 40,
            torn_tail_repaired: true,
            discarded_wal_tail_bytes: 3,
            torn_tail_reason: Some("private tail".to_string()),
            recovered_commit_epoch: 10,
            ..Default::default()
        };

        let combined = combine_recovery_reports(sealed, private);

        assert_eq!(combined.wal_generation, Some(8));
        assert_eq!(combined.wal_replay_start_lsn, Some(9));
        assert_eq!(combined.next_lsn_after_replay, Some(11));
        assert_eq!(combined.replayed_wal_entries, 6);
        assert_eq!(combined.replayed_wal_bytes, 120);
        assert!(combined.torn_tail_ignored);
        assert!(combined.torn_tail_repaired);
        assert_eq!(combined.discarded_wal_tail_bytes, 9);
        assert_eq!(
            combined.torn_tail_reason.as_deref(),
            Some("sealed root WAL: sealed tail; private WAL: private tail")
        );
        assert_eq!(combined.recovered_commit_epoch, 10);
    }

    #[test]
    fn admits_a_child_directly_from_its_head_after_parent_directory_removal() {
        let project = temp_dir("direct-branch-admission");
        let parent_directory = project.join("former-parent");
        let objects = project.join("objects");
        let catalog_path = project.join("catalog.hawdb");
        let child_directory = project.join("branches/child");
        let child_head_path = child_directory.join("branch.head");
        let sibling_directory = project.join("branches/sibling");
        let sibling_head_path = sibling_directory.join("branch.head");
        fs::create_dir_all(&child_directory).expect("create child directory");
        fs::create_dir_all(&sibling_directory).expect("create sibling directory");

        let project_id = crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(91))
            .expect("project id");
        let child_id = crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(92))
            .expect("child id");
        let sibling_id = crate::branch_catalog::BranchId::new(hawdb_core::Uuid::from_u128(93))
            .expect("sibling id");
        let mut source_catalog = Catalog::default();
        let mut source =
            GraphStore::open(&parent_directory, &mut source_catalog).expect("open parent");
        source
            .create_node(
                &mut source_catalog,
                "Memory",
                BTreeMap::from([("id".into(), Value::Int(1))]),
            )
            .expect("create checkpoint node");
        source
            .checkpoint(&source_catalog)
            .expect("publish checkpoint");
        source
            .create_node(
                &mut source_catalog,
                "Memory",
                BTreeMap::from([("id".into(), Value::Int(2))]),
            )
            .expect("create sealed-WAL node");
        let prepared = source
            .prepare_immutable_root_handoff(&objects)
            .expect("prepare immutable root");
        let root_reference = prepared.root.object_reference().expect("root reference");
        let active_generation = prepared.rotation.next_generation;
        let active_wal_path = child_directory.join(crate::artifact_files::wal_generation_file(
            active_generation,
        ));
        crate::branch_head::create_child_branch_head(
            &child_head_path,
            crate::branch_head::ChildBranchHeadRequest {
                project_id: *project_id.as_uuid().as_bytes(),
                branch_id: *child_id.as_uuid().as_bytes(),
                sealed_root: root_reference,
                logical_commit_epoch: prepared.root.commit_epoch,
                active_wal_generation: active_generation,
                replay_start_lsn: prepared.root.sealed_wals[0].end_lsn,
                head_path: child_head_path.clone(),
                wal_path: active_wal_path.clone(),
            },
            u64::MAX,
        )
        .expect("create child head");
        let sibling_active_wal_path = sibling_directory.join(
            crate::artifact_files::wal_generation_file(active_generation),
        );
        crate::branch_head::create_child_branch_head(
            &sibling_head_path,
            crate::branch_head::ChildBranchHeadRequest {
                project_id: *project_id.as_uuid().as_bytes(),
                branch_id: *sibling_id.as_uuid().as_bytes(),
                sealed_root: root_reference,
                logical_commit_epoch: prepared.root.commit_epoch,
                active_wal_generation: active_generation,
                replay_start_lsn: prepared.root.sealed_wals[0].end_lsn,
                head_path: sibling_head_path.clone(),
                wal_path: sibling_active_wal_path,
            },
            u64::MAX,
        )
        .expect("create sibling head");
        let catalog = crate::branch_catalog::Catalog {
            project_id,
            revision: 1,
            branches: vec![
                crate::branch_catalog::BranchRecord {
                    id: child_id,
                    name: crate::branch_catalog::BranchName::new("child")
                        .expect("child branch name"),
                    parent_id: None,
                    source_commit_epoch: prepared.root.commit_epoch,
                    base_root_digest: Some(*root_reference.sha256.as_bytes()),
                    metadata_revision: 1,
                    state: crate::branch_catalog::BranchState::Ready,
                    owner: None,
                    create_request_key: "child-create".to_string(),
                    request_fingerprint: [9; 32],
                    create_outcome: crate::branch_catalog::CreateOutcome::Succeeded,
                },
                crate::branch_catalog::BranchRecord {
                    id: sibling_id,
                    name: crate::branch_catalog::BranchName::new("sibling")
                        .expect("sibling branch name"),
                    parent_id: None,
                    source_commit_epoch: prepared.root.commit_epoch,
                    base_root_digest: Some(*root_reference.sha256.as_bytes()),
                    metadata_revision: 1,
                    state: crate::branch_catalog::BranchState::Ready,
                    owner: None,
                    create_request_key: "sibling-create".to_string(),
                    request_fingerprint: [10; 32],
                    create_outcome: crate::branch_catalog::CreateOutcome::Succeeded,
                },
            ],
        };
        crate::branch_catalog::write_catalog(&catalog_path, &catalog).expect("write catalog");
        drop(source);
        fs::remove_dir_all(&parent_directory).expect("remove parent directory");

        let request = BranchAdmissionRequest {
            catalog_path: &catalog_path,
            branch_id: child_id,
            expected_metadata_revision: 1,
            head_path: &child_head_path,
            immutable_store_root: &objects,
            durability: DurabilityPolicy::default(),
            replay_config: WalReplayConfig::default(),
        };
        let mut admitted =
            GraphStore::admit_branch_from_head(request).expect("admit child directly");
        assert_eq!(admitted.store().node_count_for_label(None), 2);
        assert_eq!(admitted.head().sealed_root, root_reference);
        let sibling = GraphStore::admit_branch_from_head(BranchAdmissionRequest {
            catalog_path: &catalog_path,
            branch_id: sibling_id,
            expected_metadata_revision: 1,
            head_path: &sibling_head_path,
            immutable_store_root: &objects,
            durability: DurabilityPolicy::default(),
            replay_config: WalReplayConfig::default(),
        })
        .expect("admit sibling while child has its own writer lease");
        assert_eq!(sibling.store().node_count_for_label(None), 2);
        assert!(matches!(
            GraphStore::admit_branch_from_head(BranchAdmissionRequest {
                catalog_path: &catalog_path,
                branch_id: child_id,
                expected_metadata_revision: 1,
                head_path: &child_head_path,
                immutable_store_root: &objects,
                durability: DurabilityPolicy::default(),
                replay_config: WalReplayConfig::default(),
            }),
            Err(BranchAdmissionError::Lease(
                DatabaseDirectoryLeaseError::AlreadyOpen
            ))
        ));
        drop(sibling);

        {
            let (store, catalog) = admitted.store_and_catalog_mut();
            store
                .create_node(
                    catalog,
                    "Memory",
                    BTreeMap::from([("id".into(), Value::Int(3))]),
                )
                .expect("write child private WAL");
        }
        let child_head_before_reopen = *admitted.head();
        drop(admitted);

        let reopened = GraphStore::admit_branch_from_head(BranchAdmissionRequest {
            catalog_path: &catalog_path,
            branch_id: child_id,
            expected_metadata_revision: 1,
            head_path: &child_head_path,
            immutable_store_root: &objects,
            durability: DurabilityPolicy::default(),
            replay_config: WalReplayConfig::default(),
        })
        .expect("reopen child directly");
        assert_eq!(reopened.store().node_count_for_label(None), 3);
        assert_eq!(
            *reopened.head(),
            child_head_before_reopen,
            "ordinary child DML extends the private WAL without rewriting its head"
        );

        drop(reopened);

        let mut remapped = crate::branch_catalog::read_catalog(&catalog_path)
            .expect("read catalog before remapping child");
        remapped
            .branches
            .iter_mut()
            .find(|branch| branch.id == child_id)
            .expect("child catalog record before remapping")
            .metadata_revision = 2;
        remapped.revision = 2;
        crate::branch_catalog::write_catalog(&catalog_path, &remapped)
            .expect("publish remapped catalog revision");
        assert!(matches!(
            GraphStore::admit_branch_from_head(BranchAdmissionRequest {
                catalog_path: &catalog_path,
                branch_id: child_id,
                expected_metadata_revision: 1,
                head_path: &child_head_path,
                immutable_store_root: &objects,
                durability: DurabilityPolicy::default(),
                replay_config: WalReplayConfig::default(),
            }),
            Err(BranchAdmissionError::StaleMetadataRevision {
                expected: 1,
                actual: 2,
            })
        ));

        let deleting_child = remapped
            .branches
            .iter_mut()
            .find(|branch| branch.id == child_id)
            .expect("child catalog record before deletion");
        deleting_child.metadata_revision = 3;
        deleting_child.state = crate::branch_catalog::BranchState::Deleting;
        remapped.revision = 3;
        crate::branch_catalog::write_catalog(&catalog_path, &remapped)
            .expect("publish deleting catalog revision");
        assert!(matches!(
            GraphStore::admit_branch_from_head(BranchAdmissionRequest {
                catalog_path: &catalog_path,
                branch_id: child_id,
                expected_metadata_revision: 3,
                head_path: &child_head_path,
                immutable_store_root: &objects,
                durability: DurabilityPolicy::default(),
                replay_config: WalReplayConfig::default(),
            }),
            Err(BranchAdmissionError::InvalidState(
                crate::branch_catalog::BranchState::Deleting
            ))
        ));

        crate::branch_catalog::write_catalog(&catalog_path, &catalog)
            .expect("restore ready catalog before WAL corruption test");
        let original_private_wal = fs::read(&active_wal_path).expect("read child private WAL");
        let mut corrupted_private_wal = original_private_wal.clone();
        corrupted_private_wal[0] ^= 0x01;
        fs::write(&active_wal_path, corrupted_private_wal).expect("corrupt child private WAL");
        assert!(matches!(
            GraphStore::admit_branch_from_head(BranchAdmissionRequest {
                catalog_path: &catalog_path,
                branch_id: child_id,
                expected_metadata_revision: 1,
                head_path: &child_head_path,
                immutable_store_root: &objects,
                durability: DurabilityPolicy::default(),
                replay_config: WalReplayConfig::default(),
            }),
            Err(BranchAdmissionError::Recovery(_))
        ));

        fs::write(&active_wal_path, &original_private_wal)
            .expect("restore child private WAL before torn-tail test");
        let runtime_directory = child_directory.join("runtime");
        fs::remove_dir_all(&runtime_directory).expect("remove prior successful runtime");
        fs::OpenOptions::new()
            .append(true)
            .open(&active_wal_path)
            .expect("open child private WAL for torn suffix")
            .write_all(&[0x01])
            .expect("append torn private WAL suffix");
        let torn_request = || BranchAdmissionRequest {
            catalog_path: &catalog_path,
            branch_id: child_id,
            expected_metadata_revision: 1,
            head_path: &child_head_path,
            immutable_store_root: &objects,
            durability: DurabilityPolicy::default(),
            replay_config: WalReplayConfig::default(),
        };
        let mut cleaned = false;
        assert!(matches!(
            GraphStore::admit_branch_from_head_with_cleanup(torn_request(), |runtime| {
                assert!(runtime.exists(), "recovery materialized the failed runtime");
                // Pause at the cleanup boundary and run a contending opener on
                // another thread. It must fail before touching runtime files.
                std::thread::scope(|scope| {
                    let retry = scope.spawn(|| GraphStore::admit_branch_from_head(torn_request()));
                    assert!(matches!(
                        retry.join().expect("contending opener must not panic"),
                        Err(BranchAdmissionError::Lease(
                            DatabaseDirectoryLeaseError::AlreadyOpen
                        ))
                    ));
                });
                fs::remove_dir_all(runtime).expect("cleanup failed branch runtime");
                cleaned = true;
            }),
            Err(BranchAdmissionError::Recovery(_))
        ));
        assert!(cleaned, "torn-tail failure must reach the cleanup boundary");
        assert!(
            !runtime_directory.exists(),
            "failed admission must remove its materialized runtime"
        );
        fs::write(&active_wal_path, original_private_wal).expect("restore valid private WAL");
        let retried = GraphStore::admit_branch_from_head(torn_request())
            .expect("retry admission after cleanup releases the target lease");
        assert_eq!(retried.store().node_count_for_label(None), 3);
        drop(retried);

        let _ = fs::remove_dir_all(project);
    }
}
