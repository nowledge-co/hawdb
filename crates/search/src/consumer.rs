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

//! Internal projection ownership used by the embedded database facade.
use super::{SearchIndex, SEARCH_SNAPSHOT_FILE};
use crate::error::{HawDBError, Result};
use crate::out_of_core::SearchProjectionPublishLease;
use hawdb_core::Uuid;
use hawdb_integrity::IntegrityHasher;
use hawdb_storage::file_io::{self as fs, File};
use std::io::Read;
use std::path::Path;

mod control;
mod operation;
pub(crate) use control::require_unregistered_directory;
pub use operation::ConsumerPreparationLease;
pub(crate) use operation::{Authority as MutationOwner, MutationPermit};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerBinding {
    pub database_uuid: Uuid,
    pub projection_uuid: Uuid,
    pub consumer_id: String,
    pub registration_uuid: Uuid,
    pub checkpoint_uuid: Uuid,
}

impl ConsumerBinding {
    pub(super) fn parse(
        database: &str,
        projection: &str,
        id: &str,
        registration: &str,
        checkpoint: &str,
    ) -> Result<Self> {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return Err(invalid("invalid consumer ID"));
        }
        Ok(Self {
            database_uuid: canonical_uuid(database)?,
            projection_uuid: canonical_uuid(projection)?,
            consumer_id: id.into(),
            registration_uuid: canonical_uuid(registration)?,
            checkpoint_uuid: canonical_uuid(checkpoint)?,
        })
    }

    pub(super) fn record(&self) -> String {
        format!(
            "projection_consumer_binding\t{}\t{}\t{}\t{}\t{}\n",
            self.database_uuid,
            self.projection_uuid,
            self.consumer_id,
            self.registration_uuid,
            self.checkpoint_uuid
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReceipt {
    pub binding: ConsumerBinding,
    pub source_epoch: u64,
    pub encoded_len: u64,
    pub sha256: String,
}

#[derive(Debug)]
pub struct ConsumerProjection {
    index: SearchIndex,
    authority: std::sync::Arc<operation::Authority>,
    pending_mutation: bool,
}

impl ConsumerProjection {
    pub fn initialize<F>(
        root: &Path,
        binding: ConsumerBinding,
        source_epoch: u64,
        initialize: F,
    ) -> Result<Self>
    where
        F: FnOnce(&mut SearchIndex) -> Result<()>,
    {
        let project_files =
            hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_component(
                root, false,
            )?;
        let lease = SearchProjectionPublishLease::acquire_for_consumer(root)?;
        let authority = operation::Authority::new(lease);
        let operation = authority.acquire()?;
        let mut index = SearchIndex {
            path: Some(root.to_path_buf()),
            _project_files: Some(project_files),
            consumer_owner: std::sync::Arc::downgrade(&authority),
            consumer_mutation_generation: Some(operation.open_mutations()),
            ..SearchIndex::default()
        };
        {
            let scope = MutationScope {
                index: &mut index,
                operation,
            };
            initialize(scope.index)?;
        }
        if !authority.owns(&index)
            || index.path.as_deref() != Some(root)
            || index.consumer_binding.is_some()
            || index
                .import_source_graph_commit_epoch
                .is_some_and(|epoch| epoch > source_epoch)
        {
            return Err(invalid("initializer replaced the staging projection or supplied an impossible import epoch"));
        }
        if index.analyzer_lexicon != super::SearchAnalyzerLexicon::default() {
            return Err(invalid(
                "consumer reopen currently requires the default analyzer lexicon",
            ));
        }
        index.source_graph_commit_epoch = Some(source_epoch);
        index.consumer_binding = Some(binding);
        let mut owner = Self {
            index,
            authority,
            pending_mutation: true,
        };
        owner.checkpoint()?;
        Ok(owner)
    }

    pub fn open(root: &Path) -> Result<Self> {
        let _project_files =
            hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_component(
                root, false,
            )?;
        let lease = SearchProjectionPublishLease::acquire_for_consumer(root)?;
        let mut index = SearchIndex::open_under_lease(root, true)?;
        let authority = operation::Authority::new(lease);
        index.consumer_owner = std::sync::Arc::downgrade(&authority);
        let owner = Self {
            index,
            authority,
            pending_mutation: false,
        };
        let receipt = owner.read_receipt()?;
        *owner
            .index
            .consumer_receipt
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(receipt);
        Ok(owner)
    }

    pub fn index(&self) -> &SearchIndex {
        &self.index
    }

    pub fn binding(&self) -> &ConsumerBinding {
        self.index
            .consumer_binding
            .as_ref()
            .expect("consumer owns a bound projection")
    }

    pub fn receipt(&self) -> CheckpointReceipt {
        self.index
            .consumer_receipt
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .expect("consumer owns a completed checkpoint")
            .clone()
    }

    pub fn apply<T>(&mut self, apply: impl FnOnce(&mut SearchIndex) -> Result<T>) -> Result<T> {
        let operation = self.authority.acquire()?;
        self.authority.require_index(&self.index)?;
        self.pending_mutation = true;
        let result = {
            self.index.consumer_mutation_generation = Some(operation.open_mutations());
            let scope = MutationScope {
                index: &mut self.index,
                operation,
            };
            apply(scope.index)
        };
        if let Err(error) = self.authority.require_index(&self.index) {
            self.index.consumer_binding_valid = false;
            return Err(error);
        }
        result
    }

    pub fn binding_is_valid(&self) -> bool {
        self.index.consumer_binding_valid && self.authority.owns(&self.index)
    }

    pub fn checkpoint(&mut self) -> Result<CheckpointReceipt> {
        let operation = self.authority.acquire()?;
        self.authority.require_index(&self.index)?;
        if !self.index.consumer_binding_valid {
            return Err(invalid("untracked projection mutation"));
        }
        self.pending_mutation = true;
        self.index
            .consumer_binding
            .as_mut()
            .expect("bound consumer")
            .checkpoint_uuid = hawdb_core::uuidv7::generate_uuidv7()?;
        self.index.consumer_mutation_generation = Some(operation.open_mutations());
        {
            let scope = MutationScope {
                index: &mut self.index,
                operation,
            };
            scope.index.checkpoint_with_lease(true)?;
        }
        self.pending_mutation = false;
        Ok(self.receipt())
    }

    pub fn publish_directory(&mut self, destination: &Path) -> Result<()> {
        let _operation = self.authority.acquire()?;
        self.authority.require_index(&self.index)?;
        let pending_mutation = self.pending_mutation;
        self.pending_mutation = true;
        fs::create_dir(destination)?;
        let source = self.index.path.as_ref().expect("persistent consumer");
        if let Err(error) = fs::rename(source, destination) {
            if let Err(cleanup) = fs::remove_dir(destination) {
                return Err(invalid(&format!("projection directory publication failed: {error}; destination reservation cleanup failed: {cleanup}")));
            }
            return Err(error.into());
        }
        // Rename has already changed the namespace, even if a later durability
        // barrier fails. Never reuse the vacated path with the moved native lock.
        self.index.path = Some(destination.to_path_buf());
        // Revoke operations until both namespace durability and reader reload
        // succeed. A failed move retains its actual root and recovery evidence.
        self.index.consumer_binding_valid = false;
        sync_published_parent(destination)?;
        // Readers hold paths into their generation. Reload those readers while
        // retaining the lease, receipt, and initializer's runtime configuration.
        self.index.validate_registered_artifacts()?;
        #[cfg(feature = "vector-search")]
        self.index.load_registered_rabitq_projection()?;
        self.index.consumer_binding_valid = true;
        self.pending_mutation = pending_mutation;
        Ok(())
    }

    fn read_receipt(&self) -> Result<CheckpointReceipt> {
        let path = self
            .index
            .path
            .as_ref()
            .expect("persistent consumer")
            .join(SEARCH_SNAPSHOT_FILE);
        let mut file = File::open(path)?;
        let mut hasher = IntegrityHasher::new();
        let mut buffer = [0u8; 8192];
        let mut encoded_len = 0u64;
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            encoded_len = encoded_len
                .checked_add(count as u64)
                .ok_or_else(|| invalid("snapshot length overflow"))?;
        }
        Ok(CheckpointReceipt {
            binding: self.binding().clone(),
            source_epoch: self
                .index
                .source_graph_commit_epoch
                .ok_or_else(|| invalid("missing source epoch"))?,
            encoded_len,
            sha256: hasher.finish().sha256.to_string(),
        })
    }
}

#[cfg(test)]
thread_local! {
    static FAIL_PUBLISHED_PARENT_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
fn sync_published_parent(destination: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_PUBLISHED_PARENT_SYNC.with(|failure| failure.replace(false)) {
        return Err(invalid(
            "injected parent synchronization failure after directory rename",
        ));
    }
    hawdb_storage::durability::sync_parent_directory(destination)?;
    Ok(())
}

struct MutationScope<'a> {
    index: &'a mut SearchIndex,
    operation: ConsumerPreparationLease,
}
impl Drop for MutationScope<'_> {
    fn drop(&mut self) {
        if self.operation.owns(self.index)
            && self.index.consumer_mutation_generation == Some(self.operation.generation())
        {
            self.index.consumer_mutation_generation = None;
        }
        if std::thread::panicking() {
            self.index.consumer_binding_valid = false;
        }
        // Dropping the original operation revokes admission independently of
        // whichever index value now occupies the borrowed slot.
    }
}

pub(super) struct MarkerMutationAuthority {
    _consumer: Option<MutationPermit>,
    _native: Option<SearchProjectionPublishLease>,
}

impl SearchIndex {
    pub(super) fn require_marker_mutation(&self) -> Result<MarkerMutationAuthority> {
        let consumer = self.require_owned_mutation_or_unregistered()?;
        // The initializer holds runtime custody before its durable binding
        // exists. Reuse that custody rather than recursively locking its root.
        let consumer = consumer.or_else(|| self.borrow_consumer_mutation().ok());
        let native = if consumer.is_none() {
            self.path
                .as_deref()
                .map(SearchProjectionPublishLease::acquire)
                .transpose()?
        } else {
            None
        };
        Ok(MarkerMutationAuthority {
            _consumer: consumer,
            _native: native,
        })
    }

    pub(super) fn borrow_consumer_mutation(&self) -> Result<MutationPermit> {
        let generation = self
            .consumer_mutation_generation
            .ok_or_else(|| invalid("consumer mutation scope has ended"))?;
        MutationPermit::acquire(&self.consumer_owner, generation)
    }

    pub(super) fn mark_untracked_consumer_mutation(&mut self) -> Option<MutationPermit> {
        self.consumer_binding.as_ref()?;
        match self.borrow_consumer_mutation() {
            Ok(permit) => Some(permit),
            Err(_) => {
                self.consumer_binding_valid = false;
                None
            }
        }
    }

    pub(super) fn require_owned_mutation_or_unregistered(&self) -> Result<Option<MutationPermit>> {
        if !self.consumer_binding_valid {
            return Err(invalid(
                "registered projection requires its consumer mutation owner",
            ));
        }
        if self.consumer_binding.is_none() {
            return Ok(None);
        }
        self.borrow_consumer_mutation().map(Some)
    }

    #[cfg(feature = "vector-search")]
    pub(super) fn load_registered_rabitq_projection(&self) -> Result<()> {
        if self
            .documents
            .values()
            .all(|document| document.embedding.is_none())
        {
            self.invalidate_rabitq_projection();
            return Ok(());
        }
        let root = self
            .path
            .as_deref()
            .expect("persistent registered projection");
        for (generation, path) in super::rabitq_artifacts_descending(root)? {
            match super::RaBitQCandidateProjection::load_from_path_classified(
                &path,
                &self.documents,
                &self.rabitq_projection_identity(generation),
            ) {
                Ok(projection) => {
                    *self
                        .rabitq_projection
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) =
                        Some(std::sync::Arc::new(projection));
                    return Ok(());
                }
                Err(super::rabitq_projection::RaBitQCandidateProjectionLoadError::Resource(
                    error,
                )) => return Err(error),
                Err(_) => {}
            }
        }
        Err(HawDBError::StorageIntegrity(
            "missing or inconsistent registered vector projection".into(),
        ))
    }

    pub(super) fn validate_registered_artifacts(&mut self) -> Result<()> {
        let root = self
            .path
            .as_deref()
            .expect("persistent registered projection");
        let descriptor = super::read_search_segment_descriptor(root)?
            .ok_or_else(|| invalid("missing registered descriptor"))?;
        if !descriptor.matches_documents(&self.documents)
            || !descriptor.payload_artifact_is_available(root)?
        {
            return Err(invalid(
                "registered descriptor does not match its snapshot or payload",
            ));
        }
        self.segment_descriptor = Some(descriptor);
        self.load_lexical_projection()?;
        if self
            .lexical_projection
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
        {
            return Err(invalid(
                "missing or inconsistent registered lexical projection",
            ));
        }
        let reader = super::out_of_core::SearchOutOfCoreReader::open_with_config_and_analyzer(
            root,
            super::out_of_core::SearchOutOfCoreConfig::default(),
            self.analyzer_lexicon.clone(),
        )?;
        if reader.source_graph_commit_epoch() != self.source_graph_commit_epoch
            || reader.document_count() != self.documents.len()
        {
            return Err(invalid("registered generation does not match its snapshot"));
        }
        Ok(())
    }

    pub(super) fn require_unregistered_publication(&self) -> Result<()> {
        if self.consumer_binding.is_some() {
            return Err(invalid("registered projection requires its consumer owner"));
        }
        Ok(())
    }
}

fn canonical_uuid(raw: &str) -> Result<Uuid> {
    let uuid = raw.parse::<Uuid>().map_err(|_| invalid("invalid UUID"))?;
    if uuid.is_nil() || uuid.to_string() != raw {
        return Err(invalid("noncanonical UUID"));
    }
    Ok(uuid)
}
fn invalid(detail: &str) -> HawDBError {
    HawDBError::Storage(format!("projection consumer: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn binding() -> ConsumerBinding {
        ConsumerBinding {
            database_uuid: hawdb_core::generate_uuidv7().unwrap(),
            projection_uuid: hawdb_core::generate_uuidv7().unwrap(),
            consumer_id: "main".into(),
            registration_uuid: hawdb_core::generate_uuidv7().unwrap(),
            checkpoint_uuid: hawdb_core::generate_uuidv7().unwrap(),
        }
    }
    fn directory() -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "hawdb-owned-projection-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&root).unwrap();
        root
    }
    #[test]
    fn cleanup_reuses_consumer_authority_only_inside_owned_mutation() {
        let root = directory();
        let mut owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        assert!(!owner.index().projection_cleanup_report().retry_required);
        let artifact = root.join("search_rabitq.777.hawdb");
        fs::write(&artifact, b"unreferenced artifact").unwrap();
        let deferred = owner.index().retry_projection_cleanup(Default::default());
        assert!(deferred.retry_required);
        assert_eq!(deferred.delete_attempts, 0);
        assert!(artifact.exists());
        owner
            .apply(|index| {
                let cleaned = index.retry_projection_cleanup(Default::default());
                assert!(!cleaned.retry_required);
                assert!(!artifact.exists());
                Ok(())
            })
            .unwrap();
        assert!(owner.binding_is_valid());
        drop(owner);
        let owner = ConsumerProjection::open(&root).unwrap();
        assert!(!owner.index().projection_cleanup_report().retry_required);
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ordinary_mutation_and_infallible_setters_cannot_publish_an_owned_index() {
        let root = directory();
        let mut owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        assert!(owner
            .index
            .apply_projection_delta(super::super::SearchProjectionDelta::default())
            .is_err());
        owner.index.delete("missing");
        assert!(!owner.binding_is_valid());
        assert!(owner.checkpoint().is_err());
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn duplicate_and_malformed_snapshot_bindings_fail_closed() {
        let root = directory();
        let owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        drop(owner);
        let path = root.join(SEARCH_SNAPSHOT_FILE);
        let original = super::super::read_search_snapshot_text(&path).unwrap();
        let (body, _) = super::super::split_checksum(&original).unwrap();
        let line = body
            .lines()
            .find(|line| line.starts_with("projection_consumer_binding"))
            .unwrap();
        for mutated in [
            format!("{body}{line}\n"),
            body.replace(
                "projection_consumer_binding\t",
                "projection_consumer_binding\tbad-uuid\t",
            ),
            body.replace("source_graph_commit_epoch\t0\n", ""),
        ] {
            let text = format!(
                "{mutated}checksum\t{}\n",
                super::super::checksum_bytes(mutated.as_bytes())
            );
            fs::write(
                &path,
                super::super::encode_search_snapshot_text(&text).unwrap(),
            )
            .unwrap();
            assert!(ConsumerProjection::open(&root).is_err());
        }
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn ordinary_snapshot_bytes_and_query_results_remain_unchanged() {
        let root = directory();
        let ordinary = root.join("ordinary");
        let registered = root.join("registered");
        fs::create_dir(&registered).unwrap();
        let seed = |index: &mut SearchIndex| {
            index.upsert(super::super::SearchDocument {
                id: "document".into(),
                title: "searchable".into(),
                content: "searchable payload".repeat(300),
                embedding: None,
                metadata: Default::default(),
            })
        };
        let mut index = SearchIndex::open(&ordinary).unwrap();
        seed(&mut index).unwrap();
        index.checkpoint().unwrap();
        let before = fs::read(ordinary.join(SEARCH_SNAPSHOT_FILE)).unwrap();
        // A long second document record must not be treated as a bounded header.
        let mut index = SearchIndex::open(&ordinary).unwrap();
        index.checkpoint().unwrap();
        assert_eq!(
            before,
            fs::read(ordinary.join(SEARCH_SNAPSHOT_FILE)).unwrap()
        );
        index
            .apply_projection_delta(super::super::SearchProjectionDelta {
                source_graph_commit_epoch: Some(0),
                ..Default::default()
            })
            .unwrap();
        index.checkpoint().unwrap();
        let owner = ConsumerProjection::initialize(&registered, binding(), 0, seed).unwrap();
        #[cfg(feature = "full-text-search")]
        assert_eq!(
            index
                .search("searchable", None, super::super::SearchMode::Text, 10)
                .unwrap(),
            owner
                .index()
                .search("searchable", None, super::super::SearchMode::Text, 10)
                .unwrap()
        );
        #[cfg(not(feature = "full-text-search"))]
        for index in [&index, owner.index()] {
            assert!(matches!(
                index.try_search_with_options(
                    "searchable",
                    None,
                    super::super::SearchMode::Text,
                    super::super::SearchQueryOptions {
                        limit: 10,
                        offset: 0,
                        rank_window: None,
                        fusion_weights: super::super::SearchFusionWeights::default(),
                        metadata_filters: Default::default(),
                        policy_epoch: None,
                    }
                ),
                Err(HawDBError::CapabilityUnavailable {
                    capability: hawdb_core::RuntimeCapability::FullTextSearch
                })
            ));
        }
        drop(owner);
        drop(index);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn initializer_cannot_silently_publish_unrecoverable_analyzer_rules() {
        let root = directory();
        let result = ConsumerProjection::initialize(&root, binding(), 0, |index| {
            index.set_analyzer_lexicon(super::super::SearchAnalyzerLexicon::empty());
            Ok(())
        });
        assert!(result.is_err());
        assert!(!root.join(SEARCH_SNAPSHOT_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }
    #[cfg(feature = "vector-search")]
    #[test]
    fn registered_reopen_requires_its_completed_vector_artifact() {
        let root = directory();
        let owner = ConsumerProjection::initialize(&root, binding(), 0, |index| {
            index.upsert(super::super::SearchDocument {
                id: "vector".into(),
                title: String::new(),
                content: "vector document".into(),
                embedding: Some(vec![1.0, 0.5, 0.25, 0.0]),
                metadata: Default::default(),
            })
        })
        .unwrap();
        drop(owner);
        ConsumerProjection::open(&root).unwrap();
        let artifacts = super::super::rabitq_artifacts_descending(&root).unwrap();
        assert!(!artifacts.is_empty());
        for (_, artifact) in artifacts {
            fs::remove_file(artifact).unwrap();
        }
        assert!(matches!(
            ConsumerProjection::open(&root),
            Err(HawDBError::StorageIntegrity(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn preparation_excludes_mutations_before_any_side_effect() {
        let root = directory();
        let destination = root.with_extension("moved");
        let mut owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        let receipt = owner.receipt();
        let snapshot = fs::read(root.join(SEARCH_SNAPSHOT_FILE)).unwrap();
        let operation = owner.reserve_preparation().unwrap();
        assert!(matches!(
            owner.reserve_preparation(),
            Err(HawDBError::ProjectionBusy)
        ));
        let called = std::cell::Cell::new(false);
        assert!(matches!(
            owner.apply(|_| {
                called.set(true);
                Ok(())
            }),
            Err(HawDBError::ProjectionBusy)
        ));
        assert!(!called.get());
        assert!(matches!(
            owner.checkpoint(),
            Err(HawDBError::ProjectionBusy)
        ));
        assert!(matches!(
            owner.publish_directory(&destination),
            Err(HawDBError::ProjectionBusy)
        ));
        assert!(!destination.exists());
        assert_eq!(owner.binding(), &receipt.binding);
        assert_eq!(owner.receipt(), receipt);
        assert_eq!(fs::read(root.join(SEARCH_SNAPSHOT_FILE)).unwrap(), snapshot);
        assert!(!owner.has_pending_mutation());
        drop(operation);
        owner.apply(|_| Ok(())).unwrap();
        assert!(owner.has_pending_mutation());
        owner.checkpoint().unwrap();
        drop(owner.reserve_preparation().unwrap());
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preparation_retains_native_root_exclusion_after_owner_closes() {
        let root = directory();
        let mut owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        let operation = owner.reserve_preparation().unwrap();
        drop(owner);
        assert!(ConsumerProjection::open(&root).is_err());
        let deferred = SearchIndex {
            path: Some(root.clone()),
            ..SearchIndex::default()
        }
        .retry_projection_cleanup(Default::default());
        assert!(deferred.retry_required);
        assert_eq!(deferred.scanned_entries, 0);
        assert_eq!(deferred.delete_attempts, 0);
        std::thread::spawn(move || drop(operation)).join().unwrap();
        let owner = ConsumerProjection::open(&root).unwrap();
        assert_eq!(owner.receipt().source_epoch, 0);
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preparation_refuses_same_epoch_mutation_and_failed_callbacks_until_checkpoint() {
        let root = directory();
        let mut owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        owner
            .apply(|index| {
                index.upsert(crate::SearchDocument {
                    id: "same-epoch".into(),
                    title: String::new(),
                    content: "changed".into(),
                    embedding: None,
                    metadata: Default::default(),
                })
            })
            .unwrap();
        // Epoch-only freshness misses this actual resident mutation.
        assert!(
            !owner
                .index()
                .projection_freshness()
                .has_uncheckpointed_changes
        );
        assert!(owner.reserve_preparation().is_err());
        owner.checkpoint().unwrap();
        drop(owner.reserve_preparation().unwrap());
        assert!(owner
            .apply::<()>(|_| Err(HawDBError::Execution("refused callback".into())))
            .is_err());
        assert!(owner.reserve_preparation().is_err());
        owner.checkpoint().unwrap();
        drop(owner.reserve_preparation().unwrap());
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preparation_rejection_and_unwind_release_the_operation_slot() {
        let root = directory();
        let mut owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _operation = owner.reserve_preparation().unwrap();
            panic!("cancel preparation");
        }));
        assert!(failed.is_err());
        assert!(owner.binding_is_valid());
        drop(owner.reserve_preparation().unwrap());
        // Force the real snapshot publication to fail after checkpoint_uuid
        // changes, while preserving the previous complete snapshot for repair.
        let snapshot = root.join(SEARCH_SNAPSHOT_FILE);
        let previous = root.join("previous-snapshot");
        fs::rename(&snapshot, &previous).unwrap();
        fs::create_dir(&snapshot).unwrap();
        assert!(owner.checkpoint().is_err());
        assert_eq!(owner.index().source_graph_commit_epoch, Some(0));
        assert!(owner.has_pending_mutation());
        assert!(owner.reserve_preparation().is_err());
        owner.require_operation_available().unwrap();
        fs::remove_dir(&snapshot).unwrap();
        fs::rename(&previous, &snapshot).unwrap();
        // Restoring the old file cannot certify this in-memory checkpoint.
        assert!(owner.reserve_preparation().is_err());
        owner.checkpoint().unwrap();
        drop(owner.reserve_preparation().unwrap());
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn moved_index_cannot_reuse_an_ended_foreign_mutation_scope() {
        let root_a = directory();
        let root_b = directory();
        let initialize = |id: &str| crate::SearchDocument {
            id: id.into(),
            title: String::new(),
            content: id.into(),
            embedding: None,
            metadata: Default::default(),
        };
        let mut owner_a = ConsumerProjection::initialize(&root_a, binding(), 0, |index| {
            index.upsert(initialize("memory:a"))
        })
        .unwrap();
        let mut owner_b = ConsumerProjection::initialize(&root_b, binding(), 0, |index| {
            index.upsert(initialize("memory:b"))
        })
        .unwrap();
        let reached_cleanup = std::cell::Cell::new(false);
        let result = owner_a.apply(|index_a| {
            let moved = owner_b.apply(|index_b| {
                std::mem::swap(index_a, index_b);
                Ok(())
            });
            assert!(
                moved.is_err(),
                "foreign index must invalidate B's owner association"
            );
            // The B callback ended and its owner closes. The moved index must
            // not carry a live B authority into A's still-running callback.
            drop(owner_b);
            let peer_b = ConsumerProjection::open(&root_b).unwrap();
            let report = index_a.retry_projection_cleanup(Default::default());
            assert_eq!(
                report.scanned_entries, 0,
                "foreign ended scope bypassed B's current native owner: {report:?}"
            );
            assert_eq!(report.delete_attempts, 0);
            assert!(report.retry_required);
            reached_cleanup.set(true);
            drop(peer_b);
            Ok(())
        });
        assert!(
            reached_cleanup.get(),
            "rejection must not hide the cleanup regression"
        );
        assert!(
            result.is_err(),
            "foreign index must invalidate A's owner association"
        );
        assert!(!owner_a.binding_is_valid());
        assert!(owner_a.checkpoint().is_err());
        drop(owner_a);
        fs::remove_dir_all(root_a).unwrap();
        fs::remove_dir_all(root_b).unwrap();
    }
    #[test]
    fn pre_registration_handle_cannot_write_registered_root_markers() {
        let root = directory();
        let stale = SearchIndex::open(&root).unwrap();
        stale.checkpoint().unwrap();
        let owner = ConsumerProjection::initialize(&root, binding(), 0, |_| Ok(())).unwrap();
        assert!(stale.mark_full_reindex_needed("foreign marker").is_err());
        assert!(stale.mark_metadata_repair_needed("foreign repair").is_err());
        assert!(!owner.index().full_reindex_needed());
        assert!(!owner.index().metadata_repair_needed());
        drop(owner);
        // Even without a live native owner, the durable binding refuses stale I/O.
        assert!(stale.mark_full_reindex_needed("after owner close").is_err());
        assert!(stale
            .mark_metadata_repair_needed("after owner close")
            .is_err());
        assert!(!root.join(super::super::FULL_REINDEX_MARKER).exists());
        assert!(!root.join(super::super::METADATA_REPAIR_MARKER).exists());
        drop(stale);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn initializer_and_owned_markers_reuse_custody_without_recursive_locking() {
        let root = directory();
        let ordinary = SearchIndex::open(&root).unwrap();
        ordinary.mark_full_reindex_needed("ordinary").unwrap();
        assert!(ordinary.full_reindex_needed());
        drop(ordinary);
        let mut owner = ConsumerProjection::initialize(&root, binding(), 0, |index| {
            index.mark_metadata_repair_needed("initializer")
        })
        .unwrap();
        assert!(owner.index().metadata_repair_needed());
        owner
            .apply(|index| {
                index.clear_marker(super::super::METADATA_REPAIR_MARKER)?;
                index.mark_full_reindex_needed("owned")
            })
            .unwrap();
        assert!(!owner.index().metadata_repair_needed());
        assert!(owner.index().full_reindex_needed());
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn directory_sync_failure_cannot_reuse_the_vacated_path_as_native_authority() {
        let parent = directory();
        let source = parent.join("source");
        let destination = parent.join("destination");
        fs::create_dir(&source).unwrap();
        let mut owner = ConsumerProjection::initialize(&source, binding(), 0, |_| Ok(())).unwrap();
        FAIL_PUBLISHED_PARENT_SYNC.with(|failure| failure.set(true));
        assert!(owner.publish_directory(&destination).is_err());
        assert!(destination.is_dir());
        assert!(!source.exists());
        fs::create_dir(&source).unwrap();
        let peer = ConsumerProjection::initialize(&source, binding(), 0, |_| Ok(())).unwrap();
        let mut observed = None;
        let attempted = owner.apply(|index| {
            let marker = index.mark_full_reindex_needed("old directory owner");
            let cleanup = index.retry_projection_cleanup(Default::default());
            observed = Some((
                marker.is_ok(),
                cleanup.scanned_entries,
                cleanup.delete_attempts,
            ));
            Ok(())
        });
        eprintln!(
            "old-owner attempt={attempted:?}; observed(marker_written,scans,deletes)={observed:?}"
        );
        assert!(
            attempted.is_err(),
            "uncertain directory publication must revoke mutation: {observed:?}"
        );
        assert_eq!(owner.index().path.as_deref(), Some(destination.as_path()));
        assert!(!owner.binding_is_valid());
        assert!(!peer.index().full_reindex_needed());
        assert!(owner.checkpoint().is_err());
        assert!(owner.reserve_preparation().is_err());
        let retry_destination = parent.join("retry");
        assert!(owner.publish_directory(&retry_destination).is_err());
        assert!(!retry_destination.exists());
        // The failed barrier must neither release native ownership nor delete
        // the complete checkpoint already moved to the destination.
        assert!(ConsumerProjection::open(&destination).is_err());
        let original_receipt = owner.receipt();
        drop(peer);
        drop(owner);
        let recovered = ConsumerProjection::open(&destination).unwrap();
        assert_eq!(recovered.receipt(), original_receipt);
        drop(recovered);
        fs::remove_dir_all(parent).unwrap();
    }
    #[test]
    fn successful_directory_move_keeps_mutations_on_the_moved_native_root() {
        let parent = directory();
        let source = parent.join("source");
        let destination = parent.join("destination");
        fs::create_dir(&source).unwrap();
        let mut owner = ConsumerProjection::initialize(&source, binding(), 0, |_| Ok(())).unwrap();
        let receipt = owner.receipt();
        owner.publish_directory(&destination).unwrap();
        assert!(owner.binding_is_valid());
        assert!(!owner.has_pending_mutation());
        assert_eq!(owner.receipt(), receipt);
        assert!(!source.exists());
        fs::create_dir(&source).unwrap();
        let peer = ConsumerProjection::initialize(&source, binding(), 0, |_| Ok(())).unwrap();
        owner
            .apply(|index| index.mark_full_reindex_needed("moved root"))
            .unwrap();
        assert!(owner.index().full_reindex_needed());
        assert!(!peer.index().full_reindex_needed());
        let completed = owner.checkpoint().unwrap();
        drop(peer);
        drop(owner);
        let reopened = ConsumerProjection::open(&destination).unwrap();
        assert_eq!(reopened.receipt(), completed);
        assert!(reopened.index().full_reindex_needed());
        drop(reopened);
        fs::remove_dir_all(parent).unwrap();
    }
}
