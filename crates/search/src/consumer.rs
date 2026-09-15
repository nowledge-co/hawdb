//! Internal projection ownership used by the embedded database facade.
use super::{SearchIndex, SEARCH_SNAPSHOT_FILE};
use crate::error::{Result, SkeinError};
use crate::out_of_core::SearchProjectionPublishLease;
use skein_core::Uuid;
use skein_integrity::IntegrityHasher;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

mod control;
pub(crate) use control::require_unregistered_directory;

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
    _lease: SearchProjectionPublishLease,
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
        let lease = SearchProjectionPublishLease::acquire_for_consumer(root)?;
        let mut index = SearchIndex {
            path: Some(root.to_path_buf()),
            ..SearchIndex::default()
        };
        initialize(&mut index)?;
        if index.path.as_deref() != Some(root)
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
            _lease: lease,
        };
        owner.checkpoint()?;
        Ok(owner)
    }

    pub fn open(root: &Path) -> Result<Self> {
        let lease = SearchProjectionPublishLease::acquire_for_consumer(root)?;
        let index = SearchIndex::open_under_lease(root, true)?;
        let owner = Self {
            index,
            _lease: lease,
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
        self.index.consumer_owned_mutation = true;
        let scope = MutationScope(&mut self.index);
        apply(scope.0)
    }

    pub fn binding_is_valid(&self) -> bool {
        self.index.consumer_binding_valid
    }

    pub fn checkpoint(&mut self) -> Result<CheckpointReceipt> {
        if !self.index.consumer_binding_valid {
            return Err(invalid("untracked projection mutation"));
        }
        self.index
            .consumer_binding
            .as_mut()
            .expect("bound consumer")
            .checkpoint_uuid = skein_core::uuidv7::generate_uuidv7()?;
        self.index.checkpoint_with_lease(true)?;
        Ok(self.receipt())
    }

    pub fn publish_directory(&mut self, destination: &Path) -> Result<()> {
        fs::create_dir(destination)?;
        let source = self.index.path.as_ref().expect("persistent consumer");
        if let Err(error) = fs::rename(source, destination) {
            if let Err(cleanup) = fs::remove_dir(destination) {
                return Err(invalid(&format!("projection directory publication failed: {error}; destination reservation cleanup failed: {cleanup}")));
            }
            return Err(error.into());
        }
        skein_storage::durability::sync_parent_directory(destination)?;
        // Readers hold paths into their generation. Reload those readers while
        // retaining the lease, receipt, and initializer's runtime configuration.
        self.index.path = Some(destination.to_path_buf());
        self.index.validate_registered_artifacts()?;
        #[cfg(feature = "vector-search")]
        self.index.load_registered_rabitq_projection()?;
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

struct MutationScope<'a>(&'a mut SearchIndex);
impl Drop for MutationScope<'_> {
    fn drop(&mut self) {
        self.0.consumer_owned_mutation = false;
        if std::thread::panicking() {
            self.0.consumer_binding_valid = false;
        }
    }
}

impl SearchIndex {
    pub(super) fn mark_untracked_consumer_mutation(&mut self) {
        if self.consumer_binding.is_some() && !self.consumer_owned_mutation {
            self.consumer_binding_valid = false;
        }
    }

    pub(super) fn require_owned_mutation_or_unregistered(&self) -> Result<()> {
        if !self.consumer_binding_valid
            || (self.consumer_binding.is_some() && !self.consumer_owned_mutation)
        {
            return Err(invalid(
                "registered projection requires its consumer mutation owner",
            ));
        }
        Ok(())
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
        for (generation, path) in super::rabitq_artifacts_descending(root) {
            if let Ok(projection) = super::RaBitQCandidateProjection::load_from_path_classified(
                &path,
                &self.documents,
                &self.rabitq_projection_identity(generation),
            ) {
                *self
                    .rabitq_projection
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) =
                    Some(std::sync::Arc::new(projection));
                return Ok(());
            }
        }
        Err(SkeinError::StorageIntegrity(
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
            || !descriptor.payload_artifact_is_available(root)
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
fn invalid(detail: &str) -> SkeinError {
    SkeinError::Storage(format!("projection consumer: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn binding() -> ConsumerBinding {
        ConsumerBinding {
            database_uuid: skein_core::generate_uuidv7().unwrap(),
            projection_uuid: skein_core::generate_uuidv7().unwrap(),
            consumer_id: "main".into(),
            registration_uuid: skein_core::generate_uuidv7().unwrap(),
            checkpoint_uuid: skein_core::generate_uuidv7().unwrap(),
        }
    }
    fn directory() -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "skein-owned-projection-{}",
            skein_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&root).unwrap();
        root
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
            index.search("searchable", None, super::super::SearchMode::Text, 10),
            owner
                .index()
                .search("searchable", None, super::super::SearchMode::Text, 10)
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
                Err(SkeinError::CapabilityUnavailable {
                    capability: skein_core::RuntimeCapability::FullTextSearch
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
        let artifacts = super::super::rabitq_artifacts_descending(&root);
        assert!(!artifacts.is_empty());
        for (_, artifact) in artifacts {
            fs::remove_file(artifact).unwrap();
        }
        assert!(matches!(
            ConsumerProjection::open(&root),
            Err(SkeinError::StorageIntegrity(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
