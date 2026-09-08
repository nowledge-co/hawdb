use super::{
    durability, manifest, relational_row_page_artifact_file,
    relational_row_page_manifest_generation_file, relational_row_page_root_descriptor_file,
    relational_row_page_root_key_file, root, RelationalRowPagePublicationConfig,
    RelationalRowPagePublicationError, RelationalRowPageRootDescriptor,
    RelationalRowPageRootManifest, RelationalRowPageTableRoot, RELATIONAL_ROW_PAGE_MANIFEST_FILE,
};
use crate::relational::row_page::{VerifiedRowPage, VerifiedRowPageMetadata};
use crate::relational::{
    ordered_key::encode_ordered_relational_key, ImmutableRelationalRowPage, RelationalKey,
    RelationalOverflowRootBinding, RelationalOverflowRootReader, RelationalRowPageView,
};
use crate::{
    ContentDigest, ManifestGeneration, RepresentationKind, SegmentCache, SegmentCacheError,
    SegmentCacheKey, StoreId,
};
use skein_integrity::integrity_digest;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RelationalRowPageRootReader {
    directory: PathBuf,
    pub(super) manifest: Arc<RelationalRowPageRootManifest>,
    config: RelationalRowPagePublicationConfig,
}

pub(crate) struct RelationalRowPageSlotRead {
    pub page: VerifiedRowPage,
    pub cache_hit: bool,
    pub cache_miss: bool,
    pub cache_admission_rejected: bool,
}

impl RelationalRowPageRootReader {
    pub fn open_latest(
        directory: &Path,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Option<Self>, RelationalRowPagePublicationError> {
        let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
        manifest::read_manifest_if_exists(&path, config)?
            .map(|manifest| Self::from_manifest(directory, manifest, config))
            .transpose()
    }

    pub fn open_generation(
        directory: &Path,
        generation: u64,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        let path = directory.join(relational_row_page_manifest_generation_file(generation));
        let manifest = manifest::read_manifest(&path, config)?;
        if manifest.generation != generation {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "generation manifest {generation} identifies generation {}",
                manifest.generation
            )));
        }
        Self::from_manifest(directory, manifest, config)
    }

    pub fn open_bound_generation(
        directory: &Path,
        binding: super::RelationalRowPageGenerationArtifacts,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        let path = directory.join(relational_row_page_manifest_generation_file(
            binding.generation,
        ));
        let manifest = manifest::read_bound_manifest(&path, config, binding.manifest_artifact)?;
        if manifest.generation != binding.generation
            || manifest.source_commit_epoch != binding.source_commit_epoch
            || manifest.root_set_digest != binding.root_set_digest
        {
            return Err(RelationalRowPagePublicationError::Corrupt(
                "row-page generation identity does not match its canonical binding".to_string(),
            ));
        }
        Self::from_manifest(directory, manifest, config)
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalRowPageRootManifest,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        validate_artifact_length(
            &directory.join(relational_row_page_artifact_file(manifest.generation)),
            manifest.page_artifact.encoded_len,
            "row-page artifact",
        )?;
        validate_artifact_length(
            &directory.join(relational_row_page_root_descriptor_file(
                manifest.generation,
            )),
            manifest.root_descriptor_artifact.encoded_len,
            "row-page root descriptor artifact",
        )?;
        validate_artifact_length(
            &directory.join(relational_row_page_root_key_file(manifest.generation)),
            manifest.root_key_artifact.encoded_len,
            "row-page root key artifact",
        )?;
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest: Arc::new(manifest),
            config,
        })
    }

    pub fn manifest(&self) -> &RelationalRowPageRootManifest {
        &self.manifest
    }

    /// Verifies all referenced slots and their physical-generation accounting.
    /// Keeps one page and one counter per manifest inventory entry in memory;
    /// this explicit full scrub is not part of opening a reader.
    pub fn scrub_physical_pages(&self) -> Result<(), RelationalRowPagePublicationError> {
        let inventory = &self.manifest.physical_generations;
        let mut live_counts = vec![0u64; inventory.len()];
        for entry in inventory {
            validate_artifact_length(
                &self
                    .directory
                    .join(relational_row_page_artifact_file(entry.generation)),
                entry.allocated_pages * self.manifest.page_bytes,
                "row-page physical generation",
            )?;
        }
        for table in &self.manifest.tables {
            self.visit_table_pages(&table.table, |descriptor| {
                let index = inventory
                    .binary_search_by_key(&descriptor.physical_generation, |entry| entry.generation)
                    .map_err(|_| {
                        RelationalRowPagePublicationError::Corrupt(
                            "row-page descriptor references an unaccounted generation".to_string(),
                        )
                    })?;
                live_counts[index] += 1;
                self.read_page(descriptor)?;
                Ok(())
            })?;
        }
        for (entry, actual) in inventory.iter().zip(live_counts) {
            if entry.live_pages != actual {
                return Err(RelationalRowPagePublicationError::Corrupt(format!(
                    "row-page physical generation {} has {actual} live pages, expected {}",
                    entry.generation, entry.live_pages,
                )));
            }
        }
        Ok(())
    }

    pub(crate) const fn publication_config(&self) -> RelationalRowPagePublicationConfig {
        self.config
    }

    pub fn overflow_root_binding(&self) -> Option<RelationalOverflowRootBinding> {
        self.manifest.overflow_root
    }

    pub fn validate_overflow_root(
        &self,
        overflow_root: &RelationalOverflowRootReader,
    ) -> Result<(), RelationalRowPagePublicationError> {
        let Some(expected) = self.manifest.overflow_root else {
            return Ok(());
        };
        let actual = overflow_root.manifest().binding();
        if actual != expected {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "row-page overflow binding {expected:?} differs from selected root {actual:?}"
            )));
        }
        Ok(())
    }

    pub fn read_table_page_descriptor(
        &self,
        table: &str,
        ordinal: u64,
    ) -> Result<RelationalRowPageRootDescriptor, RelationalRowPagePublicationError> {
        let table_root = self.table_root(table)?;
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open row-page root descriptor artifact"))?;
        let mut keys =
            File::open(self.key_path()).map_err(durability("open row-page root key artifact"))?;
        self.read_table_page_descriptor_from(
            &mut descriptors,
            &mut keys,
            table,
            table_root,
            ordinal,
        )
    }

    pub fn find_table_page_descriptor(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<Option<RelationalRowPageRootDescriptor>, RelationalRowPagePublicationError> {
        self.find_table_page_descriptor_accounted(table, primary_key)
            .map(|(descriptor, _)| descriptor.map(|(_, descriptor)| descriptor))
    }

    pub(crate) fn find_table_page_descriptor_accounted(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<
        (Option<(u64, RelationalRowPageRootDescriptor)>, usize),
        RelationalRowPagePublicationError,
    > {
        let encoded_key = encode_ordered_relational_key(primary_key).map_err(|error| {
            RelationalRowPagePublicationError::Admission(format!(
                "row-page lookup key cannot be encoded: {error}"
            ))
        })?;
        self.find_table_page_descriptor_accounted_encoded(table, &encoded_key)
    }

    pub(crate) fn find_table_page_descriptor_accounted_encoded(
        &self,
        table: &str,
        encoded_key: &[u8],
    ) -> Result<
        (Option<(u64, RelationalRowPageRootDescriptor)>, usize),
        RelationalRowPagePublicationError,
    > {
        if encoded_key.len() > self.config.page_limits.max_key_bytes.get() {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "row-page lookup key contains {} bytes, exceeding limit {}",
                encoded_key.len(),
                self.config.page_limits.max_key_bytes
            )));
        }
        let table_root = self.table_root(table)?;
        if table_root.page_count == 0 {
            return Ok((None, 0));
        }
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open row-page root descriptor artifact"))?;
        let mut keys =
            File::open(self.key_path()).map_err(durability("open row-page root key artifact"))?;

        let mut lower = 0u64;
        let mut upper = table_root.page_count;
        let mut descriptor_reads = 0usize;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let descriptor = self.read_table_page_descriptor_from(
                &mut descriptors,
                &mut keys,
                table,
                table_root,
                middle,
            )?;
            descriptor_reads = descriptor_reads.checked_add(1).ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "row-page descriptor read counter overflow".to_string(),
                )
            })?;
            if descriptor.upper_bound.as_slice() < encoded_key {
                lower = middle.checked_add(1).ok_or_else(|| {
                    RelationalRowPagePublicationError::Corrupt(
                        "row-page descriptor search overflow".to_string(),
                    )
                })?;
            } else {
                upper = middle;
            }
        }
        let ordinal = lower.min(table_root.page_count - 1);
        let descriptor = self.read_table_page_descriptor_from(
            &mut descriptors,
            &mut keys,
            table,
            table_root,
            ordinal,
        )?;
        descriptor_reads = descriptor_reads.checked_add(1).ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row-page descriptor read counter overflow".to_string(),
            )
        })?;
        Ok((Some((ordinal, descriptor)), descriptor_reads))
    }

    pub fn read_page(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
    ) -> Result<ImmutableRelationalRowPage, RelationalRowPagePublicationError> {
        let slot = self.read_page_slot_bytes(descriptor)?;
        let view = self.validate_page_slot(descriptor, &slot)?;
        ImmutableRelationalRowPage::decode_view(view).map_err(Into::into)
    }

    pub(crate) fn read_page_slot_accounted(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        cache: &SegmentCache,
        store_id: StoreId,
    ) -> Result<RelationalRowPageSlotRead, RelationalRowPagePublicationError> {
        let cache_key = SegmentCacheKey {
            store_id,
            manifest_generation: ManifestGeneration(descriptor.physical_generation),
            segment_id: descriptor.physical_slot,
            content_digest: ContentDigest(descriptor.slot_integrity.slot_crc32c as u64),
            representation: RepresentationKind::RelationalRowPageSlot,
        };
        let verification_tag = *descriptor.slot_integrity.slot_sha256.as_bytes();
        if let Some(lease) = cache.get_verified(&cache_key, verification_tag) {
            let bytes = lease.into_arc();
            let metadata = self.validate_verified_page(descriptor, &bytes)?;
            return Ok(RelationalRowPageSlotRead {
                page: VerifiedRowPage::new(bytes, metadata),
                cache_hit: true,
                cache_miss: false,
                cache_admission_rejected: false,
            });
        }
        let slot = self.read_page_slot_bytes(descriptor)?;
        let view = self.validate_page_slot(descriptor, &slot)?;
        let metadata = VerifiedRowPageMetadata::from_view(&view);
        let bytes: Arc<[u8]> = Arc::from(&slot[..view.encoded_len()]);
        match cache.insert_verified(cache_key, verification_tag, Arc::clone(&bytes)) {
            Ok(lease) => Ok(RelationalRowPageSlotRead {
                page: VerifiedRowPage::new(lease.into_arc(), metadata),
                cache_hit: false,
                cache_miss: true,
                cache_admission_rejected: false,
            }),
            Err(SegmentCacheError::EntryTooLarge { .. })
            | Err(SegmentCacheError::PinnedCapacity { .. }) => Ok(RelationalRowPageSlotRead {
                page: VerifiedRowPage::new(bytes, metadata),
                cache_hit: false,
                cache_miss: true,
                cache_admission_rejected: true,
            }),
            Err(error) => Err(RelationalRowPagePublicationError::Corrupt(format!(
                "row-page cache rejected immutable slot identity: {error}"
            ))),
        }
    }

    fn read_page_slot_bytes(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
    ) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
        let page_bytes = self.config.page_limits.max_page_bytes.get() as u64;
        let offset = descriptor
            .physical_slot
            .checked_mul(page_bytes)
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Corrupt(
                    "row-page physical offset overflow".to_string(),
                )
            })?;
        let mut artifact = File::open(self.directory.join(relational_row_page_artifact_file(
            descriptor.physical_generation,
        )))
        .map_err(durability("open row-page generation artifact"))?;
        artifact
            .seek(SeekFrom::Start(offset))
            .map_err(durability("seek row-page generation slot"))?;
        let mut slot = vec![0u8; self.config.page_limits.max_page_bytes.get()];
        artifact
            .read_exact(&mut slot)
            .map_err(durability("read row-page generation slot"))?;
        Ok(slot)
    }

    fn validate_page_slot<'a>(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        slot: &'a [u8],
    ) -> Result<RelationalRowPageView<'a>, RelationalRowPagePublicationError> {
        let digest = integrity_digest(slot);
        if digest.crc32c.get() != descriptor.slot_integrity.slot_crc32c
            || digest.sha256 != descriptor.slot_integrity.slot_sha256
        {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "row-page {} slot checksum mismatch",
                descriptor.logical_page_id.get()
            )));
        }
        let view = RelationalRowPageView::open_slot(slot, self.config.page_limits)?;
        self.validate_page_identity(descriptor, &view)?;
        Ok(view)
    }

    fn validate_verified_page(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        encoded: &[u8],
    ) -> Result<VerifiedRowPageMetadata, RelationalRowPagePublicationError> {
        let view = RelationalRowPageView::open_verified(encoded, self.config.page_limits)?;
        self.validate_page_identity(descriptor, &view)?;
        Ok(VerifiedRowPageMetadata::from_view(&view))
    }

    fn validate_page_identity(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        view: &RelationalRowPageView<'_>,
    ) -> Result<(), RelationalRowPagePublicationError> {
        if view.page_id() != descriptor.logical_page_id
            || view.generation() != descriptor.physical_generation
            || view.source_commit_epoch() != descriptor.source_commit_epoch
            || view.row_count() != descriptor.row_count as usize
            || view.encoded_len() != descriptor.slot_integrity.encoded_len as usize
            || view.lower_bound_bytes() != descriptor.lower_bound
            || view.upper_bound_bytes() != descriptor.upper_bound
        {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "row-page {} content does not match its root descriptor",
                descriptor.logical_page_id.get()
            )));
        }
        Ok(())
    }

    pub fn visit_table_pages<F>(
        &self,
        table: &str,
        mut visitor: F,
    ) -> Result<(), RelationalRowPagePublicationError>
    where
        F: FnMut(&RelationalRowPageRootDescriptor) -> Result<(), RelationalRowPagePublicationError>,
    {
        let table_root = self.table_root(table)?;
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open row-page root descriptor artifact"))?;
        let mut keys =
            File::open(self.key_path()).map_err(durability("open row-page root key artifact"))?;
        let mut previous_upper: Option<Vec<u8>> = None;
        for offset in 0..table_root.page_count {
            let ordinal = table_root
                .first_descriptor
                .checked_add(offset)
                .ok_or_else(|| {
                    RelationalRowPagePublicationError::Corrupt(
                        "row-page descriptor ordinal overflow".to_string(),
                    )
                })?;
            let descriptor = root::read_descriptor(
                &mut descriptors,
                &mut keys,
                ordinal,
                &self.manifest,
                self.config,
            )?;
            if previous_upper
                .as_ref()
                .is_some_and(|upper| upper.as_slice() >= descriptor.lower_bound.as_slice())
            {
                return Err(RelationalRowPagePublicationError::Corrupt(format!(
                    "table {table} row-page bounds overlap or are unordered"
                )));
            }
            previous_upper = Some(descriptor.upper_bound.clone());
            visitor(&descriptor)?;
        }
        Ok(())
    }

    pub fn table_root(
        &self,
        table: &str,
    ) -> Result<&RelationalRowPageTableRoot, RelationalRowPagePublicationError> {
        self.manifest
            .tables
            .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
            .map(|index| &self.manifest.tables[index])
            .map_err(|_| RelationalRowPagePublicationError::MissingTable(table.to_string()))
    }

    fn descriptor_path(&self) -> PathBuf {
        self.directory
            .join(relational_row_page_root_descriptor_file(
                self.manifest.generation,
            ))
    }

    fn key_path(&self) -> PathBuf {
        self.directory
            .join(relational_row_page_root_key_file(self.manifest.generation))
    }

    fn read_table_page_descriptor_from(
        &self,
        descriptors: &mut File,
        keys: &mut File,
        table: &str,
        table_root: &RelationalRowPageTableRoot,
        ordinal: u64,
    ) -> Result<RelationalRowPageRootDescriptor, RelationalRowPagePublicationError> {
        if ordinal >= table_root.page_count {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "row-page ordinal {ordinal} exceeds table {table} page count {}",
                table_root.page_count
            )));
        }
        let descriptor_ordinal = table_root
            .first_descriptor
            .checked_add(ordinal)
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Corrupt(
                    "row-page descriptor ordinal overflow".to_string(),
                )
            })?;
        let descriptor = root::read_descriptor(
            descriptors,
            keys,
            descriptor_ordinal,
            &self.manifest,
            self.config,
        )?;
        if descriptor.logical_page_id.get() >= table_root.next_page_id.get() {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "table {table} descriptor page id {} is not below next page id {}",
                descriptor.logical_page_id.get(),
                table_root.next_page_id
            )));
        }
        Ok(descriptor)
    }
}

fn validate_artifact_length(
    path: &Path,
    expected: u64,
    context: &str,
) -> Result<(), RelationalRowPagePublicationError> {
    let actual = fs::metadata(path)
        .map_err(durability("read row-page artifact metadata"))?
        .len();
    if actual != expected {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "{context} contains {actual} bytes, expected {expected}"
        )));
    }
    Ok(())
}
