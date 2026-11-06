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

use super::{
    durability, manifest, relational_row_page_artifact_file,
    relational_row_page_manifest_generation_file, relational_row_page_root_descriptor_file,
    relational_row_page_root_key_file, root, RelationalRowPagePublicationConfig,
    RelationalRowPagePublicationError, RelationalRowPageRootDescriptor,
    RelationalRowPageRootManifest, RelationalRowPageTableRoot, RELATIONAL_ROW_PAGE_MANIFEST_FILE,
};
use crate::cache::{
    ContentDigest, ManifestGeneration, RepresentationKind, SegmentCache, SegmentCacheError,
    SegmentCacheKey, StoreId,
};
use crate::file_io::{self as fs, File};
use crate::relational::row_page::{VerifiedRowPage, VerifiedRowPageMetadata};
use crate::relational::{
    ordered_key::encode_ordered_relational_key, ImmutableRelationalRowPage, RelationalKey,
    RelationalOverflowRootBinding, RelationalOverflowRootReader, RelationalRowPageView,
};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RelationalRowPageRootReader {
    directory: PathBuf,
    pub(super) manifest: Arc<RelationalRowPageRootManifest>,
    config: RelationalRowPagePublicationConfig,
}

#[derive(Debug)]
pub(crate) struct RelationalRowPageSlotRead {
    pub page: VerifiedRowPage,
    pub cache_hit: bool,
    pub cache_miss: bool,
    pub cache_admission_rejected: bool,
}

pub(crate) struct CheckpointRootDescriptorRead {
    pub(crate) descriptor: RelationalRowPageRootDescriptor,
    // The descriptor's two Vec backings die before their retained permits.
    _memory: crate::background::CheckpointAllocationOwner,
}

impl std::ops::Deref for CheckpointRootDescriptorRead {
    type Target = RelationalRowPageRootDescriptor;
    fn deref(&self) -> &Self::Target {
        &self.descriptor
    }
}

impl RelationalRowPageRootReader {
    pub(super) fn open_latest_with_work_context(
        directory: &Path,
        config: RelationalRowPagePublicationConfig,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<Option<Self>, RelationalRowPagePublicationError> {
        let Some(work) = work else {
            return Self::open_latest(directory, config);
        };
        let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
        manifest::read_manifest_if_exists_with_work_context(&path, config, Some(work))?
            .map(|manifest| Self::from_manifest_inner(directory, manifest, config, Some(work)))
            .transpose()
    }

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
        Self::open_generation_with_work_context(directory, generation, config, None)
    }

    pub(crate) fn open_generation_with_work_context(
        directory: &Path,
        generation: u64,
        config: RelationalRowPagePublicationConfig,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        let path = directory.join(relational_row_page_manifest_generation_file(generation));
        let manifest = manifest::read_manifest_with_work_context(&path, config, work)?;
        if manifest.generation != generation {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "generation manifest {generation} identifies generation {}",
                manifest.generation
            )));
        }
        Self::from_manifest_inner(directory, manifest, config, work)
    }

    pub fn open_bound_generation(
        directory: &Path,
        binding: super::RelationalRowPageGenerationArtifacts,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        Self::open_bound_generation_with_work_context(directory, binding, config, None)
    }

    pub(crate) fn open_bound_generation_with_work_context(
        directory: &Path,
        binding: super::RelationalRowPageGenerationArtifacts,
        config: RelationalRowPagePublicationConfig,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        let path = directory.join(relational_row_page_manifest_generation_file(
            binding.generation,
        ));
        let manifest = manifest::read_bound_manifest_with_work_context(
            &path,
            config,
            binding.manifest_artifact,
            work,
        )?;
        if manifest.generation != binding.generation
            || manifest.source_commit_epoch != binding.source_commit_epoch
            || manifest.root_set_digest != binding.root_set_digest
        {
            return Err(RelationalRowPagePublicationError::Corrupt(
                "row-page generation identity does not match its canonical binding".to_string(),
            ));
        }
        Self::from_manifest_inner(directory, manifest, config, work)
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalRowPageRootManifest,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        Self::from_manifest_inner(directory, manifest, config, None)
    }

    fn from_manifest_inner(
        directory: &Path,
        manifest: RelationalRowPageRootManifest,
        config: RelationalRowPagePublicationConfig,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<Self, RelationalRowPagePublicationError> {
        super::publisher::checkpoint::io(work, || {
            validate_artifact_length(
                &directory.join(relational_row_page_artifact_file(manifest.generation)),
                manifest.page_artifact.encoded_len,
                "row-page artifact",
            )
        })?;
        super::publisher::checkpoint::io(work, || {
            validate_artifact_length(
                &directory.join(relational_row_page_root_descriptor_file(
                    manifest.generation,
                )),
                manifest.root_descriptor_artifact.encoded_len,
                "row-page root descriptor artifact",
            )
        })?;
        super::publisher::checkpoint::io(work, || {
            validate_artifact_length(
                &directory.join(relational_row_page_root_key_file(manifest.generation)),
                manifest.root_key_artifact.encoded_len,
                "row-page root key artifact",
            )
        })?;
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
                entry.allocated_bytes,
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
        self.find_table_page_descriptor_accounted_encoded_with_work_context(
            table,
            encoded_key,
            None,
        )
        .map(|(found, reads)| {
            (
                found.map(|(ordinal, owned)| {
                    debug_assert!(owned._memory.is_empty());
                    (ordinal, owned.descriptor)
                }),
                reads,
            )
        })
    }

    pub(crate) fn find_table_page_descriptor_accounted_encoded_with_work_context(
        &self,
        table: &str,
        encoded_key: &[u8],
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<
        (Option<(u64, CheckpointRootDescriptorRead)>, usize),
        RelationalRowPagePublicationError,
    > {
        if let Some(work) = work {
            work.checkpoint().map_err(root::checkpoint::work_error)?;
        }
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
        let open_wave = work
            .map(|work| work.io_wave())
            .transpose()
            .map_err(root::checkpoint::work_error)?;
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open row-page root descriptor artifact"))?;
        let mut keys =
            File::open(self.key_path()).map_err(durability("open row-page root key artifact"))?;
        drop(open_wave);

        let mut lower = 0u64;
        let mut upper = table_root.page_count;
        let mut descriptor_reads = 0usize;
        let mut candidate = None;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let allocations = work.map(|work| crate::background::CheckpointDecodeContext {
                work: work.clone(),
                memory: Default::default(),
            });
            let descriptor = self.read_table_page_descriptor_from_with_allocations(
                &mut descriptors,
                &mut keys,
                table,
                table_root,
                middle,
                allocations.as_ref(),
            )?;
            let descriptor = CheckpointRootDescriptorRead {
                descriptor,
                _memory: allocations
                    .map_or_else(Default::default, |context| context.memory.into_inner()),
            };
            descriptor_reads = descriptor_reads.checked_add(1).ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "row-page descriptor read counter overflow".to_string(),
                )
            })?;
            let compare = |bound: &[u8]| match work {
                Some(work) => root::checkpoint::compare(bound, encoded_key, work),
                None => Ok(bound.cmp(encoded_key)),
            };
            if compare(&descriptor.upper_bound)? == std::cmp::Ordering::Less {
                lower = middle.checked_add(1).ok_or_else(|| {
                    RelationalRowPagePublicationError::Corrupt(
                        "row-page descriptor search overflow".to_string(),
                    )
                })?;
                if lower == table_root.page_count {
                    // Preserve the last-page result for keys above the table.
                    candidate = Some((middle, descriptor));
                }
            } else if compare(&descriptor.lower_bound)? != std::cmp::Ordering::Greater {
                // Published page bounds do not overlap. The descriptor and
                // both keys have already passed their binding checks, so an
                // in-range key needs neither more probes nor a second read.
                return Ok((Some((middle, descriptor)), descriptor_reads));
            } else {
                upper = middle;
                candidate = Some((middle, descriptor));
            }
        }
        let candidate = candidate.ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page descriptor search did not select a page".to_string(),
            )
        })?;
        Ok((Some(candidate), descriptor_reads))
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
        self.read_page_slot_accounted_with_work_context(descriptor, cache, store_id, None)
    }

    pub(crate) fn read_page_slot_accounted_with_work_context(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        cache: &SegmentCache,
        store_id: StoreId,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<RelationalRowPageSlotRead, RelationalRowPagePublicationError> {
        if let Some(work) = work {
            return self.read_checkpoint_page_slot(descriptor, work);
        }
        let cache_key = SegmentCacheKey {
            store_id,
            manifest_generation: ManifestGeneration(descriptor.physical_generation),
            segment_id: descriptor.physical_offset,
            content_digest: ContentDigest(descriptor.page_integrity.page_crc32c as u64),
            representation: RepresentationKind::RelationalRowPageSlot,
        };
        let verification_tag = *descriptor.page_integrity.page_sha256.as_bytes();
        if let Some(lease) = cache.get_verified(&cache_key, verification_tag) {
            let bytes = lease.into_bytes();
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
        match cache.insert_verified(cache_key, verification_tag, slot) {
            Ok(lease) => Ok(RelationalRowPageSlotRead {
                page: VerifiedRowPage::new(lease.into_bytes(), metadata),
                cache_hit: false,
                cache_miss: true,
                cache_admission_rejected: false,
            }),
            Err(rejection) => {
                let (error, bytes) = rejection.into_parts();
                match error {
                    SegmentCacheError::EntryTooLarge { .. }
                    | SegmentCacheError::PinnedCapacity { .. } => Ok(RelationalRowPageSlotRead {
                        page: VerifiedRowPage::new(bytes.into(), metadata),
                        cache_hit: false,
                        cache_miss: true,
                        cache_admission_rejected: true,
                    }),
                    error => Err(RelationalRowPagePublicationError::Corrupt(format!(
                        "row-page cache rejected immutable slot identity: {error}"
                    ))),
                }
            }
        }
    }

    fn read_checkpoint_page_slot(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<RelationalRowPageSlotRead, RelationalRowPagePublicationError> {
        let page_bytes = descriptor.page_integrity.encoded_len as usize;
        let offset = descriptor.physical_offset;
        let mut scratch =
            crate::background::CheckpointBytes::zeroed(page_bytes.min(64 * 1024), work)
                .map_err(root::checkpoint::work_error)?;
        let bytes = crate::background::CheckpointSharedBytes::build_with_error(
            page_bytes,
            work,
            root::checkpoint::work_error,
            |bytes| -> Result<(), RelationalRowPagePublicationError> {
                let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
                let wave = work.io_wave().map_err(root::checkpoint::work_error)?;
                let mut artifact = File::open(self.directory.join(
                    relational_row_page_artifact_file(descriptor.physical_generation),
                ))
                .map_err(durability("open row-page generation artifact"))?;
                artifact
                    .seek(SeekFrom::Start(offset))
                    .map_err(durability("seek row-page generation page"))?;
                drop(wave);
                unit.finish();
                let mut remaining = page_bytes;
                while remaining != 0 {
                    let count = remaining.min(scratch.len());
                    let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
                    let wave = work.io_wave().map_err(root::checkpoint::work_error)?;
                    artifact
                        .read_exact(&mut scratch.as_mut_slice()[..count])
                        .map_err(durability("read row-page generation page"))?;
                    drop(wave);
                    unit.finish();
                    bytes
                        .append(&scratch[..count], work)
                        .map_err(root::checkpoint::work_error)?;
                    remaining -= count;
                }
                Ok(())
            },
        )?;
        let view = self.validate_page_slot_with_work_context(descriptor, &bytes, Some(work))?;
        let metadata = VerifiedRowPageMetadata::from_view(&view);
        Ok(RelationalRowPageSlotRead {
            page: VerifiedRowPage::new_checkpoint(bytes, metadata),
            // Private reads bypass serving-cache ownership and publication.
            cache_hit: false,
            cache_miss: false,
            cache_admission_rejected: false,
        })
    }

    /// Reads exactly the page's encoded bytes at its byte offset (format v2).
    fn read_page_slot_bytes(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
    ) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
        let mut artifact = File::open(self.directory.join(relational_row_page_artifact_file(
            descriptor.physical_generation,
        )))
        .map_err(durability("open row-page generation artifact"))?;
        artifact
            .seek(SeekFrom::Start(descriptor.physical_offset))
            .map_err(durability("seek row-page generation page"))?;
        let mut page = vec![0u8; descriptor.page_integrity.encoded_len as usize];
        artifact
            .read_exact(&mut page)
            .map_err(durability("read row-page generation page"))?;
        Ok(page)
    }

    fn validate_page_slot<'a>(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        slot: &'a [u8],
    ) -> Result<RelationalRowPageView<'a>, RelationalRowPagePublicationError> {
        self.validate_page_slot_with_work_context(descriptor, slot, None)
    }

    /// Verifies the page's own CRC32C/SHA-256 once, then binds that digest
    /// and the page identity to the descriptor.
    fn validate_page_slot_with_work_context<'a>(
        &self,
        descriptor: &RelationalRowPageRootDescriptor,
        page: &'a [u8],
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<RelationalRowPageView<'a>, RelationalRowPagePublicationError> {
        if page.len() != descriptor.page_integrity.encoded_len as usize {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "row-page {} contains {} bytes, its descriptor declares {}",
                descriptor.logical_page_id.get(),
                page.len(),
                descriptor.page_integrity.encoded_len
            )));
        }
        let view = match work {
            Some(work) => {
                // Cooperative form of the codec's own integrity check.
                if !crate::relational::row_page::verify_page_integrity_with_work_context(
                    page, work,
                )? {
                    return Err(RelationalRowPagePublicationError::Corrupt(format!(
                        "row-page {} checksum mismatch",
                        descriptor.logical_page_id.get()
                    )));
                }
                RelationalRowPageView::open_digest_verified(page, self.config.page_limits)?
            }
            None => RelationalRowPageView::open(page, self.config.page_limits)?,
        };
        let (page_crc32c, page_sha256) = crate::relational::row_page::encoded_page_digest(page);
        if page_crc32c != descriptor.page_integrity.page_crc32c
            || page_sha256 != descriptor.page_integrity.page_sha256
        {
            return Err(RelationalRowPagePublicationError::Corrupt(format!(
                "row-page {} digest does not match its descriptor",
                descriptor.logical_page_id.get()
            )));
        }
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
            || view.encoded_len() != descriptor.page_integrity.encoded_len as usize
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

    pub(super) fn visit_table_pages_with_work_context<F>(
        &self,
        table: &str,
        work: &crate::background::CheckpointWorkContext,
        mut visitor: F,
    ) -> Result<(), RelationalRowPagePublicationError>
    where
        F: FnMut(&RelationalRowPageRootDescriptor) -> Result<(), RelationalRowPagePublicationError>,
    {
        let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
        let table_root = self.table_root(table)?;
        unit.finish();
        let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
        let wave = work.io_wave().map_err(root::checkpoint::work_error)?;
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open row-page root descriptor artifact"))?;
        drop(wave);
        unit.finish();
        let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
        let wave = work.io_wave().map_err(root::checkpoint::work_error)?;
        let mut keys =
            File::open(self.key_path()).map_err(durability("open row-page root key artifact"))?;
        drop(wave);
        unit.finish();
        let mut previous_upper: Option<Vec<u8>> = None;
        for offset in 0..table_root.page_count {
            let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
            let ordinal = table_root
                .first_descriptor
                .checked_add(offset)
                .ok_or_else(|| {
                    RelationalRowPagePublicationError::Corrupt(
                        "row-page descriptor ordinal overflow".to_string(),
                    )
                })?;
            unit.finish();
            let descriptor = root::checkpoint::read_descriptor(
                &mut descriptors,
                &mut keys,
                ordinal,
                &self.manifest,
                self.config,
                work,
            )?;
            if previous_upper
                .as_ref()
                .map(|upper| root::checkpoint::compare(upper, &descriptor.lower_bound, work))
                .transpose()?
                .is_some_and(|ordering| ordering != std::cmp::Ordering::Less)
            {
                return Err(RelationalRowPagePublicationError::Corrupt(format!(
                    "table {table} row-page bounds overlap or are unordered"
                )));
            }
            previous_upper = Some(root::checkpoint::clone_bytes(
                &descriptor.upper_bound,
                work,
            )?);
            // No read, CPU or I/O lease crosses the callback into nested writers.
            visitor(&descriptor)?;
        }
        work.checkpoint().map_err(root::checkpoint::work_error)
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
        self.read_table_page_descriptor_from_with_allocations(
            descriptors,
            keys,
            table,
            table_root,
            ordinal,
            None,
        )
    }

    fn read_table_page_descriptor_from_with_allocations(
        &self,
        descriptors: &mut File,
        keys: &mut File,
        table: &str,
        table_root: &RelationalRowPageTableRoot,
        ordinal: u64,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
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
        let descriptor = match allocations {
            Some(context) => root::checkpoint::read_descriptor_with_allocations(
                descriptors,
                keys,
                descriptor_ordinal,
                &self.manifest,
                self.config,
                context,
                Some(context),
            ),
            None => root::read_descriptor(
                descriptors,
                keys,
                descriptor_ordinal,
                &self.manifest,
                self.config,
            ),
        }?;
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
