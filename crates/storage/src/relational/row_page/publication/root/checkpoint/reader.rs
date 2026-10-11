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

use super::super::{codec::WireDescriptor, ROOT_DESCRIPTOR_BINDING_OFFSET, ROOT_DESCRIPTOR_BYTES};
use super::{work_error, CheckpointWorkContext};
use crate::file_io::File;
use crate::relational::row_page::publication::{
    durability, RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPageRootDescriptor, RelationalRowPageRootManifest, RelationalRowPageSlotIntegrity,
};
use crate::relational::RelationalRowPageId;
use hawdb_integrity::IntegrityHasher;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroU64;

pub(in crate::relational::row_page::publication) fn read_descriptor(
    descriptors: &mut File,
    keys: &mut File,
    ordinal: u64,
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<RelationalRowPageRootDescriptor, RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    if ordinal >= manifest.root_page_count {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor ordinal {ordinal} exceeds root page count {}",
            manifest.root_page_count
        )));
    }
    let descriptor_offset = ordinal
        .checked_mul(ROOT_DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page descriptor offset overflow".to_string(),
            )
        })?;
    unit.finish();
    seek(
        descriptors,
        descriptor_offset,
        "seek row-page root descriptor",
        work,
    )?;
    let mut encoded = [0u8; ROOT_DESCRIPTOR_BYTES];
    read(
        descriptors,
        &mut encoded,
        "read row-page root descriptor",
        work,
    )?;
    let unit = work.start_unit().map_err(work_error)?;
    let wire = WireDescriptor::decode(&encoded);
    unit.finish();
    let lower_bound = read_key(
        keys,
        wire.lower_offset,
        wire.lower_len,
        manifest.root_key_artifact.encoded_len,
        config,
        "lower",
        work,
    )?;
    let upper_bound = read_key(
        keys,
        wire.upper_offset,
        wire.upper_len,
        manifest.root_key_artifact.encoded_len,
        config,
        "upper",
        work,
    )?;
    let mut hasher = IntegrityHasher::new();
    super::hash(&mut hasher, &manifest.generation.to_le_bytes(), work)?;
    super::hash(&mut hasher, &ordinal.to_le_bytes(), work)?;
    super::hash(
        &mut hasher,
        &encoded[..ROOT_DESCRIPTOR_BINDING_OFFSET],
        work,
    )?;
    super::hash(&mut hasher, &lower_bound, work)?;
    super::hash(&mut hasher, &upper_bound, work)?;
    let unit = work.start_unit().map_err(work_error)?;
    let digest = hasher.finish();
    if digest.crc32c.get() != wire.binding_crc32c || digest.sha256 != wire.binding_sha256 {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page root descriptor binding checksum mismatch".to_string(),
        ));
    }
    let descriptor = RelationalRowPageRootDescriptor {
        logical_page_id: RelationalRowPageId::new(
            NonZeroU64::new(wire.logical_page_id).ok_or_else(|| {
                RelationalRowPagePublicationError::Corrupt(
                    "row-page descriptor has a zero logical page id".to_string(),
                )
            })?,
        ),
        physical_generation: wire.physical_generation,
        physical_slot: wire.physical_slot,
        source_commit_epoch: wire.source_commit_epoch,
        row_count: wire.row_count,
        lower_bound,
        upper_bound,
        slot_integrity: RelationalRowPageSlotIntegrity {
            encoded_len: wire.encoded_len,
            slot_crc32c: wire.slot_crc32c,
            slot_sha256: wire.slot_sha256,
        },
    };
    unit.finish();
    validate_descriptor(
        &descriptor,
        manifest.generation,
        manifest.source_commit_epoch,
        config,
        work,
    )?;
    let unit = work.start_unit().map_err(work_error)?;
    let index = manifest
        .physical_generations
        .binary_search_by_key(&descriptor.physical_generation, |entry| entry.generation)
        .map_err(|_| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page descriptor references an unaccounted physical generation".to_string(),
            )
        })?;
    let allocated_pages = manifest.physical_generations[index].allocated_pages;
    if descriptor.physical_slot >= allocated_pages {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor references slot {} outside generation {} allocation {}",
            descriptor.physical_slot, descriptor.physical_generation, allocated_pages
        )));
    }
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(descriptor)
}

fn read_key(
    file: &mut File,
    offset: u64,
    len: u32,
    artifact_len: u64,
    config: RelationalRowPagePublicationConfig,
    bound: &str,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let len = len as usize;
    if len == 0 || len > config.page_limits.max_key_bytes.get() {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page {bound} key contains {len} bytes outside the admitted range"
        )));
    }
    let end = offset.checked_add(len as u64).ok_or_else(|| {
        RelationalRowPagePublicationError::Corrupt(format!("row-page {bound} key offset overflow"))
    })?;
    if end > artifact_len {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page {bound} key exceeds its artifact"
        )));
    }
    let mut bytes = Vec::with_capacity(len);
    unit.finish();
    while bytes.len() < len {
        let unit = work.start_unit().map_err(work_error)?;
        bytes.resize((bytes.len() + 64 * 1024).min(len), 0);
        unit.finish();
    }
    seek(file, offset, "seek row-page root key", work)?;
    read(file, &mut bytes, "read row-page root key", work)?;
    Ok(bytes)
}

pub(in crate::relational::row_page::publication) fn validate_descriptor(
    descriptor: &RelationalRowPageRootDescriptor,
    root_generation: u64,
    root_epoch: u64,
    config: RelationalRowPagePublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    if descriptor.physical_generation == 0 || descriptor.physical_generation > root_generation {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor physical generation {} is outside 1..={root_generation}",
            descriptor.physical_generation
        )));
    }
    if descriptor.source_commit_epoch == 0 || descriptor.source_commit_epoch > root_epoch {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor source epoch {} is outside 1..={root_epoch}",
            descriptor.source_commit_epoch
        )));
    }
    if descriptor.row_count == 0
        || descriptor.row_count as usize > config.page_limits.max_rows.get()
    {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor row count {} is outside the admitted range",
            descriptor.row_count
        )));
    }
    if descriptor.slot_integrity.encoded_len == 0
        || descriptor.slot_integrity.encoded_len as usize > config.page_limits.max_page_bytes.get()
    {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page descriptor encoded length {} is outside the admitted range",
            descriptor.slot_integrity.encoded_len
        )));
    }
    unit.finish();
    let ordering = super::compare(&descriptor.lower_bound, &descriptor.upper_bound, work)?;
    let unit = work.start_unit().map_err(work_error)?;
    if descriptor.lower_bound.is_empty()
        || descriptor.upper_bound.is_empty()
        || ordering == std::cmp::Ordering::Greater
        || descriptor.lower_bound.len() > config.page_limits.max_key_bytes.get()
        || descriptor.upper_bound.len() > config.page_limits.max_key_bytes.get()
    {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page descriptor has invalid key bounds".to_string(),
        ));
    }
    unit.finish();
    work.checkpoint().map_err(work_error)
}

fn seek(
    file: &mut File,
    offset: u64,
    operation: &'static str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    file.seek(SeekFrom::Start(offset))
        .map_err(durability(operation))?;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(work_error)
}
fn read(
    file: &mut File,
    bytes: &mut [u8],
    operation: &'static str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    for block in bytes.chunks_mut(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        file.read_exact(block).map_err(durability(operation))?;
        drop(wave);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}
