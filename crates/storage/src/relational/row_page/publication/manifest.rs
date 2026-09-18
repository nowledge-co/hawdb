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
    durability, RelationalRowPageArtifactMetadata, RelationalRowPagePhysicalGeneration,
    RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPageRootManifest, RelationalRowPageTableRoot,
};
use hawdb_integrity::{integrity_digest, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fs::{self, File};
use std::io::Read;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;

const MANIFEST_MAGIC: &[u8; 8] = b"SKRPGM01";
const MANIFEST_VERSION: u16 = 1;
const PHYSICAL_GENERATIONS_FLAG: u16 = 1;
pub(super) const PHYSICAL_GENERATION_BYTES: usize = 24;
pub(super) const OCCUPANCY_TRAILER_BYTES: usize = 12;
pub(super) const MANIFEST_HEADER_BYTES: usize = 316;
const MANIFEST_INTEGRITY_OFFSET: usize = 280;
const ARTIFACT_METADATA_BYTES: usize = 44;

pub(super) fn root_set_digest(
    tables: &[RelationalRowPageTableRoot],
) -> Result<Sha256Digest, RelationalRowPagePublicationError> {
    let payload = encode_tables(tables)?;
    let mut hasher = IntegrityHasher::new();
    hasher.update(&payload);
    Ok(hasher.finish().sha256)
}

pub(super) fn encode_manifest(
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    validate_manifest(manifest, config, ErrorClass::Admission)?;
    let mut payload = encode_tables(&manifest.tables)?;
    let occupancy_bytes = manifest
        .physical_generations
        .len()
        .checked_mul(PHYSICAL_GENERATION_BYTES)
        .and_then(|bytes| bytes.checked_add(OCCUPANCY_TRAILER_BYTES))
        .and_then(|bytes| bytes.checked_add(payload.len()))
        .and_then(|bytes| bytes.checked_add(MANIFEST_HEADER_BYTES))
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row-page occupancy metadata length overflow".to_string(),
            )
        })?;
    if occupancy_bytes > config.max_manifest_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(
            "row-page occupancy metadata exceeds the manifest byte limit".to_string(),
        ));
    }
    for entry in &manifest.physical_generations {
        payload.extend_from_slice(&entry.generation.to_le_bytes());
        payload.extend_from_slice(&entry.allocated_pages.to_le_bytes());
        payload.extend_from_slice(&entry.live_pages.to_le_bytes());
    }
    payload.extend_from_slice(&manifest.relocated_page_count.to_le_bytes());
    payload.extend_from_slice(&(manifest.physical_generations.len() as u32).to_le_bytes());
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest payload does not fit in u32".to_string(),
        )
    })?;
    let encoded_len = MANIFEST_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row-page manifest length overflow".to_string(),
            )
        })?;
    if encoded_len > config.max_manifest_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }

    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(MANIFEST_MAGIC);
    encoded.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    encoded.extend_from_slice(&PHYSICAL_GENERATIONS_FLAG.to_le_bytes());
    encoded.extend_from_slice(&manifest.generation.to_le_bytes());
    encoded.extend_from_slice(&manifest.source_commit_epoch.to_le_bytes());
    encoded.extend_from_slice(&manifest.previous_generation.unwrap_or(0).to_le_bytes());
    encoded.extend_from_slice(&manifest.page_bytes.to_le_bytes());
    encoded.extend_from_slice(&manifest.dirty_page_count.to_le_bytes());
    encoded.extend_from_slice(&manifest.root_page_count.to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(manifest.tables.len())
            .expect("validated table count fits in u32")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encode_artifact(manifest.page_artifact, &mut encoded);
    encode_artifact(manifest.root_descriptor_artifact, &mut encoded);
    encode_artifact(manifest.root_key_artifact, &mut encoded);
    encoded.extend_from_slice(manifest.root_set_digest.as_bytes());
    match manifest.overflow_root {
        Some(binding) => {
            encoded.extend_from_slice(&binding.generation.to_le_bytes());
            encoded.extend_from_slice(&binding.source_commit_epoch.to_le_bytes());
            encoded.extend_from_slice(binding.root_set_digest.as_bytes());
        }
        None => encoded.extend_from_slice(&[0u8; 48]),
    }
    debug_assert_eq!(encoded.len(), MANIFEST_INTEGRITY_OFFSET);
    encoded.extend_from_slice(&0u32.to_le_bytes());
    encoded.extend_from_slice(&[0u8; SHA256_BYTES]);
    debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);
    encoded.extend_from_slice(&payload);

    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    hasher.update(&payload);
    let digest = hasher.finish();
    encoded[280..284].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[284..316].copy_from_slice(digest.sha256.as_bytes());
    Ok(encoded)
}

pub(super) fn read_manifest_if_exists(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
) -> Result<Option<RelationalRowPageRootManifest>, RelationalRowPagePublicationError> {
    match fs::metadata(path) {
        Ok(_) => read_manifest(path, config).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(durability("read row-page manifest metadata")(error)),
    }
}

pub(super) fn read_manifest(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
) -> Result<RelationalRowPageRootManifest, RelationalRowPagePublicationError> {
    let encoded = read_encoded_manifest(path, config)?;
    decode_manifest(&encoded, config)
}

pub(super) fn read_bound_manifest(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
    expected: RelationalRowPageArtifactMetadata,
) -> Result<RelationalRowPageRootManifest, RelationalRowPagePublicationError> {
    let encoded = read_encoded_manifest(path, config)?;
    let digest = integrity_digest(&encoded);
    if encoded.len() as u64 != expected.encoded_len
        || digest.crc32c.get() != expected.encoded_crc32c
        || digest.sha256 != expected.encoded_sha256
    {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page generation manifest does not match its canonical binding".to_string(),
        ));
    }
    decode_manifest(&encoded, config)
}

fn read_encoded_manifest(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let max_bytes = config.max_manifest_bytes.get();
    let max_bytes_u64 = u64::try_from(max_bytes).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest limit overflows u64".to_string(),
        )
    })?;
    let read_limit = max_bytes_u64.checked_add(1).ok_or_else(|| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest read limit overflows u64".to_string(),
        )
    })?;
    let file = File::open(path).map_err(durability("open row-page manifest"))?;
    let encoded_len = file
        .metadata()
        .map_err(durability("read row-page manifest metadata"))?
        .len();
    if encoded_len > max_bytes_u64 {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let capacity = usize::try_from(encoded_len).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest length overflows usize".to_string(),
        )
    })?;
    let mut encoded = Vec::with_capacity(capacity);
    file.take(read_limit)
        .read_to_end(&mut encoded)
        .map_err(durability("read row-page manifest"))?;
    if encoded.len() > max_bytes {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest exceeds limit {}",
            config.max_manifest_bytes
        )));
    }
    Ok(encoded)
}

fn decode_manifest(
    encoded: &[u8],
    config: RelationalRowPagePublicationConfig,
) -> Result<RelationalRowPageRootManifest, RelationalRowPagePublicationError> {
    if encoded.len() < MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "invalid row-page manifest header".to_string(),
        ));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    if version == MANIFEST_VERSION && flags == 0 {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page v1 manifest lacks physical-generation accounting; recreate the development database".to_string(),
        ));
    }
    if version != MANIFEST_VERSION || flags != PHYSICAL_GENERATIONS_FLAG {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "unsupported row-page manifest version {version} or flags {flags}"
        )));
    }
    let generation = read_u64(&encoded[12..20]);
    let source_commit_epoch = read_u64(&encoded[20..28]);
    let previous = read_u64(&encoded[28..36]);
    let page_bytes = read_u64(&encoded[36..44]);
    let dirty_page_count = read_u64(&encoded[44..52]);
    let root_page_count = read_u64(&encoded[52..60]);
    let table_count = read_u32(&encoded[60..64]) as usize;
    let payload_len = read_u32(&encoded[64..68]) as usize;
    if table_count > config.max_tables.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest declares {table_count} tables, exceeding limit {}",
            config.max_tables
        )));
    }
    let expected_len = MANIFEST_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page manifest length overflow".to_string(),
            )
        })?;
    if encoded.len() != expected_len {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page manifest contains {} bytes, expected {expected_len}",
            encoded.len()
        )));
    }
    let page_artifact = decode_artifact(&encoded[68..112]);
    let root_descriptor_artifact = decode_artifact(&encoded[112..156]);
    let root_key_artifact = decode_artifact(&encoded[156..200]);
    let root_set_digest = Sha256Digest::from_bytes(
        encoded[200..232]
            .try_into()
            .expect("root-set digest length was checked"),
    );
    let overflow_generation = read_u64(&encoded[232..240]);
    let overflow_source_commit_epoch = read_u64(&encoded[240..248]);
    let overflow_root_set_digest = Sha256Digest::from_bytes(
        encoded[248..280]
            .try_into()
            .expect("overflow root digest length was checked"),
    );
    let overflow_root = if overflow_generation == 0 {
        if overflow_source_commit_epoch != 0 || overflow_root_set_digest.as_bytes() != &[0u8; 32] {
            return Err(RelationalRowPagePublicationError::Corrupt(
                "row-page manifest has a partial overflow binding".to_string(),
            ));
        }
        None
    } else {
        Some(crate::relational::RelationalOverflowRootBinding {
            generation: overflow_generation,
            source_commit_epoch: overflow_source_commit_epoch,
            root_set_digest: overflow_root_set_digest,
        })
    };
    let payload = &encoded[MANIFEST_HEADER_BYTES..];
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    hasher.update(payload);
    let digest = hasher.finish();
    if digest.crc32c.get() != read_u32(&encoded[280..284])
        || digest.sha256.as_bytes() != &encoded[284..316]
    {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page manifest checksum mismatch".to_string(),
        ));
    }
    let trailer_offset = payload
        .len()
        .checked_sub(OCCUPANCY_TRAILER_BYTES)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page manifest lacks its occupancy trailer".to_string(),
            )
        })?;
    let relocated_page_count = read_u64(&payload[trailer_offset..trailer_offset + 8]);
    let generation_count = read_u32(&payload[trailer_offset + 8..]) as usize;
    let generations_offset = generation_count
        .checked_mul(PHYSICAL_GENERATION_BYTES)
        .and_then(|bytes| trailer_offset.checked_sub(bytes))
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page physical-generation count exceeds its manifest payload".to_string(),
            )
        })?;
    let tables = decode_tables(&payload[..generations_offset], table_count, config)?;
    let physical_generations = payload[generations_offset..trailer_offset]
        .chunks_exact(PHYSICAL_GENERATION_BYTES)
        .map(|entry| RelationalRowPagePhysicalGeneration {
            generation: read_u64(&entry[..8]),
            allocated_pages: read_u64(&entry[8..16]),
            live_pages: read_u64(&entry[16..24]),
        })
        .collect();
    let manifest = RelationalRowPageRootManifest {
        generation,
        source_commit_epoch,
        previous_generation: (previous != 0).then_some(previous),
        page_bytes,
        dirty_page_count,
        relocated_page_count,
        root_page_count,
        page_artifact,
        root_descriptor_artifact,
        root_key_artifact,
        root_set_digest,
        overflow_root,
        tables,
        physical_generations,
    };
    validate_manifest(&manifest, config, ErrorClass::Corrupt)?;
    Ok(manifest)
}

fn validate_manifest(
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
    class: ErrorClass,
) -> Result<(), RelationalRowPagePublicationError> {
    let fail = |message| class.error(message);
    if manifest.generation == 0
        || (manifest.source_commit_epoch == 0
            && (manifest.root_page_count != 0 || !manifest.tables.is_empty()))
    {
        return Err(fail(format!(
            "invalid row-page generation/epoch {}/{}",
            manifest.generation, manifest.source_commit_epoch
        )));
    }
    if manifest
        .previous_generation
        .is_some_and(|previous| previous >= manifest.generation)
    {
        return Err(fail(format!(
            "previous row-page generation {:?} does not precede generation {}",
            manifest.previous_generation, manifest.generation
        )));
    }
    if let Some(binding) = manifest.overflow_root
        && (binding.generation != manifest.generation
            || binding.source_commit_epoch != manifest.source_commit_epoch)
    {
        return Err(fail(format!(
            "row-page overflow root identifies generation/epoch {}/{}, expected {}/{}",
            binding.generation,
            binding.source_commit_epoch,
            manifest.generation,
            manifest.source_commit_epoch
        )));
    }
    if manifest.page_bytes != config.page_limits.max_page_bytes.get() as u64 {
        return Err(fail(format!(
            "row-page slot contains {} bytes, configured for {}",
            manifest.page_bytes, config.page_limits.max_page_bytes
        )));
    }
    if manifest.dirty_page_count > config.max_dirty_pages.get() as u64 {
        return Err(fail(format!(
            "row-page manifest declares {} dirty pages, exceeding limit {}",
            manifest.dirty_page_count, config.max_dirty_pages
        )));
    }
    let written_page_count = manifest
        .dirty_page_count
        .checked_add(manifest.relocated_page_count)
        .ok_or_else(|| fail("row-page written-page count overflow".to_string()))?;
    if written_page_count > manifest.root_page_count {
        return Err(fail(
            "row-page written pages exceed the live root".to_string(),
        ));
    }
    let expected_page_bytes = written_page_count
        .checked_mul(manifest.page_bytes)
        .ok_or_else(|| fail("row-page artifact length overflow".to_string()))?;
    if manifest.page_artifact.encoded_len != expected_page_bytes {
        return Err(fail(format!(
            "row-page artifact declares {} bytes, expected {expected_page_bytes}",
            manifest.page_artifact.encoded_len
        )));
    }
    if manifest.root_page_count > config.max_root_pages.get() {
        return Err(fail(format!(
            "row-page root declares {} pages, exceeding limit {}",
            manifest.root_page_count, config.max_root_pages
        )));
    }
    validate_occupancy(manifest, config, class)?;
    let expected_descriptor_bytes = manifest
        .root_page_count
        .checked_mul(super::root::ROOT_DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| fail("row-page descriptor length overflow".to_string()))?;
    if manifest.root_descriptor_artifact.encoded_len != expected_descriptor_bytes {
        return Err(fail(format!(
            "row-page descriptor artifact declares {} bytes, expected {expected_descriptor_bytes}",
            manifest.root_descriptor_artifact.encoded_len
        )));
    }
    if manifest.root_key_artifact.encoded_len > config.max_root_key_bytes.get() {
        return Err(fail(format!(
            "row-page root keys contain {} bytes, exceeding limit {}",
            manifest.root_key_artifact.encoded_len, config.max_root_key_bytes
        )));
    }
    if manifest.tables.len() > config.max_tables.get() {
        return Err(fail(format!(
            "row-page manifest contains {} tables, exceeding limit {}",
            manifest.tables.len(),
            config.max_tables
        )));
    }
    let mut expected_descriptor = 0u64;
    let mut previous_table: Option<&str> = None;
    for table in &manifest.tables {
        if table.table.is_empty() || table.table.len() > config.max_table_name_bytes.get() {
            return Err(fail(format!(
                "row-page table name contains {} bytes, outside admitted range",
                table.table.len()
            )));
        }
        if previous_table.is_some_and(|previous| previous >= table.table.as_str()) {
            return Err(fail(
                "row-page table roots are not strictly ordered".to_string(),
            ));
        }
        if table.column_count.get() as usize > config.page_limits.max_columns.get() {
            return Err(fail(format!(
                "table {} declares {} columns, exceeding limit {}",
                table.table, table.column_count, config.page_limits.max_columns
            )));
        }
        if table.schema.name != table.table
            || table.schema.columns.len() != table.column_count.get() as usize
        {
            return Err(fail(format!(
                "table {} has a mismatched row-page schema",
                table.table
            )));
        }
        crate::relational::codec::validate_relational_table_schema_codec_shape(
            &table.schema,
            config.page_limits.max_columns.get(),
        )
        .map_err(|error| fail(error.to_string()))?;
        let schema_digest =
            crate::relational::index_shadow::relational_schema_digest(&table.schema)
                .map_err(|error| fail(error.to_string()))?;
        if schema_digest != table.schema_digest {
            return Err(fail(format!(
                "table {} row-page schema digest mismatch",
                table.table
            )));
        }
        if table.first_descriptor != expected_descriptor {
            return Err(fail(format!(
                "table {} starts at descriptor {}, expected {expected_descriptor}",
                table.table, table.first_descriptor
            )));
        }
        expected_descriptor = expected_descriptor
            .checked_add(table.page_count)
            .ok_or_else(|| fail("row-page table descriptor count overflow".to_string()))?;
        if table.page_count == 0 {
            if table.row_count != 0
                || !table.lower_bound.is_empty()
                || !table.upper_bound.is_empty()
            {
                return Err(fail(format!(
                    "empty table {} has rows or non-empty key bounds",
                    table.table
                )));
            }
        } else {
            let max_rows = table
                .page_count
                .checked_mul(config.page_limits.max_rows.get() as u64)
                .ok_or_else(|| fail("row-page table row limit overflow".to_string()))?;
            if table.row_count < table.page_count || table.row_count > max_rows {
                return Err(fail(format!(
                    "table {} declares {} rows across {} pages",
                    table.table, table.row_count, table.page_count
                )));
            }
            if table.lower_bound.is_empty()
                || table.upper_bound.is_empty()
                || table.lower_bound > table.upper_bound
                || table.lower_bound.len() > config.page_limits.max_key_bytes.get()
                || table.upper_bound.len() > config.page_limits.max_key_bytes.get()
            {
                return Err(fail(format!(
                    "table {} has invalid row-page key bounds",
                    table.table
                )));
            }
        }
        previous_table = Some(&table.table);
    }
    if expected_descriptor != manifest.root_page_count {
        return Err(fail(format!(
            "table roots cover {expected_descriptor} descriptors, expected {}",
            manifest.root_page_count
        )));
    }
    let payload = encode_tables(&manifest.tables)?;
    let mut hasher = IntegrityHasher::new();
    hasher.update(&payload);
    if hasher.finish().sha256 != manifest.root_set_digest {
        return Err(fail("row-page root-set digest mismatch".to_string()));
    }
    Ok(())
}

fn validate_occupancy(
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
    class: ErrorClass,
) -> Result<(), RelationalRowPagePublicationError> {
    let fail = |message: &str| class.error(message.to_string());
    if manifest.physical_generations.len()
        > config.max_manifest_bytes.get() / PHYSICAL_GENERATION_BYTES
        || u32::try_from(manifest.physical_generations.len()).is_err()
    {
        return Err(fail(
            "row-page physical-generation inventory exceeds the manifest limit",
        ));
    }
    let mut previous = 0;
    let mut live_pages = 0u64;
    let mut allocated_pages = 0u64;
    let mut current_allocation = 0;
    for entry in &manifest.physical_generations {
        if entry.generation <= previous || entry.generation > manifest.generation {
            return Err(fail(
                "row-page physical generations are unordered or outside the root generation",
            ));
        }
        if entry.live_pages == 0
            || entry.live_pages > entry.allocated_pages
            || entry.allocated_pages > config.max_root_pages.get()
        {
            return Err(fail(
                "row-page physical generation has invalid live/allocated counts",
            ));
        }
        live_pages = live_pages
            .checked_add(entry.live_pages)
            .ok_or_else(|| fail("row-page live-page count overflow"))?;
        allocated_pages = allocated_pages
            .checked_add(entry.allocated_pages)
            .ok_or_else(|| fail("row-page allocated-page count overflow"))?;
        if entry.generation == manifest.generation {
            current_allocation = entry.allocated_pages;
            if entry.live_pages != entry.allocated_pages {
                return Err(fail("new row-page generation contains unreferenced slots"));
            }
        }
        previous = entry.generation;
    }
    allocated_pages
        .checked_mul(manifest.page_bytes)
        .ok_or_else(|| fail("row-page physical allocation byte count overflow"))?;
    if live_pages != manifest.root_page_count {
        return Err(fail("row-page live-page inventory does not cover the root"));
    }
    if current_allocation != manifest.dirty_page_count + manifest.relocated_page_count {
        return Err(fail(
            "row-page current-generation inventory does not match written slots",
        ));
    }
    Ok(())
}

fn encode_tables(
    tables: &[RelationalRowPageTableRoot],
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let mut encoded = Vec::new();
    for table in tables {
        encode_bytes(&table.table, &mut encoded)?;
        let schema = crate::relational::codec::encode_relational_table_schema(&table.schema)
            .map_err(map_schema_encode_error)?;
        encode_raw_bytes(&schema, &mut encoded)?;
        encoded.extend_from_slice(table.schema_digest.as_bytes());
        encoded.extend_from_slice(&table.column_count.get().to_le_bytes());
        encoded.extend_from_slice(&table.row_count.to_le_bytes());
        encoded.extend_from_slice(&table.next_page_id.get().to_le_bytes());
        encoded.extend_from_slice(&table.first_descriptor.to_le_bytes());
        encoded.extend_from_slice(&table.page_count.to_le_bytes());
        encode_raw_bytes(&table.lower_bound, &mut encoded)?;
        encode_raw_bytes(&table.upper_bound, &mut encoded)?;
    }
    Ok(encoded)
}

fn decode_tables(
    payload: &[u8],
    table_count: usize,
    config: RelationalRowPagePublicationConfig,
) -> Result<Vec<RelationalRowPageTableRoot>, RelationalRowPagePublicationError> {
    let mut tables = Vec::with_capacity(table_count);
    let mut offset = 0usize;
    for _ in 0..table_count {
        let table = decode_utf8_bytes(
            payload,
            &mut offset,
            config.max_table_name_bytes.get(),
            "table name",
        )?;
        let schema_bytes = decode_raw_bytes(
            payload,
            &mut offset,
            config.max_manifest_bytes.get(),
            "table schema",
        )?;
        let schema = crate::relational::codec::decode_relational_table_schema(
            &schema_bytes,
            config.max_manifest_bytes.get(),
            config.page_limits.max_columns.get(),
        )
        .map_err(map_schema_decode_error)?;
        let schema_digest = Sha256Digest::from_bytes(
            take(payload, &mut offset, SHA256_BYTES, "table schema digest")?
                .try_into()
                .expect("schema digest length was checked"),
        );
        let column_count = NonZeroU32::new(read_u32(take(
            payload,
            &mut offset,
            4,
            "table column count",
        )?))
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page table column count contains zero".to_string(),
            )
        })?;
        let row_count = read_u64(take(payload, &mut offset, 8, "table row count")?);
        let next_page_id = NonZeroU64::new(read_u64(take(
            payload,
            &mut offset,
            8,
            "next logical page id",
        )?))
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page table allocator contains zero".to_string(),
            )
        })?;
        let first_descriptor = read_u64(take(payload, &mut offset, 8, "first descriptor ordinal")?);
        let page_count = read_u64(take(payload, &mut offset, 8, "table page count")?);
        let lower_bound = decode_raw_bytes(
            payload,
            &mut offset,
            config.page_limits.max_key_bytes.get(),
            "table lower bound",
        )?;
        let upper_bound = decode_raw_bytes(
            payload,
            &mut offset,
            config.page_limits.max_key_bytes.get(),
            "table upper bound",
        )?;
        tables.push(RelationalRowPageTableRoot {
            table,
            schema,
            schema_digest,
            column_count,
            row_count,
            next_page_id,
            first_descriptor,
            page_count,
            lower_bound,
            upper_bound,
        });
    }
    if offset != payload.len() {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page manifest contains trailing table bytes".to_string(),
        ));
    }
    Ok(tables)
}

fn map_schema_encode_error(
    error: crate::relational::RelationalError,
) -> RelationalRowPagePublicationError {
    match error {
        crate::relational::RelationalError::Admission(message)
        | crate::relational::RelationalError::Schema(message)
        | crate::relational::RelationalError::Constraint(message) => {
            RelationalRowPagePublicationError::Admission(message)
        }
        crate::relational::RelationalError::Durability(message) => {
            RelationalRowPagePublicationError::Durability(message)
        }
        crate::relational::RelationalError::Corruption(message) => {
            RelationalRowPagePublicationError::Corrupt(message)
        }
    }
}

fn map_schema_decode_error(
    error: crate::relational::RelationalError,
) -> RelationalRowPagePublicationError {
    match error {
        crate::relational::RelationalError::Admission(message) => {
            RelationalRowPagePublicationError::Admission(message)
        }
        crate::relational::RelationalError::Durability(message) => {
            RelationalRowPagePublicationError::Durability(message)
        }
        crate::relational::RelationalError::Schema(message)
        | crate::relational::RelationalError::Constraint(message)
        | crate::relational::RelationalError::Corruption(message) => {
            RelationalRowPagePublicationError::Corrupt(message)
        }
    }
}

fn encode_artifact(metadata: RelationalRowPageArtifactMetadata, encoded: &mut Vec<u8>) {
    encoded.extend_from_slice(&metadata.encoded_len.to_le_bytes());
    encoded.extend_from_slice(&metadata.encoded_crc32c.to_le_bytes());
    encoded.extend_from_slice(metadata.encoded_sha256.as_bytes());
}

fn decode_artifact(encoded: &[u8]) -> RelationalRowPageArtifactMetadata {
    debug_assert_eq!(encoded.len(), ARTIFACT_METADATA_BYTES);
    RelationalRowPageArtifactMetadata {
        encoded_len: read_u64(&encoded[..8]),
        encoded_crc32c: read_u32(&encoded[8..12]),
        encoded_sha256: Sha256Digest::from_bytes(
            encoded[12..44]
                .try_into()
                .expect("artifact digest length was checked"),
        ),
    }
}

fn encode_bytes(
    value: &str,
    encoded: &mut Vec<u8>,
) -> Result<(), RelationalRowPagePublicationError> {
    encode_raw_bytes(value.as_bytes(), encoded)
}

fn encode_raw_bytes(
    value: &[u8],
    encoded: &mut Vec<u8>,
) -> Result<(), RelationalRowPagePublicationError> {
    let len = u32::try_from(value.len()).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest field does not fit in u32".to_string(),
        )
    })?;
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn decode_utf8_bytes(
    encoded: &[u8],
    offset: &mut usize,
    max_len: usize,
    context: &str,
) -> Result<String, RelationalRowPagePublicationError> {
    let bytes = decode_raw_bytes(encoded, offset, max_len, context)?;
    String::from_utf8(bytes).map_err(|error| {
        RelationalRowPagePublicationError::Corrupt(format!(
            "row-page {context} is not valid UTF-8: {error}"
        ))
    })
}

fn decode_raw_bytes(
    encoded: &[u8],
    offset: &mut usize,
    max_len: usize,
    context: &str,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let len = read_u32(take(encoded, offset, 4, context)?) as usize;
    if len > max_len {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page {context} contains {len} bytes, exceeding limit {max_len}"
        )));
    }
    Ok(take(encoded, offset, len, context)?.to_vec())
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    len: usize,
    context: &str,
) -> Result<&'a [u8], RelationalRowPagePublicationError> {
    let end = offset.checked_add(len).ok_or_else(|| {
        RelationalRowPagePublicationError::Corrupt(format!("row-page {context} length overflow"))
    })?;
    let bytes = encoded.get(*offset..end).ok_or_else(|| {
        RelationalRowPagePublicationError::Corrupt(format!("truncated row-page {context}"))
    })?;
    *offset = end;
    Ok(bytes)
}

fn read_u16(encoded: &[u8]) -> u16 {
    u16::from_le_bytes(encoded.try_into().expect("u16 field has a fixed length"))
}

fn read_u32(encoded: &[u8]) -> u32 {
    u32::from_le_bytes(encoded.try_into().expect("u32 field has a fixed length"))
}

fn read_u64(encoded: &[u8]) -> u64 {
    u64::from_le_bytes(encoded.try_into().expect("u64 field has a fixed length"))
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

impl ErrorClass {
    fn error(self, message: String) -> RelationalRowPagePublicationError {
        match self {
            Self::Admission => RelationalRowPagePublicationError::Admission(message),
            Self::Corrupt => RelationalRowPagePublicationError::Corrupt(message),
        }
    }
}
