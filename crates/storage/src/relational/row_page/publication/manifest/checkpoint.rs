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

//! Controlled row-root manifest encoding; the ordinary codec is independent.
//! Encoded buffers, allocator/capacity/drop and shared hard resources remain open.

use super::super::root::checkpoint::{hash, work_error};
use super::*;
use crate::background::CheckpointWorkContext;

pub(super) fn read_encoded(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    use super::super::publisher::checkpoint::io;
    let max_bytes = config.max_manifest_bytes.get();
    let max_bytes_u64 = u64::try_from(max_bytes).map_err(|_| {
        RelationalRowPagePublicationError::Admission("row-page manifest limit overflows u64".into())
    })?;
    let read_limit = max_bytes_u64.checked_add(1).ok_or_else(|| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest read limit overflows u64".into(),
        )
    })?;
    let file = io(Some(work), || {
        File::open(path).map_err(durability("open row-page manifest"))
    })?;
    let encoded_len = io(Some(work), || {
        file.metadata()
            .map(|metadata| metadata.len())
            .map_err(durability("read row-page manifest metadata"))
    })?;
    if encoded_len > max_bytes_u64 {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let capacity = usize::try_from(encoded_len).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest length overflows usize".into(),
        )
    })?;
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoded = Vec::with_capacity(capacity);
    let mut buffer = [0; 64 * 1024];
    unit.finish();
    let mut file = file.take(read_limit);
    loop {
        let length = match io(Some(work), || Ok(file.read(&mut buffer)))? {
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(durability("read row-page manifest")(error)),
        };
        if length == 0 {
            break;
        }
        append(&mut encoded, &buffer[..length], work)?;
    }
    if encoded.len() > max_bytes {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest exceeds limit {}",
            config.max_manifest_bytes
        )));
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}

pub(super) fn decode_text(
    bytes: &[u8],
    context: &str,
    work: &CheckpointWorkContext,
) -> Result<String, RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let mut output = String::with_capacity(bytes.len());
    unit.finish();
    let mut start = 0;
    while start < bytes.len() {
        let end = start.saturating_add(64 * 1024).min(bytes.len());
        let unit = work.start_unit().map_err(work_error)?;
        match std::str::from_utf8(&bytes[start..end]) {
            Ok(text) => {
                output.push_str(text);
                start = end;
            }
            Err(error) if error.error_len().is_none() && end < bytes.len() => {
                let valid = error.valid_up_to();
                output.push_str(
                    std::str::from_utf8(&bytes[start..start + valid])
                        .expect("UTF-8 error's valid prefix is valid"),
                );
                start += valid;
            }
            Err(error) => {
                let position = start + error.valid_up_to();
                let description = match error.error_len() {
                    Some(len) => {
                        format!("invalid utf-8 sequence of {len} bytes from index {position}")
                    }
                    None => format!("incomplete utf-8 byte sequence from index {position}"),
                };
                return Err(RelationalRowPagePublicationError::Corrupt(format!(
                    "row-page {context} is not valid UTF-8: {description}"
                )));
            }
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(output)
}

pub(in crate::relational::row_page::publication) fn encode_manifest_with_work_context(
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    validate_manifest_inner(manifest, config, ErrorClass::Admission, Some(work))?;
    let mut payload = encode_tables(&manifest.tables, work)?;
    let unit = work.start_unit().map_err(work_error)?;
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
    unit.finish();
    for entry in &manifest.physical_generations {
        let unit = work.start_unit().map_err(work_error)?;
        payload.extend_from_slice(&entry.generation.to_le_bytes());
        payload.extend_from_slice(&entry.allocated_pages.to_le_bytes());
        payload.extend_from_slice(&entry.live_pages.to_le_bytes());
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
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
    unit.finish();
    append(&mut encoded, &payload, work)?;

    let mut hasher = IntegrityHasher::new();
    hash(&mut hasher, &encoded[..MANIFEST_INTEGRITY_OFFSET], work)?;
    hash(&mut hasher, &payload, work)?;
    let unit = work.start_unit().map_err(work_error)?;
    let digest = hasher.finish();
    encoded[280..284].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[284..316].copy_from_slice(digest.sha256.as_bytes());
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}

pub(in crate::relational::row_page::publication) fn root_set_digest_with_work_context(
    tables: &[RelationalRowPageTableRoot],
    work: &CheckpointWorkContext,
) -> Result<Sha256Digest, RelationalRowPagePublicationError> {
    let mut hasher = IntegrityHasher::new();
    for table in tables {
        let encoded = encode_tables(std::slice::from_ref(table), work)?;
        hash(&mut hasher, &encoded, work)?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(hasher.finish().sha256)
}

fn encode_tables(
    tables: &[RelationalRowPageTableRoot],
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let mut encoded = Vec::new();
    for table in tables {
        field(table.table.as_bytes(), &mut encoded, work)?;
        let schema = crate::relational::codec::encode_relational_table_schema_with_work_context(
            &table.schema,
            work,
        )
        .map_err(map_schema_encode_error)?;
        field(&schema, &mut encoded, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        encoded.extend_from_slice(table.schema_digest.as_bytes());
        encoded.extend_from_slice(&table.column_count.get().to_le_bytes());
        encoded.extend_from_slice(&table.row_count.to_le_bytes());
        encoded.extend_from_slice(&table.next_page_id.get().to_le_bytes());
        encoded.extend_from_slice(&table.first_descriptor.to_le_bytes());
        encoded.extend_from_slice(&table.page_count.to_le_bytes());
        unit.finish();
        field(&table.lower_bound, &mut encoded, work)?;
        field(&table.upper_bound, &mut encoded, work)?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}

fn field(
    bytes: &[u8],
    encoded: &mut Vec<u8>,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest field does not fit in u32".into(),
        )
    })?;
    encoded.extend_from_slice(&len.to_le_bytes());
    unit.finish();
    append(encoded, bytes, work)
}

fn append(
    encoded: &mut Vec<u8>,
    bytes: &[u8],
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    crate::relational::row_page::checkpoint::append(encoded, bytes, work).map_err(Into::into)
}
