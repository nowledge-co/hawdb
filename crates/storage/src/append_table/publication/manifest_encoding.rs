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

//! Shared manifest payload grammar; checkpoint output owns its byte admission.

use super::*;
use crate::append_table::binary::map_relational;
use crate::background::CheckpointBytes;
use crate::relational::{schema_encoding::Sink, RelationalError};

#[derive(Clone, Copy)]
pub(super) struct Manifest<'a> {
    pub(super) generation: u64,
    pub(super) source_commit_epoch: u64,
    pub(super) previous_generation: Option<u64>,
    pub(super) schemas: &'a BTreeMap<String, AppendTableSchema>,
    pub(super) generated_order_watermarks: &'a BTreeMap<String, i64>,
    pub(super) segments: &'a [AppendSegmentBinding],
    pub(super) tail: &'a [AppendSegmentBinding],
    pub(super) root_set_digest: Sha256Digest,
}

impl<'a> From<&'a AppendGenerationManifest> for Manifest<'a> {
    fn from(manifest: &'a AppendGenerationManifest) -> Self {
        Self {
            generation: manifest.generation,
            source_commit_epoch: manifest.source_commit_epoch,
            previous_generation: manifest.previous_generation,
            schemas: &manifest.schemas,
            generated_order_watermarks: &manifest.generated_order_watermarks,
            segments: &manifest.segments,
            tail: &[],
            root_set_digest: manifest.root_set_digest,
        }
    }
}

impl<'a> From<&Manifest<'a>> for Manifest<'a> {
    fn from(manifest: &Manifest<'a>) -> Self {
        *manifest
    }
}

impl Manifest<'_> {
    pub(super) fn segment_count(&self) -> Result<usize, AppendTableError> {
        self.segments
            .len()
            .checked_add(self.tail.len())
            .ok_or_else(size_error)
    }
}

pub(super) fn length(
    manifest: &Manifest<'_>,
    config: AppendPublicationConfig,
    work: Option<&CheckpointWorkContext>,
) -> Result<usize, AppendTableError> {
    let mut len = 0usize;
    for schema in manifest.schemas.values() {
        let bytes = super::super::codec::schema_record_len(schema, work)?;
        if bytes > config.max_schema_bytes {
            return Err(AppendTableError::Admission(format!(
                "append schema {} contains {bytes} bytes, exceeding limit {}",
                schema.name, config.max_schema_bytes,
            )));
        }
        len = len
            .checked_add(8)
            .and_then(|len| len.checked_add(bytes))
            .ok_or_else(size_error)?;
    }
    for table in manifest.generated_order_watermarks.keys() {
        if let Some(work) = work {
            work.start_unit()
                .map_err(super::super::checkpoint::work_error)?
                .finish();
        }
        len = len
            .checked_add(16)
            .and_then(|len| len.checked_add(table.len()))
            .ok_or_else(size_error)?;
    }
    manifest
        .segment_count()?
        .checked_mul(60)
        .and_then(|bytes| len.checked_add(bytes))
        .ok_or_else(size_error)
}

fn size_error() -> AppendTableError {
    AppendTableError::Admission("append manifest size overflow".into())
}

pub(super) fn payload(
    sink: &mut impl Sink,
    manifest: &Manifest<'_>,
    work: Option<&CheckpointWorkContext>,
) -> Result<(), AppendTableError> {
    for schema in manifest.schemas.values() {
        let len = super::super::codec::schema_record_len(schema, work)?;
        sink.raw(&(len as u64).to_le_bytes())
            .map_err(map_relational)?;
        super::super::codec::schema_record(sink, schema, len, work)?;
    }
    for (table, watermark) in manifest.generated_order_watermarks {
        sink.raw(&(table.len() as u64).to_le_bytes())
            .map_err(map_relational)?;
        sink.raw(table.as_bytes()).map_err(map_relational)?;
        sink.raw(&watermark.to_le_bytes()).map_err(map_relational)?;
    }
    for binding in manifest.segments.iter().chain(manifest.tail) {
        sink.raw(&binding.generation.to_le_bytes())
            .map_err(map_relational)?;
        sink.raw(&binding.source_commit_epoch.to_le_bytes())
            .map_err(map_relational)?;
        sink.raw(&binding.artifact.encoded_len.to_le_bytes())
            .map_err(map_relational)?;
        sink.raw(&binding.artifact.encoded_crc32c.to_le_bytes())
            .map_err(map_relational)?;
        sink.raw(binding.artifact.encoded_sha256.as_bytes())
            .map_err(map_relational)?;
    }
    Ok(())
}

pub(super) struct Admitted<'a> {
    pub(super) bytes: CheckpointBytes,
    pub(super) work: &'a CheckpointWorkContext,
}

impl Sink for Admitted<'_> {
    fn raw(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        self.bytes
            .append(bytes, self.work)
            .map_err(RelationalError::from_work)
    }
}

impl Sink for Encoder {
    fn raw(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        Encoder::raw(self, bytes);
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn admitted(
    manifest: &Manifest<'_>,
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<CheckpointBytes, AppendTableError> {
    let len = length(manifest, config, Some(work))?;
    let bytes = CheckpointBytes::new(len, work).map_err(super::super::checkpoint::work_error)?;
    let mut sink = Admitted { bytes, work };
    payload(&mut sink, manifest, Some(work))?;
    work.checkpoint()
        .map_err(super::super::checkpoint::work_error)?;
    debug_assert_eq!(sink.bytes.len(), len);
    Ok(sink.bytes)
}

pub(super) fn header(
    manifest: &Manifest<'_>,
    payload_len: usize,
) -> Result<[u8; MANIFEST_HEADER_BYTES], AppendTableError> {
    let mut header = [0u8; MANIFEST_HEADER_BYTES];
    header[..8].copy_from_slice(MANIFEST_MAGIC);
    header[8..10].copy_from_slice(&MANIFEST_VERSION.to_le_bytes());
    header[12..20].copy_from_slice(&manifest.generation.to_le_bytes());
    header[20..28].copy_from_slice(&manifest.source_commit_epoch.to_le_bytes());
    header[28..36].copy_from_slice(&manifest.previous_generation.unwrap_or(0).to_le_bytes());
    header[36..40].copy_from_slice(&(manifest.schemas.len() as u32).to_le_bytes());
    header[40..44].copy_from_slice(&(manifest.segment_count()? as u32).to_le_bytes());
    header[44..52].copy_from_slice(&(payload_len as u64).to_le_bytes());
    header[52..84].copy_from_slice(manifest.root_set_digest.as_bytes());
    header[120..124]
        .copy_from_slice(&(manifest.generated_order_watermarks.len() as u32).to_le_bytes());
    Ok(header)
}

pub(super) fn seal(
    encoded: &mut CheckpointBytes,
    work: &CheckpointWorkContext,
) -> Result<(), AppendTableError> {
    let mut hasher = hawdb_integrity::IntegrityHasher::new();
    for source in [
        &encoded[..MANIFEST_INTEGRITY_OFFSET],
        &encoded[MANIFEST_INTEGRITY_TRAILER_OFFSET..MANIFEST_HEADER_BYTES],
        &encoded[MANIFEST_HEADER_BYTES..],
    ] {
        for bytes in source.chunks(64 * 1024) {
            let unit = work
                .start_unit()
                .map_err(super::super::checkpoint::work_error)?;
            hasher.update(bytes);
            unit.finish();
        }
    }
    work.checkpoint()
        .map_err(super::super::checkpoint::work_error)?;
    let unit = work
        .start_unit()
        .map_err(super::super::checkpoint::work_error)?;
    let digest = hasher.finish();
    encoded.as_mut_slice()[84..88].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded.as_mut_slice()[88..120].copy_from_slice(digest.sha256.as_bytes());
    unit.finish();
    work.checkpoint()
        .map_err(super::super::checkpoint::work_error)
}

pub(super) fn encode(
    manifest: &mut Manifest<'_>,
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<CheckpointBytes, AppendTableError> {
    let payload_len = length(manifest, config, Some(work))?;
    let total = MANIFEST_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(size_error)?;
    if total > config.max_manifest_bytes {
        return Err(AppendTableError::Admission(format!(
            "append manifest contains {total} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let bytes = CheckpointBytes::new(total, work).map_err(super::super::checkpoint::work_error)?;
    let mut sink = Admitted { bytes, work };
    sink.bytes
        .append(&[0u8; MANIFEST_HEADER_BYTES], work)
        .map_err(super::super::checkpoint::work_error)?;
    payload(&mut sink, manifest, Some(work))?;
    let digest = work
        .integrity(&sink.bytes[MANIFEST_HEADER_BYTES..])
        .map_err(super::super::checkpoint::work_error)?
        .sha256;
    let mut selected = *manifest;
    selected.root_set_digest = digest;
    sink.bytes.as_mut_slice()[..MANIFEST_HEADER_BYTES]
        .copy_from_slice(&header(&selected, payload_len)?);
    seal(&mut sink.bytes, work)?;
    manifest.root_set_digest = digest;
    Ok(sink.bytes)
}
