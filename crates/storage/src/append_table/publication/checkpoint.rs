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

use super::*;
use crate::append_table::checkpoint::work_error;
use crate::file_io::OpenOptions;

pub(super) fn write_artifact(
    directory: &Path,
    file_name: &str,
    bytes: &[u8],
    work: Option<&CheckpointWorkContext>,
) -> Result<(), AppendTableError> {
    let Some(work) = work else {
        return write_durable_artifact(directory, file_name, bytes);
    };
    let destination = directory.join(file_name);
    let temporary = temporary_path(&destination);
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(durability("create append candidate"))?;
    // Own cleanup only after exclusive creation succeeds. An earlier
    // interrupted candidate's temporary artifact remains evidence.
    let mut cleanup = TemporaryArtifact(Some(temporary.clone()));
    drop(wave);
    unit.finish();
    for block in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        file.write_all(block)
            .map_err(durability("write append candidate"))?;
        drop(wave);
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    file.sync_all()
        .map_err(durability("synchronize append candidate"))?;
    drop(file);
    drop(wave);
    unit.finish();
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    durable_replace_file(&temporary, &destination)
        .map_err(durability("publish append candidate"))?;
    cleanup.0 = None;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(work_error)
}

struct TemporaryArtifact(Option<PathBuf>);

impl Drop for TemporaryArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

pub(super) fn copy_segment_bindings(
    previous: Option<&AppendGenerationReader>,
    work: Option<&CheckpointWorkContext>,
) -> Result<Vec<AppendSegmentBinding>, AppendTableError> {
    let Some(previous) = previous else {
        return Ok(Vec::new());
    };
    let Some(work) = work else {
        return Ok(previous.manifest.segments.clone());
    };
    let mut bindings = Vec::new();
    for chunk in previous.manifest.segments.chunks(1024) {
        let unit = work.start_unit().map_err(work_error)?;
        bindings.extend_from_slice(chunk);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(bindings)
}

pub(super) fn copy_schemas(
    source: &BTreeMap<String, AppendTableSchema>,
    work: Option<&CheckpointWorkContext>,
) -> Result<BTreeMap<String, AppendTableSchema>, AppendTableError> {
    let Some(work) = work else {
        return Ok(source.clone());
    };
    let mut copy = BTreeMap::new();
    for (key, schema) in source {
        let unit = work.start_unit().map_err(work_error)?;
        copy.insert(key.clone(), schema.clone());
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(copy)
}

pub(super) fn copy_watermarks(
    source: &BTreeMap<String, i64>,
    work: Option<&CheckpointWorkContext>,
) -> Result<BTreeMap<String, i64>, AppendTableError> {
    let Some(work) = work else {
        return Ok(source.clone());
    };
    let mut copy = BTreeMap::new();
    for (table, watermark) in source {
        let unit = work.start_unit().map_err(work_error)?;
        copy.insert(table.clone(), *watermark);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(copy)
}

fn copy_bytes(
    target: &mut Vec<u8>,
    source: &[u8],
    work: &CheckpointWorkContext,
) -> Result<(), AppendTableError> {
    for bytes in source.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        target.extend_from_slice(bytes);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

fn encode_bytes(
    encoder: &mut Encoder,
    bytes: &[u8],
    context: &str,
    work: &CheckpointWorkContext,
) -> Result<(), AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    let len = u64::try_from(bytes.len())
        .map_err(|_| AppendTableError::Admission(format!("{context} length overflows u64")))?;
    encoder.u64(len);
    unit.finish();
    for chunk in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        encoder.raw(chunk);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(super) fn manifest_payload(
    manifest: &AppendGenerationManifest,
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, AppendTableError> {
    let mut encoder = Encoder::default();
    for schema in manifest.schemas.values() {
        let unit = work.start_unit().map_err(work_error)?;
        let encoded = encode_append_wal_batch(
            0,
            &AppendTransaction {
                writes: vec![AppendWrite::CreateTable {
                    schema: schema.clone(),
                }],
            },
        )?;
        if encoded.len() > config.max_schema_bytes {
            return Err(AppendTableError::Admission(format!(
                "append schema {} contains {} bytes, exceeding limit {}",
                schema.name,
                encoded.len(),
                config.max_schema_bytes,
            )));
        }
        unit.finish();
        encode_bytes(
            &mut encoder,
            &encoded,
            "append manifest schema record",
            work,
        )?;
    }
    for (table, watermark) in &manifest.generated_order_watermarks {
        encode_bytes(
            &mut encoder,
            table.as_bytes(),
            "append generated-order table",
            work,
        )?;
        let unit = work.start_unit().map_err(work_error)?;
        encoder.raw(&watermark.to_le_bytes());
        unit.finish();
    }
    for binding in &manifest.segments {
        let unit = work.start_unit().map_err(work_error)?;
        encoder.u64(binding.generation);
        encoder.u64(binding.source_commit_epoch);
        encode_artifact(binding.artifact, &mut encoder);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoder.finish())
}

pub(super) fn manifest_envelope(
    manifest: &AppendGenerationManifest,
    payload: &[u8],
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    let encoded_len = MANIFEST_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| AppendTableError::Admission("append manifest size overflow".to_string()))?;
    if encoded_len > config.max_manifest_bytes {
        return Err(AppendTableError::Admission(format!(
            "append manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(MANIFEST_MAGIC);
    encoded.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&manifest.generation.to_le_bytes());
    encoded.extend_from_slice(&manifest.source_commit_epoch.to_le_bytes());
    encoded.extend_from_slice(&manifest.previous_generation.unwrap_or(0).to_le_bytes());
    encoded.extend_from_slice(&(manifest.schemas.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&(manifest.segments.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    encoded.extend_from_slice(manifest.root_set_digest.as_bytes());
    encoded.extend_from_slice(&0u32.to_le_bytes());
    encoded.extend_from_slice(&[0u8; SHA256_BYTES]);
    encoded.extend_from_slice(&(manifest.generated_order_watermarks.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&0u32.to_le_bytes());
    debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);

    unit.finish();
    copy_bytes(&mut encoded, payload, work)?;
    let mut hasher = hawdb_integrity::IntegrityHasher::new();
    for source in [
        &encoded[..MANIFEST_INTEGRITY_OFFSET],
        &encoded[MANIFEST_INTEGRITY_TRAILER_OFFSET..MANIFEST_HEADER_BYTES],
        payload,
    ] {
        for bytes in source.chunks(64 * 1024) {
            let unit = work.start_unit().map_err(work_error)?;
            hasher.update(bytes);
            unit.finish();
        }
    }
    work.checkpoint().map_err(work_error)?;
    let unit = work.start_unit().map_err(work_error)?;
    let digest = hasher.finish();
    encoded[84..88].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[88..120].copy_from_slice(digest.sha256.as_bytes());
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}

#[cfg(test)]
mod tests;
