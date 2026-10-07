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

//! Cooperative private append generation mounting.

use super::*;

pub(in crate::append_table::publication) fn open(
    directory: &Path,
    expected: AppendGenerationArtifacts,
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<AppendGenerationReader, AppendTableError> {
    work.checkpoint().map_err(work_error)?;
    validate_config(config)?;
    let manifest_path = directory.join(append_generation_manifest_file(expected.generation));
    let encoded = read_artifact(
        &manifest_path,
        config.max_manifest_bytes,
        "append generation manifest",
        work,
    )?;
    let digest = work.integrity(&encoded).map_err(work_error)?;
    if encoded.len() as u64 != expected.manifest_artifact.encoded_len
        || digest.crc32c.get() != expected.manifest_artifact.encoded_crc32c
        || digest.sha256 != expected.manifest_artifact.encoded_sha256
    {
        return Err(AppendTableError::Corruption(
            "append generation manifest does not match its canonical binding".to_string(),
        ));
    }
    let manifest = manifest_decode(&encoded, config, work)?;
    if manifest.generation != expected.generation
        || manifest.source_commit_epoch != expected.source_commit_epoch
        || manifest.root_set_digest != expected.root_set_digest
    {
        return Err(AppendTableError::Corruption(
            "append generation manifest does not match its canonical binding".to_string(),
        ));
    }

    let unit = work.start_unit().map_err(work_error)?;
    let mut readers = Vec::with_capacity(manifest.segments.len());
    let mut watermarks: BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>> = BTreeMap::new();
    unit.finish();
    for binding in &manifest.segments {
        let unit = work.start_unit().map_err(work_error)?;
        let path = directory.join(append_segment_file(binding.generation));
        let encoded_len = usize::try_from(binding.artifact.encoded_len).map_err(|_| {
            AppendTableError::Admission("append segment length overflows usize".to_string())
        })?;
        if encoded_len > config.segment.max_segment_bytes {
            return Err(AppendTableError::Admission(format!(
                "bound append segment contains {encoded_len} bytes, exceeding limit {}",
                config.segment.max_segment_bytes
            )));
        }
        unit.finish();
        let reader = AppendSegmentReader::open_bound_file_with_work_context(
            &path,
            binding.artifact,
            config.segment,
            work,
        )?;
        if reader.generation() != binding.generation
            || reader.source_commit_epoch() != binding.source_commit_epoch
        {
            return Err(AppendTableError::Corruption(
                "append segment identity differs from its generation binding".to_string(),
            ));
        }
        for descriptor in reader.descriptors() {
            let unit = work.start_unit().map_err(work_error)?;
            if !manifest.schemas.contains_key(&descriptor.table) {
                return Err(AppendTableError::Corruption(format!(
                    "append segment references unknown table {}",
                    descriptor.table
                )));
            }
            let partition_watermarks = watermarks.entry(descriptor.table.clone()).or_default();
            let prior = partition_watermarks.get(&descriptor.partition_key);
            if prior.is_some_and(|prior| prior >= &descriptor.min_order_key) {
                return Err(AppendTableError::Corruption(format!(
                    "append segments overlap or regress table {} partition watermark",
                    descriptor.table
                )));
            }
            partition_watermarks.insert(
                descriptor.partition_key.clone(),
                descriptor.max_order_key.clone(),
            );
            unit.finish();
        }
        let unit = work.start_unit().map_err(work_error)?;
        readers.push(reader);
        unit.finish();
    }
    validate_generated_watermarks(
        &manifest.schemas,
        &watermarks,
        &manifest.generated_order_watermarks,
        work,
    )?;
    let unit = work.start_unit().map_err(work_error)?;
    let reader = AppendGenerationReader {
        manifest: Arc::new(manifest),
        segments: readers.into(),
    };
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(reader)
}

pub(super) fn manifest_decode(
    encoded: &[u8],
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<AppendGenerationManifest, AppendTableError> {
    work.checkpoint().map_err(work_error)?;
    if encoded.len() < MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
        return Err(AppendTableError::Corruption(
            "append manifest header is invalid".to_string(),
        ));
    }
    let version = read_u16(encoded, 8);
    if version != MANIFEST_VERSION || read_u16(encoded, 10) != 0 || read_u32(encoded, 124) != 0 {
        return Err(AppendTableError::Corruption(format!(
            "unsupported append manifest version {version} or non-zero reserved fields"
        )));
    }
    let generation = read_u64(encoded, 12);
    let source_commit_epoch = read_u64(encoded, 20);
    let previous_raw = read_u64(encoded, 28);
    let schema_count = read_u32(encoded, 36) as usize;
    let segment_count = read_u32(encoded, 40) as usize;
    let generated_order_watermark_count = read_u32(encoded, 120) as usize;
    let payload_len = to_usize(read_u64(encoded, 44), "manifest payload length")?;
    if generation == 0
        || (source_commit_epoch == 0 && (schema_count != 0 || segment_count != 0))
        || schema_count > config.max_schemas
        || generated_order_watermark_count > config.max_schemas
        || segment_count > config.max_segments
        || MANIFEST_HEADER_BYTES.checked_add(payload_len) != Some(encoded.len())
    {
        return Err(AppendTableError::Corruption(
            "append manifest identity, counts, or payload length is invalid".to_string(),
        ));
    }
    let root_set_digest =
        Sha256Digest::from_bytes(encoded[52..84].try_into().expect("fixed root digest"));
    let payload = &encoded[MANIFEST_HEADER_BYTES..];
    if work.integrity(payload).map_err(work_error)?.sha256 != root_set_digest {
        return Err(AppendTableError::Corruption(
            "append manifest root-set digest mismatch".to_string(),
        ));
    }
    let mut hasher = hawdb_integrity::IntegrityHasher::new();
    for source in [
        &encoded[..MANIFEST_INTEGRITY_OFFSET],
        &encoded[MANIFEST_INTEGRITY_TRAILER_OFFSET..MANIFEST_HEADER_BYTES],
        payload,
    ] {
        for chunk in source.chunks(64 * 1024) {
            let unit = work.start_unit().map_err(work_error)?;
            hasher.update(chunk);
            unit.finish();
        }
    }
    let digest = hasher.finish();
    let expected_crc = read_u32(encoded, 84);
    let expected_sha: &[u8; SHA256_BYTES] = encoded[88..120].try_into().expect("fixed digest");
    if digest.crc32c.get() != expected_crc || digest.sha256.as_bytes() != expected_sha {
        return Err(AppendTableError::Corruption(
            "append manifest checksum mismatch".to_string(),
        ));
    }
    let mut decoder = Decoder::new(payload);
    let mut schemas = BTreeMap::new();
    for _ in 0..schema_count {
        let unit = work.start_unit().map_err(work_error)?;
        let schema_record = decoder.bytes(config.max_schema_bytes, "append schema")?;
        let batch = decode_append_wal_batch(schema_record, AppendDecodeLimits::wal())?;
        let [AppendWrite::CreateTable { schema }] = batch.transaction.writes.as_slice() else {
            return Err(AppendTableError::Corruption(
                "append manifest schema record has an invalid shape".to_string(),
            ));
        };
        if batch.epoch != 0
            || schemas
                .insert(schema.name.clone(), schema.clone())
                .is_some()
        {
            return Err(AppendTableError::Corruption(
                "append manifest schema record has a non-zero epoch or duplicate name".to_string(),
            ));
        }
        unit.finish();
    }
    let mut generated_order_watermarks = BTreeMap::new();
    for _ in 0..generated_order_watermark_count {
        let unit = work.start_unit().map_err(work_error)?;
        let table = decoder.string(config.max_schema_bytes, "append generated-order table")?;
        let watermark = i64::from_le_bytes(
            decoder
                .take(
                    std::mem::size_of::<i64>(),
                    "append generated-order watermark",
                )?
                .try_into()
                .expect("fixed i64"),
        );
        if generated_order_watermarks
            .insert(table, watermark)
            .is_some()
        {
            return Err(AppendTableError::Corruption(
                "append manifest contains duplicate generated-order watermarks".to_string(),
            ));
        }
        unit.finish();
    }
    let mut segments = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        let unit = work.start_unit().map_err(work_error)?;
        segments.push(AppendSegmentBinding {
            generation: decoder.u64()?,
            source_commit_epoch: decoder.u64()?,
            artifact: decode_artifact(&mut decoder)?,
        });
        unit.finish();
    }
    decoder.finish("append manifest")?;
    for pair in segments.windows(2) {
        let unit = work.start_unit().map_err(work_error)?;
        let ordered = pair[0].generation < pair[1].generation
            && pair[0].source_commit_epoch <= pair[1].source_commit_epoch;
        if !ordered {
            return Err(AppendTableError::Corruption(
                "append manifest segment bindings are unordered or newer than the generation"
                    .to_string(),
            ));
        }
        unit.finish();
    }
    if segments.last().is_some_and(|binding| {
        binding.generation > generation || binding.source_commit_epoch > source_commit_epoch
    }) {
        return Err(AppendTableError::Corruption(
            "append manifest segment bindings are unordered or newer than the generation"
                .to_string(),
        ));
    }
    let previous_generation = (previous_raw != 0).then_some(previous_raw);
    if previous_generation.is_some_and(|previous| previous >= generation) {
        return Err(AppendTableError::Corruption(
            "append manifest previous generation does not precede the current generation"
                .to_string(),
        ));
    }
    work.checkpoint().map_err(work_error)?;
    Ok(AppendGenerationManifest {
        generation,
        source_commit_epoch,
        previous_generation,
        root_set_digest,
        schemas,
        generated_order_watermarks,
        segments,
    })
}

fn read_artifact(
    path: &Path,
    max_bytes: usize,
    context: &str,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    let file = File::open(path).map_err(durability("open append artifact"))?;
    let encoded_len = file
        .metadata()
        .map_err(durability("read append artifact metadata"))?
        .len();
    if encoded_len > max_bytes as u64 {
        return Err(AppendTableError::Admission(format!(
            "{context} contains {encoded_len} bytes, exceeding limit {max_bytes}"
        )));
    }
    let mut encoded = Vec::with_capacity(encoded_len as usize);
    let mut reader = file.take((max_bytes as u64).saturating_add(1));
    drop(wave);
    unit.finish();
    let mut buffer = [0; 64 * 1024];
    loop {
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        let count = reader
            .read(&mut buffer)
            .map_err(durability("read append artifact"))?;
        encoded.extend_from_slice(&buffer[..count]);
        drop(wave);
        unit.finish();
        if count == 0 {
            break;
        }
    }
    if encoded.len() > max_bytes {
        return Err(AppendTableError::Admission(format!(
            "{context} exceeds limit {max_bytes}"
        )));
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}
