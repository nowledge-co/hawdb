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

//! Private append segment mount using admitted I/O and descriptor units.

use super::*;

pub(in crate::append_table::segment) fn open(
    path: &Path,
    expected: AppendSegmentArtifactMetadata,
    config: AppendSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<AppendSegmentReader, AppendTableError> {
    work.checkpoint().map_err(work_error)?;
    validate_config(config)?;
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    let mut file = File::open(path)
        .map_err(|error| AppendTableError::from_io("open append segment", error))?;
    let encoded_len = usize::try_from(
        file.metadata()
            .map_err(|error| AppendTableError::from_io("read append segment metadata", error))?
            .len(),
    )
    .map_err(|_| {
        AppendTableError::Admission("append segment length overflows usize".to_string())
    })?;
    if encoded_len as u64 != expected.encoded_len {
        return Err(AppendTableError::Corruption(
            "append segment length does not match its canonical binding".to_string(),
        ));
    }
    let mut header_bytes = [0; SEGMENT_HEADER_BYTES];
    file.read_exact(&mut header_bytes)
        .map_err(|error| AppendTableError::from_io("read append segment header", error))?;
    let header = decode_segment_header(&header_bytes, encoded_len, config)?;
    drop(wave);
    unit.finish();

    let mut artifact_hasher = IntegrityHasher::new();
    artifact_hasher.update(&header_bytes);
    let mut body_hasher = IntegrityHasher::new();
    let mut remaining = encoded_len - SEGMENT_HEADER_BYTES;
    let mut buffer = vec![0; remaining.min(64 * 1024)];
    while remaining > 0 {
        let chunk_len = remaining.min(buffer.len());
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        file.read_exact(&mut buffer[..chunk_len])
            .map_err(|error| AppendTableError::from_io("stream append segment", error))?;
        artifact_hasher.update(&buffer[..chunk_len]);
        body_hasher.update(&buffer[..chunk_len]);
        remaining -= chunk_len;
        drop(wave);
        unit.finish();
    }
    let artifact_digest = artifact_hasher.finish();
    if artifact_digest.crc32c.get() != expected.encoded_crc32c
        || artifact_digest.sha256 != expected.encoded_sha256
    {
        return Err(AppendTableError::Corruption(
            "append segment does not match its canonical binding".to_string(),
        ));
    }
    let body_digest = body_hasher.finish();
    if body_digest.crc32c.get() != read_u32(&header_bytes, 52)
        || body_digest.sha256.as_bytes()
            != <&[u8; SHA256_BYTES]>::try_from(&header_bytes[56..88]).expect("fixed SHA-256")
    {
        return Err(AppendTableError::Corruption(
            "append segment checksum mismatch".to_string(),
        ));
    }

    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    file.seek(SeekFrom::Start(SEGMENT_HEADER_BYTES as u64))
        .map_err(|error| AppendTableError::from_io("seek append segment directory", error))?;
    let mut directory = Vec::with_capacity(header.directory_len);
    drop(wave);
    unit.finish();
    let mut buffer = [0; 64 * 1024];
    while directory.len() < header.directory_len {
        let chunk_len = (header.directory_len - directory.len()).min(buffer.len());
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        file.read_exact(&mut buffer[..chunk_len])
            .map_err(|error| AppendTableError::from_io("read append segment directory", error))?;
        directory.extend_from_slice(&buffer[..chunk_len]);
        drop(wave);
        unit.finish();
    }
    let descriptors = directory_decode(
        &directory,
        header.descriptor_count,
        header.payload_len,
        config,
        work,
    )?;
    let unit = work.start_unit().map_err(work_error)?;
    let reader = AppendSegmentReader {
        generation: header.generation,
        source_commit_epoch: header.source_commit_epoch,
        source: AppendSegmentSource::File(Arc::new(AppendSegmentFileSource {
            file: Mutex::new(file),
            encoded_len,
        })),
        payload_start: SEGMENT_HEADER_BYTES + header.directory_len,
        descriptors: descriptors.into(),
        config,
        decoded_block_cache: Arc::new(Mutex::new(AppendDecodedBlockCache::new(
            config.decoded_block_cache_bytes / 2,
        ))),
        read_result_cache: Arc::new(Mutex::new(AppendReadResultCache::new(
            config.decoded_block_cache_bytes - config.decoded_block_cache_bytes / 2,
        ))),
    };
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(reader)
}

pub(super) fn directory_decode(
    encoded: &[u8],
    descriptor_count: usize,
    payload_len: usize,
    config: AppendSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<Vec<AppendSegmentBlockDescriptor>, AppendTableError> {
    work.checkpoint().map_err(work_error)?;
    let unit = work.start_unit().map_err(work_error)?;
    let mut decoder = Decoder::new(encoded);
    let mut descriptors = Vec::with_capacity(descriptor_count);
    let mut row_count = 0usize;
    let mut expected_offset = 0u64;
    unit.finish();
    for _ in 0..descriptor_count {
        let unit = work.start_unit().map_err(work_error)?;
        let descriptor = AppendSegmentBlockDescriptor {
            table: decoder.string(config.max_key_bytes, "append block table")?,
            partition_key: decode_append_key(
                decoder.bytes(config.max_key_bytes, "append partition key")?,
                true,
            )?,
            min_order_key: decode_append_key(
                decoder.bytes(config.max_key_bytes, "append minimum order key")?,
                false,
            )?,
            max_order_key: decode_append_key(
                decoder.bytes(config.max_key_bytes, "append maximum order key")?,
                false,
            )?,
            row_count: decoder.u32()?,
            payload_offset: decoder.u64()?,
            compressed_bytes: decoder.u64()?,
            decoded_bytes: decoder.u64()?,
            payload_crc32c: decoder.u32()?,
            payload_sha256: Sha256Digest::from_bytes(
                decoder
                    .take(SHA256_BYTES, "append block digest")?
                    .try_into()
                    .expect("fixed SHA-256"),
            ),
        };
        if descriptor.table.is_empty()
            || descriptor.row_count == 0
            || descriptor.min_order_key > descriptor.max_order_key
            || descriptor.payload_offset != expected_offset
            || descriptor.compressed_bytes == 0
            || descriptor.decoded_bytes == 0
        {
            return Err(AppendTableError::Corruption(
                "append block descriptor has invalid bounds, counts, or offsets".to_string(),
            ));
        }
        row_count = row_count
            .checked_add(descriptor.row_count as usize)
            .ok_or_else(|| AppendTableError::Corruption("append row count overflow".to_string()))?;
        if row_count > config.max_rows {
            return Err(AppendTableError::Admission(format!(
                "append segment rows exceed limit {}",
                config.max_rows
            )));
        }
        expected_offset = expected_offset
            .checked_add(descriptor.compressed_bytes)
            .ok_or_else(|| {
                AppendTableError::Corruption("append payload offset overflow".to_string())
            })?;
        if descriptor.compressed_bytes as usize > config.max_compressed_block_bytes
            || descriptor.decoded_bytes as usize > config.max_decoded_block_bytes
        {
            return Err(AppendTableError::Admission(
                "append block exceeds compressed or decoded byte limits".to_string(),
            ));
        }
        descriptors.push(descriptor);
        unit.finish();
    }
    decoder.finish("append descriptor directory")?;
    if expected_offset != payload_len as u64 {
        return Err(AppendTableError::Corruption(
            "append descriptor directory has gaps, overlap, or unordered partitions".to_string(),
        ));
    }
    for pair in descriptors.windows(2) {
        let unit = work.start_unit().map_err(work_error)?;
        let ordered = pair[0].table < pair[1].table
            || (pair[0].table == pair[1].table
                && (pair[0].partition_key < pair[1].partition_key
                    || (pair[0].partition_key == pair[1].partition_key
                        && pair[0].max_order_key < pair[1].min_order_key)));
        if !ordered {
            return Err(AppendTableError::Corruption(
                "append descriptor directory has gaps, overlap, or unordered partitions"
                    .to_string(),
            ));
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(descriptors)
}
