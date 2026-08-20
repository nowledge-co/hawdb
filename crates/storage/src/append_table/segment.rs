use super::binary::{
    compare_rows, map_relational, read_u16_at as read_u16, read_u32_at as read_u32,
    read_u64_at as read_u64, to_usize, Decoder, Encoder,
};
use super::{AppendTableError, AppendTableRow};
use crate::relational::overflow::{decode_overflow_envelope, encode_overflow_envelope};
use crate::relational::{decode_relational_row_payload, encode_relational_row_payload};
use crate::{
    decode_relational_primary_key, encode_relational_primary_key, RelationalHydrationBudget,
    RelationalKey, RelationalOverflowConfig, RelationalRow, RelationalScalarType, RelationalValue,
};
use skein_integrity::{integrity_digest, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

const SEGMENT_MAGIC: &[u8; 8] = b"SKAPSEG1";
const SEGMENT_VERSION: u16 = 2;
const SEGMENT_HEADER_BYTES: usize = 96;
const GLOBAL_PARTITION_TAG: u8 = 0;
const ORDERED_KEY_TAG: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendSegmentConfig {
    pub compression_level: i32,
    pub max_segment_bytes: usize,
    pub max_blocks: usize,
    pub max_rows: usize,
    pub max_values: usize,
    pub max_key_bytes: usize,
    pub max_row_bytes: usize,
    pub overflow_threshold_bytes: usize,
    pub overflow_compression_level: i32,
    pub target_decoded_block_bytes: usize,
    pub max_compressed_block_bytes: usize,
    pub max_decoded_block_bytes: usize,
    pub decoded_block_cache_bytes: usize,
}

impl Default for AppendSegmentConfig {
    fn default() -> Self {
        Self {
            compression_level: 3,
            max_segment_bytes: 1024 * 1024 * 1024,
            max_blocks: 1_000_000,
            max_rows: 10_000_000,
            max_values: 100_000_000,
            max_key_bytes: 1024 * 1024,
            max_row_bytes: 64 * 1024 * 1024,
            overflow_threshold_bytes: 4 * 1024,
            overflow_compression_level: 3,
            target_decoded_block_bytes: 1024 * 1024,
            max_compressed_block_bytes: 64 * 1024 * 1024,
            max_decoded_block_bytes: 256 * 1024 * 1024,
            decoded_block_cache_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendSegmentArtifactMetadata {
    pub encoded_len: u64,
    pub encoded_crc32c: u32,
    pub encoded_sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSegmentBlockDescriptor {
    pub table: String,
    pub partition_key: RelationalKey,
    pub min_order_key: RelationalKey,
    pub max_order_key: RelationalKey,
    pub row_count: u32,
    pub payload_offset: u64,
    pub compressed_bytes: u64,
    pub decoded_bytes: u64,
    pub payload_crc32c: u32,
    pub payload_sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSegmentWriteOutput {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub encoded: Arc<[u8]>,
    pub descriptors: Vec<AppendSegmentBlockDescriptor>,
    pub artifact: AppendSegmentArtifactMetadata,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppendSegmentReadReport {
    pub segments_examined: usize,
    pub segments_pruned: usize,
    pub blocks_read: usize,
    pub compressed_bytes_read: usize,
    pub decoded_bytes: usize,
    pub rows_decoded: usize,
    pub rows_returned: usize,
    pub output_payload_bytes: usize,
    pub live_batches_examined: usize,
    pub live_batches_pruned: usize,
    pub live_rows_examined: usize,
    pub overflow_values_hydrated: usize,
    pub overflow_compressed_bytes: usize,
    pub overflow_decompressed_bytes: usize,
    pub decoded_block_cache_hits: usize,
    pub decoded_block_cache_resident_bytes: usize,
    pub read_result_cache_hits: usize,
    pub read_result_cache_resident_bytes: usize,
}

impl AppendSegmentReadReport {
    pub(super) fn merge(&mut self, other: Self) {
        self.segments_examined = self
            .segments_examined
            .saturating_add(other.segments_examined);
        self.segments_pruned = self.segments_pruned.saturating_add(other.segments_pruned);
        self.blocks_read = self.blocks_read.saturating_add(other.blocks_read);
        self.compressed_bytes_read = self
            .compressed_bytes_read
            .saturating_add(other.compressed_bytes_read);
        self.decoded_bytes = self.decoded_bytes.saturating_add(other.decoded_bytes);
        self.rows_decoded = self.rows_decoded.saturating_add(other.rows_decoded);
        self.rows_returned = self.rows_returned.saturating_add(other.rows_returned);
        self.output_payload_bytes = self
            .output_payload_bytes
            .saturating_add(other.output_payload_bytes);
        self.live_batches_examined = self
            .live_batches_examined
            .saturating_add(other.live_batches_examined);
        self.live_batches_pruned = self
            .live_batches_pruned
            .saturating_add(other.live_batches_pruned);
        self.live_rows_examined = self
            .live_rows_examined
            .saturating_add(other.live_rows_examined);
        self.overflow_values_hydrated = self
            .overflow_values_hydrated
            .saturating_add(other.overflow_values_hydrated);
        self.overflow_compressed_bytes = self
            .overflow_compressed_bytes
            .saturating_add(other.overflow_compressed_bytes);
        self.overflow_decompressed_bytes = self
            .overflow_decompressed_bytes
            .saturating_add(other.overflow_decompressed_bytes);
        self.decoded_block_cache_hits = self
            .decoded_block_cache_hits
            .saturating_add(other.decoded_block_cache_hits);
        self.decoded_block_cache_resident_bytes = self
            .decoded_block_cache_resident_bytes
            .saturating_add(other.decoded_block_cache_resident_bytes);
        self.read_result_cache_hits = self
            .read_result_cache_hits
            .saturating_add(other.read_result_cache_hits);
        self.read_result_cache_resident_bytes = self
            .read_result_cache_resident_bytes
            .saturating_add(other.read_result_cache_resident_bytes);
    }

    fn merge_block(&mut self, mut other: Self) {
        let decoded_cache_resident_bytes = self
            .decoded_block_cache_resident_bytes
            .max(other.decoded_block_cache_resident_bytes);
        let read_cache_resident_bytes = self
            .read_result_cache_resident_bytes
            .max(other.read_result_cache_resident_bytes);
        other.decoded_block_cache_resident_bytes = 0;
        other.read_result_cache_resident_bytes = 0;
        self.merge(other);
        self.decoded_block_cache_resident_bytes = decoded_cache_resident_bytes;
        self.read_result_cache_resident_bytes = read_cache_resident_bytes;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSegmentReadOutput {
    pub rows: Vec<AppendTableRow>,
    pub report: AppendSegmentReadReport,
}

pub struct AppendSegmentWriter;

impl AppendSegmentWriter {
    pub fn encode(
        generation: u64,
        source_commit_epoch: u64,
        rows: &[AppendTableRow],
        config: AppendSegmentConfig,
    ) -> Result<AppendSegmentWriteOutput, AppendTableError> {
        validate_config(config)?;
        if rows.len() > config.max_rows {
            return Err(AppendTableError::Admission(format!(
                "append segment contains {} rows, exceeding limit {}",
                rows.len(),
                config.max_rows
            )));
        }
        if !rows
            .windows(2)
            .all(|pair| compare_rows(&pair[0], &pair[1]).is_lt())
        {
            return Err(AppendTableError::Constraint(
                "append segment rows must be strictly ordered by table, partition, and order key"
                    .to_string(),
            ));
        }

        let mut descriptors = Vec::new();
        let mut payload = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let first = &rows[start];
            let partition_end = rows[start..]
                .iter()
                .position(|row| {
                    row.table != first.table || row.partition_key != first.partition_key
                })
                .map_or(rows.len(), |offset| start + offset);
            let (end, raw) = encode_next_block(rows, start, partition_end, config)?;
            if descriptors.len() == config.max_blocks {
                return Err(AppendTableError::Admission(format!(
                    "append segment block count exceeds limit {}",
                    config.max_blocks
                )));
            }
            if raw.len() > config.max_decoded_block_bytes {
                return Err(AppendTableError::Admission(format!(
                    "append block contains {} decoded bytes, exceeding limit {}",
                    raw.len(),
                    config.max_decoded_block_bytes
                )));
            }
            let compressed = zstd::stream::encode_all(raw.as_slice(), config.compression_level)
                .map_err(|error| {
                    AppendTableError::Corruption(format!(
                        "failed to compress append segment block: {error}"
                    ))
                })?;
            if compressed.len() > config.max_compressed_block_bytes {
                return Err(AppendTableError::Admission(format!(
                    "append block contains {} compressed bytes, exceeding limit {}",
                    compressed.len(),
                    config.max_compressed_block_bytes
                )));
            }
            let payload_offset = u64::try_from(payload.len()).map_err(|_| {
                AppendTableError::Admission("append payload offset overflows u64".to_string())
            })?;
            let digest = integrity_digest(&compressed);
            payload.extend_from_slice(&compressed);
            descriptors.push(AppendSegmentBlockDescriptor {
                table: first.table.clone(),
                partition_key: first.partition_key.clone(),
                min_order_key: first.order_key.clone(),
                max_order_key: rows[end - 1].order_key.clone(),
                row_count: u32::try_from(end - start).map_err(|_| {
                    AppendTableError::Admission("append block row count overflows u32".to_string())
                })?,
                payload_offset,
                compressed_bytes: u64::try_from(compressed.len()).map_err(|_| {
                    AppendTableError::Admission(
                        "append compressed block length overflows u64".to_string(),
                    )
                })?,
                decoded_bytes: u64::try_from(raw.len()).map_err(|_| {
                    AppendTableError::Admission(
                        "append decoded block length overflows u64".to_string(),
                    )
                })?,
                payload_crc32c: digest.crc32c.get(),
                payload_sha256: digest.sha256,
            });
            start = end;
        }

        let directory = encode_directory(&descriptors, config)?;
        let total_len = SEGMENT_HEADER_BYTES
            .checked_add(directory.len())
            .and_then(|len| len.checked_add(payload.len()))
            .ok_or_else(|| {
                AppendTableError::Admission("append segment size overflow".to_string())
            })?;
        if total_len > config.max_segment_bytes {
            return Err(AppendTableError::Admission(format!(
                "append segment contains {total_len} bytes, exceeding limit {}",
                config.max_segment_bytes
            )));
        }
        let mut body = Vec::with_capacity(directory.len() + payload.len());
        body.extend_from_slice(&directory);
        body.extend_from_slice(&payload);
        let body_digest = integrity_digest(&body);
        let mut encoded = Vec::with_capacity(total_len);
        encoded.extend_from_slice(SEGMENT_MAGIC);
        encoded.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0u16.to_le_bytes());
        encoded.extend_from_slice(&generation.to_le_bytes());
        encoded.extend_from_slice(&source_commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&(descriptors.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&(directory.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&body_digest.crc32c.get().to_le_bytes());
        encoded.extend_from_slice(body_digest.sha256.as_bytes());
        encoded.extend_from_slice(&0u64.to_le_bytes());
        debug_assert_eq!(encoded.len(), SEGMENT_HEADER_BYTES);
        encoded.extend_from_slice(&body);
        let artifact_digest = integrity_digest(&encoded);
        let artifact = AppendSegmentArtifactMetadata {
            encoded_len: encoded.len() as u64,
            encoded_crc32c: artifact_digest.crc32c.get(),
            encoded_sha256: artifact_digest.sha256,
        };
        Ok(AppendSegmentWriteOutput {
            generation,
            source_commit_epoch,
            encoded: Arc::from(encoded),
            descriptors,
            artifact,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AppendSegmentReader {
    generation: u64,
    source_commit_epoch: u64,
    source: AppendSegmentSource,
    payload_start: usize,
    descriptors: Arc<[AppendSegmentBlockDescriptor]>,
    config: AppendSegmentConfig,
    decoded_block_cache: Arc<Mutex<AppendDecodedBlockCache>>,
    read_result_cache: Arc<Mutex<AppendReadResultCache>>,
}

#[derive(Debug, Clone)]
enum AppendSegmentSource {
    Memory(Arc<[u8]>),
    File(Arc<AppendSegmentFileSource>),
}

#[derive(Debug)]
struct AppendSegmentFileSource {
    file: Mutex<File>,
    encoded_len: usize,
}

#[derive(Debug)]
struct AppendDecodedBlockCache {
    capacity_bytes: usize,
    resident_bytes: usize,
    insertion_order: VecDeque<u64>,
    blocks: BTreeMap<u64, Arc<[u8]>>,
}

impl AppendDecodedBlockCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            resident_bytes: 0,
            insertion_order: VecDeque::new(),
            blocks: BTreeMap::new(),
        }
    }

    fn get(&self, payload_offset: u64) -> Option<Arc<[u8]>> {
        self.blocks.get(&payload_offset).cloned()
    }

    fn insert(&mut self, payload_offset: u64, decoded: Arc<[u8]>) {
        if decoded.len() > self.capacity_bytes || self.capacity_bytes == 0 {
            return;
        }
        if self.blocks.contains_key(&payload_offset) {
            return;
        }
        while self.resident_bytes.saturating_add(decoded.len()) > self.capacity_bytes {
            let Some(oldest) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.blocks.remove(&oldest) {
                self.resident_bytes = self.resident_bytes.saturating_sub(evicted.len());
            }
        }
        self.resident_bytes = self.resident_bytes.saturating_add(decoded.len());
        self.insertion_order.push_back(payload_offset);
        self.blocks.insert(payload_offset, decoded);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AppendReadCacheKey {
    payload_offset: u64,
    after: Option<RelationalKey>,
    max_rows: usize,
    max_payload_bytes: usize,
}

#[derive(Debug)]
struct AppendReadResultCache {
    capacity_bytes: usize,
    resident_bytes: usize,
    insertion_order: VecDeque<AppendReadCacheKey>,
    results: BTreeMap<AppendReadCacheKey, Arc<AppendSegmentReadOutput>>,
}

impl AppendReadResultCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            resident_bytes: 0,
            insertion_order: VecDeque::new(),
            results: BTreeMap::new(),
        }
    }

    fn get(&self, key: &AppendReadCacheKey) -> Option<Arc<AppendSegmentReadOutput>> {
        self.results.get(key).cloned()
    }

    fn insert(&mut self, key: AppendReadCacheKey, output: Arc<AppendSegmentReadOutput>) {
        let bytes = output
            .report
            .output_payload_bytes
            .saturating_add(output.rows.len().saturating_mul(128));
        if bytes > self.capacity_bytes
            || self.capacity_bytes == 0
            || self.results.contains_key(&key)
        {
            return;
        }
        while self.resident_bytes.saturating_add(bytes) > self.capacity_bytes {
            let Some(oldest) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.results.remove(&oldest) {
                self.resident_bytes = self.resident_bytes.saturating_sub(
                    evicted
                        .report
                        .output_payload_bytes
                        .saturating_add(evicted.rows.len().saturating_mul(128)),
                );
            }
        }
        self.resident_bytes = self.resident_bytes.saturating_add(bytes);
        self.insertion_order.push_back(key.clone());
        self.results.insert(key, output);
    }
}

impl AppendSegmentSource {
    fn read_range(&self, start: usize, len: usize) -> Result<Vec<u8>, AppendTableError> {
        let end = start.checked_add(len).ok_or_else(|| {
            AppendTableError::Corruption("append segment read range overflow".to_string())
        })?;
        match self {
            Self::Memory(encoded) => encoded.get(start..end).map(<[u8]>::to_vec).ok_or_else(|| {
                AppendTableError::Corruption(
                    "append block lies outside segment payload".to_string(),
                )
            }),
            Self::File(source) => {
                if end > source.encoded_len {
                    return Err(AppendTableError::Corruption(
                        "append block lies outside segment payload".to_string(),
                    ));
                }
                let mut file = source.file.lock().map_err(|_| {
                    AppendTableError::Durability(
                        "append segment file reader lock is poisoned".to_string(),
                    )
                })?;
                file.seek(SeekFrom::Start(start as u64)).map_err(|error| {
                    AppendTableError::Durability(format!("seek append segment block: {error}"))
                })?;
                let mut encoded = vec![0; len];
                file.read_exact(&mut encoded).map_err(|error| {
                    AppendTableError::Durability(format!("read append segment block: {error}"))
                })?;
                Ok(encoded)
            }
        }
    }

    fn payload_resident_bytes(&self) -> usize {
        match self {
            Self::Memory(encoded) => encoded.len(),
            Self::File(_) => 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AppendSegmentHeader {
    generation: u64,
    source_commit_epoch: u64,
    descriptor_count: usize,
    directory_len: usize,
    payload_len: usize,
}

impl AppendSegmentReader {
    pub fn open(encoded: Arc<[u8]>, config: AppendSegmentConfig) -> Result<Self, AppendTableError> {
        validate_config(config)?;
        let header = decode_segment_header(
            &encoded[..encoded.len().min(SEGMENT_HEADER_BYTES)],
            encoded.len(),
            config,
        )?;
        let body = &encoded[SEGMENT_HEADER_BYTES..];
        let digest = integrity_digest(body);
        let expected_crc = read_u32(&encoded, 52);
        let expected_sha: &[u8; SHA256_BYTES] = encoded[56..88].try_into().expect("fixed SHA-256");
        if digest.crc32c.get() != expected_crc || digest.sha256.as_bytes() != expected_sha {
            return Err(AppendTableError::Corruption(
                "append segment checksum mismatch".to_string(),
            ));
        }
        let descriptors = decode_directory(
            &encoded[SEGMENT_HEADER_BYTES..SEGMENT_HEADER_BYTES + header.directory_len],
            header.descriptor_count,
            header.payload_len,
            config,
        )?;
        Ok(Self {
            generation: header.generation,
            source_commit_epoch: header.source_commit_epoch,
            source: AppendSegmentSource::Memory(encoded),
            payload_start: SEGMENT_HEADER_BYTES + header.directory_len,
            descriptors: descriptors.into(),
            config,
            decoded_block_cache: Arc::new(Mutex::new(AppendDecodedBlockCache::new(0))),
            read_result_cache: Arc::new(Mutex::new(AppendReadResultCache::new(0))),
        })
    }

    pub fn open_bound_file(
        path: &Path,
        expected: AppendSegmentArtifactMetadata,
        config: AppendSegmentConfig,
    ) -> Result<Self, AppendTableError> {
        validate_config(config)?;
        let mut file = File::open(path).map_err(|error| {
            AppendTableError::Durability(format!("open append segment: {error}"))
        })?;
        let encoded_len = usize::try_from(
            file.metadata()
                .map_err(|error| {
                    AppendTableError::Durability(format!("read append segment metadata: {error}"))
                })?
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
        file.read_exact(&mut header_bytes).map_err(|error| {
            AppendTableError::Durability(format!("read append segment header: {error}"))
        })?;
        let header = decode_segment_header(&header_bytes, encoded_len, config)?;

        let mut artifact_hasher = IntegrityHasher::new();
        artifact_hasher.update(&header_bytes);
        let mut body_hasher = IntegrityHasher::new();
        let mut remaining = encoded_len - SEGMENT_HEADER_BYTES;
        let mut buffer = vec![0; remaining.min(64 * 1024)];
        while remaining > 0 {
            let chunk_len = remaining.min(buffer.len());
            file.read_exact(&mut buffer[..chunk_len]).map_err(|error| {
                AppendTableError::Durability(format!("stream append segment: {error}"))
            })?;
            artifact_hasher.update(&buffer[..chunk_len]);
            body_hasher.update(&buffer[..chunk_len]);
            remaining -= chunk_len;
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

        file.seek(SeekFrom::Start(SEGMENT_HEADER_BYTES as u64))
            .map_err(|error| {
                AppendTableError::Durability(format!("seek append segment directory: {error}"))
            })?;
        let mut directory = vec![0; header.directory_len];
        file.read_exact(&mut directory).map_err(|error| {
            AppendTableError::Durability(format!("read append segment directory: {error}"))
        })?;
        let descriptors = decode_directory(
            &directory,
            header.descriptor_count,
            header.payload_len,
            config,
        )?;
        Ok(Self {
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
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn source_commit_epoch(&self) -> u64 {
        self.source_commit_epoch
    }

    pub fn descriptors(&self) -> &[AppendSegmentBlockDescriptor] {
        &self.descriptors
    }

    pub fn payload_resident_bytes(&self) -> usize {
        self.source
            .payload_resident_bytes()
            .saturating_add(self.decoded_block_cache_resident_bytes())
            .saturating_add(self.read_result_cache_resident_bytes())
    }

    pub fn deep_scrub(&self) -> Result<AppendSegmentReadReport, AppendTableError> {
        let mut report = AppendSegmentReadReport {
            segments_examined: 1,
            ..AppendSegmentReadReport::default()
        };
        for descriptor in self.descriptors.iter() {
            let max_rows = usize::try_from(descriptor.row_count).map_err(|_| {
                AppendTableError::Admission("append block row count overflows usize".to_string())
            })?;
            let read = self.read_block_uncached(descriptor, None, max_rows, usize::MAX)?;
            if read.rows.len() != max_rows {
                return Err(AppendTableError::Corruption(format!(
                    "append block decoded {} rows, expected {max_rows}",
                    read.rows.len()
                )));
            }
            report.merge(read.report);
        }
        Ok(report)
    }

    pub fn checkpoint_rows(
        &self,
        max_rows: usize,
    ) -> Result<Vec<AppendTableRow>, AppendTableError> {
        let expected_rows = self
            .descriptors
            .iter()
            .try_fold(0usize, |total, descriptor| {
                total
                    .checked_add(descriptor.row_count as usize)
                    .ok_or_else(|| {
                        AppendTableError::Admission("append row count overflow".to_string())
                    })
            })?;
        if expected_rows > max_rows {
            return Err(AppendTableError::Admission(format!(
                "append segment contains {expected_rows} rows, exceeding checkpoint limit {max_rows}"
            )));
        }
        let mut rows = Vec::with_capacity(expected_rows);
        for descriptor in self.descriptors.iter() {
            let expected = descriptor.row_count as usize;
            let read = self.read_block(descriptor, None, expected, usize::MAX)?;
            if read.rows.len() != expected {
                return Err(AppendTableError::Corruption(format!(
                    "append block decoded {} rows, expected {expected}",
                    read.rows.len()
                )));
            }
            rows.extend(read.rows);
        }
        Ok(rows)
    }

    pub(crate) fn checkpoint_rows_bounded(
        &self,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<Option<Vec<AppendTableRow>>, AppendTableError> {
        let expected_rows = self
            .descriptors
            .iter()
            .try_fold(0usize, |total, descriptor| {
                total
                    .checked_add(descriptor.row_count as usize)
                    .ok_or_else(|| {
                        AppendTableError::Admission("append row count overflow".to_string())
                    })
            })?;
        if expected_rows > max_rows {
            return Ok(None);
        }

        let mut rows = Vec::with_capacity(expected_rows);
        let mut payload_bytes = 0usize;
        for descriptor in self.descriptors.iter() {
            let read = match self.read_block(
                descriptor,
                None,
                descriptor.row_count as usize,
                max_payload_bytes.saturating_sub(payload_bytes),
            ) {
                Ok(read) => read,
                Err(AppendTableError::Admission(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            payload_bytes = match payload_bytes.checked_add(read.report.output_payload_bytes) {
                Some(payload_bytes) if payload_bytes <= max_payload_bytes => payload_bytes,
                _ => return Ok(None),
            };
            rows.extend(read.rows);
        }
        if rows.len() != expected_rows {
            return Err(AppendTableError::Corruption(format!(
                "append segment decoded {} rows, expected {expected_rows}",
                rows.len()
            )));
        }
        Ok(Some(rows))
    }

    pub fn read_partition(
        &self,
        table: &str,
        partition: &RelationalKey,
        after: Option<&RelationalKey>,
        max_rows: usize,
    ) -> Result<AppendSegmentReadOutput, AppendTableError> {
        self.read_partition_bounded(table, partition, after, max_rows, usize::MAX)
    }

    pub fn read_partition_bounded(
        &self,
        table: &str,
        partition: &RelationalKey,
        after: Option<&RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput, AppendTableError> {
        if max_rows == 0 {
            return Ok(AppendSegmentReadOutput {
                rows: Vec::new(),
                report: AppendSegmentReadReport::default(),
            });
        }
        let start = lower_bound_partition(&self.descriptors, table, partition);
        let mut rows = Vec::with_capacity(max_rows);
        let mut report = AppendSegmentReadReport {
            segments_examined: 1,
            ..AppendSegmentReadReport::default()
        };
        for descriptor in self.descriptors[start..].iter().take_while(|descriptor| {
            descriptor.table == table && descriptor.partition_key == *partition
        }) {
            if rows.len() == max_rows {
                break;
            }
            if after.is_some_and(|key| key >= &descriptor.max_order_key) {
                continue;
            }
            let read = self.read_block(
                descriptor,
                after,
                max_rows - rows.len(),
                max_payload_bytes.saturating_sub(report.output_payload_bytes),
            )?;
            report.merge_block(read.report);
            rows.extend(read.rows);
        }
        if report.blocks_read == 0 {
            report.segments_pruned = 1;
        }
        Ok(AppendSegmentReadOutput { rows, report })
    }

    fn read_block(
        &self,
        descriptor: &AppendSegmentBlockDescriptor,
        after: Option<&RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput, AppendTableError> {
        let key = AppendReadCacheKey {
            payload_offset: descriptor.payload_offset,
            after: after.cloned(),
            max_rows,
            max_payload_bytes,
        };
        if let Some(cached) = self.cached_read_result(&key)? {
            let mut output = (*cached).clone();
            output.report.blocks_read = 0;
            output.report.compressed_bytes_read = 0;
            output.report.decoded_bytes = 0;
            output.report.rows_decoded = 0;
            output.report.decoded_block_cache_hits = 0;
            output.report.read_result_cache_hits = 1;
            output.report.decoded_block_cache_resident_bytes =
                self.decoded_block_cache_resident_bytes();
            output.report.read_result_cache_resident_bytes =
                self.read_result_cache_resident_bytes();
            return Ok(output);
        }
        let mut output =
            self.read_block_inner(descriptor, after, max_rows, max_payload_bytes, true)?;
        self.cache_read_result(key, Arc::new(output.clone()))?;
        output.report.read_result_cache_resident_bytes = self.read_result_cache_resident_bytes();
        Ok(output)
    }

    fn read_block_uncached(
        &self,
        descriptor: &AppendSegmentBlockDescriptor,
        after: Option<&RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput, AppendTableError> {
        self.read_block_inner(descriptor, after, max_rows, max_payload_bytes, false)
    }

    fn read_block_inner(
        &self,
        descriptor: &AppendSegmentBlockDescriptor,
        after: Option<&RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
        use_cache: bool,
    ) -> Result<AppendSegmentReadOutput, AppendTableError> {
        let cached = use_cache.then(|| self.cached_decoded_block(descriptor.payload_offset));
        let cached = cached.transpose()?.flatten();
        let cache_hit = cached.is_some();
        let mut compressed_bytes_read = 0usize;
        let decoded = if let Some(decoded) = cached {
            decoded
        } else {
            let compressed_start = self
                .payload_start
                .checked_add(to_usize(descriptor.payload_offset, "block offset")?)
                .ok_or_else(|| {
                    AppendTableError::Corruption("append block offset overflow".to_string())
                })?;
            let compressed_end = compressed_start
                .checked_add(to_usize(
                    descriptor.compressed_bytes,
                    "compressed block length",
                )?)
                .ok_or_else(|| {
                    AppendTableError::Corruption("append block length overflow".to_string())
                })?;
            let compressed = self
                .source
                .read_range(compressed_start, compressed_end - compressed_start)?;
            compressed_bytes_read = compressed.len();
            let compressed_digest = integrity_digest(&compressed);
            if compressed_digest.crc32c.get() != descriptor.payload_crc32c
                || compressed_digest.sha256 != descriptor.payload_sha256
            {
                return Err(AppendTableError::Corruption(
                    "append block checksum mismatch".to_string(),
                ));
            }
            let expected_decoded = to_usize(descriptor.decoded_bytes, "decoded block length")?;
            if expected_decoded > self.config.max_decoded_block_bytes {
                return Err(AppendTableError::Admission(format!(
                    "append block declares {expected_decoded} decoded bytes, exceeding limit {}",
                    self.config.max_decoded_block_bytes
                )));
            }
            let decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed.as_slice()))
                .map_err(|error| {
                    AppendTableError::Corruption(format!(
                        "failed to open append block decoder: {error}"
                    ))
                })?;
            let read_limit = u64::try_from(self.config.max_decoded_block_bytes)
                .unwrap_or(u64::MAX)
                .saturating_add(1);
            let mut decoded = Vec::with_capacity(expected_decoded);
            decoder
                .take(read_limit)
                .read_to_end(&mut decoded)
                .map_err(|error| {
                    AppendTableError::Corruption(format!(
                        "failed to decompress append block: {error}"
                    ))
                })?;
            if decoded.len() != expected_decoded {
                return Err(AppendTableError::Corruption(format!(
                    "append block decoded length mismatch: expected {expected_decoded}, got {}",
                    decoded.len()
                )));
            }
            let decoded: Arc<[u8]> = decoded.into();
            if use_cache && matches!(self.source, AppendSegmentSource::File(_)) {
                self.cache_decoded_block(descriptor.payload_offset, Arc::clone(&decoded))?;
            }
            decoded
        };
        let (rows, decoded_rows, hydration, hydrated_values) = decode_block(
            descriptor,
            decoded.as_ref(),
            after,
            max_rows,
            max_payload_bytes,
            self.config,
        )?;
        let output_payload_bytes = rows.iter().try_fold(0usize, |total, row| {
            let row_bytes = super::estimated_row_bytes(&row.row)?;
            total.checked_add(row_bytes).ok_or_else(|| {
                AppendTableError::Admission("append read payload size overflow".to_string())
            })
        })?;
        if output_payload_bytes > max_payload_bytes {
            return Err(AppendTableError::Admission(format!(
                "append read produced {output_payload_bytes} payload bytes, exceeding limit {max_payload_bytes}"
            )));
        }
        Ok(AppendSegmentReadOutput {
            report: AppendSegmentReadReport {
                blocks_read: 1,
                compressed_bytes_read,
                decoded_bytes: decoded.len(),
                rows_decoded: decoded_rows,
                rows_returned: rows.len(),
                output_payload_bytes,
                overflow_values_hydrated: hydrated_values,
                overflow_compressed_bytes: hydration.compressed_bytes,
                overflow_decompressed_bytes: hydration.decompressed_bytes,
                decoded_block_cache_hits: usize::from(cache_hit),
                decoded_block_cache_resident_bytes: self.decoded_block_cache_resident_bytes(),
                read_result_cache_resident_bytes: self.read_result_cache_resident_bytes(),
                ..AppendSegmentReadReport::default()
            },
            rows,
        })
    }

    fn cached_decoded_block(
        &self,
        payload_offset: u64,
    ) -> Result<Option<Arc<[u8]>>, AppendTableError> {
        self.decoded_block_cache
            .lock()
            .map_err(|_| {
                AppendTableError::Durability(
                    "append decoded block cache lock is poisoned".to_string(),
                )
            })
            .map(|cache| cache.get(payload_offset))
    }

    fn cache_decoded_block(
        &self,
        payload_offset: u64,
        decoded: Arc<[u8]>,
    ) -> Result<(), AppendTableError> {
        let mut cache = self.decoded_block_cache.lock().map_err(|_| {
            AppendTableError::Durability("append decoded block cache lock is poisoned".to_string())
        })?;
        cache.insert(payload_offset, decoded);
        Ok(())
    }

    fn decoded_block_cache_resident_bytes(&self) -> usize {
        self.decoded_block_cache
            .lock()
            .map(|cache| cache.resident_bytes)
            .unwrap_or_default()
    }

    fn cached_read_result(
        &self,
        key: &AppendReadCacheKey,
    ) -> Result<Option<Arc<AppendSegmentReadOutput>>, AppendTableError> {
        self.read_result_cache
            .lock()
            .map_err(|_| {
                AppendTableError::Durability(
                    "append read result cache lock is poisoned".to_string(),
                )
            })
            .map(|cache| cache.get(key))
    }

    fn cache_read_result(
        &self,
        key: AppendReadCacheKey,
        output: Arc<AppendSegmentReadOutput>,
    ) -> Result<(), AppendTableError> {
        let mut cache = self.read_result_cache.lock().map_err(|_| {
            AppendTableError::Durability("append read result cache lock is poisoned".to_string())
        })?;
        cache.insert(key, output);
        Ok(())
    }

    fn read_result_cache_resident_bytes(&self) -> usize {
        self.read_result_cache
            .lock()
            .map(|cache| cache.resident_bytes)
            .unwrap_or_default()
    }
}

fn decode_segment_header(
    encoded: &[u8],
    encoded_len: usize,
    config: AppendSegmentConfig,
) -> Result<AppendSegmentHeader, AppendTableError> {
    if encoded.len() < SEGMENT_HEADER_BYTES || encoded_len > config.max_segment_bytes {
        return Err(AppendTableError::Admission(format!(
            "append segment contains {encoded_len} bytes, outside limit {}",
            config.max_segment_bytes
        )));
    }
    if &encoded[..8] != SEGMENT_MAGIC {
        return Err(AppendTableError::Corruption(
            "append segment magic mismatch".to_string(),
        ));
    }
    let version = read_u16(encoded, 8);
    if version != SEGMENT_VERSION || read_u16(encoded, 10) != 0 || read_u64(encoded, 88) != 0 {
        return Err(AppendTableError::Corruption(format!(
            "unsupported append segment version {version} or non-zero reserved fields"
        )));
    }
    let descriptor_count = to_usize(read_u64(encoded, 28), "descriptor count")?;
    let directory_len = to_usize(read_u64(encoded, 36), "directory length")?;
    let payload_len = to_usize(read_u64(encoded, 44), "payload length")?;
    if descriptor_count > config.max_blocks {
        return Err(AppendTableError::Admission(format!(
            "append segment contains {descriptor_count} blocks, exceeding limit {}",
            config.max_blocks
        )));
    }
    let expected_len = SEGMENT_HEADER_BYTES
        .checked_add(directory_len)
        .and_then(|len| len.checked_add(payload_len))
        .ok_or_else(|| {
            AppendTableError::Corruption("append segment length overflow".to_string())
        })?;
    if expected_len != encoded_len {
        return Err(AppendTableError::Corruption(format!(
            "append segment length mismatch: expected {expected_len}, got {encoded_len}"
        )));
    }
    Ok(AppendSegmentHeader {
        generation: read_u64(encoded, 12),
        source_commit_epoch: read_u64(encoded, 20),
        descriptor_count,
        directory_len,
        payload_len,
    })
}

fn encode_next_block(
    rows: &[AppendTableRow],
    start: usize,
    partition_end: usize,
    config: AppendSegmentConfig,
) -> Result<(usize, Vec<u8>), AppendTableError> {
    let mut encoder = Encoder::default();
    encoder.u32(0);
    let mut overflows = std::collections::BTreeMap::new();
    let mut end = start;
    while end < partition_end {
        let row = &rows[end];
        let order_key = encode_append_key(&row.order_key, config.max_key_bytes)?;
        let (externalized, row_overflows) = externalize_row(&row.row, config)?;
        let row_payload = encode_relational_row_payload(&externalized).map_err(map_relational)?;
        if row_payload.len() > config.max_row_bytes {
            return Err(AppendTableError::Admission(format!(
                "append row contains {} bytes, exceeding limit {}",
                row_payload.len(),
                config.max_row_bytes
            )));
        }
        let new_overflow_bytes = row_overflows.iter().try_fold(0usize, |total, overflow| {
            if overflows.contains_key(&overflow.reference.digest) {
                return Ok(total);
            }
            total
                .checked_add(SHA256_BYTES + 8)
                .and_then(|bytes| bytes.checked_add(overflow.bytes.len()))
                .ok_or_else(|| {
                    AppendTableError::Admission("append block size overflow".to_string())
                })
        })?;
        let next_len = encoder
            .len()
            .checked_add(16)
            .and_then(|len| len.checked_add(order_key.len()))
            .and_then(|len| len.checked_add(row_payload.len()))
            .and_then(|len| len.checked_add(new_overflow_bytes))
            .ok_or_else(|| AppendTableError::Admission("append block size overflow".to_string()))?;
        if end > start && next_len > config.target_decoded_block_bytes {
            break;
        }
        encoder.bytes(&order_key, "append order key")?;
        encoder.bytes(&row_payload, "append row payload")?;
        for overflow in row_overflows {
            if let Some(existing) = overflows.insert(overflow.reference.digest, overflow.clone())
                && existing != overflow
            {
                return Err(AppendTableError::Corruption(
                    "append overflow digest has conflicting envelopes".to_string(),
                ));
            }
        }
        end += 1;
    }
    let row_count = u32::try_from(end - start).map_err(|_| {
        AppendTableError::Admission("append block row count exceeds durable limit".to_string())
    })?;
    encoder.overwrite_u32(0, row_count);
    encoder.u32(u32::try_from(overflows.len()).map_err(|_| {
        AppendTableError::Admission("append overflow count exceeds durable limit".to_string())
    })?);
    for (digest, overflow) in overflows {
        encoder.raw(digest.as_bytes());
        encoder.bytes(&overflow.bytes, "append overflow envelope")?;
    }
    Ok((end, encoder.finish()))
}

fn externalize_row(
    row: &RelationalRow,
    config: AppendSegmentConfig,
) -> Result<
    (
        RelationalRow,
        Vec<crate::relational::overflow::EncodedRelationalOverflow>,
    ),
    AppendTableError,
> {
    let overflow_config = RelationalOverflowConfig {
        threshold_bytes: config.overflow_threshold_bytes,
        compression_level: config.overflow_compression_level,
        max_value_bytes: config.max_row_bytes,
    };
    let mut values = Vec::with_capacity(row.values().len());
    let mut overflows = Vec::new();
    for value in row.values() {
        let encoded = match value {
            RelationalValue::Text(value) if value.len() >= config.overflow_threshold_bytes => Some(
                encode_overflow_envelope(
                    RelationalScalarType::Text,
                    value.as_bytes(),
                    overflow_config,
                )
                .map_err(map_relational)?,
            ),
            RelationalValue::Bytea(value) if value.len() >= config.overflow_threshold_bytes => {
                Some(
                    encode_overflow_envelope(RelationalScalarType::Bytea, value, overflow_config)
                        .map_err(map_relational)?,
                )
            }
            RelationalValue::Overflow(_) => {
                return Err(AppendTableError::Constraint(
                    "append input rows cannot contain unresolved overflow references".to_string(),
                ));
            }
            _ => None,
        };
        if let Some(encoded) = encoded {
            values.push(RelationalValue::Overflow(encoded.reference));
            overflows.push(encoded);
        } else {
            values.push(value.clone());
        }
    }
    Ok((RelationalRow::new(values), overflows))
}

fn decode_block(
    descriptor: &AppendSegmentBlockDescriptor,
    decoded: &[u8],
    after: Option<&RelationalKey>,
    max_rows: usize,
    max_payload_bytes: usize,
    config: AppendSegmentConfig,
) -> Result<(Vec<AppendTableRow>, usize, RelationalHydrationBudget, usize), AppendTableError> {
    let mut decoder = Decoder::new(decoded);
    let row_count = decoder.count(config.max_rows, "append block rows")?;
    if row_count != descriptor.row_count as usize {
        return Err(AppendTableError::Corruption(format!(
            "append block row count mismatch: expected {}, got {row_count}",
            descriptor.row_count
        )));
    }
    let mut rows = Vec::with_capacity(max_rows.min(row_count));
    let mut prior = None;
    let mut value_count = 0usize;
    let mut references = std::collections::BTreeMap::new();
    for _ in 0..row_count {
        let key = decode_append_key(
            decoder.bytes(config.max_key_bytes, "append order key")?,
            false,
        )?;
        if prior.as_ref().is_some_and(|prior| prior >= &key) {
            return Err(AppendTableError::Corruption(
                "append block order keys are duplicated or unordered".to_string(),
            ));
        }
        let row_payload = decoder.bytes(config.max_row_bytes, "append row")?;
        let row = decode_relational_row_payload(
            row_payload,
            config.max_values.saturating_sub(value_count),
            config.max_value_bytes(),
        )
        .map_err(map_relational)?;
        value_count = value_count.checked_add(row.values().len()).ok_or_else(|| {
            AppendTableError::Corruption("append block value count overflow".to_string())
        })?;
        if value_count > config.max_values {
            return Err(AppendTableError::Admission(format!(
                "append block values exceed limit {}",
                config.max_values
            )));
        }
        for value in row.values() {
            let RelationalValue::Overflow(reference) = value else {
                continue;
            };
            if let Some(existing) = references.insert(reference.digest, *reference)
                && existing != *reference
            {
                return Err(AppendTableError::Corruption(
                    "append overflow digest has conflicting references".to_string(),
                ));
            }
        }
        if after.is_none_or(|watermark| key > *watermark) && rows.len() < max_rows {
            rows.push(AppendTableRow {
                table: descriptor.table.clone(),
                partition_key: descriptor.partition_key.clone(),
                order_key: key.clone(),
                row,
            });
        }
        prior = Some(key);
    }
    let overflow_count = decoder.count(config.max_values, "append overflow values")?;
    let mut envelopes = std::collections::BTreeMap::new();
    for _ in 0..overflow_count {
        let digest = Sha256Digest::from_bytes(
            decoder
                .take(SHA256_BYTES, "append overflow digest")?
                .try_into()
                .expect("fixed SHA-256"),
        );
        let envelope = decoder.bytes(
            config.max_row_bytes.saturating_add(32),
            "append overflow envelope",
        )?;
        if envelopes.insert(digest, envelope).is_some() {
            return Err(AppendTableError::Corruption(
                "append block contains duplicate overflow envelopes".to_string(),
            ));
        }
    }
    decoder.finish("append block")?;
    if prior.as_ref() != Some(&descriptor.max_order_key) {
        return Err(AppendTableError::Corruption(
            "append block maximum order key differs from its descriptor".to_string(),
        ));
    }
    if references.len() != envelopes.len()
        || references
            .keys()
            .any(|digest| !envelopes.contains_key(digest))
    {
        return Err(AppendTableError::Corruption(
            "append block overflow envelopes do not match its row closure".to_string(),
        ));
    }
    let mut budget = RelationalHydrationBudget {
        max_rows: rows.len().max(1),
        max_compressed_bytes: config.max_decoded_block_bytes.min(max_payload_bytes.max(1)),
        max_decompressed_bytes: config.max_decoded_block_bytes.min(max_payload_bytes.max(1)),
        max_memory_bytes: config.max_decoded_block_bytes.min(max_payload_bytes.max(1)),
        ..RelationalHydrationBudget::default()
    };
    let mut hydrated_values = 0usize;
    let mut hydrated: std::collections::BTreeMap<Sha256Digest, RelationalValue> =
        std::collections::BTreeMap::new();
    for append_row in &mut rows {
        let mut values = append_row.row.values().to_vec();
        for value in &mut values {
            let RelationalValue::Overflow(reference) = value else {
                continue;
            };
            *value = if let Some(value) = hydrated.get(&reference.digest) {
                value.clone()
            } else {
                let envelope = envelopes.get(&reference.digest).ok_or_else(|| {
                    AppendTableError::Corruption(format!(
                        "append block is missing overflow envelope {}",
                        reference.digest
                    ))
                })?;
                let value = decode_overflow_envelope(reference, envelope, &mut budget, None)
                    .map_err(map_relational)?;
                hydrated.insert(reference.digest, value.clone());
                value
            };
            hydrated_values = hydrated_values.saturating_add(1);
        }
        append_row.row = RelationalRow::new(values);
    }
    Ok((rows, row_count, budget, hydrated_values))
}

impl AppendSegmentConfig {
    fn max_value_bytes(self) -> usize {
        self.max_row_bytes.max(self.max_key_bytes)
    }
}

fn encode_directory(
    descriptors: &[AppendSegmentBlockDescriptor],
    config: AppendSegmentConfig,
) -> Result<Vec<u8>, AppendTableError> {
    let mut encoder = Encoder::default();
    for descriptor in descriptors {
        encoder.string(&descriptor.table, "append block table")?;
        encoder.bytes(
            &encode_append_key(&descriptor.partition_key, config.max_key_bytes)?,
            "append partition key",
        )?;
        encoder.bytes(
            &encode_append_key(&descriptor.min_order_key, config.max_key_bytes)?,
            "append minimum order key",
        )?;
        encoder.bytes(
            &encode_append_key(&descriptor.max_order_key, config.max_key_bytes)?,
            "append maximum order key",
        )?;
        encoder.u32(descriptor.row_count);
        encoder.u64(descriptor.payload_offset);
        encoder.u64(descriptor.compressed_bytes);
        encoder.u64(descriptor.decoded_bytes);
        encoder.u32(descriptor.payload_crc32c);
        encoder.raw(descriptor.payload_sha256.as_bytes());
    }
    Ok(encoder.finish())
}

fn decode_directory(
    encoded: &[u8],
    descriptor_count: usize,
    payload_len: usize,
    config: AppendSegmentConfig,
) -> Result<Vec<AppendSegmentBlockDescriptor>, AppendTableError> {
    let mut decoder = Decoder::new(encoded);
    let mut descriptors = Vec::with_capacity(descriptor_count);
    let mut row_count = 0usize;
    let mut expected_offset = 0u64;
    for _ in 0..descriptor_count {
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
    }
    decoder.finish("append descriptor directory")?;
    if expected_offset != payload_len as u64
        || !descriptors.windows(2).all(|pair| {
            pair[0].table < pair[1].table
                || (pair[0].table == pair[1].table
                    && (pair[0].partition_key < pair[1].partition_key
                        || (pair[0].partition_key == pair[1].partition_key
                            && pair[0].max_order_key < pair[1].min_order_key)))
        })
    {
        return Err(AppendTableError::Corruption(
            "append descriptor directory has gaps, overlap, or unordered partitions".to_string(),
        ));
    }
    Ok(descriptors)
}

fn encode_append_key(
    key: &RelationalKey,
    max_key_bytes: usize,
) -> Result<Vec<u8>, AppendTableError> {
    if key.0.is_empty() {
        return Ok(vec![GLOBAL_PARTITION_TAG]);
    }
    let key = encode_relational_primary_key(key).map_err(map_relational)?;
    let encoded_len = key.len().saturating_add(1);
    if encoded_len > max_key_bytes {
        return Err(AppendTableError::Admission(format!(
            "append key contains {encoded_len} bytes, exceeding limit {max_key_bytes}"
        )));
    }
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.push(ORDERED_KEY_TAG);
    encoded.extend_from_slice(&key);
    Ok(encoded)
}

fn decode_append_key(
    encoded: &[u8],
    allow_global: bool,
) -> Result<RelationalKey, AppendTableError> {
    match encoded {
        [GLOBAL_PARTITION_TAG] if allow_global => Ok(RelationalKey(Vec::new())),
        [GLOBAL_PARTITION_TAG] => Err(AppendTableError::Corruption(
            "append order key cannot be empty".to_string(),
        )),
        [ORDERED_KEY_TAG, body @ ..] if !body.is_empty() => {
            decode_relational_primary_key(body).map_err(map_relational)
        }
        _ => Err(AppendTableError::Corruption(
            "append key has an invalid encoding tag".to_string(),
        )),
    }
}

fn lower_bound_partition(
    descriptors: &[AppendSegmentBlockDescriptor],
    table: &str,
    partition: &RelationalKey,
) -> usize {
    let mut left = 0;
    let mut right = descriptors.len();
    while left < right {
        let middle = left + (right - left) / 2;
        let ordering = descriptors[middle]
            .table
            .as_str()
            .cmp(table)
            .then_with(|| descriptors[middle].partition_key.cmp(partition));
        if ordering.is_lt() {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    left
}

fn validate_config(config: AppendSegmentConfig) -> Result<(), AppendTableError> {
    if config.max_segment_bytes < SEGMENT_HEADER_BYTES
        || config.max_blocks == 0
        || config.max_rows == 0
        || config.max_values == 0
        || config.max_key_bytes < 2
        || config.max_row_bytes == 0
        || config.overflow_threshold_bytes == 0
        || config.overflow_threshold_bytes > config.max_row_bytes
        || config.target_decoded_block_bytes == 0
        || config.target_decoded_block_bytes > config.max_decoded_block_bytes
        || config.max_compressed_block_bytes == 0
        || config.max_decoded_block_bytes == 0
    {
        return Err(AppendTableError::Admission(
            "append segment limits must be non-zero and admit the fixed header".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationalRow, RelationalValue};

    #[test]
    fn read_report_merge_preserves_cache_residency_scope() {
        let mut segment = AppendSegmentReadReport {
            rows_returned: 2,
            decoded_block_cache_resident_bytes: 8,
            ..AppendSegmentReadReport::default()
        };
        segment.merge_block(AppendSegmentReadReport {
            rows_returned: 3,
            decoded_block_cache_resident_bytes: 5,
            ..AppendSegmentReadReport::default()
        });
        assert_eq!(segment.rows_returned, 5);
        assert_eq!(segment.decoded_block_cache_resident_bytes, 8);

        let mut generation = AppendSegmentReadReport::default();
        generation.merge(segment);
        generation.merge(AppendSegmentReadReport {
            decoded_block_cache_resident_bytes: 7,
            ..AppendSegmentReadReport::default()
        });
        assert_eq!(generation.decoded_block_cache_resident_bytes, 15);
    }

    fn append_row(stream: &str, sequence: i64, payload_bytes: usize) -> AppendTableRow {
        AppendTableRow {
            table: "events".to_string(),
            partition_key: RelationalKey(vec![RelationalValue::Text(stream.to_string())]),
            order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
            row: RelationalRow::new(vec![
                RelationalValue::Text(stream.to_string()),
                RelationalValue::BigInt(sequence),
                RelationalValue::Bytea(vec![sequence as u8; payload_bytes]),
            ]),
        }
    }

    #[test]
    fn partition_read_decompresses_only_target_block() {
        let rows = vec![
            append_row("a", 1, 4096),
            append_row("a", 2, 4096),
            append_row("b", 1, 4096),
            append_row("b", 2, 4096),
        ];
        let output = AppendSegmentWriter::encode(7, 11, &rows, AppendSegmentConfig::default())
            .expect("encode segment");
        assert_eq!(output.descriptors.len(), 2);
        let reader =
            AppendSegmentReader::open(output.encoded.clone(), AppendSegmentConfig::default())
                .expect("open segment");
        let read = reader
            .read_partition(
                "events",
                &RelationalKey(vec![RelationalValue::Text("b".to_string())]),
                Some(&RelationalKey(vec![RelationalValue::BigInt(1)])),
                10,
            )
            .expect("read partition");

        assert_eq!(read.rows.len(), 1);
        assert_eq!(read.report.blocks_read, 1);
        assert_eq!(read.report.rows_decoded, 2);
        assert_eq!(reader.generation(), 7);
        assert_eq!(reader.source_commit_epoch(), 11);
    }

    #[test]
    fn descriptor_pruning_reads_no_payload() {
        let rows = vec![append_row("a", 1, 64), append_row("a", 2, 64)];
        let output = AppendSegmentWriter::encode(1, 1, &rows, AppendSegmentConfig::default())
            .expect("encode segment");
        let reader = AppendSegmentReader::open(output.encoded, AppendSegmentConfig::default())
            .expect("open segment");
        let read = reader
            .read_partition(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())]),
                Some(&RelationalKey(vec![RelationalValue::BigInt(2)])),
                10,
            )
            .expect("prune partition");

        assert!(read.rows.is_empty());
        assert_eq!(read.report.segments_examined, 1);
        assert_eq!(read.report.segments_pruned, 1);
        assert_eq!(read.report.blocks_read, 0);
        assert_eq!(read.report.compressed_bytes_read, 0);
        assert_eq!(read.report.decoded_bytes, 0);
    }

    #[test]
    fn hot_partition_is_split_and_read_stops_at_limit() {
        let rows = (1..=12)
            .map(|sequence| append_row("a", sequence, 4096))
            .collect::<Vec<_>>();
        let config = AppendSegmentConfig {
            target_decoded_block_bytes: 12 * 1024,
            overflow_threshold_bytes: 8 * 1024,
            ..AppendSegmentConfig::default()
        };
        let output =
            AppendSegmentWriter::encode(1, 1, &rows, config).expect("encode split segment");
        assert!(output.descriptors.len() > 1);
        let reader = AppendSegmentReader::open(output.encoded, config).expect("open segment");
        let read = reader
            .read_partition(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())]),
                Some(&RelationalKey(vec![RelationalValue::BigInt(4)])),
                3,
            )
            .expect("read bounded split partition");

        assert_eq!(read.rows.len(), 3);
        assert!(read.report.blocks_read < output.descriptors.len());
        assert_eq!(
            read.rows
                .iter()
                .map(|row| row.order_key.clone())
                .collect::<Vec<_>>(),
            vec![
                RelationalKey(vec![RelationalValue::BigInt(5)]),
                RelationalKey(vec![RelationalValue::BigInt(6)]),
                RelationalKey(vec![RelationalValue::BigInt(7)]),
            ]
        );
    }

    #[test]
    fn repeated_large_values_are_deduplicated_and_hydrated_with_budgets() {
        let payload_bytes = 16 * 1024;
        let payload = vec![7; payload_bytes];
        let rows = (1..=2)
            .map(|sequence| AppendTableRow {
                table: "events".to_string(),
                partition_key: RelationalKey(vec![RelationalValue::Text("a".to_string())]),
                order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
                row: RelationalRow::new(vec![
                    RelationalValue::Text("a".to_string()),
                    RelationalValue::BigInt(sequence),
                    RelationalValue::Bytea(payload.clone()),
                ]),
            })
            .collect::<Vec<_>>();
        let config = AppendSegmentConfig {
            overflow_threshold_bytes: 1024,
            ..AppendSegmentConfig::default()
        };
        let output = AppendSegmentWriter::encode(1, 1, &rows, config)
            .expect("encode rows with large values");
        let reader = AppendSegmentReader::open(output.encoded, config).expect("open segment");
        let read = reader
            .read_partition(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())]),
                None,
                10,
            )
            .expect("hydrate large values");

        assert_eq!(read.rows.len(), 2);
        assert!(read.rows.iter().all(|row| {
            matches!(
                &row.row.values()[2],
                RelationalValue::Bytea(value) if value.len() == payload_bytes
            )
        }));
        assert_eq!(read.report.overflow_values_hydrated, 2);
        assert!(read.report.overflow_compressed_bytes > 0);
        assert_eq!(read.report.overflow_decompressed_bytes, payload_bytes);
        assert!(read.report.output_payload_bytes >= payload_bytes * 2);
        assert!(matches!(
            reader.read_partition_bounded(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())]),
                None,
                10,
                1024,
            ),
            Err(AppendTableError::Admission(_))
        ));
    }

    #[test]
    fn segment_rejects_corruption_and_decoded_budget_violation() {
        let rows = vec![append_row("a", 1, 4096)];
        let output = AppendSegmentWriter::encode(1, 1, &rows, AppendSegmentConfig::default())
            .expect("encode segment");
        let mut corrupt = output.encoded.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(matches!(
            AppendSegmentReader::open(corrupt.into(), AppendSegmentConfig::default()),
            Err(AppendTableError::Corruption(_))
        ));

        let limits = AppendSegmentConfig {
            max_decoded_block_bytes: 32,
            ..AppendSegmentConfig::default()
        };
        assert!(matches!(
            AppendSegmentReader::open(output.encoded, limits),
            Err(AppendTableError::Admission(_))
        ));
    }

    #[test]
    fn global_partition_round_trips() {
        let mut row = append_row("unused", 1, 16);
        row.partition_key = RelationalKey(Vec::new());
        let output = AppendSegmentWriter::encode(1, 1, &[row], AppendSegmentConfig::default())
            .expect("encode global partition");
        let reader = AppendSegmentReader::open(output.encoded, AppendSegmentConfig::default())
            .expect("open segment");
        let read = reader
            .read_partition("events", &RelationalKey(Vec::new()), None, 1)
            .expect("read global partition");
        assert_eq!(read.rows.len(), 1);
    }
}
