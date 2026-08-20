use super::binary::{
    compare_rows, read_u16_at as read_u16, read_u32_at as read_u32, read_u64_at as read_u64,
    to_usize, Decoder, Encoder,
};
use super::{
    decode_append_wal_batch, encode_append_wal_batch, AppendDecodeLimits,
    AppendSegmentArtifactMetadata, AppendSegmentConfig, AppendSegmentReadOutput,
    AppendSegmentReadReport, AppendSegmentReader, AppendSegmentWriter, AppendTableError,
    AppendTableRow, AppendTableSchema, AppendTransaction, AppendWrite,
};
use crate::{durable_replace_file, RelationalKey};
use skein_integrity::{integrity_digest, Sha256Digest, SHA256_BYTES};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MANIFEST_MAGIC: &[u8; 8] = b"SKAPMAN1";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_HEADER_BYTES: usize = 128;
const MANIFEST_INTEGRITY_OFFSET: usize = 84;

pub fn append_segment_file(generation: u64) -> String {
    format!("append-{generation}.segment.skein")
}

pub fn append_generation_manifest_file(generation: u64) -> String {
    format!("append-{generation}.manifest.skein")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendPublicationConfig {
    pub segment: AppendSegmentConfig,
    pub max_manifest_bytes: usize,
    pub max_schemas: usize,
    pub max_segments: usize,
    pub compact_after_segments: usize,
    pub max_compaction_rows: usize,
    pub max_compaction_payload_bytes: usize,
    pub max_schema_bytes: usize,
}

impl Default for AppendPublicationConfig {
    fn default() -> Self {
        Self {
            segment: AppendSegmentConfig::default(),
            max_manifest_bytes: 64 * 1024 * 1024,
            max_schemas: 4096,
            max_segments: 65_536,
            compact_after_segments: 16,
            max_compaction_rows: 1_000_000,
            max_compaction_payload_bytes: 256 * 1024 * 1024,
            max_schema_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendSegmentBinding {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub artifact: AppendSegmentArtifactMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendGenerationManifest {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub previous_generation: Option<u64>,
    pub root_set_digest: Sha256Digest,
    pub schemas: BTreeMap<String, AppendTableSchema>,
    pub segments: Vec<AppendSegmentBinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendGenerationArtifacts {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub root_set_digest: Sha256Digest,
    pub manifest_artifact: AppendSegmentArtifactMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendPublicationPhase {
    CandidateStarted,
    CandidateSegmentDurable,
    CandidateManifestDurable,
    CanonicalSelectionDeferred,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendPublicationReport {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub rows_written: usize,
    pub segment_bytes_written: u64,
    pub manifest_bytes_written: u64,
    pub compacted_segments: usize,
    pub rows_rewritten: usize,
    pub compaction_deferred: bool,
    pub generation_artifacts: AppendGenerationArtifacts,
    pub events: [AppendPublicationPhase; 4],
}

pub struct AppendPublisher;

struct AppendCompactionPlan {
    due: bool,
    checkpoint_rows: Option<Vec<AppendTableRow>>,
}

fn plan_compaction(
    previous: Option<&AppendGenerationReader>,
    rows: &[AppendTableRow],
    config: AppendPublicationConfig,
) -> Result<AppendCompactionPlan, AppendTableError> {
    let prior_segment_count = previous.map_or(0, |reader| reader.manifest.segments.len());
    let due =
        !rows.is_empty() && prior_segment_count.saturating_add(1) > config.compact_after_segments;
    if !due || rows.len() > config.max_compaction_rows {
        return Ok(AppendCompactionPlan {
            due,
            checkpoint_rows: None,
        });
    }

    let live_payload_bytes = rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(super::estimated_row_bytes(&row.row)?)
            .ok_or_else(|| {
                AppendTableError::Admission(
                    "append compaction payload size overflows usize".to_string(),
                )
            })
    })?;
    if live_payload_bytes > config.max_compaction_payload_bytes {
        return Ok(AppendCompactionPlan {
            due,
            checkpoint_rows: None,
        });
    }

    let previous = previous.ok_or_else(|| {
        AppendTableError::Corruption("append compaction requires a previous generation".to_string())
    })?;
    let mut checkpoint_rows = previous.checkpoint_rows_bounded(
        config.max_compaction_rows - rows.len(),
        config.max_compaction_payload_bytes - live_payload_bytes,
    )?;
    if let Some(checkpoint_rows) = checkpoint_rows.as_mut() {
        checkpoint_rows.extend_from_slice(rows);
        checkpoint_rows.sort_unstable_by(compare_rows);
    }
    Ok(AppendCompactionPlan {
        due,
        checkpoint_rows,
    })
}

impl AppendPublisher {
    pub fn publish_candidate(
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        previous: Option<&AppendGenerationReader>,
        schemas: &BTreeMap<String, AppendTableSchema>,
        rows: &[AppendTableRow],
        config: AppendPublicationConfig,
    ) -> Result<AppendPublicationReport, AppendTableError> {
        validate_publication_request(
            generation,
            source_commit_epoch,
            previous,
            schemas,
            rows,
            config,
        )?;
        fs::create_dir_all(directory).map_err(durability("create append directory"))?;

        let prior_segment_count = previous.map_or(0, |reader| reader.manifest.segments.len());
        let compaction = plan_compaction(previous, rows, config)?;
        let compact = compaction.checkpoint_rows.is_some();
        let rows_to_write = compaction.checkpoint_rows.as_deref().unwrap_or(rows);
        let mut segments = if compact {
            Vec::new()
        } else {
            previous
                .map(|reader| reader.manifest.segments.clone())
                .unwrap_or_default()
        };
        let mut segment_bytes_written = 0;
        if !rows_to_write.is_empty() {
            let output = AppendSegmentWriter::encode(
                generation,
                source_commit_epoch,
                rows_to_write,
                config.segment,
            )?;
            write_durable_artifact(directory, &append_segment_file(generation), &output.encoded)?;
            segment_bytes_written = output.artifact.encoded_len;
            segments.push(AppendSegmentBinding {
                generation,
                source_commit_epoch,
                artifact: output.artifact,
            });
        }
        if segments.len() > config.max_segments {
            return Err(AppendTableError::Admission(format!(
                "append generation contains {} segments, exceeding limit {}",
                segments.len(),
                config.max_segments
            )));
        }

        let mut manifest = AppendGenerationManifest {
            generation,
            source_commit_epoch,
            previous_generation: previous.map(|reader| reader.manifest.generation),
            root_set_digest: integrity_digest(&[]).sha256,
            schemas: schemas.clone(),
            segments,
        };
        let payload = encode_manifest_payload(&manifest, config)?;
        manifest.root_set_digest = integrity_digest(&payload).sha256;
        let encoded_manifest = encode_manifest_with_payload(&manifest, payload, config)?;
        let manifest_digest = integrity_digest(&encoded_manifest);
        let manifest_artifact = AppendSegmentArtifactMetadata {
            encoded_len: encoded_manifest.len() as u64,
            encoded_crc32c: manifest_digest.crc32c.get(),
            encoded_sha256: manifest_digest.sha256,
        };
        write_durable_artifact(
            directory,
            &append_generation_manifest_file(generation),
            &encoded_manifest,
        )?;
        let generation_artifacts = AppendGenerationArtifacts {
            generation,
            source_commit_epoch,
            root_set_digest: manifest.root_set_digest,
            manifest_artifact,
        };
        Ok(AppendPublicationReport {
            generation,
            source_commit_epoch,
            rows_written: rows.len(),
            segment_bytes_written,
            manifest_bytes_written: manifest_artifact.encoded_len,
            compacted_segments: if compact { prior_segment_count } else { 0 },
            rows_rewritten: if compact { rows_to_write.len() } else { 0 },
            compaction_deferred: compaction.due && !compact,
            generation_artifacts,
            events: [
                AppendPublicationPhase::CandidateStarted,
                AppendPublicationPhase::CandidateSegmentDurable,
                AppendPublicationPhase::CandidateManifestDurable,
                AppendPublicationPhase::CanonicalSelectionDeferred,
            ],
        })
    }
}

#[derive(Debug, Clone)]
pub struct AppendGenerationReader {
    manifest: Arc<AppendGenerationManifest>,
    segments: Arc<[AppendSegmentReader]>,
}

impl AppendGenerationReader {
    pub fn open_bound(
        directory: &Path,
        expected: AppendGenerationArtifacts,
        config: AppendPublicationConfig,
    ) -> Result<Self, AppendTableError> {
        validate_config(config)?;
        let manifest_path = directory.join(append_generation_manifest_file(expected.generation));
        let encoded = read_bounded(
            &manifest_path,
            config.max_manifest_bytes,
            "append generation manifest",
        )?;
        verify_artifact(
            &encoded,
            expected.manifest_artifact,
            "append generation manifest",
        )?;
        let manifest = decode_manifest(&encoded, config)?;
        if manifest.generation != expected.generation
            || manifest.source_commit_epoch != expected.source_commit_epoch
            || manifest.root_set_digest != expected.root_set_digest
        {
            return Err(AppendTableError::Corruption(
                "append generation manifest does not match its canonical binding".to_string(),
            ));
        }

        let mut readers = Vec::with_capacity(manifest.segments.len());
        let mut watermarks: BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>> =
            BTreeMap::new();
        for binding in &manifest.segments {
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
            let reader =
                AppendSegmentReader::open_bound_file(&path, binding.artifact, config.segment)?;
            if reader.generation() != binding.generation
                || reader.source_commit_epoch() != binding.source_commit_epoch
            {
                return Err(AppendTableError::Corruption(
                    "append segment identity differs from its generation binding".to_string(),
                ));
            }
            for descriptor in reader.descriptors() {
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
            }
            readers.push(reader);
        }
        Ok(Self {
            manifest: Arc::new(manifest),
            segments: readers.into(),
        })
    }

    pub fn manifest(&self) -> &AppendGenerationManifest {
        &self.manifest
    }

    pub fn segment_bindings(&self) -> &[AppendSegmentBinding] {
        &self.manifest.segments
    }

    pub fn segment_payload_resident_bytes(&self) -> usize {
        self.segments
            .iter()
            .map(AppendSegmentReader::payload_resident_bytes)
            .fold(0, usize::saturating_add)
    }

    pub fn descriptor_count(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.descriptors().len())
            .fold(0, usize::saturating_add)
    }

    pub fn deep_scrub(&self) -> Result<AppendSegmentReadReport, AppendTableError> {
        let mut report = AppendSegmentReadReport::default();
        for segment in self.segments.iter() {
            report.merge(segment.deep_scrub()?);
        }
        Ok(report)
    }

    pub fn checkpoint_rows(
        &self,
        max_rows: usize,
    ) -> Result<Vec<AppendTableRow>, AppendTableError> {
        let mut rows = Vec::new();
        for segment in self.segments.iter() {
            let remaining = max_rows.checked_sub(rows.len()).ok_or_else(|| {
                AppendTableError::Admission(
                    "append checkpoint row count exceeds configured limit".to_string(),
                )
            })?;
            rows.extend(segment.checkpoint_rows(remaining)?);
        }
        rows.sort_unstable_by(compare_rows);
        if !rows
            .windows(2)
            .all(|pair| compare_rows(&pair[0], &pair[1]).is_lt())
        {
            return Err(AppendTableError::Corruption(
                "append checkpoint rows overlap or regress".to_string(),
            ));
        }
        Ok(rows)
    }

    pub fn checkpoint_rows_bounded(
        &self,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<Option<Vec<AppendTableRow>>, AppendTableError> {
        let expected_rows = self.segments.iter().try_fold(0usize, |total, segment| {
            segment
                .descriptors()
                .iter()
                .try_fold(total, |total, descriptor| {
                    total
                        .checked_add(descriptor.row_count as usize)
                        .ok_or_else(|| {
                            AppendTableError::Admission("append row count overflow".to_string())
                        })
                })
        })?;
        if expected_rows > max_rows {
            return Ok(None);
        }

        let mut rows = Vec::with_capacity(expected_rows);
        let mut payload_bytes = 0usize;
        for segment in self.segments.iter() {
            let Some(segment_rows) = segment.checkpoint_rows_bounded(
                max_rows.saturating_sub(rows.len()),
                max_payload_bytes.saturating_sub(payload_bytes),
            )?
            else {
                return Ok(None);
            };
            for row in &segment_rows {
                let Ok(row_bytes) = super::estimated_row_bytes(&row.row) else {
                    return Ok(None);
                };
                payload_bytes = match payload_bytes.checked_add(row_bytes) {
                    Some(payload_bytes) if payload_bytes <= max_payload_bytes => payload_bytes,
                    _ => return Ok(None),
                };
            }
            rows.extend(segment_rows);
        }
        rows.sort_unstable_by(compare_rows);
        if rows.len() != expected_rows
            || !rows
                .windows(2)
                .all(|pair| compare_rows(&pair[0], &pair[1]).is_lt())
        {
            return Err(AppendTableError::Corruption(
                "append checkpoint rows overlap or regress".to_string(),
            ));
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
        if !self.manifest.schemas.contains_key(table) {
            return Err(AppendTableError::Schema(format!(
                "unknown append table {table}"
            )));
        }
        let mut rows = Vec::with_capacity(max_rows);
        let mut report = AppendSegmentReadReport::default();
        for segment in self.segments.iter() {
            if rows.len() == max_rows {
                break;
            }
            let read = segment.read_partition_bounded(
                table,
                partition,
                after,
                max_rows - rows.len(),
                max_payload_bytes.saturating_sub(report.output_payload_bytes),
            )?;
            report.merge(read.report);
            rows.extend(read.rows);
        }
        Ok(AppendSegmentReadOutput { rows, report })
    }

    pub fn watermarks(&self) -> BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>> {
        let mut watermarks: BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>> =
            BTreeMap::new();
        for reader in self.segments.iter() {
            for descriptor in reader.descriptors() {
                watermarks
                    .entry(descriptor.table.clone())
                    .or_default()
                    .insert(
                        descriptor.partition_key.clone(),
                        descriptor.max_order_key.clone(),
                    );
            }
        }
        watermarks
    }
}

fn validate_publication_request(
    generation: u64,
    source_commit_epoch: u64,
    previous: Option<&AppendGenerationReader>,
    schemas: &BTreeMap<String, AppendTableSchema>,
    rows: &[AppendTableRow],
    config: AppendPublicationConfig,
) -> Result<(), AppendTableError> {
    validate_config(config)?;
    let empty_epoch_zero = source_commit_epoch == 0
        && schemas.is_empty()
        && rows.is_empty()
        && previous.is_none_or(|reader| {
            reader.manifest.schemas.is_empty() && reader.manifest.segments.is_empty()
        });
    if generation == 0 || (source_commit_epoch == 0 && !empty_epoch_zero) {
        return Err(AppendTableError::Admission(
            "append generation must be non-zero and source commit epoch zero is reserved for an empty append generation"
                .to_string(),
        ));
    }
    if schemas.len() > config.max_schemas {
        return Err(AppendTableError::Admission(format!(
            "append generation contains {} schemas, exceeding limit {}",
            schemas.len(),
            config.max_schemas
        )));
    }
    for (name, schema) in schemas {
        if name != &schema.name {
            return Err(AppendTableError::Schema(format!(
                "append schema map key {name} differs from schema name {}",
                schema.name
            )));
        }
    }
    if let Some(previous) = previous {
        if generation <= previous.manifest.generation
            || source_commit_epoch < previous.manifest.source_commit_epoch
        {
            return Err(AppendTableError::Constraint(
                "append generation and commit epoch must advance monotonically".to_string(),
            ));
        }
        for (name, prior) in &previous.manifest.schemas {
            if schemas.get(name) != Some(prior) {
                return Err(AppendTableError::Schema(format!(
                    "append table {name} cannot be removed or rewritten"
                )));
            }
        }
    }
    if let Some(row) = rows.iter().find(|row| !schemas.contains_key(&row.table)) {
        return Err(AppendTableError::Schema(format!(
            "append checkpoint row references unknown table {}",
            row.table
        )));
    }
    Ok(())
}

fn validate_config(config: AppendPublicationConfig) -> Result<(), AppendTableError> {
    if config.max_manifest_bytes < MANIFEST_HEADER_BYTES
        || config.max_schemas == 0
        || config.max_segments == 0
        || config.compact_after_segments == 0
        || config.compact_after_segments > config.max_segments
        || config.max_compaction_rows == 0
        || config.max_compaction_rows > config.segment.max_rows
        || config.max_compaction_payload_bytes == 0
        || config.max_schema_bytes == 0
    {
        return Err(AppendTableError::Admission(
            "append publication limits must be non-zero and admit the fixed manifest header"
                .to_string(),
        ));
    }
    Ok(())
}

fn encode_manifest_payload(
    manifest: &AppendGenerationManifest,
    config: AppendPublicationConfig,
) -> Result<Vec<u8>, AppendTableError> {
    let mut encoder = Encoder::default();
    for schema in manifest.schemas.values() {
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
                config.max_schema_bytes
            )));
        }
        encoder.bytes(&encoded, "append manifest schema record")?;
    }
    for binding in &manifest.segments {
        encoder.u64(binding.generation);
        encoder.u64(binding.source_commit_epoch);
        encode_artifact(binding.artifact, &mut encoder);
    }
    Ok(encoder.finish())
}

fn encode_manifest_with_payload(
    manifest: &AppendGenerationManifest,
    payload: Vec<u8>,
    config: AppendPublicationConfig,
) -> Result<Vec<u8>, AppendTableError> {
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
    encoded.extend_from_slice(&0u64.to_le_bytes());
    debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);
    encoded.extend_from_slice(&payload);
    let mut integrity_input = Vec::with_capacity(MANIFEST_INTEGRITY_OFFSET + payload.len());
    integrity_input.extend_from_slice(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    integrity_input.extend_from_slice(&payload);
    let digest = integrity_digest(&integrity_input);
    encoded[84..88].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[88..120].copy_from_slice(digest.sha256.as_bytes());
    Ok(encoded)
}

fn decode_manifest(
    encoded: &[u8],
    config: AppendPublicationConfig,
) -> Result<AppendGenerationManifest, AppendTableError> {
    if encoded.len() < MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
        return Err(AppendTableError::Corruption(
            "append manifest header is invalid".to_string(),
        ));
    }
    let version = read_u16(encoded, 8);
    if version != MANIFEST_VERSION || read_u16(encoded, 10) != 0 || read_u64(encoded, 120) != 0 {
        return Err(AppendTableError::Corruption(format!(
            "unsupported append manifest version {version} or non-zero reserved fields"
        )));
    }
    let generation = read_u64(encoded, 12);
    let source_commit_epoch = read_u64(encoded, 20);
    let previous_raw = read_u64(encoded, 28);
    let schema_count = read_u32(encoded, 36) as usize;
    let segment_count = read_u32(encoded, 40) as usize;
    let payload_len = to_usize(read_u64(encoded, 44), "manifest payload length")?;
    if generation == 0
        || (source_commit_epoch == 0 && (schema_count != 0 || segment_count != 0))
        || schema_count > config.max_schemas
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
    if integrity_digest(payload).sha256 != root_set_digest {
        return Err(AppendTableError::Corruption(
            "append manifest root-set digest mismatch".to_string(),
        ));
    }
    let mut integrity_input = Vec::with_capacity(MANIFEST_INTEGRITY_OFFSET + payload.len());
    integrity_input.extend_from_slice(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    integrity_input.extend_from_slice(payload);
    let digest = integrity_digest(&integrity_input);
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
    }
    let mut segments = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        segments.push(AppendSegmentBinding {
            generation: decoder.u64()?,
            source_commit_epoch: decoder.u64()?,
            artifact: decode_artifact(&mut decoder)?,
        });
    }
    decoder.finish("append manifest")?;
    if !segments.windows(2).all(|pair| {
        pair[0].generation < pair[1].generation
            && pair[0].source_commit_epoch <= pair[1].source_commit_epoch
    }) || segments.last().is_some_and(|binding| {
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
    Ok(AppendGenerationManifest {
        generation,
        source_commit_epoch,
        previous_generation,
        root_set_digest,
        schemas,
        segments,
    })
}

fn encode_artifact(artifact: AppendSegmentArtifactMetadata, encoder: &mut Encoder) {
    encoder.u64(artifact.encoded_len);
    encoder.u32(artifact.encoded_crc32c);
    encoder.raw(artifact.encoded_sha256.as_bytes());
}

fn decode_artifact(
    decoder: &mut Decoder<'_>,
) -> Result<AppendSegmentArtifactMetadata, AppendTableError> {
    let artifact = AppendSegmentArtifactMetadata {
        encoded_len: decoder.u64()?,
        encoded_crc32c: decoder.u32()?,
        encoded_sha256: Sha256Digest::from_bytes(
            decoder
                .take(SHA256_BYTES, "append artifact digest")?
                .try_into()
                .expect("fixed SHA-256"),
        ),
    };
    if artifact.encoded_len == 0 {
        return Err(AppendTableError::Corruption(
            "append artifact binding has zero length".to_string(),
        ));
    }
    Ok(artifact)
}

fn write_durable_artifact(
    directory: &Path,
    file_name: &str,
    bytes: &[u8],
) -> Result<(), AppendTableError> {
    let destination = directory.join(file_name);
    let temporary = temporary_path(&destination);
    {
        let mut file = File::create(&temporary).map_err(durability("create append candidate"))?;
        file.write_all(bytes)
            .map_err(durability("write append candidate"))?;
        file.sync_all()
            .map_err(durability("synchronize append candidate"))?;
    }
    durable_replace_file(&temporary, &destination).map_err(durability("publish append candidate"))
}

fn temporary_path(destination: &Path) -> PathBuf {
    let mut name = destination.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    destination.with_file_name(name)
}

fn read_bounded(path: &Path, max_bytes: usize, context: &str) -> Result<Vec<u8>, AppendTableError> {
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
    file.take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut encoded)
        .map_err(durability("read append artifact"))?;
    if encoded.len() > max_bytes {
        return Err(AppendTableError::Admission(format!(
            "{context} exceeds limit {max_bytes}"
        )));
    }
    Ok(encoded)
}

fn verify_artifact(
    encoded: &[u8],
    expected: AppendSegmentArtifactMetadata,
    context: &str,
) -> Result<(), AppendTableError> {
    let digest = integrity_digest(encoded);
    if encoded.len() as u64 != expected.encoded_len
        || digest.crc32c.get() != expected.encoded_crc32c
        || digest.sha256 != expected.encoded_sha256
    {
        return Err(AppendTableError::Corruption(format!(
            "{context} does not match its canonical binding"
        )));
    }
    Ok(())
}

fn durability(context: &'static str) -> impl FnOnce(std::io::Error) -> AppendTableError {
    move |error| AppendTableError::Durability(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationalColumnSchema, RelationalRow, RelationalScalarType, RelationalValue};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn directory(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("skein-append-{name}-{nonce}"))
    }

    fn schema() -> AppendTableSchema {
        AppendTableSchema {
            name: "events".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "stream".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "sequence".to_string(),
                    scalar_type: RelationalScalarType::BigInt,
                    nullable: false,
                    default: None,
                },
            ],
            partition_key: vec!["stream".to_string()],
            order_key: vec!["sequence".to_string()],
        }
    }

    fn row(sequence: i64) -> AppendTableRow {
        AppendTableRow {
            table: "events".to_string(),
            partition_key: RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
            order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
            row: RelationalRow::new(vec![
                RelationalValue::Text("alpha".to_string()),
                RelationalValue::BigInt(sequence),
            ]),
        }
    }

    #[test]
    fn candidate_is_opened_only_through_exact_binding() {
        let directory = directory("binding");
        let schemas = BTreeMap::from([("events".to_string(), schema())]);
        let report = AppendPublisher::publish_candidate(
            &directory,
            1,
            1,
            None,
            &schemas,
            &[row(1), row(2)],
            AppendPublicationConfig::default(),
        )
        .expect("publish candidate");
        assert_eq!(
            report.events[3],
            AppendPublicationPhase::CanonicalSelectionDeferred
        );
        let reader = AppendGenerationReader::open_bound(
            &directory,
            report.generation_artifacts,
            AppendPublicationConfig::default(),
        )
        .expect("open bound generation");
        assert_eq!(reader.segment_payload_resident_bytes(), 0);
        assert_eq!(reader.descriptor_count(), 1);
        assert!(reader.segment_bindings()[0].artifact.encoded_len > 0);
        let read = reader
            .read_partition(
                "events",
                &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                None,
                10,
            )
            .expect("read generation");
        assert_eq!(read.rows.len(), 2);
        assert!(reader.segment_payload_resident_bytes() > 0);
        let cached = reader
            .read_partition(
                "events",
                &RelationalKey(vec![RelationalValue::Text("alpha".to_string())]),
                None,
                10,
            )
            .expect("read cached generation");
        assert_eq!(cached.report.read_result_cache_hits, 1);
        assert_eq!(cached.report.compressed_bytes_read, 0);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn source_epoch_zero_is_reserved_for_an_empty_generation() {
        let directory = directory("empty-epoch-zero");
        let empty = AppendPublisher::publish_candidate(
            &directory,
            1,
            0,
            None,
            &BTreeMap::new(),
            &[],
            AppendPublicationConfig::default(),
        )
        .expect("publish empty epoch-zero generation");
        AppendGenerationReader::open_bound(
            &directory,
            empty.generation_artifacts,
            AppendPublicationConfig::default(),
        )
        .expect("open empty epoch-zero generation");

        let schemas = BTreeMap::from([("events".to_string(), schema())]);
        assert!(matches!(
            AppendPublisher::publish_candidate(
                &directory,
                2,
                0,
                None,
                &schemas,
                &[],
                AppendPublicationConfig::default(),
            ),
            Err(AppendTableError::Admission(_))
        ));
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn tampered_segment_is_rejected_during_streaming_open() {
        let directory = directory("segment-tamper");
        let schemas = BTreeMap::from([("events".to_string(), schema())]);
        let report = AppendPublisher::publish_candidate(
            &directory,
            1,
            1,
            None,
            &schemas,
            &[row(1)],
            AppendPublicationConfig::default(),
        )
        .expect("publish candidate");
        let path = directory.join(append_segment_file(1));
        let mut encoded = fs::read(&path).expect("read segment");
        let last = encoded.len() - 1;
        encoded[last] ^= 1;
        fs::write(&path, encoded).expect("tamper segment");

        assert!(matches!(
            AppendGenerationReader::open_bound(
                &directory,
                report.generation_artifacts,
                AppendPublicationConfig::default()
            ),
            Err(AppendTableError::Corruption(_))
        ));
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn newer_orphan_candidate_does_not_change_pinned_generation() {
        let directory = directory("orphan");
        let schemas = BTreeMap::from([("events".to_string(), schema())]);
        let first = AppendPublisher::publish_candidate(
            &directory,
            1,
            1,
            None,
            &schemas,
            &[row(1)],
            AppendPublicationConfig::default(),
        )
        .expect("publish first candidate");
        let first_reader = AppendGenerationReader::open_bound(
            &directory,
            first.generation_artifacts,
            AppendPublicationConfig::default(),
        )
        .expect("open first generation");
        let _orphan = AppendPublisher::publish_candidate(
            &directory,
            2,
            2,
            Some(&first_reader),
            &schemas,
            &[row(2)],
            AppendPublicationConfig::default(),
        )
        .expect("publish orphan candidate");

        let pinned = AppendGenerationReader::open_bound(
            &directory,
            first.generation_artifacts,
            AppendPublicationConfig::default(),
        )
        .expect("reopen pinned generation");
        assert_eq!(pinned.manifest().generation, 1);
        assert_eq!(pinned.manifest().segments.len(), 1);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn tampered_manifest_is_rejected_by_canonical_binding() {
        let directory = directory("tamper");
        let schemas = BTreeMap::from([("events".to_string(), schema())]);
        let report = AppendPublisher::publish_candidate(
            &directory,
            1,
            1,
            None,
            &schemas,
            &[row(1)],
            AppendPublicationConfig::default(),
        )
        .expect("publish candidate");
        let path = directory.join(append_generation_manifest_file(1));
        let mut encoded = fs::read(&path).expect("read manifest");
        let last = encoded.len() - 1;
        encoded[last] ^= 1;
        fs::write(&path, encoded).expect("tamper manifest");
        assert!(matches!(
            AppendGenerationReader::open_bound(
                &directory,
                report.generation_artifacts,
                AppendPublicationConfig::default()
            ),
            Err(AppendTableError::Corruption(_))
        ));
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn bounded_compaction_replaces_incremental_segments_without_changing_rows() {
        let directory = directory("compaction");
        let schemas = BTreeMap::from([("events".to_string(), schema())]);
        let config = AppendPublicationConfig {
            compact_after_segments: 2,
            ..AppendPublicationConfig::default()
        };
        let first =
            AppendPublisher::publish_candidate(&directory, 1, 1, None, &schemas, &[row(1)], config)
                .expect("publish first segment");
        let first_reader =
            AppendGenerationReader::open_bound(&directory, first.generation_artifacts, config)
                .expect("open first generation");
        let second = AppendPublisher::publish_candidate(
            &directory,
            2,
            2,
            Some(&first_reader),
            &schemas,
            &[row(2)],
            config,
        )
        .expect("publish second segment");
        let second_reader =
            AppendGenerationReader::open_bound(&directory, second.generation_artifacts, config)
                .expect("open second generation");
        assert_eq!(second_reader.segment_bindings().len(), 2);

        let third = AppendPublisher::publish_candidate(
            &directory,
            3,
            3,
            Some(&second_reader),
            &schemas,
            &[row(3)],
            config,
        )
        .expect("compact third generation");
        assert_eq!(third.compacted_segments, 2);
        assert_eq!(third.rows_rewritten, 3);
        assert!(!third.compaction_deferred);
        let third_reader =
            AppendGenerationReader::open_bound(&directory, third.generation_artifacts, config)
                .expect("open compacted generation");
        assert_eq!(third_reader.segment_bindings().len(), 1);
        assert_eq!(third_reader.segment_bindings()[0].generation, 3);
        assert_eq!(third_reader.checkpoint_rows(3).expect("read rows").len(), 3);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn compaction_defers_without_blocking_incremental_checkpoint() {
        let directory = directory("compaction-deferred");
        let schemas = BTreeMap::from([("events".to_string(), schema())]);
        let config = AppendPublicationConfig {
            compact_after_segments: 2,
            max_compaction_rows: 2,
            ..AppendPublicationConfig::default()
        };
        let first =
            AppendPublisher::publish_candidate(&directory, 1, 1, None, &schemas, &[row(1)], config)
                .expect("publish first segment");
        let first_reader =
            AppendGenerationReader::open_bound(&directory, first.generation_artifacts, config)
                .expect("open first generation");
        let second = AppendPublisher::publish_candidate(
            &directory,
            2,
            2,
            Some(&first_reader),
            &schemas,
            &[row(2)],
            config,
        )
        .expect("publish second segment");
        let second_reader =
            AppendGenerationReader::open_bound(&directory, second.generation_artifacts, config)
                .expect("open second generation");
        let third = AppendPublisher::publish_candidate(
            &directory,
            3,
            3,
            Some(&second_reader),
            &schemas,
            &[row(3)],
            config,
        )
        .expect("publish incremental segment when compaction is over budget");

        assert!(third.compaction_deferred);
        assert_eq!(third.compacted_segments, 0);
        assert_eq!(third.rows_rewritten, 0);
        let third_reader =
            AppendGenerationReader::open_bound(&directory, third.generation_artifacts, config)
                .expect("open deferred generation");
        assert_eq!(third_reader.segment_bindings().len(), 3);
        assert_eq!(third_reader.checkpoint_rows(3).expect("read rows").len(), 3);
        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
