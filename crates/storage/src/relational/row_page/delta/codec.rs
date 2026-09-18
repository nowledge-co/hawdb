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
    durability, RelationalRowDeltaBaseBinding, RelationalRowDeltaConfig, RelationalRowDeltaError,
    RelationalRowDeltaManifest, RelationalRowDeltaTableMetadata, RowDeltaBound, RowDeltaKey,
    RowDeltaRunDescriptor, RowDeltaValue,
};
use crate::relational::row_page::{RelationalRowPageRootReader, RelationalRowPageTableRoot};
use crate::relational::{RelationalRecoverySourceIdentity, RELATIONAL_RECOVERY_SOURCE_BYTES};
use hawdb_integrity::{IntegrityDigest, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const MANIFEST_MAGIC: &[u8; 8] = b"SKRDMF01";
const RUN_MAGIC: &[u8; 8] = b"SKRDLT01";
const FORMAT_VERSION: u16 = 1;
pub(super) const MANIFEST_HEADER_BYTES: usize = 304;
const MANIFEST_INTEGRITY_OFFSET: usize = 268;
pub(super) const RUN_HEADER_BYTES: usize = 176;
const RUN_INTEGRITY_OFFSET: usize = 140;
pub(super) const ENTRY_DESCRIPTOR_BYTES: usize = 80;
const ENTRY_BINDING_OFFSET: usize = 48;
const TABLE_DESCRIPTOR_BYTES: usize = 48;
const RUN_DESCRIPTOR_BYTES: usize = 100;
const DIRTY_ENTRY_OVERHEAD_BYTES: usize = 96;

pub(super) fn validate_tables_against_base(
    base: &RelationalRowPageRootReader,
    mut tables: Vec<RelationalRowDeltaTableMetadata>,
    config: RelationalRowDeltaConfig,
) -> Result<Vec<RelationalRowDeltaTableMetadata>, RelationalRowDeltaError> {
    if tables.len() > config.max_tables.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta declares {} tables, exceeding limit {}",
            tables.len(),
            config.max_tables
        )));
    }
    tables.sort_by(|left, right| left.table.cmp(&right.table));
    if tables.windows(2).any(|pair| pair[0].table == pair[1].table) {
        return Err(RelationalRowDeltaError::Admission(
            "row delta table schemas contain duplicate names".to_string(),
        ));
    }
    if tables.len() != base.manifest().tables.len() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta declares {} tables, base root declares {}",
            tables.len(),
            base.manifest().tables.len()
        )));
    }
    for (table, base_table) in tables.iter().zip(&base.manifest().tables) {
        validate_table_schema(table, config, ErrorClass::Admission)?;
        validate_table_against_base(table, base_table)?;
    }
    let projected_len = manifest_payload_len(&tables, &[])?
        .checked_add(MANIFEST_HEADER_BYTES)
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta manifest length overflow".to_string())
        })?;
    if projected_len > config.max_manifest_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta table metadata requires {projected_len} manifest bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    Ok(tables)
}

fn validate_table_against_base(
    table: &RelationalRowDeltaTableMetadata,
    base: &RelationalRowPageTableRoot,
) -> Result<(), RelationalRowDeltaError> {
    if table.table != base.table
        || table.schema_digest != base.schema_digest
        || table.column_count != base.column_count
    {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta schema for table {} does not match the selected row root",
            table.table
        )));
    }
    Ok(())
}

pub(super) fn schema_set_digest(
    tables: &[RelationalRowDeltaTableMetadata],
) -> Result<Sha256Digest, RelationalRowDeltaError> {
    let mut hasher = IntegrityHasher::new();
    for table in tables {
        let name_len = u32::try_from(table.table.len()).map_err(|_| {
            RelationalRowDeltaError::Admission(
                "row delta table name length does not fit u32".to_string(),
            )
        })?;
        hasher.update(&name_len.to_le_bytes());
        hasher.update(&table.column_count.get().to_le_bytes());
        hasher.update(table.schema_digest.as_bytes());
        hasher.update(table.table.as_bytes());
    }
    Ok(hasher.finish().sha256)
}

pub(super) fn run_set_digest(
    runs: &[RowDeltaRunDescriptor],
) -> Result<Sha256Digest, RelationalRowDeltaError> {
    let mut hasher = IntegrityHasher::new();
    for run in runs {
        let encoded = encode_run_descriptor(run)?;
        hasher.update(&encoded);
    }
    Ok(hasher.finish().sha256)
}

pub(super) fn ensure_manifest_capacity(
    tables: &[RelationalRowDeltaTableMetadata],
    runs: &[RowDeltaRunDescriptor],
    config: RelationalRowDeltaConfig,
) -> Result<(), RelationalRowDeltaError> {
    let encoded_len = MANIFEST_HEADER_BYTES
        .checked_add(manifest_payload_len(tables, runs)?)
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta manifest length overflow".to_string())
        })?;
    if encoded_len > config.max_manifest_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta manifest requires {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    Ok(())
}

pub(super) fn ensure_next_run_manifest_capacity(
    tables: &[RelationalRowDeltaTableMetadata],
    runs: &[RowDeltaRunDescriptor],
    lower_key_bytes: usize,
    upper_key_bytes: usize,
    config: RelationalRowDeltaConfig,
) -> Result<(), RelationalRowDeltaError> {
    let encoded_len = MANIFEST_HEADER_BYTES
        .checked_add(manifest_payload_len(tables, runs)?)
        .and_then(|bytes| bytes.checked_add(RUN_DESCRIPTOR_BYTES))
        .and_then(|bytes| bytes.checked_add(lower_key_bytes))
        .and_then(|bytes| bytes.checked_add(upper_key_bytes))
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta manifest length overflow".to_string())
        })?;
    if encoded_len > config.max_manifest_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta manifest requires {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    Ok(())
}

pub(super) fn encode_manifest(
    manifest: &RelationalRowDeltaManifest,
    config: RelationalRowDeltaConfig,
) -> Result<Vec<u8>, RelationalRowDeltaError> {
    validate_manifest(manifest, config, ErrorClass::Admission)?;
    let payload_len = manifest_payload_len(&manifest.tables, &manifest.runs)?;
    let total_len = MANIFEST_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta manifest length overflow".to_string())
        })?;
    if total_len > config.max_manifest_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta manifest contains {total_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let table_count = u32::try_from(manifest.tables.len()).map_err(|_| {
        RelationalRowDeltaError::Admission(
            "row delta manifest table count does not fit u32".to_string(),
        )
    })?;
    let run_count = u32::try_from(manifest.runs.len()).map_err(|_| {
        RelationalRowDeltaError::Admission(
            "row delta manifest run count does not fit u32".to_string(),
        )
    })?;
    let payload_len_u64 = u64::try_from(payload_len).map_err(|_| {
        RelationalRowDeltaError::Admission(
            "row delta manifest payload length does not fit u64".to_string(),
        )
    })?;
    let mut payload = Vec::with_capacity(payload_len);
    for table in &manifest.tables {
        encode_table_metadata(table, &mut payload)?;
    }
    for run in &manifest.runs {
        payload.extend_from_slice(&encode_run_descriptor(run)?);
    }
    debug_assert_eq!(payload.len(), payload_len);

    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(MANIFEST_MAGIC);
    encoded.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&manifest.base.generation.to_le_bytes());
    encoded.extend_from_slice(&manifest.delta_generation.to_le_bytes());
    encoded.extend_from_slice(&manifest.base.source_commit_epoch.to_le_bytes());
    encoded.extend_from_slice(&manifest.visible_commit_epoch.to_le_bytes());
    manifest
        .recovery_source
        .encode_into(&mut encoded)
        .map_err(|reason| RelationalRowDeltaError::Admission(reason.to_string()))?;
    encoded.extend_from_slice(manifest.base.root_set_digest.as_bytes());
    encoded.extend_from_slice(manifest.schema_set_digest.as_bytes());
    encoded.extend_from_slice(manifest.run_set_digest.as_bytes());
    match manifest.overflow_root {
        Some(binding) => {
            encoded.extend_from_slice(&binding.generation.to_le_bytes());
            encoded.extend_from_slice(&binding.source_commit_epoch.to_le_bytes());
            encoded.extend_from_slice(binding.root_set_digest.as_bytes());
        }
        None => encoded.extend_from_slice(&[0u8; 48]),
    }
    encoded.extend_from_slice(&table_count.to_le_bytes());
    encoded.extend_from_slice(&run_count.to_le_bytes());
    encoded.extend_from_slice(&manifest.total_entries.to_le_bytes());
    encoded.extend_from_slice(&payload_len_u64.to_le_bytes());
    debug_assert_eq!(encoded.len(), MANIFEST_INTEGRITY_OFFSET);
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded);
    hasher.update(&payload);
    let digest = hasher.finish();
    encoded.extend_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded.extend_from_slice(digest.sha256.as_bytes());
    debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub(super) fn read_manifest_if_exists(
    path: &Path,
    config: RelationalRowDeltaConfig,
) -> Result<Option<RelationalRowDeltaManifest>, RelationalRowDeltaError> {
    match fs::metadata(path) {
        Ok(_) => read_manifest(path, config).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(durability("read row delta manifest metadata")(error)),
    }
}

pub(super) fn read_manifest(
    path: &Path,
    config: RelationalRowDeltaConfig,
) -> Result<RelationalRowDeltaManifest, RelationalRowDeltaError> {
    let encoded_len = fs::metadata(path)
        .map_err(durability("read row delta manifest metadata"))?
        .len();
    if encoded_len > config.max_manifest_bytes.get() as u64 {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let capacity = usize::try_from(encoded_len).map_err(|_| {
        RelationalRowDeltaError::Admission(
            "row delta manifest length exceeds this target".to_string(),
        )
    })?;
    let mut encoded = Vec::with_capacity(capacity);
    File::open(path)
        .map_err(durability("open row delta manifest"))?
        .read_to_end(&mut encoded)
        .map_err(durability("read row delta manifest"))?;
    decode_manifest(&encoded, config)
}

fn decode_manifest(
    encoded: &[u8],
    config: RelationalRowDeltaConfig,
) -> Result<RelationalRowDeltaManifest, RelationalRowDeltaError> {
    if encoded.len() < MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
        return Err(RelationalRowDeltaError::Corrupt(
            "invalid row delta manifest header".to_string(),
        ));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    if version != FORMAT_VERSION || flags != 0 {
        return Err(RelationalRowDeltaError::Corrupt(format!(
            "unsupported row delta manifest version {version} or flags {flags}"
        )));
    }
    let table_count = read_u32(&encoded[244..248]) as usize;
    let run_count = read_u32(&encoded[248..252]) as usize;
    if table_count > config.max_tables.get() || run_count > config.max_runs.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta manifest declares {table_count} tables/{run_count} runs beyond limits {}/{}",
            config.max_tables, config.max_runs
        )));
    }
    let payload_len = usize::try_from(read_u64(&encoded[260..268])).map_err(|_| {
        RelationalRowDeltaError::Corrupt(
            "row delta manifest payload length overflows usize".to_string(),
        )
    })?;
    let expected_len = MANIFEST_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| {
            RelationalRowDeltaError::Corrupt("row delta manifest length overflow".to_string())
        })?;
    if encoded.len() != expected_len || encoded.len() > config.max_manifest_bytes.get() {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta manifest length mismatch".to_string(),
        ));
    }
    let payload = &encoded[MANIFEST_HEADER_BYTES..];
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    hasher.update(payload);
    let digest = hasher.finish();
    if digest.crc32c.get() != read_u32(&encoded[268..272])
        || digest.sha256.as_bytes() != &encoded[272..304]
    {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta manifest checksum mismatch".to_string(),
        ));
    }

    let mut offset = 0usize;
    let mut tables = Vec::with_capacity(table_count);
    for _ in 0..table_count {
        tables.push(decode_table_metadata(payload, &mut offset, config)?);
    }
    let mut runs = Vec::with_capacity(run_count);
    for _ in 0..run_count {
        runs.push(decode_run_descriptor(payload, &mut offset, config)?);
    }
    if offset != payload.len() {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta manifest contains trailing bytes".to_string(),
        ));
    }
    let overflow_generation = read_u64(&encoded[196..204]);
    let overflow_epoch = read_u64(&encoded[204..212]);
    let overflow_digest = Sha256Digest::from_bytes(
        encoded[212..244]
            .try_into()
            .expect("overflow digest has a fixed length"),
    );
    let overflow_root = if overflow_generation == 0 {
        if overflow_epoch != 0 || overflow_digest.as_bytes() != &[0u8; SHA256_BYTES] {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta manifest has a partial overflow binding".to_string(),
            ));
        }
        None
    } else {
        Some(crate::relational::RelationalOverflowRootBinding {
            generation: overflow_generation,
            source_commit_epoch: overflow_epoch,
            root_set_digest: overflow_digest,
        })
    };
    let manifest = RelationalRowDeltaManifest {
        base: RelationalRowDeltaBaseBinding {
            generation: read_u64(&encoded[12..20]),
            source_commit_epoch: read_u64(&encoded[28..36]),
            root_set_digest: Sha256Digest::from_bytes(
                encoded[100..132]
                    .try_into()
                    .expect("base root digest has a fixed length"),
            ),
        },
        delta_generation: read_u64(&encoded[20..28]),
        visible_commit_epoch: read_u64(&encoded[36..44]),
        recovery_source: RelationalRecoverySourceIdentity::decode(
            &encoded[44..44 + RELATIONAL_RECOVERY_SOURCE_BYTES],
        )
        .map_err(|reason| RelationalRowDeltaError::Corrupt(reason.to_string()))?,
        schema_set_digest: Sha256Digest::from_bytes(
            encoded[132..164]
                .try_into()
                .expect("schema digest has a fixed length"),
        ),
        run_set_digest: Sha256Digest::from_bytes(
            encoded[164..196]
                .try_into()
                .expect("run digest has a fixed length"),
        ),
        overflow_root,
        tables,
        runs,
        total_entries: read_u64(&encoded[252..260]),
    };
    validate_manifest(&manifest, config, ErrorClass::Corrupt)?;
    Ok(manifest)
}

fn manifest_payload_len(
    tables: &[RelationalRowDeltaTableMetadata],
    runs: &[RowDeltaRunDescriptor],
) -> Result<usize, RelationalRowDeltaError> {
    let table_bytes = tables.iter().try_fold(0usize, |bytes, table| {
        bytes
            .checked_add(TABLE_DESCRIPTOR_BYTES)
            .and_then(|bytes| bytes.checked_add(table.table.len()))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta table metadata length overflow".to_string(),
                )
            })
    })?;
    runs.iter().try_fold(table_bytes, |bytes, run| {
        bytes
            .checked_add(RUN_DESCRIPTOR_BYTES)
            .and_then(|bytes| bytes.checked_add(run.lower_bound.encoded_primary_key.len()))
            .and_then(|bytes| bytes.checked_add(run.upper_bound.encoded_primary_key.len()))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta run metadata length overflow".to_string(),
                )
            })
    })
}

fn encode_table_metadata(
    table: &RelationalRowDeltaTableMetadata,
    encoded: &mut Vec<u8>,
) -> Result<(), RelationalRowDeltaError> {
    let name_len = u32::try_from(table.table.len()).map_err(|_| {
        RelationalRowDeltaError::Admission(
            "row delta table name length does not fit u32".to_string(),
        )
    })?;
    encoded.extend_from_slice(&name_len.to_le_bytes());
    encoded.extend_from_slice(&table.column_count.get().to_le_bytes());
    encoded.extend_from_slice(&table.row_count.to_le_bytes());
    encoded.extend_from_slice(table.schema_digest.as_bytes());
    encoded.extend_from_slice(table.table.as_bytes());
    Ok(())
}

fn decode_table_metadata(
    encoded: &[u8],
    offset: &mut usize,
    config: RelationalRowDeltaConfig,
) -> Result<RelationalRowDeltaTableMetadata, RelationalRowDeltaError> {
    let fixed = take(
        encoded,
        offset,
        TABLE_DESCRIPTOR_BYTES,
        "row delta table descriptor",
    )?;
    let name_len = read_u32(&fixed[..4]) as usize;
    let column_count = std::num::NonZeroU32::new(read_u32(&fixed[4..8])).ok_or_else(|| {
        RelationalRowDeltaError::Corrupt("row delta table has zero columns".to_string())
    })?;
    let name = take(encoded, offset, name_len, "row delta table name")?;
    let table = RelationalRowDeltaTableMetadata {
        table: std::str::from_utf8(name)
            .map_err(|error| {
                RelationalRowDeltaError::Corrupt(format!(
                    "row delta table name is not UTF-8: {error}"
                ))
            })?
            .to_string(),
        schema_digest: Sha256Digest::from_bytes(
            fixed[16..48]
                .try_into()
                .expect("schema digest has a fixed length"),
        ),
        column_count,
        row_count: read_u64(&fixed[8..16]),
    };
    validate_table_schema(&table, config, ErrorClass::Corrupt)?;
    Ok(table)
}

fn encode_run_descriptor(run: &RowDeltaRunDescriptor) -> Result<Vec<u8>, RelationalRowDeltaError> {
    let lower_len = u32::try_from(run.lower_bound.encoded_primary_key.len()).map_err(|_| {
        RelationalRowDeltaError::Admission(
            "row delta lower key length does not fit u32".to_string(),
        )
    })?;
    let upper_len = u32::try_from(run.upper_bound.encoded_primary_key.len()).map_err(|_| {
        RelationalRowDeltaError::Admission(
            "row delta upper key length does not fit u32".to_string(),
        )
    })?;
    let capacity = RUN_DESCRIPTOR_BYTES
        .checked_add(lower_len as usize)
        .and_then(|bytes| bytes.checked_add(upper_len as usize))
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission(
                "row delta run descriptor length overflow".to_string(),
            )
        })?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&run.ordinal.to_le_bytes());
    encoded.extend_from_slice(&run.start_epoch.to_le_bytes());
    encoded.extend_from_slice(&run.end_epoch.to_le_bytes());
    encoded.extend_from_slice(&run.entry_count.to_le_bytes());
    encoded.extend_from_slice(&run.encoded_len.to_le_bytes());
    encoded.extend_from_slice(&run.descriptor_bytes.to_le_bytes());
    encoded.extend_from_slice(&run.payload_bytes.to_le_bytes());
    encoded.extend_from_slice(&run.digest.crc32c.get().to_le_bytes());
    encoded.extend_from_slice(run.digest.sha256.as_bytes());
    encoded.extend_from_slice(&run.lower_bound.table_ordinal.to_le_bytes());
    encoded.extend_from_slice(&lower_len.to_le_bytes());
    encoded.extend_from_slice(&run.upper_bound.table_ordinal.to_le_bytes());
    encoded.extend_from_slice(&upper_len.to_le_bytes());
    debug_assert_eq!(encoded.len(), RUN_DESCRIPTOR_BYTES);
    encoded.extend_from_slice(&run.lower_bound.encoded_primary_key);
    encoded.extend_from_slice(&run.upper_bound.encoded_primary_key);
    Ok(encoded)
}

fn decode_run_descriptor(
    encoded: &[u8],
    offset: &mut usize,
    config: RelationalRowDeltaConfig,
) -> Result<RowDeltaRunDescriptor, RelationalRowDeltaError> {
    let fixed = take(
        encoded,
        offset,
        RUN_DESCRIPTOR_BYTES,
        "row delta run descriptor",
    )?;
    let lower_len = read_u32(&fixed[88..92]) as usize;
    let upper_len = read_u32(&fixed[96..100]) as usize;
    if lower_len > config.row_limits.max_key_bytes.get()
        || upper_len > config.row_limits.max_key_bytes.get()
    {
        return Err(RelationalRowDeltaError::Admission(
            "row delta run bound exceeds key limit".to_string(),
        ));
    }
    let lower_key = take(encoded, offset, lower_len, "row delta lower bound")?.to_vec();
    let upper_key = take(encoded, offset, upper_len, "row delta upper bound")?.to_vec();
    Ok(RowDeltaRunDescriptor {
        ordinal: read_u32(&fixed[..4]),
        start_epoch: read_u64(&fixed[4..12]),
        end_epoch: read_u64(&fixed[12..20]),
        entry_count: read_u32(&fixed[20..24]),
        encoded_len: read_u64(&fixed[24..32]),
        descriptor_bytes: read_u64(&fixed[32..40]),
        payload_bytes: read_u64(&fixed[40..48]),
        digest: IntegrityDigest {
            crc32c: hawdb_integrity::Crc32c::new(read_u32(&fixed[48..52])),
            sha256: Sha256Digest::from_bytes(
                fixed[52..84]
                    .try_into()
                    .expect("run digest has a fixed length"),
            ),
        },
        lower_bound: RowDeltaBound {
            table_ordinal: read_u32(&fixed[84..88]),
            encoded_primary_key: lower_key,
        },
        upper_bound: RowDeltaBound {
            table_ordinal: read_u32(&fixed[92..96]),
            encoded_primary_key: upper_key,
        },
    })
}

fn validate_manifest(
    manifest: &RelationalRowDeltaManifest,
    config: RelationalRowDeltaConfig,
    class: ErrorClass,
) -> Result<(), RelationalRowDeltaError> {
    let fail = |message| class.error(message);
    if manifest.base.generation == 0
        || manifest.delta_generation == 0
        || manifest.visible_commit_epoch < manifest.base.source_commit_epoch
    {
        return Err(fail("invalid row delta generation fence".to_string()));
    }
    manifest
        .recovery_source
        .validate()
        .map_err(|reason| fail(reason.to_string()))?;
    if manifest.recovery_source.end_lsn - manifest.recovery_source.start_lsn
        != manifest.visible_commit_epoch - manifest.base.source_commit_epoch
    {
        return Err(fail(
            "row delta recovery source length does not match its commit epoch range".to_string(),
        ));
    }
    if manifest.tables.len() > config.max_tables.get()
        || manifest.runs.len() > config.max_runs.get()
    {
        return Err(fail(
            "row delta manifest exceeds table or run limit".to_string(),
        ));
    }
    let mut previous_table = None;
    for table in &manifest.tables {
        validate_table_schema(table, config, class)?;
        if previous_table.is_some_and(|name: &str| name >= table.table.as_str()) {
            return Err(fail(
                "row delta table schemas are not strictly ordered".to_string(),
            ));
        }
        previous_table = Some(table.table.as_str());
    }
    if schema_set_digest(&manifest.tables)? != manifest.schema_set_digest {
        return Err(fail("row delta schema-set digest mismatch".to_string()));
    }
    if run_set_digest(&manifest.runs)? != manifest.run_set_digest {
        return Err(fail("row delta run-set digest mismatch".to_string()));
    }
    if manifest
        .overflow_root
        .is_some_and(|binding| binding.source_commit_epoch != manifest.visible_commit_epoch)
    {
        return Err(fail(
            "row delta overflow root epoch does not match visible epoch".to_string(),
        ));
    }
    let mut total_entries = 0u64;
    let mut total_run_bytes = 0u64;
    let mut previous_end = manifest.base.source_commit_epoch;
    for (position, run) in manifest.runs.iter().enumerate() {
        let expected_descriptor_bytes = (run.entry_count as u64)
            .checked_mul(ENTRY_DESCRIPTOR_BYTES as u64)
            .ok_or_else(|| fail("row delta descriptor length overflow".to_string()))?;
        let expected_encoded_len = (RUN_HEADER_BYTES as u64)
            .checked_add(run.descriptor_bytes)
            .and_then(|bytes| bytes.checked_add(run.payload_bytes))
            .ok_or_else(|| fail("row delta run length overflow".to_string()))?;
        if run.ordinal as usize != position
            || run.entry_count == 0
            || run.entry_count as usize > config.max_dirty_entries.get()
            || run.start_epoch <= manifest.base.source_commit_epoch
            || run.start_epoch > run.end_epoch
            || run.start_epoch < previous_end
            || run.end_epoch > manifest.visible_commit_epoch
            || run.descriptor_bytes != expected_descriptor_bytes
            || run.encoded_len != expected_encoded_len
            || run
                .descriptor_bytes
                .checked_add(run.payload_bytes)
                .is_none()
            || run.descriptor_bytes + run.payload_bytes > config.max_dirty_bytes.get() as u64
            || run.lower_bound > run.upper_bound
            || run.upper_bound.table_ordinal as usize >= manifest.tables.len()
        {
            return Err(fail(format!(
                "invalid row delta run descriptor at ordinal {position}"
            )));
        }
        total_entries = total_entries
            .checked_add(run.entry_count as u64)
            .ok_or_else(|| fail("row delta total entry count overflow".to_string()))?;
        total_run_bytes = total_run_bytes
            .checked_add(run.encoded_len)
            .ok_or_else(|| fail("row delta total run byte count overflow".to_string()))?;
        previous_end = run.end_epoch;
    }
    if total_entries != manifest.total_entries {
        return Err(fail(format!(
            "row delta manifest declares {} entries, descriptors contain {total_entries}",
            manifest.total_entries
        )));
    }
    if total_run_bytes > config.max_run_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta runs contain {total_run_bytes} bytes, exceeding limit {}",
            config.max_run_bytes
        )));
    }
    ensure_manifest_capacity(&manifest.tables, &manifest.runs, config)
        .map_err(|error| class.reclassify(error))?;
    Ok(())
}

fn validate_table_schema(
    table: &RelationalRowDeltaTableMetadata,
    config: RelationalRowDeltaConfig,
    class: ErrorClass,
) -> Result<(), RelationalRowDeltaError> {
    let fail = |message| class.error(message);
    if table.table.is_empty()
        || table.table.len() > config.row_limits.max_key_bytes.get()
        || table.column_count.get() as usize > config.row_limits.max_columns.get()
    {
        return Err(fail(format!(
            "invalid row delta schema for table {}",
            table.table
        )));
    }
    Ok(())
}

pub(super) struct RunWrite<'a> {
    pub base: RelationalRowDeltaBaseBinding,
    pub delta_generation: u64,
    pub schema_set_digest: Sha256Digest,
    pub ordinal: u32,
    pub entries: &'a BTreeMap<RowDeltaKey, RowDeltaValue>,
}

pub(super) fn estimated_run_encoded_len(
    entries: &BTreeMap<RowDeltaKey, RowDeltaValue>,
) -> Result<u64, RelationalRowDeltaError> {
    let descriptor_bytes = u64::try_from(entries.len())
        .ok()
        .and_then(|entries| entries.checked_mul(ENTRY_DESCRIPTOR_BYTES as u64))
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission(
                "row delta run descriptor length overflow".to_string(),
            )
        })?;
    let payload_bytes = entries.iter().try_fold(0u64, |bytes, (key, value)| {
        let key_bytes = u64::try_from(key.encoded_primary_key.len()).map_err(|_| {
            RelationalRowDeltaError::Admission("row delta key length does not fit u64".to_string())
        })?;
        let row_bytes = u64::try_from(value.encoded_row.len()).map_err(|_| {
            RelationalRowDeltaError::Admission("row delta row length does not fit u64".to_string())
        })?;
        bytes
            .checked_add(key_bytes)
            .and_then(|bytes| bytes.checked_add(row_bytes))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta run payload length overflow".to_string(),
                )
            })
    })?;
    (RUN_HEADER_BYTES as u64)
        .checked_add(descriptor_bytes)
        .and_then(|bytes| bytes.checked_add(payload_bytes))
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta encoded length overflow".to_string())
        })
}

pub(super) fn write_run(
    path: &Path,
    run: RunWrite<'_>,
    config: RelationalRowDeltaConfig,
) -> Result<RowDeltaRunDescriptor, RelationalRowDeltaError> {
    if run.entries.is_empty() {
        return Err(RelationalRowDeltaError::Admission(
            "row delta run must not be empty".to_string(),
        ));
    }
    let entry_count = u32::try_from(run.entries.len()).map_err(|_| {
        RelationalRowDeltaError::Admission("row delta run entry count does not fit u32".to_string())
    })?;
    let descriptor_bytes = (entry_count as u64)
        .checked_mul(ENTRY_DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission(
                "row delta run descriptor length overflow".to_string(),
            )
        })?;
    let payload_bytes = run.entries.iter().try_fold(0u64, |bytes, (key, value)| {
        let key_bytes = u64::try_from(key.encoded_primary_key.len()).map_err(|_| {
            RelationalRowDeltaError::Admission("row delta key length does not fit u64".to_string())
        })?;
        let row_bytes = u64::try_from(value.encoded_row.len()).map_err(|_| {
            RelationalRowDeltaError::Admission("row delta row length does not fit u64".to_string())
        })?;
        bytes
            .checked_add(key_bytes)
            .and_then(|bytes| bytes.checked_add(row_bytes))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta run payload length overflow".to_string(),
                )
            })
    })?;
    let admitted_bytes = descriptor_bytes.checked_add(payload_bytes).ok_or_else(|| {
        RelationalRowDeltaError::Admission("row delta run length overflow".to_string())
    })?;
    if admitted_bytes > config.max_dirty_bytes.get() as u64 {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta run requires {admitted_bytes} bytes, exceeding dirty limit {}",
            config.max_dirty_bytes
        )));
    }
    let encoded_len = (RUN_HEADER_BYTES as u64)
        .checked_add(admitted_bytes)
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta encoded length overflow".to_string())
        })?;
    debug_assert_eq!(encoded_len, estimated_run_encoded_len(run.entries)?);
    let start_epoch = run
        .entries
        .values()
        .map(|value| value.last_modified_epoch)
        .min()
        .expect("non-empty row delta run has a start epoch");
    let end_epoch = run
        .entries
        .values()
        .map(|value| value.last_modified_epoch)
        .max()
        .expect("non-empty row delta run has an end epoch");
    let (lower_key, _) = run.entries.first_key_value().expect("non-empty run");
    let (upper_key, _) = run.entries.last_key_value().expect("non-empty run");
    let lower_bound = RowDeltaBound {
        table_ordinal: lower_key.table_ordinal,
        encoded_primary_key: lower_key.encoded_primary_key.clone(),
    };
    let upper_bound = RowDeltaBound {
        table_ordinal: upper_key.table_ordinal,
        encoded_primary_key: upper_key.encoded_primary_key.clone(),
    };
    let header_fields = RunHeaderFields {
        base: run.base,
        delta_generation: run.delta_generation,
        schema_set_digest: run.schema_set_digest,
        ordinal: run.ordinal,
        start_epoch,
        end_epoch,
        entry_count,
        descriptor_bytes,
        payload_bytes,
    };
    let prefix = encode_run_prefix(&header_fields);
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(durability("create row delta run candidate"))?;
    file.write_all(&[0u8; RUN_HEADER_BYTES])
        .map_err(durability("reserve row delta run header"))?;
    let mut content_hasher = IntegrityHasher::new();
    content_hasher.update(&prefix);
    let mut payload_offset = 0u64;
    for (entry_ordinal, (key, value)) in run.entries.iter().enumerate() {
        let descriptor = encode_entry_descriptor(
            run.base,
            run.delta_generation,
            run.ordinal,
            entry_ordinal as u32,
            key,
            value,
            payload_offset,
        )?;
        payload_offset = payload_offset
            .checked_add(key.encoded_primary_key.len() as u64)
            .and_then(|bytes| bytes.checked_add(value.encoded_row.len() as u64))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission("row delta payload offset overflow".to_string())
            })?;
        file.write_all(&descriptor)
            .map_err(durability("write row delta entry descriptor"))?;
        content_hasher.update(&descriptor);
    }
    debug_assert_eq!(payload_offset, payload_bytes);
    for (key, value) in run.entries {
        file.write_all(&key.encoded_primary_key)
            .map_err(durability("write row delta primary key"))?;
        file.write_all(&value.encoded_row)
            .map_err(durability("write row delta row"))?;
        content_hasher.update(&key.encoded_primary_key);
        content_hasher.update(&value.encoded_row);
    }
    let content_digest = content_hasher.finish();
    let mut header = prefix;
    header.extend_from_slice(&content_digest.crc32c.get().to_le_bytes());
    header.extend_from_slice(content_digest.sha256.as_bytes());
    debug_assert_eq!(header.len(), RUN_HEADER_BYTES);
    file.seek(SeekFrom::Start(0))
        .map_err(durability("seek row delta run header"))?;
    file.write_all(&header)
        .map_err(durability("write row delta run header"))?;
    file.sync_all()
        .map_err(durability("sync row delta run candidate"))?;
    file.seek(SeekFrom::Start(0))
        .map_err(durability("seek row delta run for digest"))?;
    let artifact_digest = digest_reader(&mut file)?;
    Ok(RowDeltaRunDescriptor {
        ordinal: run.ordinal,
        start_epoch,
        end_epoch,
        entry_count,
        encoded_len,
        descriptor_bytes,
        payload_bytes,
        digest: artifact_digest,
        lower_bound,
        upper_bound,
    })
}

struct RunHeaderFields {
    base: RelationalRowDeltaBaseBinding,
    delta_generation: u64,
    schema_set_digest: Sha256Digest,
    ordinal: u32,
    start_epoch: u64,
    end_epoch: u64,
    entry_count: u32,
    descriptor_bytes: u64,
    payload_bytes: u64,
}

fn encode_run_prefix(fields: &RunHeaderFields) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(RUN_INTEGRITY_OFFSET);
    prefix.extend_from_slice(RUN_MAGIC);
    prefix.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    prefix.extend_from_slice(&0u16.to_le_bytes());
    prefix.extend_from_slice(&fields.base.generation.to_le_bytes());
    prefix.extend_from_slice(&fields.delta_generation.to_le_bytes());
    prefix.extend_from_slice(&fields.base.source_commit_epoch.to_le_bytes());
    prefix.extend_from_slice(&fields.ordinal.to_le_bytes());
    prefix.extend_from_slice(&fields.start_epoch.to_le_bytes());
    prefix.extend_from_slice(&fields.end_epoch.to_le_bytes());
    prefix.extend_from_slice(&fields.entry_count.to_le_bytes());
    prefix.extend_from_slice(&fields.descriptor_bytes.to_le_bytes());
    prefix.extend_from_slice(&fields.payload_bytes.to_le_bytes());
    prefix.extend_from_slice(fields.base.root_set_digest.as_bytes());
    prefix.extend_from_slice(fields.schema_set_digest.as_bytes());
    debug_assert_eq!(prefix.len(), RUN_INTEGRITY_OFFSET);
    prefix
}

fn encode_entry_descriptor(
    base: RelationalRowDeltaBaseBinding,
    delta_generation: u64,
    run_ordinal: u32,
    entry_ordinal: u32,
    key: &RowDeltaKey,
    value: &RowDeltaValue,
    key_offset: u64,
) -> Result<[u8; ENTRY_DESCRIPTOR_BYTES], RelationalRowDeltaError> {
    let key_len = u32::try_from(key.encoded_primary_key.len()).map_err(|_| {
        RelationalRowDeltaError::Admission("row delta key length does not fit u32".to_string())
    })?;
    let row_len = u32::try_from(value.encoded_row.len()).map_err(|_| {
        RelationalRowDeltaError::Admission("row delta row length does not fit u32".to_string())
    })?;
    let row_offset = key_offset.checked_add(key_len as u64).ok_or_else(|| {
        RelationalRowDeltaError::Admission("row delta row offset overflow".to_string())
    })?;
    let mut entry_hasher = IntegrityHasher::new();
    entry_hasher.update(&key.encoded_primary_key);
    entry_hasher.update(&value.encoded_row);
    let entry_crc32c = entry_hasher.finish().crc32c.get();
    let kind = u8::from(value.is_present);
    let mut encoded = [0u8; ENTRY_DESCRIPTOR_BYTES];
    encoded[..4].copy_from_slice(&key.table_ordinal.to_le_bytes());
    encoded[4] = kind;
    encoded[8..16].copy_from_slice(&value.last_modified_epoch.to_le_bytes());
    encoded[16..24].copy_from_slice(&key_offset.to_le_bytes());
    encoded[24..28].copy_from_slice(&key_len.to_le_bytes());
    encoded[28..32].copy_from_slice(&row_len.to_le_bytes());
    encoded[32..40].copy_from_slice(&row_offset.to_le_bytes());
    encoded[40..44].copy_from_slice(&entry_crc32c.to_le_bytes());
    let binding = entry_binding(
        base,
        delta_generation,
        run_ordinal,
        entry_ordinal,
        &encoded[..ENTRY_BINDING_OFFSET],
        &key.encoded_primary_key,
        &value.encoded_row,
    );
    encoded[ENTRY_BINDING_OFFSET..].copy_from_slice(binding.as_bytes());
    Ok(encoded)
}

pub(super) fn entry_binding(
    base: RelationalRowDeltaBaseBinding,
    delta_generation: u64,
    run_ordinal: u32,
    entry_ordinal: u32,
    descriptor_prefix: &[u8],
    key: &[u8],
    row: &[u8],
) -> Sha256Digest {
    let mut hasher = IntegrityHasher::new();
    hasher.update(&base.generation.to_le_bytes());
    hasher.update(&base.source_commit_epoch.to_le_bytes());
    hasher.update(base.root_set_digest.as_bytes());
    hasher.update(&delta_generation.to_le_bytes());
    hasher.update(&run_ordinal.to_le_bytes());
    hasher.update(&entry_ordinal.to_le_bytes());
    hasher.update(descriptor_prefix);
    hasher.update(key);
    hasher.update(row);
    hasher.finish().sha256
}

pub(super) fn charged_entry_bytes(
    key_bytes: usize,
    row_bytes: usize,
) -> Result<usize, RelationalRowDeltaError> {
    DIRTY_ENTRY_OVERHEAD_BYTES
        .checked_add(ENTRY_DESCRIPTOR_BYTES)
        .and_then(|bytes| bytes.checked_add(key_bytes))
        .and_then(|bytes| bytes.checked_add(row_bytes))
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta entry charge overflow".to_string())
        })
}

fn digest_reader(reader: &mut File) -> Result<IntegrityDigest, RelationalRowDeltaError> {
    let mut hasher = IntegrityHasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(durability("read row delta run for digest"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finish())
}

pub(super) struct DecodedRunHeader {
    pub content_digest: IntegrityDigest,
}

pub(super) fn decode_run_header(
    encoded: &[u8; RUN_HEADER_BYTES],
    base: RelationalRowDeltaBaseBinding,
    delta_generation: u64,
    schema_set_digest: Sha256Digest,
    descriptor: &RowDeltaRunDescriptor,
) -> Result<DecodedRunHeader, RelationalRowDeltaError> {
    if &encoded[..8] != RUN_MAGIC {
        return Err(RelationalRowDeltaError::Corrupt(
            "invalid row delta run header".to_string(),
        ));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    if version != FORMAT_VERSION
        || flags != 0
        || read_u64(&encoded[12..20]) != base.generation
        || read_u64(&encoded[20..28]) != delta_generation
        || read_u64(&encoded[28..36]) != base.source_commit_epoch
        || read_u32(&encoded[36..40]) != descriptor.ordinal
        || read_u64(&encoded[40..48]) != descriptor.start_epoch
        || read_u64(&encoded[48..56]) != descriptor.end_epoch
        || read_u32(&encoded[56..60]) != descriptor.entry_count
        || read_u64(&encoded[60..68]) != descriptor.descriptor_bytes
        || read_u64(&encoded[68..76]) != descriptor.payload_bytes
        || &encoded[76..108] != base.root_set_digest.as_bytes()
        || &encoded[108..140] != schema_set_digest.as_bytes()
    {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta run fence disagrees with its manifest".to_string(),
        ));
    }
    Ok(DecodedRunHeader {
        content_digest: IntegrityDigest {
            crc32c: hawdb_integrity::Crc32c::new(read_u32(&encoded[140..144])),
            sha256: Sha256Digest::from_bytes(
                encoded[144..176]
                    .try_into()
                    .expect("run content digest has a fixed length"),
            ),
        },
    })
}

pub(super) struct DecodedEntryDescriptor {
    pub table_ordinal: u32,
    pub kind: u8,
    pub last_modified_epoch: u64,
    pub key_offset: u64,
    pub key_len: u32,
    pub row_len: u32,
    pub row_offset: u64,
    pub entry_crc32c: u32,
    pub binding: Sha256Digest,
}

pub(super) fn decode_entry_descriptor(
    encoded: &[u8; ENTRY_DESCRIPTOR_BYTES],
) -> Result<DecodedEntryDescriptor, RelationalRowDeltaError> {
    if encoded[5..8] != [0u8; 3] || encoded[44..48] != [0u8; 4] {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta entry has non-zero reserved bytes".to_string(),
        ));
    }
    let kind = encoded[4];
    if !matches!(kind, 0 | 1) {
        return Err(RelationalRowDeltaError::Corrupt(format!(
            "unknown row delta operation {kind}"
        )));
    }
    Ok(DecodedEntryDescriptor {
        table_ordinal: read_u32(&encoded[..4]),
        kind,
        last_modified_epoch: read_u64(&encoded[8..16]),
        key_offset: read_u64(&encoded[16..24]),
        key_len: read_u32(&encoded[24..28]),
        row_len: read_u32(&encoded[28..32]),
        row_offset: read_u64(&encoded[32..40]),
        entry_crc32c: read_u32(&encoded[40..44]),
        binding: Sha256Digest::from_bytes(
            encoded[48..80]
                .try_into()
                .expect("entry binding has a fixed length"),
        ),
    })
}

pub(super) fn run_integrity_prefix(encoded: &[u8; RUN_HEADER_BYTES]) -> &[u8] {
    &encoded[..RUN_INTEGRITY_OFFSET]
}

pub(super) fn validate_artifact_length(
    path: &Path,
    expected: u64,
) -> Result<(), RelationalRowDeltaError> {
    let actual = fs::metadata(path)
        .map_err(durability("read row delta run metadata"))?
        .len();
    if actual != expected {
        return Err(RelationalRowDeltaError::Corrupt(format!(
            "row delta run {} contains {actual} bytes, expected {expected}",
            path.display()
        )));
    }
    Ok(())
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
    context: &str,
) -> Result<&'a [u8], RelationalRowDeltaError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| RelationalRowDeltaError::Corrupt(format!("{context} offset overflow")))?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or_else(|| RelationalRowDeltaError::Corrupt(format!("truncated {context}")))?;
    *offset = end;
    Ok(bytes)
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

impl ErrorClass {
    fn error(self, message: String) -> RelationalRowDeltaError {
        match self {
            Self::Admission => RelationalRowDeltaError::Admission(message),
            Self::Corrupt => RelationalRowDeltaError::Corrupt(message),
        }
    }

    fn reclassify(self, error: RelationalRowDeltaError) -> RelationalRowDeltaError {
        match error {
            RelationalRowDeltaError::Admission(message)
            | RelationalRowDeltaError::Corrupt(message) => self.error(message),
            error => error,
        }
    }
}

fn read_u16(encoded: &[u8]) -> u16 {
    u16::from_le_bytes(encoded.try_into().expect("u16 slice has a fixed length"))
}

fn read_u32(encoded: &[u8]) -> u32 {
    u32::from_le_bytes(encoded.try_into().expect("u32 slice has a fixed length"))
}

fn read_u64(encoded: &[u8]) -> u64 {
    u64::from_le_bytes(encoded.try_into().expect("u64 slice has a fixed length"))
}
