use super::binary::{map_relational, Decoder, Encoder};
use super::{
    validate_table_schema, AppendOrderMode, AppendTableError, AppendTableSchema, AppendTransaction,
    AppendWrite,
};
use crate::relational::{
    decode_relational_row_payload, decode_relational_table_schema, encode_relational_row_payload,
    encode_relational_table_schema,
};
use crate::{
    RelationalTableSchema, DEFAULT_MAX_WAL_BATCH_OPERATIONS, DEFAULT_MAX_WAL_RECORD_BYTES,
};
use skein_integrity::{integrity_digest, SHA256_BYTES};

const APPEND_WAL_MAGIC: &[u8; 8] = b"SKAPWAL1";
const APPEND_CODEC_VERSION: u16 = 2;
const APPEND_HEADER_BYTES: usize = 64;
const CREATE_TABLE_TAG: u8 = 1;
const APPEND_ROWS_TAG: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendDecodeLimits {
    pub max_record_bytes: usize,
    pub max_writes: usize,
    pub max_rows: usize,
    pub max_values: usize,
    pub max_value_bytes: usize,
    pub max_schema_bytes: usize,
}

impl AppendDecodeLimits {
    pub fn wal() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_WAL_RECORD_BYTES,
            max_writes: DEFAULT_MAX_WAL_BATCH_OPERATIONS,
            max_rows: 100_000,
            max_values: 2_000_000,
            max_value_bytes: 64 * 1024 * 1024,
            max_schema_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendWalBatch {
    pub epoch: u64,
    pub transaction: AppendTransaction,
}

pub fn encode_append_wal_batch(
    epoch: u64,
    transaction: &AppendTransaction,
) -> Result<Vec<u8>, AppendTableError> {
    let limits = AppendDecodeLimits::wal();
    if transaction.writes.len() > limits.max_writes {
        return Err(AppendTableError::Admission(format!(
            "append WAL contains {} writes, exceeding limit {}",
            transaction.writes.len(),
            limits.max_writes
        )));
    }

    let mut payload = Encoder::default();
    payload.count(transaction.writes.len(), "append WAL writes")?;
    let mut row_count = 0usize;
    let mut value_count = 0usize;
    for write in &transaction.writes {
        match write {
            AppendWrite::CreateTable { schema } => {
                validate_table_schema(schema)?;
                payload.u8(CREATE_TABLE_TAG);
                encode_schema(&mut payload, schema, limits)?;
            }
            AppendWrite::Append { table, rows } => {
                row_count = row_count.checked_add(rows.len()).ok_or_else(|| {
                    AppendTableError::Admission("append WAL row count overflow".to_string())
                })?;
                value_count = rows.iter().try_fold(value_count, |count, row| {
                    count.checked_add(row.values().len()).ok_or_else(|| {
                        AppendTableError::Admission("append WAL value count overflow".to_string())
                    })
                })?;
                if row_count > limits.max_rows || value_count > limits.max_values {
                    return Err(AppendTableError::Admission(format!(
                        "append WAL contains {row_count} rows and {value_count} values, exceeding limits {}/{}",
                        limits.max_rows, limits.max_values
                    )));
                }
                payload.u8(APPEND_ROWS_TAG);
                payload.string(table, "append WAL table name")?;
                payload.count(rows.len(), "append WAL rows")?;
                for row in rows {
                    let encoded = encode_relational_row_payload(row).map_err(map_relational)?;
                    payload.bytes(&encoded, "append WAL row payload")?;
                }
            }
            AppendWrite::AppendGenerated { table, .. } => {
                return Err(AppendTableError::Constraint(format!(
                    "append WAL cannot encode unresolved generated rows for table {table}"
                )));
            }
        }
    }

    let payload = payload.finish();
    let record_bytes = APPEND_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| AppendTableError::Admission("append WAL size overflow".to_string()))?;
    if record_bytes > limits.max_record_bytes {
        return Err(AppendTableError::Admission(format!(
            "append WAL contains {record_bytes} bytes, exceeding limit {}",
            limits.max_record_bytes
        )));
    }
    encode_envelope(epoch, payload)
}

pub fn decode_append_wal_batch(
    encoded: &[u8],
    limits: AppendDecodeLimits,
) -> Result<AppendWalBatch, AppendTableError> {
    let (epoch, payload) = decode_envelope(encoded, limits.max_record_bytes)?;
    let mut decoder = Decoder::new(payload);
    let write_count = decoder.count(limits.max_writes, "append WAL writes")?;
    let mut writes = Vec::with_capacity(write_count);
    let mut row_count = 0usize;
    let mut value_count = 0usize;
    for _ in 0..write_count {
        match decoder.u8("append WAL payload")? {
            CREATE_TABLE_TAG => {
                writes.push(AppendWrite::CreateTable {
                    schema: decode_schema(&mut decoder, limits)?,
                });
            }
            APPEND_ROWS_TAG => {
                let table = decoder.string(limits.max_value_bytes, "append table name")?;
                let count =
                    decoder.count(limits.max_rows.saturating_sub(row_count), "append WAL rows")?;
                row_count = row_count.checked_add(count).ok_or_else(|| {
                    AppendTableError::Corruption("append WAL row count overflow".to_string())
                })?;
                let mut rows = Vec::with_capacity(count);
                for _ in 0..count {
                    let row_payload =
                        decoder.bytes(limits.max_record_bytes, "append WAL row payload")?;
                    let row = decode_relational_row_payload(
                        row_payload,
                        limits.max_values.saturating_sub(value_count),
                        limits.max_value_bytes,
                    )
                    .map_err(map_relational)?;
                    value_count = value_count.checked_add(row.values().len()).ok_or_else(|| {
                        AppendTableError::Corruption("append WAL value count overflow".to_string())
                    })?;
                    if value_count > limits.max_values {
                        return Err(AppendTableError::Admission(format!(
                            "append WAL value count exceeds limit {}",
                            limits.max_values
                        )));
                    }
                    rows.push(row);
                }
                writes.push(AppendWrite::Append { table, rows });
            }
            tag => {
                return Err(AppendTableError::Corruption(format!(
                    "unknown append WAL write tag {tag}"
                )))
            }
        }
    }
    decoder.finish("append WAL")?;
    Ok(AppendWalBatch {
        epoch,
        transaction: AppendTransaction { writes },
    })
}

fn encode_schema(
    encoder: &mut Encoder,
    schema: &AppendTableSchema,
    limits: AppendDecodeLimits,
) -> Result<(), AppendTableError> {
    let mut primary_key = schema.partition_key.clone();
    primary_key.extend(schema.order_key.iter().cloned());
    let relational = RelationalTableSchema {
        name: schema.name.clone(),
        columns: schema.columns.clone(),
        primary_key,
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    let encoded = encode_relational_table_schema(&relational).map_err(map_relational)?;
    if encoded.len() > limits.max_schema_bytes {
        return Err(AppendTableError::Admission(format!(
            "append schema contains {} bytes, exceeding limit {}",
            encoded.len(),
            limits.max_schema_bytes
        )));
    }
    encoder.count(schema.partition_key.len(), "append partition key columns")?;
    encoder.u8(match schema.order_mode {
        AppendOrderMode::CallerProvided => 0,
        AppendOrderMode::CommitSequence => 1,
    });
    encoder.bytes(&encoded, "append table schema")
}

fn decode_schema(
    decoder: &mut Decoder<'_>,
    limits: AppendDecodeLimits,
) -> Result<AppendTableSchema, AppendTableError> {
    let partition_count = decoder.count(limits.max_values, "append partition key columns")?;
    let order_mode = match decoder.u8("append order mode")? {
        0 => AppendOrderMode::CallerProvided,
        1 => AppendOrderMode::CommitSequence,
        value => {
            return Err(AppendTableError::Corruption(format!(
                "unknown append order mode {value}"
            )));
        }
    };
    let encoded = decoder.bytes(limits.max_schema_bytes, "append table schema")?;
    let relational =
        decode_relational_table_schema(encoded, limits.max_schema_bytes, limits.max_values)
            .map_err(map_relational)?;
    if partition_count > relational.primary_key.len()
        || partition_count == relational.primary_key.len()
        || !relational.unique_constraints.is_empty()
        || !relational.foreign_keys.is_empty()
        || !relational.indexes.is_empty()
    {
        return Err(AppendTableError::Corruption(
            "append schema has an invalid key split or unsupported relational metadata".to_string(),
        ));
    }
    let schema = AppendTableSchema {
        name: relational.name,
        columns: relational.columns,
        partition_key: relational.primary_key[..partition_count].to_vec(),
        order_key: relational.primary_key[partition_count..].to_vec(),
        order_mode,
    };
    validate_table_schema(&schema)?;
    Ok(schema)
}

fn encode_envelope(epoch: u64, payload: Vec<u8>) -> Result<Vec<u8>, AppendTableError> {
    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        AppendTableError::Admission("append WAL payload length overflows u64".to_string())
    })?;
    let digest = integrity_digest(&payload);
    let mut encoded = Vec::with_capacity(APPEND_HEADER_BYTES + payload.len());
    encoded.extend_from_slice(APPEND_WAL_MAGIC);
    encoded.extend_from_slice(&APPEND_CODEC_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&epoch.to_le_bytes());
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded.extend_from_slice(digest.sha256.as_bytes());
    debug_assert_eq!(encoded.len(), APPEND_HEADER_BYTES);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_envelope(
    encoded: &[u8],
    max_record_bytes: usize,
) -> Result<(u64, &[u8]), AppendTableError> {
    if encoded.len() > max_record_bytes || encoded.len() < APPEND_HEADER_BYTES {
        return Err(AppendTableError::Admission(format!(
            "append WAL contains {} bytes, outside limit {max_record_bytes}",
            encoded.len()
        )));
    }
    if &encoded[..8] != APPEND_WAL_MAGIC {
        return Err(AppendTableError::Corruption(
            "append WAL magic mismatch".to_string(),
        ));
    }
    let version = u16::from_le_bytes(encoded[8..10].try_into().expect("fixed header"));
    if version != APPEND_CODEC_VERSION || encoded[10..12] != [0, 0] {
        return Err(AppendTableError::Corruption(format!(
            "unsupported append WAL version {version}"
        )));
    }
    let epoch = u64::from_le_bytes(encoded[12..20].try_into().expect("fixed header"));
    let payload_len = usize::try_from(u64::from_le_bytes(
        encoded[20..28].try_into().expect("fixed header"),
    ))
    .map_err(|_| AppendTableError::Corruption("append WAL length overflows usize".to_string()))?;
    let expected_len = APPEND_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| AppendTableError::Corruption("append WAL length overflow".to_string()))?;
    if expected_len != encoded.len() {
        return Err(AppendTableError::Corruption(format!(
            "append WAL length mismatch: expected {expected_len}, got {}",
            encoded.len()
        )));
    }
    let payload = &encoded[APPEND_HEADER_BYTES..];
    let digest = integrity_digest(payload);
    let expected_crc = u32::from_le_bytes(encoded[28..32].try_into().expect("fixed header"));
    let expected_sha: &[u8; SHA256_BYTES] = encoded[32..64].try_into().expect("fixed header");
    if digest.crc32c.get() != expected_crc || digest.sha256.as_bytes() != expected_sha {
        return Err(AppendTableError::Corruption(
            "append WAL checksum mismatch".to_string(),
        ));
    }
    Ok((epoch, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationalColumnSchema, RelationalRow, RelationalScalarType, RelationalValue};

    fn transaction() -> AppendTransaction {
        AppendTransaction {
            writes: vec![
                AppendWrite::CreateTable {
                    schema: AppendTableSchema {
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
                        order_mode: AppendOrderMode::CallerProvided,
                    },
                },
                AppendWrite::Append {
                    table: "events".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("alpha".to_string()),
                        RelationalValue::BigInt(7),
                    ])],
                },
            ],
        }
    }

    #[test]
    fn append_wal_round_trips() {
        let transaction = transaction();
        let encoded = encode_append_wal_batch(42, &transaction).expect("encode append WAL");
        let decoded = decode_append_wal_batch(&encoded, AppendDecodeLimits::wal())
            .expect("decode append WAL");

        assert_eq!(decoded.epoch, 42);
        assert_eq!(decoded.transaction, transaction);
    }

    #[test]
    fn append_wal_rejects_torn_or_corrupted_tail() {
        let encoded = encode_append_wal_batch(42, &transaction()).expect("encode append WAL");
        assert!(matches!(
            decode_append_wal_batch(&encoded[..encoded.len() - 1], AppendDecodeLimits::wal()),
            Err(AppendTableError::Corruption(_))
        ));

        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 1;
        assert!(matches!(
            decode_append_wal_batch(&corrupted, AppendDecodeLimits::wal()),
            Err(AppendTableError::Corruption(_))
        ));
    }

    #[test]
    fn append_wal_fails_closed_on_decoder_limits() {
        let encoded = encode_append_wal_batch(42, &transaction()).expect("encode append WAL");
        let limits = AppendDecodeLimits {
            max_rows: 0,
            ..AppendDecodeLimits::wal()
        };
        assert!(matches!(
            decode_append_wal_batch(&encoded, limits),
            Err(AppendTableError::Admission(_))
        ));
    }

    #[test]
    fn append_wal_rejects_unmaterialized_generated_rows() {
        let transaction = AppendTransaction {
            writes: vec![AppendWrite::AppendGenerated {
                table: "events".to_string(),
                rows: vec![super::super::AppendGeneratedRow::new(vec![
                    RelationalValue::Text("alpha".to_string()),
                ])],
            }],
        };

        assert!(matches!(
            encode_append_wal_batch(42, &transaction),
            Err(AppendTableError::Constraint(_))
        ));
    }
}
