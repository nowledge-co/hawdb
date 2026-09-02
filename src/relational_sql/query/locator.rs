use crate::error::{Result, SkeinError};
use crate::sql::{SqlNullOrder, SqlOrderDirection};
use skein_executor::columnar::RelationalRowLocator;
use skein_executor::external_order::ExternalOrderRecord;
use skein_expression::BindingId;
use skein_storage::{RelationalKey, RelationalScalarType, RelationalTableSchema, RelationalValue};
use std::cmp::Ordering;
use std::io::{Cursor, Read};

const TYPED_LOCATOR_RECORD_VERSION: u8 = 1;
const HASH_SPILL_LOCATOR_RECORD_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationalRowSetLocator {
    rows: Box<[Option<RelationalRowLocator>]>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct RelationalSortKey {
    value: RelationalValue,
    direction: SqlOrderDirection,
    nulls: SqlNullOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct RelationalSortRecord {
    sort_keys: Box<[RelationalSortKey]>,
    locator: RelationalRowSetLocator,
}

impl RelationalRowSetLocator {
    pub(super) fn new(rows: Vec<Option<RelationalRowLocator>>) -> Self {
        Self {
            rows: rows.into_boxed_slice(),
        }
    }

    pub(super) fn rows(&self) -> &[Option<RelationalRowLocator>] {
        &self.rows
    }

    /// Encodes a compact, typed reference to relational rows for a spill run.
    ///
    /// The payload deliberately carries only primary-key locators. The executor
    /// must re-read rows from the query snapshot before evaluating join keys or
    /// residual predicates, so spill files never retain a second copy of rows.
    pub(super) fn encode_hash_spill_record(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        output.push(HASH_SPILL_LOCATOR_RECORD_VERSION);
        write_len(&mut output, self.rows.len())?;
        for locator in &self.rows {
            match locator {
                None => output.push(0),
                Some(locator) => {
                    output.push(1);
                    output.extend_from_slice(&locator.table_id().to_le_bytes());
                    write_len(&mut output, locator.primary_key().0.len())?;
                    for value in &locator.primary_key().0 {
                        write_relational_value(&mut output, value, false)?;
                    }
                }
            }
        }
        Ok(output)
    }

    pub(super) fn decode_hash_spill_record(input: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(input);
        if read_u8(&mut cursor)? != HASH_SPILL_LOCATOR_RECORD_VERSION {
            return Err(invalid_typed_record(
                "unsupported hash spill locator version",
            ));
        }
        let locator_count = read_len(&mut cursor, input.len())?;
        let mut rows = Vec::with_capacity(locator_count);
        for _ in 0..locator_count {
            match read_u8(&mut cursor)? {
                0 => rows.push(None),
                1 => {
                    let table_id = read_u32(&mut cursor)?;
                    let value_count = read_len(&mut cursor, input.len())?;
                    let mut values = Vec::with_capacity(value_count);
                    for _ in 0..value_count {
                        values.push(read_relational_value(&mut cursor, input.len(), false)?);
                    }
                    rows.push(Some(RelationalRowLocator::new(
                        table_id,
                        RelationalKey(values),
                    )));
                }
                _ => {
                    return Err(invalid_typed_record(
                        "invalid hash spill locator presence tag",
                    ))
                }
            }
        }
        if cursor.position() != input.len() as u64 {
            return Err(invalid_typed_record("trailing hash spill locator bytes"));
        }
        Ok(Self::new(rows))
    }

    pub(super) fn memory_bytes(&self) -> usize {
        self.rows.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.rows
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<RelationalRowLocator>>()),
            ),
            |total, locator| {
                locator.as_ref().map_or(total, |locator| {
                    total.saturating_add(locator.allocated_bytes())
                })
            },
        )
    }
}

impl RelationalSortKey {
    pub(super) fn new(
        value: RelationalValue,
        direction: SqlOrderDirection,
        nulls: SqlNullOrder,
    ) -> Result<Self> {
        if matches!(value, RelationalValue::Overflow(_)) {
            return Err(SkeinError::Execution(
                "ORDER BY requires overflow hydration before qualification".to_string(),
            ));
        }
        Ok(Self {
            value,
            direction,
            nulls,
        })
    }

    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.value.estimated_payload_bytes())
    }

    fn compare(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.direction, other.direction);
        debug_assert_eq!(self.nulls, other.nulls);
        let left_null = matches!(self.value, RelationalValue::Null);
        let right_null = matches!(other.value, RelationalValue::Null);
        if left_null || right_null {
            let nulls_first = match self.nulls {
                SqlNullOrder::First => true,
                SqlNullOrder::Last => false,
                SqlNullOrder::DialectDefault => self.direction == SqlOrderDirection::Desc,
            };
            return match (left_null, right_null, nulls_first) {
                (true, true, _) => Ordering::Equal,
                (true, false, true) | (false, true, false) => Ordering::Less,
                (true, false, false) | (false, true, true) => Ordering::Greater,
                (false, false, _) => unreachable!("null branch requires at least one null"),
            };
        }
        match self.direction {
            SqlOrderDirection::Asc => self.value.cmp(&other.value),
            SqlOrderDirection::Desc => self.value.cmp(&other.value).reverse(),
        }
    }
}

impl RelationalSortRecord {
    pub(super) fn new(sort_keys: Vec<RelationalSortKey>, locator: RelationalRowSetLocator) -> Self {
        Self {
            sort_keys: sort_keys.into_boxed_slice(),
            locator,
        }
    }

    pub(super) fn into_locator(self) -> RelationalRowSetLocator {
        self.locator
    }
}

impl ExternalOrderRecord for RelationalSortRecord {
    fn compare(&self, other: &Self) -> Ordering {
        for (left, right) in self.sort_keys.iter().zip(&other.sort_keys) {
            let ordering = left.compare(right);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        self.sort_keys.len().cmp(&other.sort_keys.len())
    }

    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.sort_keys
                    .iter()
                    .map(RelationalSortKey::memory_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(self.locator.memory_bytes())
    }

    fn encoded_len(&self) -> Result<usize> {
        typed_record_encoded_len(self)
    }

    fn encode(&self, output: &mut Vec<u8>) -> Result<()> {
        encode_typed_record(self, output)
    }

    fn decode(input: &[u8]) -> Result<Self> {
        decode_typed_record(input)
    }
}

pub(super) struct RelationalLocatorLayout<'a> {
    pub(super) bindings: Vec<RelationalLocatorBindingLayout<'a>>,
}

pub(super) struct RelationalLocatorBindingLayout<'a> {
    pub(super) binding: BindingId,
    pub(super) table: &'a str,
    pub(super) qualifier: &'a str,
    pub(super) schema: &'a RelationalTableSchema,
    primary_key_types: Vec<RelationalScalarType>,
}

impl<'a> RelationalLocatorLayout<'a> {
    pub(super) fn from_bindings(
        bindings: impl IntoIterator<Item = (BindingId, &'a str, &'a str, &'a RelationalTableSchema)>,
    ) -> Result<Self> {
        bindings
            .into_iter()
            .map(|(binding, table, qualifier, schema)| {
                RelationalLocatorBindingLayout::new(binding, table, qualifier, schema)
            })
            .collect::<Result<Vec<_>>>()
            .map(|bindings| Self { bindings })
    }

    pub(super) fn validate(&self, locator: &RelationalRowSetLocator) -> Result<()> {
        if locator.rows.len() != self.bindings.len() {
            return Err(SkeinError::Execution(format!(
                "typed relational locator has {} bindings but the query layout requires {}",
                locator.rows.len(),
                self.bindings.len()
            )));
        }
        for (table_id, (locator, layout)) in locator.rows.iter().zip(&self.bindings).enumerate() {
            let Some(locator) = locator else {
                continue;
            };
            let expected_table_id = u32::try_from(table_id).map_err(|_| {
                SkeinError::Execution(
                    "typed relational locator table count exceeds u32".to_string(),
                )
            })?;
            if locator.table_id() != expected_table_id {
                return Err(SkeinError::Execution(format!(
                    "typed relational locator table id {} does not match layout slot {expected_table_id}",
                    locator.table_id()
                )));
            }
            layout.validate_primary_key(locator.primary_key())?;
        }
        Ok(())
    }
}

impl<'a> RelationalLocatorBindingLayout<'a> {
    fn new(
        binding: BindingId,
        table: &'a str,
        qualifier: &'a str,
        schema: &'a RelationalTableSchema,
    ) -> Result<Self> {
        if schema.primary_key.is_empty() {
            return Err(SkeinError::Storage(format!(
                "relational locator layout requires a primary key for table {table}"
            )));
        }
        let primary_key_types = schema
            .primary_key
            .iter()
            .map(|column| {
                schema
                    .column_position(column)
                    .map(|position| schema.columns[position].scalar_type)
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "relational locator layout references unknown primary-key column {column} in table {table}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            binding,
            table,
            qualifier,
            schema,
            primary_key_types,
        })
    }

    fn validate_primary_key(&self, key: &RelationalKey) -> Result<()> {
        if key.0.len() != self.primary_key_types.len() {
            return Err(SkeinError::Execution(format!(
                "typed relational locator for table {} has {} key values but the schema requires {}",
                self.table,
                key.0.len(),
                self.primary_key_types.len()
            )));
        }
        for (value, scalar_type) in key.0.iter().zip(&self.primary_key_types) {
            let matches = matches!(
                (scalar_type, value),
                (RelationalScalarType::Boolean, RelationalValue::Boolean(_))
                    | (RelationalScalarType::BigInt, RelationalValue::BigInt(_))
                    | (
                        RelationalScalarType::DoublePrecision,
                        RelationalValue::DoublePrecision(_)
                    )
                    | (RelationalScalarType::Text, RelationalValue::Text(_))
                    | (RelationalScalarType::Bytea, RelationalValue::Bytea(_))
            );
            if !matches {
                return Err(SkeinError::Execution(format!(
                    "typed relational locator key for table {} does not match schema type {scalar_type:?}",
                    self.table
                )));
            }
        }
        Ok(())
    }
}

fn typed_record_encoded_len(record: &RelationalSortRecord) -> Result<usize> {
    let mut len = 1usize;
    len = checked_add(len, 4, "sort-key count")?;
    for key in &record.sort_keys {
        len = checked_add(len, 2, "sort-key metadata")?;
        len = checked_add(len, relational_value_encoded_len(&key.value)?, "sort key")?;
    }
    len = checked_add(len, 4, "locator count")?;
    for locator in &record.locator.rows {
        len = checked_add(len, 1, "locator presence")?;
        let Some(locator) = locator else {
            continue;
        };
        len = checked_add(len, 8, "locator metadata")?;
        for value in &locator.primary_key().0 {
            len = checked_add(len, relational_value_encoded_len(value)?, "primary key")?;
        }
    }
    Ok(len)
}

fn encode_typed_record(record: &RelationalSortRecord, output: &mut Vec<u8>) -> Result<()> {
    output.push(TYPED_LOCATOR_RECORD_VERSION);
    write_len(output, record.sort_keys.len())?;
    for key in &record.sort_keys {
        output.push(match key.direction {
            SqlOrderDirection::Asc => 0,
            SqlOrderDirection::Desc => 1,
        });
        output.push(match key.nulls {
            SqlNullOrder::DialectDefault => 0,
            SqlNullOrder::First => 1,
            SqlNullOrder::Last => 2,
        });
        write_relational_value(output, &key.value, true)?;
    }
    write_len(output, record.locator.rows.len())?;
    for locator in &record.locator.rows {
        match locator {
            None => output.push(0),
            Some(locator) => {
                output.push(1);
                output.extend_from_slice(&locator.table_id().to_le_bytes());
                write_len(output, locator.primary_key().0.len())?;
                for value in &locator.primary_key().0 {
                    write_relational_value(output, value, false)?;
                }
            }
        }
    }
    Ok(())
}

fn decode_typed_record(input: &[u8]) -> Result<RelationalSortRecord> {
    let mut cursor = Cursor::new(input);
    if read_u8(&mut cursor)? != TYPED_LOCATOR_RECORD_VERSION {
        return Err(invalid_typed_record("unsupported version"));
    }
    let sort_key_count = read_len(&mut cursor, input.len())?;
    let mut sort_keys = Vec::with_capacity(sort_key_count);
    for _ in 0..sort_key_count {
        let direction = match read_u8(&mut cursor)? {
            0 => SqlOrderDirection::Asc,
            1 => SqlOrderDirection::Desc,
            _ => return Err(invalid_typed_record("invalid sort direction")),
        };
        let nulls = match read_u8(&mut cursor)? {
            0 => SqlNullOrder::DialectDefault,
            1 => SqlNullOrder::First,
            2 => SqlNullOrder::Last,
            _ => return Err(invalid_typed_record("invalid null ordering")),
        };
        sort_keys.push(RelationalSortKey {
            value: read_relational_value(&mut cursor, input.len(), true)?,
            direction,
            nulls,
        });
    }
    let locator_count = read_len(&mut cursor, input.len())?;
    let mut rows = Vec::with_capacity(locator_count);
    for _ in 0..locator_count {
        match read_u8(&mut cursor)? {
            0 => rows.push(None),
            1 => {
                let table_id = read_u32(&mut cursor)?;
                let value_count = read_len(&mut cursor, input.len())?;
                let mut values = Vec::with_capacity(value_count);
                for _ in 0..value_count {
                    values.push(read_relational_value(&mut cursor, input.len(), false)?);
                }
                rows.push(Some(RelationalRowLocator::new(
                    table_id,
                    RelationalKey(values),
                )));
            }
            _ => return Err(invalid_typed_record("invalid locator presence tag")),
        }
    }
    if cursor.position() != input.len() as u64 {
        return Err(invalid_typed_record("trailing bytes"));
    }
    Ok(RelationalSortRecord::new(
        sort_keys,
        RelationalRowSetLocator::new(rows),
    ))
}

fn relational_value_encoded_len(value: &RelationalValue) -> Result<usize> {
    match value {
        RelationalValue::Null => Ok(1),
        RelationalValue::Boolean(_) => Ok(2),
        RelationalValue::BigInt(_) | RelationalValue::DoublePrecision(_) => Ok(9),
        RelationalValue::Text(value) => checked_add(5, value.len(), "text value"),
        RelationalValue::Bytea(value) => checked_add(5, value.len(), "bytea value"),
        RelationalValue::Overflow(_) => Err(SkeinError::Execution(
            "typed relational locator cannot spill an overflow reference".to_string(),
        )),
    }
}

fn write_relational_value(
    output: &mut Vec<u8>,
    value: &RelationalValue,
    allow_null: bool,
) -> Result<()> {
    match value {
        RelationalValue::Null if allow_null => output.push(0),
        RelationalValue::Null => {
            return Err(SkeinError::Execution(
                "typed relational locator contains a null primary-key value".to_string(),
            ));
        }
        RelationalValue::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        RelationalValue::BigInt(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        RelationalValue::DoublePrecision(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        RelationalValue::Text(value) => {
            output.push(4);
            write_bytes(output, value.as_bytes())?;
        }
        RelationalValue::Bytea(value) => {
            output.push(5);
            write_bytes(output, value)?;
        }
        RelationalValue::Overflow(_) => {
            return Err(SkeinError::Execution(
                "typed relational locator cannot spill an overflow reference".to_string(),
            ));
        }
    }
    Ok(())
}

fn read_relational_value(
    input: &mut Cursor<&[u8]>,
    record_bytes: usize,
    allow_null: bool,
) -> Result<RelationalValue> {
    Ok(match read_u8(input)? {
        0 if allow_null => RelationalValue::Null,
        0 => return Err(invalid_typed_record("null primary-key value")),
        1 => match read_u8(input)? {
            0 => RelationalValue::Boolean(false),
            1 => RelationalValue::Boolean(true),
            _ => return Err(invalid_typed_record("invalid boolean value")),
        },
        2 => RelationalValue::BigInt(i64::from_le_bytes(read_array(input)?)),
        3 => {
            RelationalValue::DoublePrecision(f64::from_bits(u64::from_le_bytes(read_array(input)?)))
        }
        4 => RelationalValue::Text(
            String::from_utf8(read_bytes(input, record_bytes)?)
                .map_err(|_| invalid_typed_record("invalid UTF-8 text"))?,
        ),
        5 => RelationalValue::Bytea(read_bytes(input, record_bytes)?),
        _ => return Err(invalid_typed_record("invalid relational value tag")),
    })
}

fn checked_add(left: usize, right: usize, field: &str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| SkeinError::Execution(format!("typed relational {field} size overflow")))
}

fn write_len(output: &mut Vec<u8>, value: usize) -> Result<()> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| {
                SkeinError::Execution("typed relational collection exceeds u32".to_string())
            })?
            .to_le_bytes(),
    );
    Ok(())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    write_len(output, value.len())?;
    output.extend_from_slice(value);
    Ok(())
}

fn read_len(input: &mut Cursor<&[u8]>, record_bytes: usize) -> Result<usize> {
    let len = read_u32(input)? as usize;
    let remaining = record_bytes.saturating_sub(input.position() as usize);
    if len > remaining {
        return Err(invalid_typed_record(
            "declared collection length exceeds remaining payload",
        ));
    }
    Ok(len)
}

fn read_bytes(input: &mut Cursor<&[u8]>, record_bytes: usize) -> Result<Vec<u8>> {
    let len = read_len(input, record_bytes)?;
    let remaining = record_bytes.saturating_sub(input.position() as usize);
    if len > remaining {
        return Err(invalid_typed_record("truncated byte string"));
    }
    let mut value = vec![0; len];
    input
        .read_exact(&mut value)
        .map_err(|_| invalid_typed_record("truncated byte string"))?;
    Ok(value)
}

fn read_u8(input: &mut Cursor<&[u8]>) -> Result<u8> {
    Ok(read_array::<1>(input)?[0])
}

fn read_u32(input: &mut Cursor<&[u8]>) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(input)?))
}

fn read_array<const N: usize>(input: &mut Cursor<&[u8]>) -> Result<[u8; N]> {
    let mut value = [0; N];
    input
        .read_exact(&mut value)
        .map_err(|_| invalid_typed_record("truncated fixed-width value"))?;
    Ok(value)
}

fn invalid_typed_record(reason: &str) -> SkeinError {
    SkeinError::Execution(format!(
        "typed relational spill record is invalid: {reason}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_executor::binding::value_memory_bytes;
    use skein_storage::RelationalColumnSchema;
    use std::collections::BTreeMap;

    use crate::value::Value;

    #[test]
    fn typed_locator_record_round_trips_schema_typed_composite_keys() {
        let primary_schema = locator_schema(
            "records",
            &[
                ("active", RelationalScalarType::Boolean),
                ("sequence", RelationalScalarType::BigInt),
                ("score", RelationalScalarType::DoublePrecision),
                ("name", RelationalScalarType::Text),
                ("digest", RelationalScalarType::Bytea),
            ],
        );
        let optional_schema =
            locator_schema("optional_records", &[("id", RelationalScalarType::Text)]);
        let layout = RelationalLocatorLayout::from_bindings([
            (BindingId::new(0), "records", "r", &primary_schema),
            (
                BindingId::new(1),
                "optional_records",
                "optional",
                &optional_schema,
            ),
        ])
        .expect("locator layout");
        let key = RelationalKey(vec![
            RelationalValue::Boolean(true),
            RelationalValue::BigInt(42),
            RelationalValue::DoublePrecision(3.5),
            RelationalValue::Text("record-42".to_string()),
            RelationalValue::Bytea(vec![0, 1, 127, 255]),
        ]);
        let record = RelationalSortRecord::new(
            vec![
                RelationalSortKey::new(
                    RelationalValue::Text("sort-key".to_string()),
                    SqlOrderDirection::Asc,
                    SqlNullOrder::Last,
                )
                .unwrap(),
                RelationalSortKey::new(
                    RelationalValue::Null,
                    SqlOrderDirection::Desc,
                    SqlNullOrder::DialectDefault,
                )
                .unwrap(),
            ],
            RelationalRowSetLocator::new(vec![
                Some(RelationalRowLocator::new(0, key.clone())),
                None,
            ]),
        );
        let mut encoded = Vec::new();
        record.encode(&mut encoded).unwrap();
        assert_eq!(encoded.len(), record.encoded_len().unwrap());
        let decoded = RelationalSortRecord::decode(&encoded).unwrap();
        layout.validate(&decoded.locator).unwrap();
        assert_eq!(decoded, record);

        let legacy = legacy_locator_value(&key);
        assert!(
            record.locator.memory_bytes() < value_memory_bytes(&legacy),
            "typed locator must retain fewer estimated bytes than self-describing Value maps"
        );
    }

    #[test]
    fn hash_spill_locator_round_trips_without_row_payloads() {
        let locator = RelationalRowSetLocator::new(vec![
            Some(RelationalRowLocator::new(
                0,
                RelationalKey(vec![
                    RelationalValue::Text("left-7".to_string()),
                    RelationalValue::BigInt(7),
                ]),
            )),
            None,
        ]);

        let encoded = locator
            .encode_hash_spill_record()
            .expect("encode typed hash spill locator");
        let decoded = RelationalRowSetLocator::decode_hash_spill_record(&encoded)
            .expect("decode typed hash spill locator");
        assert_eq!(decoded, locator);
        assert!(
            RelationalRowSetLocator::decode_hash_spill_record(&encoded[..encoded.len() - 1])
                .is_err()
        );
    }

    #[test]
    fn typed_locator_rejects_layout_identity_and_type_drift() {
        let schema = locator_schema("records", &[("id", RelationalScalarType::BigInt)]);
        let layout =
            RelationalLocatorLayout::from_bindings([(BindingId::new(0), "records", "r", &schema)])
                .expect("locator layout");

        let error = layout
            .validate(&RelationalRowSetLocator::new(Vec::new()))
            .expect_err("binding count drift must fail closed");
        assert!(error.to_string().contains("requires 1"));

        let error = layout
            .validate(&RelationalRowSetLocator::new(vec![Some(
                RelationalRowLocator::new(1, RelationalKey(vec![RelationalValue::BigInt(7)])),
            )]))
            .expect_err("table identity drift must fail closed");
        assert!(error.to_string().contains("does not match layout slot 0"));

        let error = layout
            .validate(&RelationalRowSetLocator::new(vec![Some(
                RelationalRowLocator::new(
                    0,
                    RelationalKey(vec![RelationalValue::Text("wrong-type".to_string())]),
                ),
            )]))
            .expect_err("key type drift must fail closed");
        assert!(error
            .to_string()
            .contains("does not match schema type BigInt"));
    }

    #[test]
    fn typed_locator_codec_rejects_truncation_and_trailing_bytes() {
        let record = RelationalSortRecord::new(
            vec![RelationalSortKey::new(
                RelationalValue::BigInt(7),
                SqlOrderDirection::Asc,
                SqlNullOrder::DialectDefault,
            )
            .unwrap()],
            RelationalRowSetLocator::new(vec![Some(RelationalRowLocator::new(
                0,
                RelationalKey(vec![RelationalValue::BigInt(7)]),
            ))]),
        );
        let mut encoded = Vec::new();
        record.encode(&mut encoded).unwrap();
        assert!(RelationalSortRecord::decode(&encoded[..encoded.len() - 1]).is_err());
        encoded.push(0);
        assert!(RelationalSortRecord::decode(&encoded).is_err());
    }

    #[test]
    fn typed_sort_keys_preserve_postgres_null_and_direction_ordering() {
        let record = |value, direction, nulls| {
            RelationalSortRecord::new(
                vec![RelationalSortKey::new(value, direction, nulls).unwrap()],
                RelationalRowSetLocator::new(Vec::new()),
            )
        };

        let asc_null = record(
            RelationalValue::Null,
            SqlOrderDirection::Asc,
            SqlNullOrder::DialectDefault,
        );
        let asc_value = record(
            RelationalValue::BigInt(7),
            SqlOrderDirection::Asc,
            SqlNullOrder::DialectDefault,
        );
        assert_eq!(asc_null.compare(&asc_value), Ordering::Greater);

        let desc_null = record(
            RelationalValue::Null,
            SqlOrderDirection::Desc,
            SqlNullOrder::DialectDefault,
        );
        let desc_value = record(
            RelationalValue::BigInt(7),
            SqlOrderDirection::Desc,
            SqlNullOrder::DialectDefault,
        );
        assert_eq!(desc_null.compare(&desc_value), Ordering::Less);

        let explicit_first = record(
            RelationalValue::Null,
            SqlOrderDirection::Asc,
            SqlNullOrder::First,
        );
        let explicit_first_value = record(
            RelationalValue::BigInt(7),
            SqlOrderDirection::Asc,
            SqlNullOrder::First,
        );
        assert_eq!(
            explicit_first.compare(&explicit_first_value),
            Ordering::Less
        );
    }

    fn locator_schema(
        table: &str,
        columns: &[(&str, RelationalScalarType)],
    ) -> RelationalTableSchema {
        RelationalTableSchema {
            name: table.to_string(),
            columns: columns
                .iter()
                .map(|(name, scalar_type)| RelationalColumnSchema {
                    name: (*name).to_string(),
                    scalar_type: *scalar_type,
                    nullable: false,
                    default: None,
                })
                .collect(),
            primary_key: columns
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        }
    }

    fn legacy_locator_value(key: &RelationalKey) -> Value {
        Value::List(vec![
            Value::Map(BTreeMap::from([
                ("table".to_string(), Value::String("records".to_string())),
                ("qualifier".to_string(), Value::String("r".to_string())),
                (
                    "key".to_string(),
                    Value::List(key.0.iter().map(legacy_key_value).collect()),
                ),
            ])),
            Value::Map(BTreeMap::from([
                (
                    "table".to_string(),
                    Value::String("optional_records".to_string()),
                ),
                (
                    "qualifier".to_string(),
                    Value::String("optional".to_string()),
                ),
                ("key".to_string(), Value::Null),
            ])),
        ])
    }

    fn legacy_key_value(value: &RelationalValue) -> Value {
        let (kind, value) = match value {
            RelationalValue::Boolean(value) => ("bool", Value::Bool(*value)),
            RelationalValue::BigInt(value) => ("int", Value::Int(*value)),
            RelationalValue::DoublePrecision(value) => ("float", Value::Float(*value)),
            RelationalValue::Text(value) => ("text", Value::String(value.clone())),
            RelationalValue::Bytea(value) => ("bytea", Value::Binary(value.clone())),
            RelationalValue::Null | RelationalValue::Overflow(_) => {
                panic!("test key must be inline and non-null")
            }
        };
        Value::Map(BTreeMap::from([
            ("kind".to_string(), Value::String(kind.to_string())),
            ("value".to_string(), value),
        ]))
    }
}
