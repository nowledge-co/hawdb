//! Physical column chunk encodings (§3.2.1).
//!
//! A chunk body is `u32 row_count | u32 value_count | validity bitmap |
//! payload`, little-endian throughout. The validity bitmap carries one bit
//! per row (set = non-null); the payload encodes exactly the non-null values
//! densely in row order. A body may additionally be zstd-compressed as a
//! whole; the compression flag travels in the chunk directory entry, not in
//! the body. Every chunk is independently decodable: no sibling chunk or
//! group state is needed beyond the encoding id and compression flag.

use super::{corrupt, unsupported, ColumnGroupError};
use skein_core::Value;
use std::io::Read;

/// Hard ceiling on rows in one chunk, defending decode-time allocations.
pub const MAX_CHUNK_ROWS: u32 = 1 << 24;
/// Hard ceiling on a decompressed chunk body.
pub const MAX_CHUNK_BODY_BYTES: u64 = 256 * 1024 * 1024;
/// Bodies smaller than this are never worth compressing.
const MIN_COMPRESS_BYTES: usize = 64;
const ZSTD_LEVEL: i32 = 3;

/// Physical encoding identifiers recorded in the group directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChunkEncoding {
    /// Fixed-width `i64` little-endian values.
    PlainInt,
    /// Fixed-width `f64` little-endian values.
    PlainFloat,
    /// One bit per non-null value.
    BoolBitmap,
    /// Scalar value table plus bit-packed `u32` codes.
    Dictionary,
    /// `(run length, scalar value)` pairs over the dense non-null values.
    RunLength,
    /// Frame-of-reference bit-packed integers.
    BitPackedInt,
    /// Cumulative `u32` offsets plus concatenated UTF-8 bytes.
    StringTable,
}

impl ChunkEncoding {
    pub const fn id(self) -> u8 {
        match self {
            Self::PlainInt => 1,
            Self::PlainFloat => 2,
            Self::BoolBitmap => 3,
            Self::Dictionary => 4,
            Self::RunLength => 5,
            Self::BitPackedInt => 6,
            Self::StringTable => 7,
        }
    }

    pub fn from_id(id: u8) -> Result<Self, ColumnGroupError> {
        match id {
            1 => Ok(Self::PlainInt),
            2 => Ok(Self::PlainFloat),
            3 => Ok(Self::BoolBitmap),
            4 => Ok(Self::Dictionary),
            5 => Ok(Self::RunLength),
            6 => Ok(Self::BitPackedInt),
            7 => Ok(Self::StringTable),
            _ => Err(corrupt(format!("unknown chunk encoding id {id}"))),
        }
    }
}

/// One encoded chunk ready to be written: stored bytes plus the directory
/// facts required to decode them again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedChunk {
    pub encoding: ChunkEncoding,
    pub compressed: bool,
    pub bytes: Vec<u8>,
}

/// Encodes `values` (with `Value::Null` marking nulls) into an uncompressed
/// chunk body using one specific physical encoding.
pub fn encode_chunk_body(
    values: &[Value],
    encoding: ChunkEncoding,
) -> Result<Vec<u8>, ColumnGroupError> {
    let row_count = u32::try_from(values.len())
        .ok()
        .filter(|count| *count <= MAX_CHUNK_ROWS)
        .ok_or_else(|| {
            unsupported(format!(
                "chunk row count {} exceeds the {MAX_CHUNK_ROWS} row limit",
                values.len()
            ))
        })?;
    let dense = values
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .collect::<Vec<_>>();
    let value_count = dense.len() as u32;
    let payload = match encoding {
        ChunkEncoding::PlainInt => encode_plain_int(&dense)?,
        ChunkEncoding::PlainFloat => encode_plain_float(&dense)?,
        ChunkEncoding::BoolBitmap => encode_bool_bitmap(&dense)?,
        ChunkEncoding::Dictionary => encode_dictionary(&dense)?,
        ChunkEncoding::RunLength => encode_run_length(&dense)?,
        ChunkEncoding::BitPackedInt => encode_bit_packed_int(&dense)?,
        ChunkEncoding::StringTable => encode_string_table(&dense)?,
    };
    let mut body = Vec::with_capacity(8 + validity_bytes(row_count) + payload.len());
    body.extend(row_count.to_le_bytes());
    body.extend(value_count.to_le_bytes());
    let mut validity = vec![0u8; validity_bytes(row_count)];
    for (row, value) in values.iter().enumerate() {
        if !matches!(value, Value::Null) {
            validity[row / 8] |= 1 << (row % 8);
        }
    }
    body.extend(validity);
    body.extend(payload);
    Ok(body)
}

/// Encodes with one specific encoding and optionally compresses the body.
pub fn encode_chunk_with(
    values: &[Value],
    encoding: ChunkEncoding,
    compress: bool,
) -> Result<EncodedChunk, ColumnGroupError> {
    let body = encode_chunk_body(values, encoding)?;
    Ok(finish_chunk(encoding, body, compress))
}

/// Picks the smallest valid physical encoding for `values`, optionally
/// compressing the winner (§3.5.1: encoding choice is per chunk).
pub fn encode_chunk_auto(
    values: &[Value],
    compress: bool,
) -> Result<EncodedChunk, ColumnGroupError> {
    let mut best: Option<(ChunkEncoding, Vec<u8>)> = None;
    for encoding in candidate_encodings(values) {
        let body = encode_chunk_body(values, encoding)?;
        if best
            .as_ref()
            .is_none_or(|(_, current)| body.len() < current.len())
        {
            best = Some((encoding, body));
        }
    }
    let (encoding, body) = best.ok_or_else(|| {
        unsupported(
            "no physical encoding accepts this column; list and map values \
             belong to the residual column"
                .to_string(),
        )
    })?;
    Ok(finish_chunk(encoding, body, compress))
}

/// Decodes a stored chunk back to one `Value` per row, with `Value::Null`
/// at null positions per the validity bitmap.
pub fn decode_chunk(
    bytes: &[u8],
    encoding: ChunkEncoding,
    compressed: bool,
) -> Result<Vec<Value>, ColumnGroupError> {
    let body;
    let body = if compressed {
        body = decompress_body(bytes)?;
        body.as_slice()
    } else {
        bytes
    };
    let mut cursor = Cursor::new(body);
    let row_count = cursor.read_u32("chunk row count")?;
    let value_count = cursor.read_u32("chunk value count")?;
    if row_count > MAX_CHUNK_ROWS {
        return Err(corrupt(format!(
            "chunk row count {row_count} exceeds the {MAX_CHUNK_ROWS} row limit"
        )));
    }
    if value_count > row_count {
        return Err(corrupt(format!(
            "chunk value count {value_count} exceeds its row count {row_count}"
        )));
    }
    let validity = cursor
        .read_bytes(validity_bytes(row_count), "chunk validity bitmap")?
        .to_vec();
    let set_bits = validity
        .iter()
        .map(|byte| u32::from(byte.count_ones() as u8))
        .sum::<u32>();
    if set_bits != value_count {
        return Err(corrupt(format!(
            "chunk validity bitmap marks {set_bits} values but the header \
             declares {value_count}"
        )));
    }
    if row_count % 8 != 0 {
        let tail = validity[validity.len().saturating_sub(1)];
        if tail & !((1u16 << (row_count % 8)) as u8).wrapping_sub(1) != 0 {
            return Err(corrupt(
                "chunk validity bitmap sets bits beyond the row count".to_string(),
            ));
        }
    }
    let dense = match encoding {
        ChunkEncoding::PlainInt => decode_plain_int(&mut cursor, value_count)?,
        ChunkEncoding::PlainFloat => decode_plain_float(&mut cursor, value_count)?,
        ChunkEncoding::BoolBitmap => decode_bool_bitmap(&mut cursor, value_count)?,
        ChunkEncoding::Dictionary => decode_dictionary(&mut cursor, value_count)?,
        ChunkEncoding::RunLength => decode_run_length(&mut cursor, value_count)?,
        ChunkEncoding::BitPackedInt => decode_bit_packed_int(&mut cursor, value_count)?,
        ChunkEncoding::StringTable => decode_string_table(&mut cursor, value_count)?,
    };
    cursor.expect_exhausted("chunk payload")?;
    debug_assert_eq!(dense.len(), value_count as usize);
    let mut dense = dense.into_iter();
    let values = (0..row_count as usize)
        .map(|row| {
            if validity[row / 8] & (1 << (row % 8)) != 0 {
                dense.next().ok_or_else(|| {
                    corrupt("chunk payload ends before its validity bitmap".to_string())
                })
            } else {
                Ok(Value::Null)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(values)
}

fn finish_chunk(encoding: ChunkEncoding, body: Vec<u8>, compress: bool) -> EncodedChunk {
    if compress
        && body.len() >= MIN_COMPRESS_BYTES
        && let Ok(packed) = zstd::stream::encode_all(body.as_slice(), ZSTD_LEVEL)
        && packed.len() < body.len()
    {
        return EncodedChunk {
            encoding,
            compressed: true,
            bytes: packed,
        };
    }
    EncodedChunk {
        encoding,
        compressed: false,
        bytes: body,
    }
}

fn decompress_body(bytes: &[u8]) -> Result<Vec<u8>, ColumnGroupError> {
    let decoder = zstd::stream::read::Decoder::new(bytes)
        .map_err(|error| corrupt(format!("chunk zstd stream is invalid: {error}")))?;
    let mut body = Vec::new();
    let read = decoder
        .take(MAX_CHUNK_BODY_BYTES + 1)
        .read_to_end(&mut body)
        .map_err(|error| corrupt(format!("chunk zstd stream is invalid: {error}")))?;
    if read as u64 > MAX_CHUNK_BODY_BYTES {
        return Err(corrupt(format!(
            "chunk decompresses beyond the {MAX_CHUNK_BODY_BYTES} byte limit"
        )));
    }
    Ok(body)
}

fn candidate_encodings(values: &[Value]) -> Vec<ChunkEncoding> {
    let mut all_int = true;
    let mut all_float = true;
    let mut all_bool = true;
    let mut all_string = true;
    let mut all_scalar = true;
    let mut any = false;
    for value in values {
        match value {
            Value::Null => continue,
            Value::Int(_) => {
                all_float = false;
                all_bool = false;
                all_string = false;
            }
            Value::Float(_) => {
                all_int = false;
                all_bool = false;
                all_string = false;
            }
            Value::Bool(_) => {
                all_int = false;
                all_float = false;
                all_string = false;
            }
            Value::String(_) => {
                all_int = false;
                all_float = false;
                all_bool = false;
            }
            Value::List(_) | Value::Map(_) => {
                all_int = false;
                all_float = false;
                all_bool = false;
                all_string = false;
                all_scalar = false;
            }
        }
        any = true;
    }
    if !all_scalar {
        return Vec::new();
    }
    if !any {
        // Empty or all-null: every payload is empty; plain int is canonical.
        return vec![ChunkEncoding::PlainInt];
    }
    let mut candidates = vec![ChunkEncoding::Dictionary, ChunkEncoding::RunLength];
    if all_int {
        candidates.push(ChunkEncoding::PlainInt);
        candidates.push(ChunkEncoding::BitPackedInt);
    }
    if all_float {
        candidates.push(ChunkEncoding::PlainFloat);
    }
    if all_bool {
        candidates.push(ChunkEncoding::BoolBitmap);
    }
    if all_string {
        candidates.push(ChunkEncoding::StringTable);
    }
    candidates
}

fn validity_bytes(row_count: u32) -> usize {
    (row_count as usize).div_ceil(8)
}

// --- typed payload codecs ---------------------------------------------------

fn expect_int(value: &Value) -> Result<i64, ColumnGroupError> {
    match value {
        Value::Int(value) => Ok(*value),
        other => Err(unsupported(format!(
            "integer chunk encoding cannot hold {other:?}"
        ))),
    }
}

fn encode_plain_int(dense: &[&Value]) -> Result<Vec<u8>, ColumnGroupError> {
    let mut payload = Vec::with_capacity(dense.len() * 8);
    for value in dense {
        payload.extend(expect_int(value)?.to_le_bytes());
    }
    Ok(payload)
}

fn decode_plain_int(cursor: &mut Cursor<'_>, count: u32) -> Result<Vec<Value>, ColumnGroupError> {
    (0..count)
        .map(|_| cursor.read_i64("plain int value").map(Value::Int))
        .collect()
}

fn encode_plain_float(dense: &[&Value]) -> Result<Vec<u8>, ColumnGroupError> {
    let mut payload = Vec::with_capacity(dense.len() * 8);
    for value in dense {
        match value {
            Value::Float(value) => payload.extend(value.to_le_bytes()),
            other => {
                return Err(unsupported(format!(
                    "float chunk encoding cannot hold {other:?}"
                )))
            }
        }
    }
    Ok(payload)
}

fn decode_plain_float(cursor: &mut Cursor<'_>, count: u32) -> Result<Vec<Value>, ColumnGroupError> {
    (0..count)
        .map(|_| {
            cursor
                .read_u64("plain float value")
                .map(|bits| Value::Float(f64::from_bits(bits)))
        })
        .collect()
}

fn encode_bool_bitmap(dense: &[&Value]) -> Result<Vec<u8>, ColumnGroupError> {
    let mut payload = vec![0u8; dense.len().div_ceil(8)];
    for (index, value) in dense.iter().enumerate() {
        match value {
            Value::Bool(value) => {
                if *value {
                    payload[index / 8] |= 1 << (index % 8);
                }
            }
            other => {
                return Err(unsupported(format!(
                    "bool chunk encoding cannot hold {other:?}"
                )))
            }
        }
    }
    Ok(payload)
}

fn decode_bool_bitmap(cursor: &mut Cursor<'_>, count: u32) -> Result<Vec<Value>, ColumnGroupError> {
    let bytes = cursor.read_bytes((count as usize).div_ceil(8), "bool bitmap payload")?;
    Ok((0..count as usize)
        .map(|index| Value::Bool(bytes[index / 8] & (1 << (index % 8)) != 0))
        .collect())
}

fn encode_dictionary(dense: &[&Value]) -> Result<Vec<u8>, ColumnGroupError> {
    let mut encoded_values = Vec::with_capacity(dense.len());
    for value in dense {
        let mut encoded = Vec::new();
        encode_scalar(value, &mut encoded)?;
        encoded_values.push(encoded);
    }
    // The table is sorted by encoded bytes, making the layout deterministic.
    let mut table = encoded_values.clone();
    table.sort_unstable();
    table.dedup();
    let codes = encoded_values
        .iter()
        .map(|encoded| {
            table
                .binary_search(encoded)
                .expect("every value is in the deduplicated table") as u64
        })
        .collect::<Vec<_>>();
    let table_len = u32::try_from(table.len())
        .map_err(|_| unsupported("dictionary table exceeds u32 entries".to_string()))?;
    let width = bits_for(u64::from(table_len.saturating_sub(1)));
    let mut payload = Vec::new();
    payload.extend(table_len.to_le_bytes());
    for entry in &table {
        payload.extend(entry);
    }
    payload.push(width);
    payload.extend(pack_values(&codes, width));
    Ok(payload)
}

fn decode_dictionary(cursor: &mut Cursor<'_>, count: u32) -> Result<Vec<Value>, ColumnGroupError> {
    let table_len = cursor.read_u32("dictionary table length")?;
    if table_len > count && !(count == 0 && table_len == 0) {
        return Err(corrupt(format!(
            "dictionary table holds {table_len} entries for {count} values"
        )));
    }
    let table = (0..table_len)
        .map(|_| decode_scalar(cursor))
        .collect::<Result<Vec<_>, _>>()?;
    let width = cursor.read_u8("dictionary code width")?;
    let codes = unpack_values(cursor, width, count as usize, "dictionary codes")?;
    codes
        .into_iter()
        .map(|code| {
            usize::try_from(code)
                .ok()
                .and_then(|code| table.get(code))
                .cloned()
                .ok_or_else(|| corrupt(format!("dictionary code {code} is out of range")))
        })
        .collect()
}

fn encode_run_length(dense: &[&Value]) -> Result<Vec<u8>, ColumnGroupError> {
    let mut runs: Vec<(u32, Vec<u8>)> = Vec::new();
    for value in dense {
        let mut encoded = Vec::new();
        encode_scalar(value, &mut encoded)?;
        match runs.last_mut() {
            Some((length, current)) if *current == encoded && *length < u32::MAX => {
                *length += 1;
            }
            _ => runs.push((1, encoded)),
        }
    }
    let run_count = u32::try_from(runs.len())
        .map_err(|_| unsupported("run-length chunk exceeds u32 runs".to_string()))?;
    let mut payload = Vec::new();
    payload.extend(run_count.to_le_bytes());
    for (length, encoded) in runs {
        payload.extend(length.to_le_bytes());
        payload.extend(encoded);
    }
    Ok(payload)
}

fn decode_run_length(cursor: &mut Cursor<'_>, count: u32) -> Result<Vec<Value>, ColumnGroupError> {
    let run_count = cursor.read_u32("run-length run count")?;
    if run_count > count {
        return Err(corrupt(format!(
            "run-length chunk declares {run_count} runs for {count} values"
        )));
    }
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..run_count {
        let length = cursor.read_u32("run length")?;
        let value = decode_scalar(cursor)?;
        if length == 0 {
            return Err(corrupt(
                "run-length chunk contains an empty run".to_string(),
            ));
        }
        if values.len() + length as usize > count as usize {
            return Err(corrupt(format!(
                "run-length chunk overflows its declared {count} values"
            )));
        }
        values.extend(std::iter::repeat_n(value, length as usize));
    }
    if values.len() != count as usize {
        return Err(corrupt(format!(
            "run-length chunk decodes {} of {count} declared values",
            values.len()
        )));
    }
    Ok(values)
}

fn encode_bit_packed_int(dense: &[&Value]) -> Result<Vec<u8>, ColumnGroupError> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut ints = Vec::with_capacity(dense.len());
    for value in dense {
        let value = expect_int(value)?;
        min = min.min(value);
        max = max.max(value);
        ints.push(value);
    }
    if ints.is_empty() {
        min = 0;
        max = 0;
    }
    // The i64 range always fits u64, so wrapping subtraction is exact.
    let width = bits_for(max.wrapping_sub(min) as u64);
    let deltas = ints
        .iter()
        .map(|value| value.wrapping_sub(min) as u64)
        .collect::<Vec<_>>();
    let mut payload = Vec::new();
    payload.extend(min.to_le_bytes());
    payload.push(width);
    payload.extend(pack_values(&deltas, width));
    Ok(payload)
}

fn decode_bit_packed_int(
    cursor: &mut Cursor<'_>,
    count: u32,
) -> Result<Vec<Value>, ColumnGroupError> {
    let min = cursor.read_i64("bit-packed frame of reference")?;
    let width = cursor.read_u8("bit-packed width")?;
    let deltas = unpack_values(cursor, width, count as usize, "bit-packed values")?;
    deltas
        .into_iter()
        .map(|delta| {
            let value = min.wrapping_add_unsigned(delta);
            if value < min {
                return Err(corrupt(
                    "bit-packed value overflows the i64 range".to_string(),
                ));
            }
            Ok(Value::Int(value))
        })
        .collect()
}

fn encode_string_table(dense: &[&Value]) -> Result<Vec<u8>, ColumnGroupError> {
    let mut offsets = Vec::with_capacity(dense.len() + 1);
    let mut bytes: Vec<u8> = Vec::new();
    offsets.push(0u32);
    for value in dense {
        match value {
            Value::String(value) => {
                bytes.extend(value.as_bytes());
                let end = u32::try_from(bytes.len())
                    .map_err(|_| unsupported("string table chunk exceeds u32 bytes".to_string()))?;
                offsets.push(end);
            }
            other => {
                return Err(unsupported(format!(
                    "string table encoding cannot hold {other:?}"
                )))
            }
        }
    }
    let mut payload = Vec::with_capacity(4 * offsets.len() + bytes.len());
    for offset in offsets {
        payload.extend(offset.to_le_bytes());
    }
    payload.extend(bytes);
    Ok(payload)
}

fn decode_string_table(
    cursor: &mut Cursor<'_>,
    count: u32,
) -> Result<Vec<Value>, ColumnGroupError> {
    let mut offsets = Vec::with_capacity(count as usize + 1);
    for _ in 0..=count {
        offsets.push(cursor.read_u32("string table offset")?);
    }
    let total = *offsets.last().expect("offsets holds count + 1 entries");
    let bytes = cursor.read_bytes(total as usize, "string table bytes")?;
    offsets
        .windows(2)
        .map(|window| {
            let (start, end) = (window[0] as usize, window[1] as usize);
            if window[0] > window[1] || end > bytes.len() {
                return Err(corrupt(
                    "string table offsets are not monotonically increasing".to_string(),
                ));
            }
            String::from_utf8(bytes[start..end].to_vec())
                .map(Value::String)
                .map_err(|_| corrupt("string table bytes are not valid UTF-8".to_string()))
        })
        .collect()
}

// --- tagged scalar codec ----------------------------------------------------

const SCALAR_BOOL: u8 = 1;
const SCALAR_INT: u8 = 2;
const SCALAR_FLOAT: u8 = 3;
const SCALAR_STRING: u8 = 4;

fn encode_scalar(value: &Value, out: &mut Vec<u8>) -> Result<(), ColumnGroupError> {
    match value {
        Value::Bool(value) => {
            out.push(SCALAR_BOOL);
            out.push(u8::from(*value));
        }
        Value::Int(value) => {
            out.push(SCALAR_INT);
            out.extend(value.to_le_bytes());
        }
        Value::Float(value) => {
            out.push(SCALAR_FLOAT);
            out.extend(value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            out.push(SCALAR_STRING);
            let length = u32::try_from(value.len())
                .map_err(|_| unsupported("scalar string exceeds u32 bytes".to_string()))?;
            out.extend(length.to_le_bytes());
            out.extend(value.as_bytes());
        }
        other => {
            return Err(unsupported(format!(
                "scalar encoding cannot hold {other:?}"
            )))
        }
    }
    Ok(())
}

fn decode_scalar(cursor: &mut Cursor<'_>) -> Result<Value, ColumnGroupError> {
    match cursor.read_u8("scalar tag")? {
        SCALAR_BOOL => match cursor.read_u8("scalar bool")? {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            other => Err(corrupt(format!("scalar bool byte {other} is invalid"))),
        },
        SCALAR_INT => cursor.read_i64("scalar int").map(Value::Int),
        SCALAR_FLOAT => cursor
            .read_u64("scalar float")
            .map(|bits| Value::Float(f64::from_bits(bits))),
        SCALAR_STRING => {
            let length = cursor.read_u32("scalar string length")?;
            let bytes = cursor.read_bytes(length as usize, "scalar string bytes")?;
            String::from_utf8(bytes.to_vec())
                .map(Value::String)
                .map_err(|_| corrupt("scalar string bytes are not valid UTF-8".to_string()))
        }
        tag => Err(corrupt(format!("unknown scalar tag {tag}"))),
    }
}

// --- bit packing ------------------------------------------------------------

/// The bit width needed to represent `value`.
pub(super) fn bits_for(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

/// Packs `values` at `width` bits each, LSB-first within a little-endian
/// bit stream. A width of zero packs to no bytes (all values are zero).
pub(super) fn pack_values(values: &[u64], width: u8) -> Vec<u8> {
    debug_assert!(width <= 64);
    if width == 0 {
        return Vec::new();
    }
    let total_bits = values.len() * usize::from(width);
    let mut bytes = vec![0u8; total_bits.div_ceil(8)];
    let mut bit = 0usize;
    for value in values {
        debug_assert!(width == 64 || *value < 1u64 << width);
        for offset in 0..usize::from(width) {
            if value >> offset & 1 != 0 {
                bytes[(bit + offset) / 8] |= 1 << ((bit + offset) % 8);
            }
        }
        bit += usize::from(width);
    }
    bytes
}

pub(super) fn unpack_values(
    cursor: &mut Cursor<'_>,
    width: u8,
    count: usize,
    what: &str,
) -> Result<Vec<u64>, ColumnGroupError> {
    if width > 64 {
        return Err(corrupt(format!("{what} declare a bit width of {width}")));
    }
    if width == 0 {
        return Ok(vec![0; count]);
    }
    let total_bits = count
        .checked_mul(usize::from(width))
        .ok_or_else(|| corrupt(format!("{what} overflow the addressable bit range")))?;
    let bytes = cursor.read_bytes(total_bits.div_ceil(8), what)?;
    let mut values = Vec::with_capacity(count);
    let mut bit = 0usize;
    for _ in 0..count {
        let mut value = 0u64;
        for offset in 0..usize::from(width) {
            if bytes[(bit + offset) / 8] & (1 << ((bit + offset) % 8)) != 0 {
                value |= 1u64 << offset;
            }
        }
        values.push(value);
        bit += usize::from(width);
    }
    Ok(values)
}

// --- cursor -----------------------------------------------------------------

pub(super) struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(super) fn read_bytes(
        &mut self,
        length: usize,
        what: &str,
    ) -> Result<&'a [u8], ColumnGroupError> {
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| corrupt(format!("{what} extend past the end of the buffer")))?;
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    pub(super) fn read_u8(&mut self, what: &str) -> Result<u8, ColumnGroupError> {
        Ok(self.read_bytes(1, what)?[0])
    }

    pub(super) fn read_u32(&mut self, what: &str) -> Result<u32, ColumnGroupError> {
        let bytes = self.read_bytes(4, what)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("4 bytes")))
    }

    pub(super) fn read_u64(&mut self, what: &str) -> Result<u64, ColumnGroupError> {
        let bytes = self.read_bytes(8, what)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    pub(super) fn read_i64(&mut self, what: &str) -> Result<i64, ColumnGroupError> {
        self.read_u64(what).map(|value| value as i64)
    }

    pub(super) fn expect_exhausted(&self, what: &str) -> Result<(), ColumnGroupError> {
        if self.position != self.bytes.len() {
            return Err(corrupt(format!(
                "{what} leaves {} undecoded trailing bytes",
                self.bytes.len() - self.position
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(values: &[Value], encoding: ChunkEncoding, compress: bool) {
        let chunk = encode_chunk_with(values, encoding, compress).unwrap();
        let decoded = decode_chunk(&chunk.bytes, chunk.encoding, chunk.compressed).unwrap();
        assert_eq!(decoded, values, "{encoding:?} compress={compress}");
    }

    fn every_flag(values: &[Value], encoding: ChunkEncoding) {
        round_trip(values, encoding, false);
        round_trip(values, encoding, true);
    }

    #[test]
    fn plain_int_round_trips_with_nulls_and_compression() {
        let values = (0..1000)
            .map(|index| {
                if index % 7 == 0 {
                    Value::Null
                } else {
                    Value::Int(index * 3 - 500)
                }
            })
            .collect::<Vec<_>>();
        every_flag(&values, ChunkEncoding::PlainInt);
        every_flag(&values, ChunkEncoding::BitPackedInt);
        every_flag(&values, ChunkEncoding::Dictionary);
        every_flag(&values, ChunkEncoding::RunLength);
    }

    #[test]
    fn plain_float_round_trips_including_nan_and_infinities() {
        let values = vec![
            Value::Float(0.0),
            Value::Null,
            Value::Float(-0.0),
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Float(f64::MIN),
        ];
        let chunk = encode_chunk_with(&values, ChunkEncoding::PlainFloat, false).unwrap();
        let decoded = decode_chunk(&chunk.bytes, chunk.encoding, chunk.compressed).unwrap();
        assert_eq!(decoded.len(), values.len());
        for (decoded, original) in decoded.iter().zip(&values) {
            match (decoded, original) {
                (Value::Float(decoded), Value::Float(original)) => {
                    assert_eq!(decoded.to_bits(), original.to_bits());
                }
                (decoded, original) => assert_eq!(decoded, original),
            }
        }
    }

    #[test]
    fn bool_bitmap_round_trips() {
        let values = (0..77)
            .map(|index| match index % 3 {
                0 => Value::Bool(true),
                1 => Value::Bool(false),
                _ => Value::Null,
            })
            .collect::<Vec<_>>();
        every_flag(&values, ChunkEncoding::BoolBitmap);
        every_flag(&values, ChunkEncoding::RunLength);
        every_flag(&values, ChunkEncoding::Dictionary);
    }

    #[test]
    fn string_table_round_trips_with_empty_and_unicode_strings() {
        let values = vec![
            Value::String(String::new()),
            Value::Null,
            Value::String("skein".to_string()),
            Value::String("列存储".to_string()),
            Value::String("z".repeat(300)),
        ];
        every_flag(&values, ChunkEncoding::StringTable);
        every_flag(&values, ChunkEncoding::Dictionary);
        every_flag(&values, ChunkEncoding::RunLength);
    }

    #[test]
    fn bit_packed_int_covers_extreme_ranges() {
        let values = vec![
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Int(0),
            Value::Null,
            Value::Int(-1),
        ];
        every_flag(&values, ChunkEncoding::BitPackedInt);
        every_flag(&values, ChunkEncoding::PlainInt);
    }

    #[test]
    fn empty_and_all_null_chunks_round_trip_for_every_encoding() {
        for encoding in [
            ChunkEncoding::PlainInt,
            ChunkEncoding::PlainFloat,
            ChunkEncoding::BoolBitmap,
            ChunkEncoding::Dictionary,
            ChunkEncoding::RunLength,
            ChunkEncoding::BitPackedInt,
            ChunkEncoding::StringTable,
        ] {
            every_flag(&[], encoding);
            every_flag(&[Value::Null, Value::Null, Value::Null], encoding);
        }
    }

    #[test]
    fn single_value_chunks_round_trip_for_matching_encodings() {
        every_flag(&[Value::Int(42)], ChunkEncoding::PlainInt);
        every_flag(&[Value::Int(42)], ChunkEncoding::BitPackedInt);
        every_flag(&[Value::Float(1.5)], ChunkEncoding::PlainFloat);
        every_flag(&[Value::Bool(true)], ChunkEncoding::BoolBitmap);
        every_flag(
            &[Value::String("only".to_string())],
            ChunkEncoding::StringTable,
        );
        every_flag(
            &[Value::String("only".to_string())],
            ChunkEncoding::Dictionary,
        );
        every_flag(&[Value::Int(9)], ChunkEncoding::RunLength);
    }

    #[test]
    fn auto_encoding_round_trips_and_beats_or_matches_plain() {
        let runs = (0..4096)
            .map(|index| Value::Int(i64::from(index / 512)))
            .collect::<Vec<_>>();
        let auto = encode_chunk_auto(&runs, false).unwrap();
        let plain = encode_chunk_with(&runs, ChunkEncoding::PlainInt, false).unwrap();
        assert!(auto.bytes.len() <= plain.bytes.len());
        assert_eq!(
            decode_chunk(&auto.bytes, auto.encoding, auto.compressed).unwrap(),
            runs
        );
        let mixed = vec![Value::Int(1), Value::String("two".to_string()), Value::Null];
        let chunk = encode_chunk_auto(&mixed, true).unwrap();
        assert_eq!(
            decode_chunk(&chunk.bytes, chunk.encoding, chunk.compressed).unwrap(),
            mixed
        );
    }

    #[test]
    fn nested_values_are_rejected_as_unsupported() {
        let values = vec![Value::List(vec![Value::Int(1)])];
        assert!(matches!(
            encode_chunk_auto(&values, false),
            Err(ColumnGroupError::Unsupported(_))
        ));
        assert!(matches!(
            encode_chunk_with(&values, ChunkEncoding::PlainInt, false),
            Err(ColumnGroupError::Unsupported(_))
        ));
    }

    #[test]
    fn wrong_typed_values_are_rejected_per_encoding() {
        let ints = vec![Value::Int(5)];
        assert!(matches!(
            encode_chunk_with(&ints, ChunkEncoding::StringTable, false),
            Err(ColumnGroupError::Unsupported(_))
        ));
        assert!(matches!(
            encode_chunk_with(&ints, ChunkEncoding::PlainFloat, false),
            Err(ColumnGroupError::Unsupported(_))
        ));
        assert!(matches!(
            encode_chunk_with(&ints, ChunkEncoding::BoolBitmap, false),
            Err(ColumnGroupError::Unsupported(_))
        ));
    }

    #[test]
    fn truncated_and_tampered_bodies_decode_to_corrupt_errors() {
        let values = (0..64).map(Value::Int).collect::<Vec<_>>();
        let chunk = encode_chunk_with(&values, ChunkEncoding::PlainInt, false).unwrap();
        for cut in [0, 1, 4, 7, chunk.bytes.len() - 1] {
            assert!(matches!(
                decode_chunk(&chunk.bytes[..cut], chunk.encoding, false),
                Err(ColumnGroupError::Corrupt(_))
            ));
        }
        // Trailing garbage is also rejected.
        let mut padded = chunk.bytes.clone();
        padded.push(0);
        assert!(matches!(
            decode_chunk(&padded, chunk.encoding, false),
            Err(ColumnGroupError::Corrupt(_))
        ));
        // A dictionary code pointing past the table is rejected: three
        // entries need two-bit codes, so a flipped code byte yields code 3.
        let dict = encode_chunk_with(
            &[
                Value::String("a".to_string()),
                Value::String("b".to_string()),
                Value::String("c".to_string()),
            ],
            ChunkEncoding::Dictionary,
            false,
        )
        .unwrap();
        let mut tampered = dict.bytes.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        assert!(decode_chunk(&tampered, ChunkEncoding::Dictionary, false).is_err());
        // Invalid zstd bytes are corrupt, not a panic.
        assert!(matches!(
            decode_chunk(&chunk.bytes, chunk.encoding, true),
            Err(ColumnGroupError::Corrupt(_))
        ));
    }

    #[test]
    fn validity_bitmap_mismatches_are_corrupt() {
        let values = vec![Value::Int(1), Value::Null, Value::Int(3)];
        let chunk = encode_chunk_with(&values, ChunkEncoding::PlainInt, false).unwrap();
        // Flip a validity bit: bitmap starts at offset 8.
        let mut tampered = chunk.bytes.clone();
        tampered[8] ^= 0b10;
        assert!(matches!(
            decode_chunk(&tampered, chunk.encoding, false),
            Err(ColumnGroupError::Corrupt(_))
        ));
    }

    #[test]
    fn bit_packing_round_trips_across_widths() {
        for width in [0u8, 1, 3, 7, 13, 31, 33, 63, 64] {
            let values = (0..17u64)
                .map(|index| {
                    if width == 64 {
                        u64::MAX - index
                    } else if width == 0 {
                        0
                    } else {
                        (index * 0x9e37_79b9) & ((1u64 << width) - 1)
                    }
                })
                .collect::<Vec<_>>();
            let packed = pack_values(&values, width);
            let mut cursor = Cursor::new(&packed);
            let unpacked = unpack_values(&mut cursor, width, values.len(), "test").unwrap();
            assert_eq!(unpacked, values, "width {width}");
        }
    }
}
