//! Checkpoint-only predicate text decoding. Temporary text is destroyed before
//! releasing its buffer token. Owned leaf values, vectors and conservative map
//! node coverage join the enclosing WAL inventory and survive replay/capture.
//! Standard map comparisons, allocator latency, error strings and destruction
//! still need whole-candidate resource/time qualification.

use crate::background::CheckpointAllocationToken;
use crate::background::{
    checkpoint_decode_allocation as allocation, CheckpointDecodeContext as DecodeContext,
};
use crate::projection::ProjectedRelationshipPredicate;
use hawdb_core::{HawDBError, Result, Value};
use std::collections::BTreeMap;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

pub(crate) fn decode(
    encoded: &str,
    work: &DecodeContext,
) -> Result<BTreeMap<String, ProjectedRelationshipPredicate>> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    if encoded.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Value::Map(values) = decode_value(encoded, work)? else {
        return Err(HawDBError::Storage(
            "projected relationship predicates must decode to a map".into(),
        ));
    };
    let mut result = BTreeMap::new();
    let mut memory = MapMemory::<ProjectedRelationshipPredicate>::default();
    for (key, value) in values {
        let item = from_value(value, work)?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        memory.before_insert(result.len(), work)?;
        result.insert(key, item);
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(result)
}

struct TemporaryText {
    text: Option<String>,
    memory: Option<CheckpointAllocationToken>,
}

impl std::ops::Deref for TemporaryText {
    type Target = str;
    fn deref(&self) -> &str {
        self.text.as_deref().expect("live temporary text")
    }
}

impl Drop for TemporaryText {
    fn drop(&mut self) {
        drop(self.text.take());
        if let Some(memory) = self.memory.take() {
            memory.release_buffer();
        }
    }
}

fn temporary_text(encoded: &str, work: &DecodeContext) -> Result<TemporaryText> {
    let text = decode_string(encoded, work)?;
    let memory = work.find(text.as_ptr() as usize)?;
    Ok(TemporaryText {
        text: Some(text),
        memory: Some(memory),
    })
}

fn hex_bytes(input: &str, binary: bool, work: &DecodeContext) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(HawDBError::Storage(if binary {
            "binary value has an odd number of hex digits".into()
        } else {
            format!("invalid hex string length: {}", input.len())
        }));
    }
    let length = input.len() / 2;
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let token = work.reserve(length)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|error| allocation(error, length, work))?;
    if bytes.capacity() != length {
        return Err(allocation(
            "predicate byte capacity differs from admission",
            length,
            work,
        ));
    }
    token.address(bytes.as_ptr() as usize);
    unit.finish();
    for start in (0..input.len()).step_by(128 * 1024) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        for offset in (start..input.len().min(start.saturating_add(128 * 1024))).step_by(2) {
            let byte = if binary {
                let digits = &input.as_bytes()[offset..offset + 2];
                (hex_digit(digits[0])? << 4) | hex_digit(digits[1])?
            } else {
                input
                    .get(offset..offset + 2)
                    .and_then(|digits| u8::from_str_radix(digits, 16).ok())
                    .ok_or_else(|| {
                        HawDBError::Storage(format!("invalid hex string at byte offset {offset}"))
                    })?
            };
            bytes.push(byte);
        }
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(bytes)
}

fn hex_digit(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(HawDBError::Storage(format!(
            "binary value contains invalid hex digit {:?}",
            char::from(digit)
        ))),
    }
}

fn validate_utf8(bytes: &[u8], work: &DecodeContext) -> Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let end = offset.saturating_add(64 * 1024).min(bytes.len());
        match std::str::from_utf8(&bytes[offset..end]) {
            Ok(_) => offset = end,
            Err(error) if error.error_len().is_none() && end < bytes.len() => {
                offset += error.valid_up_to()
            }
            Err(error) => {
                let index = offset + error.valid_up_to();
                return Err(HawDBError::Storage(match error.error_len() {
                    Some(length) => {
                        format!("invalid utf-8 sequence of {length} bytes from index {index}")
                    }
                    None => format!("incomplete utf-8 byte sequence from index {index}"),
                }));
            }
        }
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)
}

pub(crate) fn decode_string(input: &str, work: &DecodeContext) -> Result<String> {
    let bytes = hex_bytes(input, false, work)?;
    validate_utf8(&bytes, work)?;
    // SAFETY: validate_utf8 checked every byte, retaining incomplete code points
    // across chunk boundaries. Moving the Vec does not alter its contents.
    Ok(unsafe { String::from_utf8_unchecked(bytes) })
}

pub(crate) struct Items<'a> {
    input: &'a str,
    delimiter: u8,
    offset: usize,
    done: bool,
}
impl<'a> Items<'a> {
    pub(crate) fn new(input: &'a str, delimiter: u8) -> Self {
        Self {
            input,
            delimiter,
            offset: 0,
            done: false,
        }
    }
    pub(crate) fn next(&mut self, work: &DecodeContext) -> Result<Option<&'a str>> {
        if self.done {
            return Ok(None);
        }
        let start = self.offset;
        if let Some(relative) = find_delimiter(&self.input[start..], self.delimiter, work)? {
            let end = start + relative;
            self.offset = end + 1;
            return Ok(Some(&self.input[start..end]));
        }
        self.done = true;
        Ok(Some(&self.input[start..]))
    }
}

fn find_delimiter(input: &str, delimiter: u8, work: &DecodeContext) -> Result<Option<usize>> {
    for (index, chunk) in input.as_bytes().chunks(64 * 1024).enumerate() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let found = chunk.iter().position(|byte| *byte == delimiter);
        unit.finish();
        if let Some(found) = found {
            return Ok(Some(index * 64 * 1024 + found));
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(None)
}

fn magnitude(input: &str, limit: u64, work: &DecodeContext) -> Result<Option<u64>> {
    if input.is_empty() {
        return Ok(None);
    }
    let mut output = 0u64;
    for chunk in input.as_bytes().chunks(64 * 1024) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        for digit in chunk {
            if !digit.is_ascii_digit() {
                return Ok(None);
            }
            let Some(value) = output
                .checked_mul(10)
                .and_then(|value| value.checked_add(u64::from(digit - b'0')))
                .filter(|value| *value <= limit)
            else {
                return Ok(None);
            };
            output = value;
        }
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(Some(output))
}

pub(crate) fn parse_u64(input: &str, name: &str, work: &DecodeContext) -> Result<u64> {
    magnitude(input.strip_prefix('+').unwrap_or(input), u64::MAX, work)?
        .ok_or_else(|| HawDBError::Storage(format!("invalid {name}: {input}")))
}
pub(crate) fn parse_usize(input: &str, name: &str, work: &DecodeContext) -> Result<usize> {
    magnitude(
        input.strip_prefix('+').unwrap_or(input),
        usize::MAX as u64,
        work,
    )?
    .map(|value| value as usize)
    .ok_or_else(|| HawDBError::Storage(format!("invalid {name}: {input}")))
}
fn unsigned(input: &str, work: &DecodeContext) -> Result<u64> {
    parse_u64(input, "float value", work)
}

fn integer(input: &str, work: &DecodeContext) -> Result<i64> {
    let negative = input.starts_with('-');
    let digits = if input.starts_with(['+', '-']) {
        &input[1..]
    } else {
        input
    };
    let limit = if negative { 1 << 63 } else { i64::MAX as u64 };
    let value = magnitude(digits, limit, work)?
        .ok_or_else(|| HawDBError::Storage(format!("invalid integer value: {input}")))?;
    if negative {
        Ok(if value == 1 << 63 {
            i64::MIN
        } else {
            -(value as i64)
        })
    } else {
        Ok(value as i64)
    }
}

struct ControlledValueDecoder<'a>(&'a DecodeContext);

impl crate::text::value_decode::Decoder for ControlledValueDecoder<'_> {
    type Items<'a> = Items<'a>;
    type TemporaryText = TemporaryText;
    type MapMemory = MapMemory<Value>;
    fn visit(&self) -> Result<()> {
        self.0
            .start_unit()
            .map_err(HawDBError::from_storage_error)?
            .finish();
        Ok(())
    }
    fn integer(&self, input: &str) -> Result<i64> {
        integer(input, self.0)
    }
    fn unsigned(&self, input: &str) -> Result<u64> {
        unsigned(input, self.0)
    }
    fn string(&self, input: &str) -> Result<String> {
        decode_string(input, self.0)
    }
    fn binary(&self, input: &str) -> Result<Vec<u8>> {
        hex_bytes(input, true, self.0)
    }
    fn temporary(&self, input: &str) -> Result<TemporaryText> {
        temporary_text(input, self.0)
    }
    fn items<'a>(&self, input: &'a str, delimiter: u8) -> Items<'a> {
        Items::new(input, delimiter)
    }
    fn next<'a>(&self, items: &mut Self::Items<'a>) -> Result<Option<&'a str>> {
        items.next(self.0)
    }
    fn equal(&self, input: &str) -> Result<Option<usize>> {
        find_delimiter(input, b'=', self.0)
    }
    fn push(&self, values: &mut Vec<Value>, value: Value) -> Result<()> {
        self.0.push(values, value)
    }
    fn insert(
        &self,
        values: &mut BTreeMap<String, Value>,
        memory: &mut MapMemory<Value>,
        key: String,
        value: Value,
    ) -> Result<()> {
        let unit = self
            .0
            .start_unit()
            .map_err(HawDBError::from_storage_error)?;
        memory.before_insert(values.len(), self.0)?;
        values.insert(key, value);
        unit.finish();
        Ok(())
    }
}

fn decode_value(input: &str, work: &DecodeContext) -> Result<Value> {
    crate::text::value_decode::decode(input, &ControlledValueDecoder(work))
}

pub(crate) struct MapMemory<T> {
    admitted_nodes: usize,
    kind: std::marker::PhantomData<T>,
}
impl<T> Default for MapMemory<T> {
    fn default() -> Self {
        Self {
            admitted_nodes: 0,
            kind: std::marker::PhantomData,
        }
    }
}
impl<T> MapMemory<T> {
    pub(crate) fn before_insert(&mut self, len: usize, work: &DecodeContext) -> Result<()> {
        let keys = len.checked_add(1).ok_or_else(|| {
            allocation(
                "predicate map cardinality overflows usize",
                usize::MAX,
                work,
            )
        })?;
        let nodes = keys / 4 + 2;
        if nodes > self.admitted_nodes {
            // Pinned Rust 1.97.1 B=6, matching the existing WAL map bound.
            let alignment = align_of::<String>()
                .max(align_of::<T>())
                .max(align_of::<NonNull<u8>>())
                .max(align_of::<u16>());
            let node = size_of::<Option<NonNull<u8>>>()
                + 2 * size_of::<u16>()
                + 11 * (size_of::<String>() + size_of::<T>())
                + 12 * size_of::<NonNull<u8>>()
                + 8 * (alignment - 1);
            let bytes = (nodes - self.admitted_nodes)
                .checked_mul(node)
                .ok_or_else(|| {
                    allocation(
                        "predicate map node capacity overflows usize",
                        usize::MAX,
                        work,
                    )
                })?;
            let _token = work.reserve(bytes)?;
            self.admitted_nodes = nodes;
        }
        Ok(())
    }
}

struct ControlledPredicateDecoder<'a>(&'a DecodeContext);

impl super::super::predicate_decode::Decoder for ControlledPredicateDecoder<'_> {
    fn visit(&self) -> Result<()> {
        self.0
            .start_unit()
            .map_err(HawDBError::from_storage_error)?
            .finish();
        Ok(())
    }

    fn push(
        &self,
        values: &mut Vec<ProjectedRelationshipPredicate>,
        value: ProjectedRelationshipPredicate,
    ) -> Result<()> {
        self.0.push(values, value)
    }
}

fn from_value(value: Value, work: &DecodeContext) -> Result<ProjectedRelationshipPredicate> {
    super::super::predicate_decode::decode(value, &ControlledPredicateDecoder(work))
}
