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

//! Borrow the typed predicate tree instead of cloning it into an intermediate
//! Value::Map and recursively materializing text. Counting never visits binary
//! or string bytes. Emission owns one admitted 64 KiB scratch buffer; callbacks
//! run after releasing the local unit and may acquire their own local permit.

use super::ProjectedRelationshipPredicate;
use crate::background::{
    CheckpointBytes, CheckpointWorkContext, CheckpointWorkError, CheckpointWorkUnit,
};
use hawdb_core::{HawDBError, Result, Value};
use std::collections::BTreeMap;
use std::fmt::{self, Write};

const CHUNK: usize = 64 * 1024;
const HEX: &[u8; 16] = b"0123456789abcdef";

trait Sink {
    fn node(&mut self) -> Result<()>;
    fn bytes(&mut self, bytes: &[u8], hex_layers: u32) -> Result<()>;
}

struct Counter<'a> {
    length: usize,
    work: &'a CheckpointWorkContext,
}

impl Sink for Counter<'_> {
    fn node(&mut self) -> Result<()> {
        self.work
            .start_unit()
            .map_err(HawDBError::from_storage_error)?
            .finish();
        Ok(())
    }

    fn bytes(&mut self, bytes: &[u8], hex_layers: u32) -> Result<()> {
        let unit = self
            .work
            .start_unit()
            .map_err(HawDBError::from_storage_error)?;
        let expanded = 1usize
            .checked_shl(hex_layers)
            .and_then(|factor| bytes.len().checked_mul(factor));
        self.length = expanded
            .and_then(|length| self.length.checked_add(length))
            .ok_or_else(|| overflow(self.work))?;
        unit.finish();
        Ok(())
    }
}

fn overflow(work: &CheckpointWorkContext) -> HawDBError {
    HawDBError::from_storage_error(work.record_failure(CheckpointWorkError::Allocation {
        bytes: u64::MAX,
        reason: "projected relationship predicate text capacity overflows usize".into(),
    }))
}

struct Emitter<'a, F> {
    scratch: CheckpointBytes,
    used: usize,
    work: &'a CheckpointWorkContext,
    unit: Option<CheckpointWorkUnit>,
    write: F,
}

impl<F: FnMut(&[u8]) -> Result<()>> Emitter<'_, F> {
    fn flush(&mut self) -> Result<()> {
        if let Some(unit) = self.unit.take() {
            unit.finish();
        }
        if self.used != 0 {
            (self.write)(&self.scratch[..self.used])?;
            self.used = 0;
        }
        Ok(())
    }

    fn byte(&mut self, byte: u8, hex_layers: u32) -> Result<()> {
        if hex_layers != 0 {
            self.byte(HEX[usize::from(byte >> 4)], hex_layers - 1)?;
            return self.byte(HEX[usize::from(byte & 15)], hex_layers - 1);
        }
        if self.unit.is_none() {
            self.unit = Some(
                self.work
                    .start_unit()
                    .map_err(HawDBError::from_storage_error)?,
            );
        }
        self.scratch.as_mut_slice()[self.used] = byte;
        self.used += 1;
        if self.used == self.scratch.len() {
            self.flush()?;
        }
        Ok(())
    }
}

impl<F: FnMut(&[u8]) -> Result<()>> Sink for Emitter<'_, F> {
    fn node(&mut self) -> Result<()> {
        if let Some(unit) = self.unit.take() {
            unit.finish();
        }
        self.unit = Some(
            self.work
                .start_unit()
                .map_err(HawDBError::from_storage_error)?,
        );
        Ok(())
    }

    fn bytes(&mut self, bytes: &[u8], hex_layers: u32) -> Result<()> {
        for byte in bytes {
            self.byte(*byte, hex_layers)?;
        }
        Ok(())
    }
}

// All scalar displays fit in 36 bytes, without a temporary heap string.
struct Scalar {
    bytes: [u8; 64],
    used: usize,
}

impl Write for Scalar {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let end = self.used.checked_add(value.len()).ok_or(fmt::Error)?;
        let output = self.bytes.get_mut(self.used..end).ok_or(fmt::Error)?;
        output.copy_from_slice(value.as_bytes());
        self.used = end;
        Ok(())
    }
}

fn scalar(sink: &mut impl Sink, value: impl fmt::Display, layers: u32) -> Result<()> {
    let mut scalar = Scalar {
        bytes: [0; 64],
        used: 0,
    };
    write!(&mut scalar, "{value}").expect("numeric and UUID displays fit the stack buffer");
    sink.bytes(&scalar.bytes[..scalar.used], layers)
}

fn string(sink: &mut impl Sink, value: &str, layers: u32) -> Result<()> {
    sink.bytes(b"s", layers)?;
    sink.bytes(value.as_bytes(), layers + 1)
}

fn value(sink: &mut impl Sink, value: &Value, layers: u32) -> Result<()> {
    sink.node()?;
    match value {
        Value::Null => sink.bytes(b"n", layers),
        Value::Bool(false) => sink.bytes(b"b0", layers),
        Value::Bool(true) => sink.bytes(b"b1", layers),
        Value::Int(number) => {
            sink.bytes(b"i", layers)?;
            scalar(sink, number, layers)
        }
        Value::Float(number) => {
            sink.bytes(b"f", layers)?;
            scalar(sink, number.to_bits(), layers)
        }
        Value::Uuid(uuid) => {
            sink.bytes(b"u", layers)?;
            scalar(sink, uuid, layers)
        }
        Value::String(text) => string(sink, text, layers),
        Value::Binary(bytes) => {
            sink.bytes(b"x", layers)?;
            sink.bytes(bytes, layers + 1)
        }
        Value::List(values) => {
            sink.bytes(b"l", layers)?;
            for (index, item) in values.iter().enumerate() {
                if index != 0 {
                    sink.bytes(b",", layers)?;
                }
                self::value(sink, item, layers + 1)?;
            }
            Ok(())
        }
        Value::Map(values) => {
            sink.bytes(b"m", layers)?;
            for (index, (key, item)) in values.iter().enumerate() {
                if index != 0 {
                    sink.bytes(b";", layers)?;
                }
                sink.bytes(key.as_bytes(), layers + 1)?;
                sink.bytes(b"=", layers)?;
                self::value(sink, item, layers + 1)?;
            }
            Ok(())
        }
    }
}

fn predicate(
    sink: &mut impl Sink,
    item: &ProjectedRelationshipPredicate,
    layers: u32,
) -> Result<()> {
    sink.node()?;
    sink.bytes(b"l", layers)?;
    match item {
        ProjectedRelationshipPredicate::And(predicates) => {
            string(sink, "and", layers + 1)?;
            sink.bytes(b",", layers)?;
            sink.bytes(b"l", layers + 1)?;
            for (index, item) in predicates.iter().enumerate() {
                if index != 0 {
                    sink.bytes(b",", layers + 1)?;
                }
                predicate(sink, item, layers + 2)?;
            }
        }
        ProjectedRelationshipPredicate::Eq {
            property,
            value: leaf,
        }
        | ProjectedRelationshipPredicate::Gte {
            property,
            value: leaf,
        } => {
            let operator = if matches!(item, ProjectedRelationshipPredicate::Eq { .. }) {
                "eq"
            } else {
                "gte"
            };
            string(sink, operator, layers + 1)?;
            sink.bytes(b",", layers)?;
            string(sink, property, layers + 1)?;
            sink.bytes(b",", layers)?;
            value(sink, leaf, layers + 1)?;
        }
    }
    Ok(())
}

fn predicates(
    sink: &mut impl Sink,
    items: &BTreeMap<String, ProjectedRelationshipPredicate>,
) -> Result<()> {
    sink.node()?;
    sink.bytes(b"m", 0)?;
    for (index, (rel_type, item)) in items.iter().enumerate() {
        if index != 0 {
            sink.bytes(b";", 0)?;
        }
        sink.bytes(rel_type.as_bytes(), 1)?;
        sink.bytes(b"=", 0)?;
        predicate(sink, item, 1)?;
    }
    Ok(())
}

pub(crate) fn encoded_len(
    items: &BTreeMap<String, ProjectedRelationshipPredicate>,
    work: &CheckpointWorkContext,
) -> Result<usize> {
    let mut counter = Counter { length: 0, work };
    predicates(&mut counter, items)?;
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(counter.length)
}

pub(crate) fn encode_into(
    items: &BTreeMap<String, ProjectedRelationshipPredicate>,
    work: &CheckpointWorkContext,
    write: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let length = encoded_len(items, work)?;
    let scratch =
        CheckpointBytes::zeroed(length.min(CHUNK), work).map_err(HawDBError::from_storage_error)?;
    let mut output = Emitter {
        scratch,
        used: 0,
        work,
        unit: None,
        write,
    };
    predicates(&mut output, items)?;
    output.flush()?;
    work.checkpoint().map_err(HawDBError::from_storage_error)
}

pub(crate) mod decode;
pub(super) mod evaluate;
