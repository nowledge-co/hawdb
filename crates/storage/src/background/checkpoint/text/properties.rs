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

//! Stream the existing tagged property grammar into admitted text. Container
//! escaping goes directly to the destination without intermediate value copies.

use super::*;
use hawdb_core::Value;
use std::collections::BTreeMap;

impl CheckpointText {
    pub(crate) fn values(&mut self, values: &[Value], work: &CheckpointWorkContext) -> Result<()> {
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                self.append(":", work)?;
            }
            self.tagged_value(value, 1, work)?;
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }

    pub(crate) fn properties(
        &mut self,
        properties: &BTreeMap<String, Value>,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        for (index, (name, value)) in properties.iter().enumerate() {
            if index != 0 {
                self.append(";", work)?;
            }
            self.hex(name, work)?;
            self.append("=", work)?;
            self.tagged_value(value, 0, work)?;
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }

    fn tagged_value(
        &mut self,
        value: &Value,
        escapes: u32,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        match value {
            Value::Null => self.escaped(b"n", escapes, work)?,
            Value::Bool(value) => {
                self.escaped(if *value { b"b1" } else { b"b0" }, escapes, work)?;
            }
            Value::Int(value) => self.escaped_scalar(format_args!("i{value}"), escapes, work)?,
            Value::Float(value) => {
                self.escaped_scalar(format_args!("f{}", value.to_bits()), escapes, work)?;
            }
            Value::Uuid(value) => {
                self.escaped_scalar(format_args!("u{value}"), escapes, work)?;
            }
            Value::String(value) => {
                self.escaped(b"s", escapes, work)?;
                self.escaped(value.as_bytes(), escapes + 1, work)?;
            }
            Value::Binary(value) => {
                self.escaped(b"x", escapes, work)?;
                self.escaped(value, escapes + 1, work)?;
            }
            Value::List(values) => {
                self.escaped(b"l", escapes, work)?;
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        self.escaped(b",", escapes, work)?;
                    }
                    self.tagged_value(value, escapes + 1, work)?;
                }
            }
            Value::Map(values) => {
                self.escaped(b"m", escapes, work)?;
                for (index, (name, value)) in values.iter().enumerate() {
                    if index != 0 {
                        self.escaped(b";", escapes, work)?;
                    }
                    self.escaped(name.as_bytes(), escapes + 1, work)?;
                    self.escaped(b"=", escapes, work)?;
                    self.tagged_value(value, escapes + 1, work)?;
                }
            }
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }

    fn escaped_scalar(
        &mut self,
        args: fmt::Arguments<'_>,
        escapes: u32,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let mut scalar = Scalar {
            bytes: [0; 64],
            used: 0,
        };
        scalar
            .write_fmt(args)
            .expect("tagged scalar displays fit 64 bytes");
        unit.finish();
        self.escaped(&scalar.bytes[..scalar.used], escapes, work)
    }

    fn escaped(&mut self, input: &[u8], escapes: u32, work: &CheckpointWorkContext) -> Result<()> {
        if escapes == 0 {
            return self.append(
                std::str::from_utf8(input).expect("unescaped property grammar is ASCII"),
                work,
            );
        }
        let factor = 1usize.checked_shl(escapes);
        let length = factor.and_then(|factor| input.len().checked_mul(factor));
        let Some(length) = length else {
            return Err(HawDBError::from_storage_error(work.record_failure(
                CheckpointWorkError::Allocation {
                    bytes: u64::MAX,
                    reason: "escaped property text length overflows usize".into(),
                },
            )));
        };
        self.reserve(length, work)?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        // Each unit transforms at most 2048 bytes with at most usize::BITS
        // nibble steps per byte. Even deep containers use fixed stack scratch.
        let mut scratch = [0u8; 2048];
        for start in (0..length).step_by(scratch.len()) {
            let count = (length - start).min(scratch.len());
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            for (offset, byte) in scratch[..count].iter_mut().enumerate() {
                let position = start + offset;
                let mut value = input[position >> escapes];
                for bit in (0..escapes).rev() {
                    let nibble = if position & (1usize << bit) == 0 {
                        value >> 4
                    } else {
                        value & 15
                    };
                    value = HEX[usize::from(nibble)];
                }
                *byte = value;
            }
            unit.finish();
            self.append(
                std::str::from_utf8(&scratch[..count]).expect("hex digits are ASCII"),
                work,
            )?;
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }
}
