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

//! One ordered-key grammar for ordinary, counting and admitted output sinks.

use super::{CheckpointKeyEncodeError, OrderedRelationalKeyError, RelationalKey, RelationalValue};
use crate::background::{CheckpointBytes, CheckpointWorkContext, CheckpointWorkError};

pub(super) trait Output {
    type Error: From<OrderedRelationalKeyError> + From<CheckpointWorkError>;

    fn work(&self) -> Option<&CheckpointWorkContext> {
        None
    }

    fn raw(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;

    fn unit(
        &mut self,
        write: impl FnOnce(&mut Self) -> Result<(), Self::Error>,
    ) -> Result<(), Self::Error> {
        let unit = self
            .work()
            .map(CheckpointWorkContext::start_unit)
            .transpose()
            .map_err(Self::Error::from)?;
        write(self)?;
        if let Some(unit) = unit {
            unit.finish();
        }
        Ok(())
    }
}

impl Output for Vec<u8> {
    type Error = OrderedRelationalKeyError;

    fn raw(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.extend_from_slice(bytes);
        Ok(())
    }
}

struct Cooperative<'a> {
    bytes: Vec<u8>,
    work: &'a CheckpointWorkContext,
}

impl Output for Cooperative<'_> {
    type Error = CheckpointKeyEncodeError;

    fn work(&self) -> Option<&CheckpointWorkContext> {
        Some(self.work)
    }

    fn raw(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

pub(super) fn with_work_context(
    key: &RelationalKey,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, CheckpointKeyEncodeError> {
    work.checkpoint()?;
    nonempty(key)?;
    let mut output = Cooperative {
        bytes: Vec::new(),
        work,
    };
    for item in &key.0 {
        value(&mut output, item)?;
    }
    work.checkpoint()?;
    Ok(output.bytes)
}

fn nonempty(key: &RelationalKey) -> Result<(), OrderedRelationalKeyError> {
    if key.0.is_empty() {
        return Err(OrderedRelationalKeyError::Corrupt(
            "key contains no values".into(),
        ));
    }
    Ok(())
}

pub(super) fn value<S: Output>(output: &mut S, value: &RelationalValue) -> Result<(), S::Error> {
    match value {
        RelationalValue::Text(value) => escaped(output, 4, value.as_bytes()),
        RelationalValue::Bytea(value) => escaped(output, 5, value),
        value => output.unit(|output| match value {
            RelationalValue::Null => output.raw(&[0]),
            RelationalValue::Boolean(value) => output.raw(&[1, u8::from(*value)]),
            RelationalValue::BigInt(value) => {
                output.raw(&[2])?;
                output.raw(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes())
            }
            RelationalValue::DoublePrecision(value) => {
                let bits = value.to_bits();
                let ordered = if bits >> 63 == 0 {
                    bits ^ (1_u64 << 63)
                } else {
                    !bits
                };
                output.raw(&[3])?;
                output.raw(&ordered.to_be_bytes())
            }
            RelationalValue::Decimal(value) => {
                output.raw(&[9])?;
                output.raw(&value.to_ordered_bytes())
            }
            RelationalValue::Uuid(value) => {
                output.raw(&[6])?;
                output.raw(value.as_bytes())
            }
            RelationalValue::Vector(_) => Err(OrderedRelationalKeyError::UnsupportedVector.into()),
            RelationalValue::Overflow(_) => {
                Err(OrderedRelationalKeyError::UnsupportedOverflow.into())
            }
            RelationalValue::Text(_) | RelationalValue::Bytea(_) => unreachable!("handled above"),
        }),
    }
}

fn escaped<S: Output>(output: &mut S, tag: u8, bytes: &[u8]) -> Result<(), S::Error> {
    output.unit(|output| output.raw(&[tag]))?;
    for chunk in bytes.chunks(64 * 1024) {
        output.unit(|output| {
            let mut start = 0;
            for (index, byte) in chunk.iter().enumerate() {
                if *byte == 0 {
                    output.raw(&chunk[start..index])?;
                    output.raw(&[0, 255])?;
                    start = index + 1;
                }
            }
            output.raw(&chunk[start..])
        })?;
    }
    output.unit(|output| output.raw(&[0, 0]))
}

struct Count<'a> {
    length: usize,
    work: &'a CheckpointWorkContext,
}

impl Output for Count<'_> {
    type Error = CheckpointKeyEncodeError;

    fn work(&self) -> Option<&CheckpointWorkContext> {
        Some(self.work)
    }

    fn raw(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.length = self.length.checked_add(bytes.len()).ok_or_else(|| {
            self.work.record_failure(CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                reason: "ordered key length overflows usize".into(),
            })
        })?;
        Ok(())
    }
}

struct Admitted<'a> {
    bytes: CheckpointBytes,
    offset: usize,
    work: &'a CheckpointWorkContext,
}

impl Output for Admitted<'_> {
    type Error = CheckpointKeyEncodeError;

    fn work(&self) -> Option<&CheckpointWorkContext> {
        Some(self.work)
    }

    fn raw(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        let work = self.work;
        let invalid = || {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: bytes.len() as u64,
                reason: "ordered key exceeded its admitted output".into(),
            })
        };
        let end = self.offset.checked_add(bytes.len()).ok_or_else(invalid)?;
        let target = self
            .bytes
            .as_mut_slice()
            .get_mut(self.offset..end)
            .ok_or_else(invalid)?;
        target.copy_from_slice(bytes);
        self.offset = end;
        Ok(())
    }
}

struct InventoryOutput<'a> {
    bytes: Vec<u8>,
    allocations: &'a crate::background::CheckpointDecodeContext,
}

impl Output for InventoryOutput<'_> {
    type Error = CheckpointKeyEncodeError;

    fn work(&self) -> Option<&CheckpointWorkContext> {
        Some(self.allocations)
    }

    fn raw(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        if bytes.len() > self.bytes.capacity() - self.bytes.len() {
            return Err(self
                .allocations
                .record_failure(CheckpointWorkError::Allocation {
                    bytes: bytes.len() as u64,
                    reason: "ordered key exceeded its admitted output".into(),
                })
                .into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

/// A borrowed immutable key whose exact output size was computed using the
/// same grammar that materializes it. Callers apply their format limit before
/// allocating; the result retains its allocation permit through destruction.
pub(crate) struct CheckpointKeyEncoding<'a> {
    key: &'a RelationalKey,
    prefix: &'a [u8],
    encoded_len: usize,
}

impl<'a> CheckpointKeyEncoding<'a> {
    pub(crate) fn new(
        key: &'a RelationalKey,
        prefix: &'a [u8],
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointKeyEncodeError> {
        work.checkpoint()?;
        nonempty(key)?;
        let mut count = Count {
            length: prefix.len(),
            work,
        };
        for item in &key.0 {
            value(&mut count, item)?;
        }
        work.checkpoint()?;
        Ok(Self {
            key,
            prefix,
            encoded_len: count.length,
        })
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub(crate) fn encode_owned(
        self,
        allocations: &crate::background::CheckpointDecodeContext,
    ) -> Result<Vec<u8>, CheckpointKeyEncodeError> {
        let bytes = allocations
            .vector_capacity(self.encoded_len)
            .map_err(|error| {
                CheckpointKeyEncodeError::Key(super::super::RelationalError::Admission(
                    error.to_string(),
                ))
            })?;
        let mut output = InventoryOutput { bytes, allocations };
        for chunk in self.prefix.chunks(64 * 1024) {
            output.unit(|output| output.raw(chunk))?;
        }
        for item in &self.key.0 {
            value(&mut output, item)?;
        }
        allocations.checkpoint()?;
        if output.bytes.len() != self.encoded_len {
            return Err(OrderedRelationalKeyError::Corrupt(
                "ordered key size changed during encoding".into(),
            )
            .into());
        }
        Ok(output.bytes)
    }

    pub(crate) fn encode(
        self,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointBytes, CheckpointKeyEncodeError> {
        let bytes = CheckpointBytes::zeroed(self.encoded_len, work)?;
        let mut output = Admitted {
            bytes,
            offset: 0,
            work,
        };
        for chunk in self.prefix.chunks(64 * 1024) {
            output.unit(|output| output.raw(chunk))?;
        }
        for item in &self.key.0 {
            value(&mut output, item)?;
        }
        work.checkpoint()?;
        if output.offset != self.encoded_len {
            return Err(OrderedRelationalKeyError::Corrupt(
                "ordered key size changed during encoding".into(),
            )
            .into());
        }
        Ok(output.bytes)
    }
}
