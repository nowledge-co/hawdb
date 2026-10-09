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

//! Keep encoded text and its admitted capacity together. Growth reserves both
//! old and new capacities before copying in bounded units. Producer allocations
//! and allocator rounding/latency remain separate resource-ledger requirements.

use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use hawdb_core::{HawDBError, Result, RuntimeMemoryPermit};
use std::fmt::{self, Write};

#[doc(hidden)]
#[derive(Debug)]
pub struct CheckpointText {
    #[cfg(test)]
    pub(crate) text: String,
    #[cfg(not(test))]
    text: String,
    memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

impl std::ops::Deref for CheckpointText {
    type Target = str;

    fn deref(&self) -> &str {
        &self.text
    }
}

impl PartialEq<String> for CheckpointText {
    fn eq(&self, other: &String) -> bool {
        self.text == *other
    }
}

impl CheckpointText {
    pub(crate) const fn new() -> Self {
        Self {
            text: String::new(),
            memory: None,
        }
    }

    // Only the ordinary API's default context can take this path.
    pub(crate) fn into_unadmitted(self) -> String {
        assert!(
            self.memory.is_none(),
            "admitted text cannot detach its lease"
        );
        self.text
    }

    fn reserve(&mut self, additional: usize, work: &CheckpointWorkContext) -> Result<()> {
        let required = self.text.len().checked_add(additional).ok_or_else(|| {
            HawDBError::from_storage_error(work.record_failure(CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                reason: "projected artifact text length overflows usize".into(),
            }))
        })?;
        if required <= self.text.capacity() {
            return Ok(());
        }
        let capacity = required
            .max(self.text.capacity().saturating_mul(2))
            .max(128);
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let memory = work
            .reserve_memory(capacity)
            .map_err(HawDBError::from_storage_error)?;
        let mut text = String::new();
        text.try_reserve_exact(capacity).map_err(|error| {
            HawDBError::from_storage_error(work.record_failure(CheckpointWorkError::Allocation {
                bytes: capacity as u64,
                reason: error.to_string(),
            }))
        })?;
        if text.capacity() != capacity {
            return Err(HawDBError::from_storage_error(work.record_failure(
                CheckpointWorkError::Allocation {
                    bytes: capacity as u64,
                    reason: format!(
                        "text allocator granted {} bytes instead of admitted {capacity}",
                        text.capacity()
                    ),
                },
            )));
        }
        let mut replacement = Self { text, memory };
        unit.finish();
        let mut remaining = self.text.as_str();
        while !remaining.is_empty() {
            let mut end = remaining.len().min(64 * 1024);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            replacement.text.push_str(&remaining[..end]);
            unit.finish();
            remaining = &remaining[end..];
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        std::mem::swap(self, &mut replacement);
        // The old String dies before its permit, including on cancellation.
        drop(replacement);
        Ok(())
    }

    pub(crate) fn append(&mut self, mut text: &str, work: &CheckpointWorkContext) -> Result<()> {
        self.reserve(text.len(), work)?;
        while !text.is_empty() {
            let mut end = text.len().min(64 * 1024);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            self.text.push_str(&text[..end]);
            unit.finish();
            text = &text[end..];
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }

    pub(crate) fn fields(
        &mut self,
        args: fmt::Arguments<'_>,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let mut scalar = Scalar {
            bytes: [0; 64],
            used: 0,
        };
        scalar
            .write_fmt(args)
            .expect("fixed artifact fields and u64 displays fit 64 bytes");
        unit.finish();
        self.append(
            std::str::from_utf8(&scalar.bytes[..scalar.used]).expect("numeric fields are ASCII"),
            work,
        )
    }

    pub(crate) fn hex(&mut self, name: &str, work: &CheckpointWorkContext) -> Result<()> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        // 32 KiB source bytes produce at most 64 KiB of output per unit.
        for source in name.as_bytes().chunks(32 * 1024) {
            self.reserve(source.len() * 2, work)?;
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            for byte in source {
                self.text.push(char::from(HEX[usize::from(byte >> 4)]));
                self.text.push(char::from(HEX[usize::from(byte & 15)]));
            }
            unit.finish();
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }

    pub(crate) fn names(&mut self, names: &[String], work: &CheckpointWorkContext) -> Result<()> {
        self.append("\t", work)?;
        for (index, name) in names.iter().enumerate() {
            if index != 0 {
                self.append(":", work)?;
            }
            self.hex(name, work)?;
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }

    pub(crate) fn numbers(
        &mut self,
        name: &str,
        mut values: impl ExactSizeIterator<Item = u64>,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        let mut index = 0usize;
        loop {
            let count = values.len().min(1024);
            // Decimal u64 takes at most 20 bytes, plus a separator. No numeric
            // batch can grow the admitted String while holding a local permit.
            self.reserve(count * 21 + name.len() + 2, work)?;
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            if index == 0 {
                self.text.push_str(name);
                self.text.push('\t');
            }
            for value in values.by_ref().take(count) {
                if index != 0 {
                    self.text.push(',');
                }
                write!(self.text, "{value}").expect("admitted numeric batch fits its capacity");
                index += 1;
            }
            let complete = values.len() == 0;
            if complete {
                self.text.push('\n');
            }
            unit.finish();
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            if complete {
                return Ok(());
            }
        }
    }
}

struct Scalar {
    bytes: [u8; 64],
    used: usize,
}

impl Write for Scalar {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self.used.checked_add(text.len()).ok_or(fmt::Error)?;
        self.bytes
            .get_mut(self.used..end)
            .ok_or(fmt::Error)?
            .copy_from_slice(text.as_bytes());
        self.used = end;
        Ok(())
    }
}
