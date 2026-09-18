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

use super::{AppendTableError, AppendTableRow};
use crate::RelationalError;
use std::cmp::Ordering;

#[derive(Default)]
pub(super) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(super) fn finish(self) -> Vec<u8> {
        self.bytes
    }

    pub(super) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(super) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(super) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(super) fn raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub(super) fn overwrite_u32(&mut self, offset: usize, value: u32) {
        self.bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub(super) fn count(&mut self, count: usize, context: &str) -> Result<(), AppendTableError> {
        let count = u32::try_from(count).map_err(|_| {
            AppendTableError::Admission(format!("{context} exceeds durable codec limit"))
        })?;
        self.u32(count);
        Ok(())
    }

    pub(super) fn string(&mut self, value: &str, context: &str) -> Result<(), AppendTableError> {
        self.bytes(value.as_bytes(), context)
    }

    pub(super) fn bytes(&mut self, value: &[u8], context: &str) -> Result<(), AppendTableError> {
        let len = u64::try_from(value.len())
            .map_err(|_| AppendTableError::Admission(format!("{context} length overflows u64")))?;
        self.u64(len);
        self.bytes.extend_from_slice(value);
        Ok(())
    }
}

pub(super) struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(super) fn finish(self, context: &str) -> Result<(), AppendTableError> {
        if self.position != self.bytes.len() {
            return Err(AppendTableError::Corruption(format!(
                "{context} contains {} trailing bytes",
                self.bytes.len() - self.position
            )));
        }
        Ok(())
    }

    pub(super) fn u8(&mut self, context: &str) -> Result<u8, AppendTableError> {
        Ok(self.take(1, context)?[0])
    }

    pub(super) fn count(&mut self, limit: usize, context: &str) -> Result<usize, AppendTableError> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| AppendTableError::Corruption(format!("{context} overflows usize")))?;
        if count > limit {
            return Err(AppendTableError::Admission(format!(
                "{context} count {count} exceeds limit {limit}"
            )));
        }
        Ok(count)
    }

    pub(super) fn u32(&mut self) -> Result<u32, AppendTableError> {
        Ok(u32::from_le_bytes(
            self.take(4, "u32")?.try_into().expect("fixed u32"),
        ))
    }

    pub(super) fn u64(&mut self) -> Result<u64, AppendTableError> {
        Ok(u64::from_le_bytes(
            self.take(8, "u64")?.try_into().expect("fixed u64"),
        ))
    }

    pub(super) fn string(
        &mut self,
        limit: usize,
        context: &str,
    ) -> Result<String, AppendTableError> {
        String::from_utf8(self.bytes(limit, context)?.to_vec()).map_err(|error| {
            AppendTableError::Corruption(format!("{context} is not valid UTF-8: {error}"))
        })
    }

    pub(super) fn bytes(
        &mut self,
        limit: usize,
        context: &str,
    ) -> Result<&'a [u8], AppendTableError> {
        let len = to_usize(self.u64()?, context)?;
        if len > limit {
            return Err(AppendTableError::Admission(format!(
                "{context} contains {len} bytes, exceeding limit {limit}"
            )));
        }
        self.take(len, context)
    }

    pub(super) fn take(&mut self, len: usize, context: &str) -> Result<&'a [u8], AppendTableError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| AppendTableError::Corruption(format!("{context} offset overflow")))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| AppendTableError::Corruption(format!("{context} is truncated")))?;
        self.position = end;
        Ok(bytes)
    }
}

pub(super) fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("fixed u16"))
}

pub(super) fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed u32"))
}

pub(super) fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed u64"))
}

pub(super) fn to_usize(value: u64, context: &str) -> Result<usize, AppendTableError> {
    usize::try_from(value)
        .map_err(|_| AppendTableError::Corruption(format!("append {context} overflows usize")))
}

pub(super) fn map_relational(error: RelationalError) -> AppendTableError {
    match error {
        RelationalError::Admission(message) => AppendTableError::Admission(message),
        RelationalError::Schema(message) => AppendTableError::Schema(message),
        RelationalError::Constraint(message) => AppendTableError::Constraint(message),
        RelationalError::Durability(message) => AppendTableError::Durability(message),
        RelationalError::Corruption(message) => AppendTableError::Corruption(message),
    }
}

pub(super) fn compare_rows(left: &AppendTableRow, right: &AppendTableRow) -> Ordering {
    left.table
        .cmp(&right.table)
        .then_with(|| left.partition_key.cmp(&right.partition_key))
        .then_with(|| left.order_key.cmp(&right.order_key))
}
