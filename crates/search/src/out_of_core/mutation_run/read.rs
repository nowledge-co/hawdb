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

//! Fresh query admission for immutable mutation-term ranges.
use super::{terms::TermFile, HawDBError};
use crate::lexical_projection::QueryContext;
use std::io::{self, Read};

pub(in crate::out_of_core) struct Range<'a> {
    pub(super) file: &'a TermFile,
    pub(super) offset: u64,
    pub(super) remaining: u64,
    pub(super) context: Option<QueryContext<'a>>,
}
impl Read for Range<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self.remaining.min(output.len() as u64).min(8192) as usize;
        if count == 0 {
            return Ok(0);
        }
        let count = match self.context {
            None => {
                if self.file.requires_context() {
                    return Err(io::Error::other(HawDBError::Execution(
                        "admitted mutation terms require a fresh query context".into(),
                    )));
                }
                let mut reader = super::super::hydration::RangeReader {
                    file: &self.file.file,
                    offset: self.offset,
                    remaining: self.remaining,
                };
                reader.read(&mut output[..count])?
            }
            Some(context) => crate::build_memory::read::QueryFileRange {
                file: &self.file.file,
                expected_length: self.file.expected_length,
                offset: self.offset,
                remaining: self.remaining,
                context,
                require_execution: self.file.requires_context(),
            }
            .read(&mut output[..count])?,
        };
        self.offset = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("mutation range offset overflow"))?;
        self.remaining -= count as u64;
        Ok(count)
    }
}

pub(super) fn io_error(error: io::Error) -> HawDBError {
    crate::build_memory::read::io_error(error)
}

pub(super) fn json_error(error: serde_json::Error, label: &str) -> HawDBError {
    if error.is_io() {
        io_error(error.into())
    } else {
        HawDBError::Storage(format!("{label}: {error}"))
    }
}
