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

//! Shared, admitted record scratch for projection and metadata streaming.

use super::invalid;
use crate::build_memory::{checked_add as add, read::io_error, reserve_capacity};
use crate::Result;
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use std::io::BufRead;

pub(in crate::out_of_core) fn read_record(
    text: &mut dyn BufRead,
    line: &mut Vec<u8>,
    limit: usize,
    task: &RuntimeTaskContext,
    memory: &mut QueryMemoryLease,
) -> Result<bool> {
    line.clear();
    loop {
        crate::build_control::checkpoint(task)?;
        let available = match text.fill_buf() {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result.map_err(io_error)?,
        };
        if available.is_empty() {
            return Ok(!line.is_empty());
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |at| at + 1);
        if add(line.len(), count)? > limit {
            return Err(invalid("encoded record exceeds scratch cap"));
        }
        let chunk = std::str::from_utf8(&available[..count])
            .map_err(|_| invalid("non-ASCII encoded record"))?;
        if !chunk.is_ascii() {
            return Err(invalid("non-ASCII encoded record"));
        }
        let required = add(line.len(), count)?;
        if required > line.capacity() {
            let capacity = required.max(line.capacity().saturating_mul(2)).min(limit);
            reserve_capacity(line, capacity, memory)?;
        }
        line.extend_from_slice(chunk.as_bytes());
        let done = chunk.ends_with('\n');
        text.consume(count);
        if done {
            return Ok(true);
        }
    }
}
