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

//! Streaming field proof: delimiters, not read boundaries, end identifiers.

use super::*;
use crate::build_memory::{checked_add, reserve_capacity};
use crate::HawDBError;
use hawdb_executor::QueryMemoryLease;
use std::io::{self, Read};

#[cfg(test)]
mod capture;
pub(crate) mod utf8;

const BUFFER_BYTES: usize = 8192;

pub(crate) fn visit_reader(
    reader: &mut impl Read,
    analyzer: &SearchAnalyzerLexicon,
    control: Control<'_>,
    max_source_bytes: u64,
    max_identifier_bytes: usize,
    mut emit: impl FnMut(Term, TokenOccurrence) -> Result<()>,
) -> Result<u64> {
    let checkpoints = CheckpointThrottle::new();
    let control = control.with_checkpoint_throttle(&checkpoints);
    let mut raw = IdentifierBuffer {
        bytes: Vec::new(),
        memory: control
            .memory
            .map(|memory| memory.input.reserve(0))
            .transpose()?,
        limit: max_identifier_bytes,
    };
    let mut previous = None::<Term>;
    let bytes = utf8::visit(reader, control, max_source_bytes, |text| {
        for ch in text.chars() {
            control.check()?;
            if ch.is_alphanumeric() || ch == '_' {
                raw.push(ch)?;
            } else {
                raw.emit(&mut previous, analyzer, control, &mut emit)?;
            }
        }
        Ok(())
    })?;
    raw.emit(&mut previous, analyzer, control, &mut emit)?;
    Ok(bytes)
}

struct IdentifierBuffer {
    bytes: Vec<u8>,
    memory: Option<QueryMemoryLease>,
    limit: usize,
}

impl IdentifierBuffer {
    fn push(&mut self, ch: char) -> Result<()> {
        let required = checked_add(self.bytes.len(), ch.len_utf8())?;
        if required > self.limit {
            return Err(HawDBError::Execution(format!(
                "streamed analyzer minimum unit requires {required} bytes, exceeding {}",
                self.limit
            )));
        }
        if required > self.bytes.capacity() {
            let capacity = required.max(self.bytes.capacity().saturating_mul(2).min(self.limit));
            if let Some(memory) = &mut self.memory {
                reserve_capacity(&mut self.bytes, capacity, memory)?;
            } else {
                self.bytes
                    .try_reserve_exact(capacity - self.bytes.len())
                    .map_err(|error| {
                        HawDBError::Execution(format!("reserve streamed identifier: {error}"))
                    })?;
            }
        }
        self.bytes
            .extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
        Ok(())
    }

    fn emit(
        &mut self,
        previous: &mut Option<Term>,
        analyzer: &SearchAnalyzerLexicon,
        control: Control<'_>,
        emit: &mut impl FnMut(Term, TokenOccurrence) -> Result<()>,
    ) -> Result<()> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        let raw = std::str::from_utf8(&self.bytes).expect("identifier assembled from characters");
        let mut seen = Dedup::new();
        let mut in_phrase = true;
        let last = visit_raw_identifier(
            raw,
            previous.as_ref().map(Term::as_str),
            analyzer,
            control,
            |token, phrase| {
                if in_phrase != phrase {
                    seen.reset();
                    in_phrase = phrase;
                }
                if let Some(token) = seen.insert(token, control)? {
                    emit(
                        token,
                        if phrase {
                            TokenOccurrence::UniqueInField
                        } else {
                            TokenOccurrence::Repeated
                        },
                    )?;
                }
                Ok(())
            },
        )?;
        if let Some(last) = last {
            *previous = Some(last.materialize(control)?);
        }
        drop(seen);
        self.bytes.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
