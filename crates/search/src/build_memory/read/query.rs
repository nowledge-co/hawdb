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

//! Serving reads borrow current execution independently of retained files.
use crate::lexical_projection::QueryContext;
use crate::{HawDBError, Result};
use hawdb_executor::QueryMemoryLease;
use hawdb_storage::file_io::{File, ReadAdmissionControl};
use std::io::{self, Read};
use std::num::NonZeroUsize;

pub(crate) struct QueryFileRange<'a> {
    pub(crate) file: &'a File,
    pub(crate) expected_length: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) remaining: u64,
    pub(crate) context: QueryContext<'a>,
    pub(crate) require_execution: bool,
}
impl Read for QueryFileRange<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self.remaining.min(output.len() as u64).min(8192) as usize;
        if count == 0 {
            return Ok(0);
        }
        let count = read_at(self, self.context, &mut output[..count], self.offset)
            .map_err(io::Error::other)?;
        self.offset = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("query range offset overflow"))?;
        self.remaining -= count as u64;
        Ok(count)
    }
}

fn read_at(
    range: &QueryFileRange<'_>,
    context: QueryContext<'_>,
    output: &mut [u8],
    offset: u64,
) -> Result<usize> {
    context.checkpoint()?;
    if range.require_execution && context.execution.is_none() {
        return Err(HawDBError::Execution(
            "admitted file range require borrowed query execution".into(),
        ));
    }
    let _io = context
        .task
        .acquire_io_wave(NonZeroUsize::MIN)
        .map_err(|error| HawDBError::Execution(error.to_string()))?;
    if range.require_execution && _io.is_none() {
        return Err(HawDBError::Execution(
            "admitted file range require query I/O admission".into(),
        ));
    }
    let mut control = Control {
        expected_length: range.expected_length,
        context,
        validation: None,
        error: None,
    };
    let result = range.file.with_admitted_read(&mut control, |reader| {
        if let Some(expected) = range.expected_length
            && reader.metadata()?.len() != expected
        {
            return Err(io::Error::other(HawDBError::Storage(
                "query range artifact length changed".into(),
            )));
        }
        reader.read_at(output, offset)
    });
    context.checkpoint()?;
    if let Some(error) = control.error.take() {
        return Err(error);
    }
    result.map(|(count, _)| count).map_err(io_error)
}

struct Control<'a> {
    expected_length: Option<u64>,
    context: QueryContext<'a>,
    validation: Option<QueryMemoryLease>,
    error: Option<HawDBError>,
}
impl ReadAdmissionControl for Control<'_> {
    fn admit_validation(&mut self, bytes: u64) -> io::Result<()> {
        let result = (|| {
            if self
                .expected_length
                .is_some_and(|expected| expected != bytes)
            {
                return Err(HawDBError::Storage(
                    "query range object length changed".into(),
                ));
            }
            self.validation = Some(self.context.memory.reserve(64 << 10)?);
            Ok(())
        })();
        result.map_err(|error| {
            self.error = Some(error);
            io::Error::other("query range read admission refused")
        })
    }
    fn checkpoint(&mut self) -> io::Result<()> {
        self.context.checkpoint().map_err(io::Error::other)
    }
}

pub(crate) fn io_error(error: io::Error) -> HawDBError {
    if error
        .get_ref()
        .is_some_and(|inner| inner.is::<HawDBError>())
    {
        return *error
            .into_inner()
            .expect("checked I/O payload")
            .downcast::<HawDBError>()
            .expect("checked error type");
    }
    HawDBError::from(error)
}
