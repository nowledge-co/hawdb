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

//! Bounded head reads whose length is learned under admitted native custody.
use super::*;
use hawdb_storage::file_io::{File, ReadAdmissionControl};
use std::io::{self, Read};
use std::path::Path;

impl<'execution> ReadAdmission<'execution> {
    pub(crate) fn read_unbound(&self, path: &Path, maximum: u64) -> Result<ReadBytes<'execution>> {
        crate::build_control::checkpoint(self.task)?;
        let _native = self.scratch(checked_add(
            super::super::reserved::native_path::bytes(path)?,
            4096,
        )?)?;
        let _io = self
            .io()
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let file = File::open(path)?;
        #[cfg(test)]
        if let Some(callback) = AFTER_FILE_OPEN.with(|slot| slot.replace(None)) {
            callback();
        }
        crate::build_control::checkpoint(self.task)?;
        let mut control = Control {
            admission: self,
            maximum,
            validation: None,
            error: None,
        };
        let result = file.with_admitted_read(&mut control, |reader| {
            Ok((|| -> Result<ReadBytes<'execution>> {
                let length = reader.metadata()?.len();
                if length > maximum {
                    return Err(HawDBError::Execution(
                        "reader head exceeds its encoded/physical limit".into(),
                    ));
                }
                let count = usize::try_from(length).map_err(|_| {
                    HawDBError::Execution("reader head exceeds address space".into())
                })?;
                let memory = self.scratch(count)?;
                let mut output = ReadBytes {
                    _execution: std::marker::PhantomData,
                    value: Vec::new(),
                    _memory: Some(memory),
                };
                output.value.try_reserve_exact(count).map_err(|error| {
                    HawDBError::Execution(format!("reader head allocation failed: {error}"))
                })?;
                if output.value.capacity() > count {
                    return Err(HawDBError::Execution(
                        "reader head exceeded admitted capacity".into(),
                    ));
                }
                output.value.resize(count, 0);
                reader.read_exact(&mut output.value).map_err(io_error)?;
                if reader.metadata()?.len() != length {
                    return Err(HawDBError::Storage(
                        "reader head changed length during read".into(),
                    ));
                }
                Ok(output)
            })())
        });
        crate::build_control::checkpoint(self.task)?;
        if let Some(error) = control.error.take() {
            return Err(error);
        }
        result.map_err(io_error)?.0
    }
}

struct Control<'a, 'execution> {
    admission: &'a ReadAdmission<'execution>,
    maximum: u64,
    validation: Option<QueryMemoryLease>,
    error: Option<HawDBError>,
}
impl ReadAdmissionControl for Control<'_, '_> {
    fn admit_validation(&mut self, bytes: u64) -> io::Result<()> {
        let result = (|| {
            self.admission.require_read_limit(bytes, self.maximum)?;
            self.validation = Some(self.admission.scratch(64 << 10)?);
            Ok(())
        })();
        result.map_err(|error| {
            self.error = Some(error);
            io::Error::other("reader head validation refused")
        })
    }
    fn checkpoint(&mut self) -> io::Result<()> {
        crate::build_control::checkpoint(self.admission.task).map_err(io::Error::other)
    }
}
