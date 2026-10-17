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

//! Scoped reads keep a managed native lease through validation and consumption.

use super::{File, FileBacking, Metadata};
use std::io::{self, Read};

/// Internal cross-crate admission for managed immutable-object reads.
/// Callers retain their validation capacity and I/O admission until the scoped
/// read returns. Validation uses at most 64 KiB between checkpoints.
#[doc(hidden)]
pub trait ReadAdmissionControl {
    fn admit_validation(&mut self, bytes: u64) -> io::Result<()>;
    fn checkpoint(&mut self) -> io::Result<()>;
}

/// A sequential view of one leased native file. It exposes neither raw native
/// handles nor cloning, and its cursor is independent of other cached readers.
#[doc(hidden)]
pub struct AdmittedFileRead<'a> {
    file: &'a File,
    control: &'a mut dyn ReadAdmissionControl,
    length: u64,
    offset: u64,
}

impl AdmittedFileRead<'_> {
    /// One bounded positioned read without exposing or cloning the native file.
    pub fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        self.control.checkpoint()?;
        let length = self
            .length
            .saturating_sub(offset)
            .min(buffer.len() as u64)
            .min(8192) as usize;
        if length == 0 {
            return Ok(0);
        }
        self.file.with_native(|native| {
            crate::io::read_native_at_once(native, &mut buffer[..length], offset)
        })
    }

    pub fn metadata(&mut self) -> io::Result<Metadata> {
        self.control.checkpoint()?;
        self.file.metadata()
    }
}

impl Read for AdmittedFileRead<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.read_at(buffer, self.offset)?;
        self.offset += read as u64;
        Ok(read)
    }
}

impl File {
    /// Pin one admitted native lease for immutable construction APIs that
    /// perform multiple reads or create a mapping. Native handles stay private;
    /// clones share the counted lease. Dropping all pinned copies restores cache
    /// eviction. Long-lived ordinary logical readers should remain unpinned.
    #[doc(hidden)]
    pub fn pin_admitted(mut self, control: &mut impl ReadAdmissionControl) -> io::Result<Self> {
        control.checkpoint()?;
        if let FileBacking::Immutable(file) = &mut self.backing
            && file.pinned.is_none()
        {
            let (lease, _) = file
                .handles
                .get_controlled(&file.binding, &file.context, control)?;
            file.pinned = Some(lease);
        }
        control.checkpoint()?;
        Ok(self)
    }

    /// Admit cold identity validation and consume the same native lease.
    /// A warm logical file performs no identity payload read. Native descriptors,
    /// trace hooks and cache leases stay managed for the complete callback.
    #[doc(hidden)]
    pub fn with_admitted_read<T>(
        &self,
        control: &mut impl ReadAdmissionControl,
        operation: impl FnOnce(&mut AdmittedFileRead<'_>) -> io::Result<T>,
    ) -> io::Result<(T, u64)> {
        control.checkpoint()?;
        let (lease, validation_bytes) = match &self.backing {
            FileBacking::Native(_) => (None, 0),
            FileBacking::Immutable(file) => {
                let (lease, bytes) = match &file.pinned {
                    Some(lease) => (lease.clone(), 0),
                    None => file
                        .handles
                        .get_controlled(&file.binding, &file.context, control)?,
                };
                (Some(lease), bytes)
            }
        };
        control.checkpoint()?;
        let file = lease.as_deref().unwrap_or(self);
        let length = file.metadata()?.len();
        let result = operation(&mut AdmittedFileRead {
            file,
            control,
            length,
            offset: 0,
        });
        control.checkpoint()?;
        result.map(|value| (value, validation_bytes))
    }
}
