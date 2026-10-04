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

use super::image::InodeId;
use super::{invalid, ModelCore};
use std::io::{self, Seek, Write};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct NativeTrace {
    core: Arc<ModelCore>,
    inode: InodeId,
    append: bool,
}

impl NativeTrace {
    pub(crate) fn open(
        core: Arc<ModelCore>,
        path: &Path,
        options: &std::fs::OpenOptions,
        mutable: [bool; 5],
        native_options: bool,
    ) -> io::Result<(std::fs::File, Self)> {
        if native_options {
            return Err(invalid(
                "custom native open flags are outside the captured fault model",
            ));
        }
        let relative = core.relative(path)?;
        let mut engine = core.lock()?;
        let existing = match engine.inode(&relative) {
            Ok(inode) => Some(inode),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let native_exists = match std::fs::symlink_metadata(path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(invalid("symlinks are outside the captured fault model"));
                }
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error),
        };
        if existing.is_some() != native_exists {
            return Err(invalid(
                "native open encountered an uncaptured namespace change",
            ));
        }
        let mut candidate = engine.clone();
        let inode = match existing {
            Some(inode) => inode,
            None if mutable[3] || mutable[4] => candidate.create_file(&relative)?,
            None => {
                // Preserve the real NotFound error of an ordinary open.
                return match options.open(path) {
                    Err(error) => Err(error),
                    Ok(_) => Err(invalid(
                        "native open created a file without a create option",
                    )),
                };
            }
        };
        // create_new rejects an existing name without truncating its inode.
        if mutable[2] && !(mutable[4] && existing.is_some()) {
            candidate.truncate(inode, 0)?;
        }
        let file = options.open(path)?;
        *engine = candidate;
        drop(engine);
        Ok((
            file,
            Self {
                core,
                inode,
                append: mutable[1],
            },
        ))
    }

    pub(crate) fn io<T>(&self, native: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        let _engine = self.core.lock()?;
        native()
    }

    pub(crate) fn sync(&self, native: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        self.core.mutate(
            |engine| {
                if engine.is_directory(self.inode) {
                    engine.sync_directory(self.inode)
                } else {
                    engine.sync_file(self.inode)
                }
            },
            native,
        )
    }

    pub(crate) fn truncate(
        &self,
        length: u64,
        native: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        self.core.mutate(
            |engine| engine.truncate(self.inode, length).map(|_| ()),
            native,
        )
    }

    pub(crate) fn write(&self, file: &std::fs::File, bytes: &[u8]) -> io::Result<usize> {
        self.write_with(file, bytes, || {
            let mut handle = file;
            handle.write(bytes)
        })
    }

    pub(crate) fn write_vectored(
        &self,
        file: &std::fs::File,
        buffers: &[io::IoSlice<'_>],
    ) -> io::Result<usize> {
        let length = buffers
            .iter()
            .try_fold(0_usize, |length, buffer| length.checked_add(buffer.len()))
            .ok_or_else(|| invalid("captured vectored-write length overflows"))?;
        if length > self.core.lock()?.maximum_write_length() {
            return Err(invalid(
                "captured vectored-write byte admission limit exceeded",
            ));
        }
        let bytes: Vec<u8> = buffers
            .iter()
            .flat_map(|buffer| buffer.iter().copied())
            .collect();
        self.write_with(file, &bytes, || {
            let mut handle = file;
            handle.write_vectored(buffers)
        })
    }

    fn write_with(
        &self,
        file: &std::fs::File,
        bytes: &[u8],
        native: impl FnOnce() -> io::Result<usize>,
    ) -> io::Result<usize> {
        let mut engine = self.core.lock()?;
        if bytes.is_empty() {
            return native();
        }
        let offset = if self.append {
            file.metadata()?.len()
        } else {
            let mut handle = file;
            handle.stream_position()?
        };
        let mut candidate = engine.clone();
        candidate.write(self.inode, offset, bytes)?;
        let written = native()?;
        if written == bytes.len() {
            *engine = candidate;
        } else if written > 0 {
            // Admission covered the full buffer, so any short prefix also fits.
            engine.write(self.inode, offset, &bytes[..written])?;
        }
        Ok(written)
    }
}
