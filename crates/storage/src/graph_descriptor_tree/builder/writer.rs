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

use super::{BufWriter, File};
use std::ops::{Deref, DerefMut};

/// A private abort cannot perform implicit I/O after its admission closes.
/// Successful builders explicitly flush and synchronize under their I/O wave.
/// Ordinary public builders retain the standard buffered-writer drop contract.
pub(super) struct BufferedWriter {
    writer: Option<BufWriter<File>>,
    discard_on_drop: bool,
}

impl BufferedWriter {
    pub(super) fn new(capacity: usize, file: File, discard_on_drop: bool) -> Self {
        Self {
            writer: Some(BufWriter::with_capacity(capacity, file)),
            discard_on_drop,
        }
    }
}

impl Deref for BufferedWriter {
    type Target = BufWriter<File>;

    fn deref(&self) -> &Self::Target {
        self.writer
            .as_ref()
            .expect("descriptor writer is present until drop")
    }
}

impl DerefMut for BufferedWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.writer
            .as_mut()
            .expect("descriptor writer is present until drop")
    }
}

impl Drop for BufferedWriter {
    fn drop(&mut self) {
        if self.discard_on_drop {
            let writer = self.writer.take().expect("descriptor writer is present");
            // into_parts never flushes, including a previously failed write.
            // Destroy buffered data and close the file before the enclosing
            // builder releases its allocation inventory.
            let (file, buffer) = writer.into_parts();
            drop(buffer);
            drop(file);
        }
    }
}

#[cfg(all(test, unix))]
mod tests;
