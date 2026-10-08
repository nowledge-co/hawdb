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

//! Shared bounded source and budgets for verified-body fixtures.

use crate::{SearchDocumentHeader, SearchOutOfCoreGenerationBuildOptions};
#[cfg(feature = "full-text-search")]
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};
use std::collections::BTreeMap;
#[cfg(feature = "full-text-search")]
use std::io::{self, Read};
use std::num::{NonZeroU64, NonZeroUsize};

#[cfg(feature = "full-text-search")]
pub(crate) struct Generated {
    pub(crate) remaining: u64,
    pub(crate) position: usize,
}

#[cfg(feature = "full-text-search")]
impl Read for Generated {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        // Many complete repeated terms, separated by padding. The producer and
        // consumer own only bounded buffers even in the 128-MiB case.
        const WORDS: &[u8] = b"streamed graph document ";
        let count = self.remaining.min(output.len() as u64) as usize;
        for byte in &mut output[..count] {
            *byte = WORDS.get(self.position % 128).copied().unwrap_or(b' ');
            self.position += 1;
        }
        self.remaining -= count as u64;
        Ok(count)
    }
}

pub(crate) fn header() -> SearchDocumentHeader {
    SearchDocumentHeader {
        id: "a".into(),
        title: "title".into(),
        embedding: None,
        metadata: BTreeMap::from([("kind".into(), "memo".into())]),
    }
}

#[cfg(feature = "full-text-search")]
pub(crate) fn task(bytes: u64) -> RuntimeTaskContext {
    RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(bytes, 0))
}

pub(crate) fn options(bytes: u64) -> SearchOutOfCoreGenerationBuildOptions {
    SearchOutOfCoreGenerationBuildOptions {
        max_record_bytes: NonZeroU64::new(bytes * 2 + 1024).unwrap(),
        max_segment_uncompressed_bytes: NonZeroU64::new(bytes * 2 + 4096).unwrap(),
        lexical_max_document_source_bytes: NonZeroU64::new(bytes + 1024).unwrap(),
        lexical_max_document_tokens: NonZeroUsize::new(8_000_000).unwrap(),
        lexical_build_memory_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
        ..Default::default()
    }
}
