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

//! Pending block and retained directory ownership for one lexical build.

use super::posting_codec;
use super::{
    block_encoding, visit_merged_postings_with_control, BlockDescriptor, Digest, HawDBError,
    LexicalProjectionConfig, Posting, Result, SpillControl, TermStatistics, ARTIFACT_HEADER,
    SPILL_IO_BUFFER_BYTES,
};
use crate::build_control::{checkpoint, CheckedWriter};
use crate::build_memory::{checked_add, grow_slots, path::OwnedPath, BuildMemory};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

#[cfg(test)]
mod tests;

pub(super) struct ArtifactBuilder {
    writer: BufWriter<File>,
    path: OwnedPath,
    generation: u64,
    config: LexicalProjectionConfig,
    offset: u64,
    next_block_id: u64,
    document_pending: Vec<(String, u32)>,
    document_pending_bytes: u64,
    document_count: u64,
    posting_pending: Vec<Posting>,
    posting_pending_bytes: u64,
    posting_count: u64,
    term_statistics: Vec<TermStatistics>,
    blocks: Vec<BlockDescriptor>,
    failed: bool,
    task: RuntimeTaskContext,
    memory: BuildMemory,
    // Payload fields precede every capacity owner, including on error.
    _writer_memory: QueryMemoryLease,
    document_slots: QueryMemoryLease,
    document_strings: QueryMemoryLease,
    posting_slots: QueryMemoryLease,
    posting_strings: QueryMemoryLease,
    directory_memory: DirectoryMemory,
}

pub(super) struct ArtifactSummary {
    pub(super) len: u64,
    pub(super) checksum: u64,
    pub(super) posting_count: u64,
    pub(super) term_statistics: Vec<TermStatistics>,
    pub(super) blocks: Vec<BlockDescriptor>,
    pub(super) memory: DirectoryMemory,
}

pub(super) struct DirectoryMemory {
    statistics_slots: QueryMemoryLease,
    statistics_strings: QueryMemoryLease,
    block_slots: QueryMemoryLease,
    block_strings: QueryMemoryLease,
}

impl DirectoryMemory {
    fn new(memory: &BuildMemory) -> Result<Self> {
        Ok(Self {
            statistics_slots: memory.retained.reserve(0)?,
            statistics_strings: memory.retained.reserve(0)?,
            block_slots: memory.retained.reserve(0)?,
            block_strings: memory.retained.reserve(0)?,
        })
    }
}

impl ArtifactBuilder {
    #[cfg(test)]
    pub(super) fn new(
        path: &Path,
        generation: u64,
        config: LexicalProjectionConfig,
    ) -> Result<Self> {
        let task = RuntimeTaskContext::default();
        Self::new_with_context(path, generation, config, BuildMemory::new(&task)?, task)
    }

    pub(super) fn new_with_context(
        path: &Path,
        generation: u64,
        config: LexicalProjectionConfig,
        memory: BuildMemory,
        task: RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(&task)?;
        let path = OwnedPath::copy(path, &memory, &task)?;
        let writer_memory = memory.spool.reserve(SPILL_IO_BUFFER_BYTES)?;
        let mut writer = BufWriter::with_capacity(SPILL_IO_BUFFER_BYTES, File::create(&path)?);
        let mut output = CheckedWriter::new(&mut writer, Some(&task));
        output.write_all(ARTIFACT_HEADER)?;
        output.write_all(&generation.to_le_bytes())?;
        Ok(Self {
            writer,
            path,
            generation,
            config,
            offset: ARTIFACT_HEADER.len() as u64 + 8,
            next_block_id: 0,
            document_pending: Vec::new(),
            document_pending_bytes: 0,
            document_count: 0,
            posting_pending: Vec::new(),
            posting_pending_bytes: 0,
            posting_count: 0,
            term_statistics: Vec::new(),
            blocks: Vec::new(),
            failed: false,
            document_slots: memory.retained.reserve(0)?,
            document_strings: memory.retained.reserve(0)?,
            posting_slots: memory.retained.reserve(0)?,
            posting_strings: memory.retained.reserve(0)?,
            directory_memory: DirectoryMemory::new(&memory)?,
            task,
            memory,
            _writer_memory: writer_memory,
        })
    }

    fn check(&self) -> Result<()> {
        if self.failed {
            return Err(HawDBError::Storage(
                "lexical artifact builder is poisoned".into(),
            ));
        }
        checkpoint(&self.task)
    }

    pub(super) fn push_document(&mut self, ordinal: u64, id: &str, length: u32) -> Result<()> {
        self.check()?;
        let result = self.push_document_inner(ordinal, id, length);
        self.failed = result.is_err();
        result
    }

    fn push_document_inner(&mut self, ordinal: u64, id: &str, length: u32) -> Result<()> {
        let expected_ordinal = self
            .document_count
            .checked_add(self.document_pending.len() as u64)
            .ok_or_else(|| {
                HawDBError::Storage("lexical document ordinal range overflows".into())
            })?;
        if ordinal != expected_ordinal {
            return Err(HawDBError::Storage(format!(
                "lexical document ordinal {ordinal} does not follow {}",
                expected_ordinal
            )));
        }
        let bytes = 4u64.saturating_add(id.len() as u64).saturating_add(4);
        if !self.document_pending.is_empty()
            && self.document_pending_bytes.saturating_add(bytes)
                > self.config.target_block_bytes.get()
        {
            self.flush_documents()?;
        }
        grow_slots(&mut self.document_pending, &mut self.document_slots)?;
        self.document_strings.grow(id.len())?;
        self.document_pending.push((id.to_owned(), length));
        self.document_pending_bytes = self.document_pending_bytes.saturating_add(bytes);
        Ok(())
    }

    pub(super) fn finish_documents(&mut self) -> Result<()> {
        self.check()?;
        let result = self.flush_documents();
        self.failed = result.is_err();
        result
    }

    fn flush_documents(&mut self) -> Result<()> {
        if self.document_pending.is_empty() {
            return Ok(());
        }
        grow_slots(&mut self.blocks, &mut self.directory_memory.block_slots)?;
        let entries = block_encoding::Entries::Documents(&self.document_pending);
        let (min, max) = entries.bounds()?;
        self.directory_memory
            .block_strings
            .grow(checked_add(min.len(), max.len())?)?;
        let mut descriptor = block_encoding::write_block_with_context(
            &mut self.writer,
            self.generation,
            self.next_block_id,
            self.offset,
            self.config.max_block_bytes.get(),
            entries,
            Some(&self.task),
        )?;
        descriptor.ordinal_start = self.document_count;
        self.commit_block(descriptor);
        self.document_count = self
            .document_count
            .checked_add(self.document_pending.len() as u64)
            .ok_or_else(|| {
                HawDBError::Storage("lexical document ordinal range overflows".into())
            })?;
        self.document_pending.clear();
        self.document_strings.shrink(self.document_strings.bytes());
        self.document_pending_bytes = 0;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn merge_postings(
        &mut self,
        paths: &[PathBuf],
        config: LexicalProjectionConfig,
    ) -> Result<()> {
        self.merge_postings_with_control(paths, config, &SpillControl::fixture(None, None))
    }

    pub(super) fn merge_postings_with_control(
        &mut self,
        paths: &[impl AsRef<Path>],
        config: LexicalProjectionConfig,
        control: &SpillControl,
    ) -> Result<()> {
        self.check()?;
        let control = control.with_task(self.task.clone());
        let result = visit_merged_postings_with_control(paths, config, &control, |posting| {
            self.push_posting(posting)
        })
        .and_then(|()| self.flush_postings());
        self.failed = result.is_err();
        result
    }

    pub(super) fn push_posting(&mut self, posting: &Posting) -> Result<()> {
        self.check()?;
        let result = self.push_posting_inner(posting);
        self.failed = result.is_err();
        result
    }

    fn push_posting_inner(&mut self, posting: &Posting) -> Result<()> {
        if self.posting_pending.len() == posting_codec::BLOCK_LEN
            || self
                .posting_pending
                .last()
                .is_some_and(|previous| previous.term != posting.term)
        {
            self.flush_postings()?;
        }
        match self.term_statistics.last_mut() {
            Some(statistics) if statistics.term.as_str() == posting.term.as_str() => {
                statistics.document_frequency = statistics
                    .document_frequency
                    .checked_add(1)
                    .ok_or_else(|| {
                        HawDBError::Storage("lexical term document frequency exceeds u64".into())
                    })?;
            }
            Some(statistics) if statistics.term.as_str() > posting.term.as_str() => {
                return Err(HawDBError::Storage(
                    "lexical merge produced unordered term statistics".into(),
                ));
            }
            _ => {
                grow_slots(
                    &mut self.term_statistics,
                    &mut self.directory_memory.statistics_slots,
                )?;
                self.directory_memory
                    .statistics_strings
                    .grow(posting.term.len())?;
                self.term_statistics.push(TermStatistics {
                    term: posting.term.as_str().to_owned(),
                    document_frequency: 1,
                });
            }
        }
        let bytes = Posting::resident_bytes(&posting.term);
        grow_slots(&mut self.posting_pending, &mut self.posting_slots)?;
        self.posting_strings
            .grow(posting.term.retained_clone_bytes())?;
        self.posting_pending.push(Posting {
            term: posting.term.clone_for_retention(),
            ordinal: posting.ordinal,
            term_frequency: posting.term_frequency,
        });
        self.posting_pending_bytes = self.posting_pending_bytes.saturating_add(bytes);
        Ok(())
    }

    fn flush_postings(&mut self) -> Result<()> {
        if self.posting_pending.is_empty() {
            return Ok(());
        }
        grow_slots(&mut self.blocks, &mut self.directory_memory.block_slots)?;
        let entries = block_encoding::Entries::Postings(&self.posting_pending);
        let (min, max) = entries.bounds()?;
        self.directory_memory
            .block_strings
            .grow(checked_add(min.len(), max.len())?)?;
        let descriptor = block_encoding::write_block_with_context(
            &mut self.writer,
            self.generation,
            self.next_block_id,
            self.offset,
            self.config.max_block_bytes.get(),
            entries,
            Some(&self.task),
        )?;
        self.posting_count = self
            .posting_count
            .saturating_add(self.posting_pending.len() as u64);
        self.commit_block(descriptor);
        self.posting_pending.clear();
        self.posting_strings.shrink(self.posting_strings.bytes());
        self.posting_pending_bytes = 0;
        Ok(())
    }

    fn commit_block(&mut self, descriptor: BlockDescriptor) {
        // Encoding checked these additions; directory storage was admitted before I/O.
        self.offset += descriptor.length;
        self.next_block_id += 1;
        self.blocks.push(descriptor);
    }

    pub(super) fn finish(mut self) -> Result<ArtifactSummary> {
        self.check()?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        checkpoint(&self.task)?;
        // Retain the writer buffer through this independently admitted digest pass.
        let _scratch_memory = self.memory.spool.reserve(SPILL_IO_BUFFER_BYTES)?;
        let mut scratch = vec![0u8; SPILL_IO_BUFFER_BYTES];
        let mut file = File::open(&self.path)?;
        let mut digest = Digest::new();
        let mut length = 0u64;
        loop {
            checkpoint(&self.task)?;
            let count = file.read(&mut scratch)?;
            if count == 0 {
                break;
            }
            digest.update(&scratch[..count]);
            length = length
                .checked_add(count as u64)
                .ok_or_else(|| HawDBError::Storage("lexical artifact length exceeds u64".into()))?;
        }
        checkpoint(&self.task)?;
        Ok(ArtifactSummary {
            len: length,
            checksum: digest.finish(),
            posting_count: self.posting_count,
            term_statistics: self.term_statistics,
            blocks: self.blocks,
            memory: self.directory_memory,
        })
    }
}
