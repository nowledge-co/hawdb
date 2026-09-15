//! Pending block and retained directory ownership for one lexical build.

use super::{
    block_encoding, visit_merged_postings_with_progress, BlockDescriptor, Digest,
    LexicalProjectionConfig, Posting, Result, SkeinError, TermStatistics, ARTIFACT_HEADER,
    SPILL_IO_BUFFER_BYTES,
};
use crate::build_control::{checkpoint, CheckedWriter};
use crate::build_memory::{checked_add, grow_slots, path::OwnedPath, BuildMemory};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
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
            return Err(SkeinError::Storage(
                "lexical artifact builder is poisoned".into(),
            ));
        }
        checkpoint(&self.task)
    }

    pub(super) fn push_document(&mut self, id: &str, length: u32) -> Result<()> {
        self.check()?;
        let result = self.push_document_inner(id, length);
        self.failed = result.is_err();
        result
    }

    fn push_document_inner(&mut self, id: &str, length: u32) -> Result<()> {
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
        let descriptor = block_encoding::write_block_with_context(
            &mut self.writer,
            self.generation,
            self.next_block_id,
            self.offset,
            self.config.max_block_bytes.get(),
            entries,
            Some(&self.task),
        )?;
        self.commit_block(descriptor);
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
        self.merge_postings_with_progress(paths, config, None)
    }

    pub(super) fn merge_postings_with_progress(
        &mut self,
        paths: &[impl AsRef<Path>],
        config: LexicalProjectionConfig,
        progress: Option<&crate::build_memory::reserved::ReservedMemory>,
    ) -> Result<()> {
        self.check()?;
        let task = self.task.clone();
        let result =
            visit_merged_postings_with_progress(paths, config, progress, Some(&task), |posting| {
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
        match self.term_statistics.last_mut() {
            Some(statistics) if statistics.term.as_str() == posting.term.as_str() => {
                statistics.document_frequency = statistics
                    .document_frequency
                    .checked_add(1)
                    .ok_or_else(|| {
                        SkeinError::Storage("lexical term document frequency exceeds u64".into())
                    })?;
            }
            Some(statistics) if statistics.term.as_str() > posting.term.as_str() => {
                return Err(SkeinError::Storage(
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
        let bytes = posting.encoded_len();
        if !self.posting_pending.is_empty()
            && self.posting_pending_bytes.saturating_add(bytes)
                > self.config.target_block_bytes.get()
        {
            self.flush_postings()?;
        }
        grow_slots(&mut self.posting_pending, &mut self.posting_slots)?;
        self.posting_strings.grow(checked_add(
            posting.term.retained_clone_bytes(),
            posting.document_id.len(),
        )?)?;
        self.posting_pending.push(Posting {
            term: posting.term.clone_for_retention(),
            document_id: posting.document_id.clone(),
            term_frequency: posting.term_frequency,
            document_len: posting.document_len,
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
                .ok_or_else(|| SkeinError::Storage("lexical artifact length exceeds u64".into()))?;
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
