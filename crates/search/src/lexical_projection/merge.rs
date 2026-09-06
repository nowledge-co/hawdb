//! Bounded k-way run iteration. A yielded posting remains owned by the cursor.

use super::{read_u32, LexicalProjectionConfig, Posting, RUN_HEADER};
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, checked_mul, BuildMemory, SPOOL_BUFFER_BYTES};
use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::mem::size_of;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(super) struct AdmittedPosting {
    pub(super) posting: Posting,
    _memory: QueryMemoryLease,
}

pub(super) struct RunReader {
    reader: BufReader<File>,
    config: LexicalProjectionConfig,
    memory: BuildMemory,
    _buffer: QueryMemoryLease,
}

impl RunReader {
    pub(super) fn open(
        path: &Path,
        config: LexicalProjectionConfig,
        memory: BuildMemory,
    ) -> Result<Self> {
        let buffer = memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut reader = BufReader::with_capacity(SPOOL_BUFFER_BYTES, File::open(path)?);
        let mut header = [0; 8];
        reader.read_exact(&mut header)?;
        if &header != RUN_HEADER {
            return Err(SkeinError::Storage(
                "lexical spill run header mismatch".to_string(),
            ));
        }
        Ok(Self {
            reader,
            config,
            memory,
            _buffer: buffer,
        })
    }

    pub(super) fn next(&mut self) -> Result<Option<AdmittedPosting>> {
        let mut length = [0; 4];
        loop {
            match self.reader.read(&mut length) {
                Ok(0) => return Ok(None),
                Ok(count) => {
                    self.reader.read_exact(&mut length[count..])?;
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        let length = u32::from_le_bytes(length) as usize;
        if length as u64 > self.config.max_term_bytes.get() {
            return Err(SkeinError::Storage(
                "lexical spill string exceeds its admitted length".to_string(),
            ));
        }
        let memory = self.memory.retained.reserve(length)?;
        #[cfg(test)]
        evidence::record();
        let mut bytes = vec![0; length];
        self.reader.read_exact(&mut bytes)?;
        let term =
            String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))?;
        let mut ordinal = [0; 8];
        self.reader.read_exact(&mut ordinal)?;
        let term_frequency = read_u32(&mut self.reader)?;
        if term.is_empty() || term_frequency == 0 {
            return Err(SkeinError::Storage(
                "invalid lexical spill posting".to_string(),
            ));
        }
        Ok(Some(AdmittedPosting {
            posting: Posting {
                term,
                ordinal: u64::from_le_bytes(ordinal),
                term_frequency,
            },
            _memory: memory,
        }))
    }
}

struct Head {
    value: AdmittedPosting,
    source: usize,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.value.posting, self.source).cmp(&(&other.value.posting, other.source))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Head {}

pub(super) struct MergedPostings {
    readers: Vec<Option<RunReader>>,
    heap: BinaryHeap<Reverse<Head>>,
    current: Option<AdmittedPosting>,
    pending: Option<usize>,
    task: RuntimeTaskContext,
    failed: bool,
    _slots: QueryMemoryLease,
}

impl MergedPostings {
    pub(super) fn new(
        paths: &[PathBuf],
        config: LexicalProjectionConfig,
        memory: BuildMemory,
        task: RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(&task)?;
        if paths.len() > config.max_merge_fan_in.get() {
            return Err(SkeinError::Storage(
                "lexical run count exceeds merge fan-in".to_string(),
            ));
        }
        let slots = memory.retained.reserve(Self::slot_bytes(paths.len())?)?;
        let mut cursor = Self {
            readers: Vec::new(),
            heap: BinaryHeap::new(),
            current: None,
            pending: None,
            task,
            failed: false,
            _slots: slots,
        };
        cursor
            .readers
            .try_reserve_exact(paths.len())
            .map_err(allocation)?;
        cursor
            .heap
            .try_reserve_exact(paths.len())
            .map_err(allocation)?;
        for (source, path) in paths.iter().enumerate() {
            checkpoint(&cursor.task)?;
            let reader = RunReader::open(path, config, memory.clone())?;
            cursor.readers.push(Some(reader));
            cursor.refill(source)?;
        }
        Ok(cursor)
    }

    fn slot_bytes(count: usize) -> Result<usize> {
        checked_mul(
            count,
            checked_add(size_of::<Option<RunReader>>(), size_of::<Head>())?,
        )
    }

    fn refill(&mut self, source: usize) -> Result<()> {
        checkpoint(&self.task)?;
        let Some(reader) = &mut self.readers[source] else {
            return Ok(());
        };
        if let Some(value) = reader.next()? {
            // One head per source; the popped source is refilled only once.
            debug_assert!(self.heap.len() < self.heap.capacity());
            self.heap.push(Reverse(Head { value, source }));
        } else {
            // Release exhausted handles and buffers before the remaining runs.
            self.readers[source] = None;
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Result<Option<&Posting>> {
        if self.failed {
            return Err(SkeinError::Storage(
                "lexical merge already failed".to_string(),
            ));
        }
        match self.advance() {
            Ok(true) => Ok(self.current.as_ref().map(|current| &current.posting)),
            Ok(false) => Ok(None),
            Err(error) => {
                // A failed decoder may have consumed a record prefix. Never
                // silently resume at that partially consumed byte position.
                self.failed = true;
                Err(error)
            }
        }
    }

    fn advance(&mut self) -> Result<bool> {
        loop {
            checkpoint(&self.task)?;
            // Defer the next read until the consumer requests another posting.
            // Consumer failure/cancellation must not trigger speculative refill.
            if let Some(source) = self.pending.take() {
                self.refill(source)?;
            }
            let Some(Reverse(head)) = self.heap.pop() else {
                self.current = None;
                self.readers = Vec::new();
                self.heap = BinaryHeap::new();
                self._slots.reset();
                return Ok(false);
            };
            self.pending = Some(head.source);
            if let Some(current) = &self.current {
                match head.value.posting.cmp(&current.posting) {
                    Ordering::Equal => continue,
                    Ordering::Less => {
                        return Err(SkeinError::Storage(
                            "lexical spill postings are not ordered".to_string(),
                        ));
                    }
                    Ordering::Greater => {}
                }
            }
            self.current = Some(head.value);
            return Ok(true);
        }
    }
}

fn allocation(error: std::collections::TryReserveError) -> SkeinError {
    SkeinError::Execution(format!("lexical merge allocation failed: {error}"))
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! {
        static DECODES: Cell<usize> = const { Cell::new(0) };
    }
    pub(super) fn record() {
        DECODES.with(|count| count.set(count.get() + 1));
    }
    pub(crate) fn take() -> usize {
        DECODES.with(|count| count.replace(0))
    }
}
