use super::{checksum, dictionary, LexicalProjectionConfig, RemoveOnDrop};
use crate::error::{Result, SkeinError};
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    pub min_term: String,
    pub max_term: String,
    pub offset: u64,
    pub length: u64,
    pub checksum: u64,
    pub term_count: u64,
    pub posting_offset: u64,
    pub posting_bytes: u64,
    pub posting_count: u64,
}

impl Descriptor {
    pub(super) fn validate_dictionary(
        &self,
        dictionary: &dictionary::Dictionary<'_>,
        document_count: u64,
    ) -> Result<()> {
        let mut end = self.posting_offset;
        let mut count = 0u64;
        let mut postings = 0u64;
        let mut last_matches = false;
        dictionary
            .visit(|term, metadata| {
                if (count == 0 && term != self.min_term)
                    || metadata.posting_offset != end
                    || metadata.df > document_count
                {
                    return Err("dictionary metadata has invalid posting bounds");
                }
                super::doclist::Cursor::new(metadata)
                    .map_err(|_| "invalid dictionary doclist metadata")?;
                end = end
                    .checked_add(metadata.posting_bytes)
                    .ok_or("dictionary posting extent overflow")?;
                postings = postings
                    .checked_add(metadata.df)
                    .ok_or("dictionary DF overflow")?;
                count += 1;
                last_matches = term == self.max_term;
                Ok(())
            })
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        if !last_matches
            || count != self.term_count
            || postings != self.posting_count
            || Some(end) != self.posting_offset.checked_add(self.posting_bytes)
        {
            return Err(SkeinError::Storage(
                "dictionary metadata disagrees with its directory".to_string(),
            ));
        }
        Ok(())
    }
}

/// Shared requested capacity for document and dictionary directories.
#[derive(Clone)]
pub(super) struct DirectoryBudget {
    used: Rc<Cell<u64>>,
    limit: u64,
}

impl DirectoryBudget {
    pub(super) fn new(limit: u64) -> Self {
        Self {
            used: Rc::new(Cell::new(0)),
            limit,
        }
    }

    pub(super) fn admit<T>(&self, entries: &mut Vec<T>, key_bytes: usize) -> Result<()> {
        let invalid = || SkeinError::Storage("lexical directory budget exceeded".to_string());
        let base = self
            .used
            .get()
            .checked_add(key_bytes as u64)
            .ok_or_else(invalid)?;
        if base > self.limit {
            return Err(invalid());
        }
        let previous_capacity = entries.capacity();
        if entries.len() == previous_capacity {
            let available = (self.limit - base) / std::mem::size_of::<T>() as u64;
            let available = usize::try_from(available).unwrap_or(usize::MAX);
            let growth = previous_capacity.max(1).min(available);
            if growth == 0 {
                return Err(invalid());
            }
            entries.try_reserve_exact(growth).map_err(|_| invalid())?;
        }
        let allocation = (entries.capacity() - previous_capacity)
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(invalid)?;
        let next = base
            .checked_add(allocation as u64)
            .filter(|&bytes| bytes <= self.limit)
            .ok_or_else(invalid)?;
        self.used.set(next);
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct SpillBudget {
    used: Rc<Cell<u64>>,
    limit: u64,
}

impl SpillBudget {
    pub(super) fn new(used: u64, limit: u64) -> Self {
        Self {
            used: Rc::new(Cell::new(used)),
            limit,
        }
    }
    pub(super) fn charge(&self, bytes: u64) -> Result<()> {
        let next = self
            .used
            .get()
            .checked_add(bytes)
            .filter(|&next| next <= self.limit)
            .ok_or_else(|| {
                SkeinError::Storage("lexical temporary artifact spill budget exceeded".to_string())
            })?;
        self.used.set(next);
        Ok(())
    }
}

pub(super) fn limits(config: LexicalProjectionConfig) -> Result<dictionary::Limits> {
    Ok(dictionary::Limits {
        max_bytes: usize::try_from(config.max_block_bytes.get().min(64 * 1024)).map_err(|_| {
            SkeinError::Storage("dictionary block exceeds the address space".to_string())
        })?,
        max_terms: 1024,
        max_key_bytes: u32::try_from(config.max_term_bytes.get())
            .map_err(|_| SkeinError::Storage("dictionary term length exceeds u32".to_string()))?,
        max_builder_bytes: usize::try_from(config.dictionary_build_memory_bytes.get()).map_err(
            |_| {
                SkeinError::Storage(
                    "dictionary builder budget exceeds the address space".to_string(),
                )
            },
        )?,
        max_validation_bytes: usize::try_from(config.dictionary_validation_bytes.get()).map_err(
            |_| {
                SkeinError::Storage(
                    "dictionary validation budget exceeds the address space".to_string(),
                )
            },
        )?,
    })
}

pub(super) struct Writer {
    file: File,
    _guard: RemoveOnDrop,
    entries: Vec<(String, dictionary::Metadata)>,
    descriptors: Vec<Descriptor>,
    directory: DirectoryBudget,
    bytes: u64,
    limits: dictionary::Limits,
    spill: SpillBudget,
}

impl Writer {
    pub(super) fn new(
        path: &Path,
        config: LexicalProjectionConfig,
        spill: SpillBudget,
        directory: DirectoryBudget,
    ) -> Result<Self> {
        let limits = limits(config)?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            file,
            _guard: RemoveOnDrop::new(path.to_owned()),
            entries: Vec::new(),
            descriptors: Vec::new(),
            directory,
            bytes: 0,
            limits,
            spill,
        })
    }

    pub(super) fn push(&mut self, term: String, metadata: dictionary::Metadata) -> Result<()> {
        if self
            .entries
            .last()
            .map(|entry| entry.0.as_str())
            .or_else(|| self.descriptors.last().map(|entry| entry.max_term.as_str()))
            .is_some_and(|previous| previous >= term.as_str())
        {
            return Err(SkeinError::Storage(
                "dictionary terms are not strictly ordered".to_string(),
            ));
        }
        let entry = (term, metadata);
        dictionary::builder_reservation(std::slice::from_ref(&entry), self.limits)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        if self.entries.len() == self.limits.max_terms as usize {
            self.flush_with_held(entry.0.capacity())?;
        }
        self.entries.push(entry);
        if self
            .staged_limits(0)
            .and_then(|limits| {
                dictionary::builder_reservation(&self.entries, limits)
                    .map_err(|error| SkeinError::Storage(error.to_string()))
            })
            .is_err()
        {
            let last = self.entries.pop().unwrap();
            self.flush_with_held(last.0.capacity())?;
            self.entries.push(last);
            dictionary::builder_reservation(&self.entries, self.staged_limits(0)?)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.flush_with_held(0)
    }

    fn staged_limits(&self, held_key_bytes: usize) -> Result<dictionary::Limits> {
        let invalid = || SkeinError::Storage("dictionary staging budget exceeded".to_string());
        let staging = self
            .entries
            .capacity()
            .checked_mul(std::mem::size_of::<(String, dictionary::Metadata)>())
            .and_then(|bytes| bytes.checked_add(held_key_bytes))
            .ok_or_else(invalid)?;
        let staging = self.entries.iter().try_fold(staging, |bytes, (key, _)| {
            bytes.checked_add(key.capacity()).ok_or_else(invalid)
        })?;
        Ok(dictionary::Limits {
            max_builder_bytes: self
                .limits
                .max_builder_bytes
                .checked_sub(staging)
                .ok_or_else(invalid)?,
            ..self.limits
        })
    }

    fn flush_with_held(&mut self, held_key_bytes: usize) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        // All keys and the complete staging allocation remain live while any
        // recursive partition builds, including an incoming key held by push.
        let limits = self.staged_limits(held_key_bytes)?;
        let mut entries = std::mem::take(&mut self.entries);
        self.write_partition(&entries, limits)?;
        entries.clear();
        self.entries = entries;
        Ok(())
    }

    fn write_partition(
        &mut self,
        entries: &[(String, dictionary::Metadata)],
        limits: dictionary::Limits,
    ) -> Result<()> {
        let bytes = match dictionary::build(entries, limits, &mut || Ok(())) {
            Ok(bytes) => bytes,
            Err(_) if entries.len() > 1 => {
                // A bounded partition may compress poorly; split deterministically
                // instead of retaining or retrying a database-sized vocabulary.
                let middle = entries.len() / 2;
                self.write_partition(&entries[..middle], limits)?;
                return self.write_partition(&entries[middle..], limits);
            }
            Err(error) => {
                return Err(SkeinError::Storage(format!(
                    "lexical dictionary build failed: {error}"
                )))
            }
        };
        let first = &entries[0];
        let last = entries.last().unwrap();
        let end = last
            .1
            .posting_offset
            .checked_add(last.1.posting_bytes)
            .ok_or_else(|| {
                SkeinError::Storage("dictionary posting extent overflows".to_string())
            })?;
        let posting_count = entries
            .iter()
            .try_fold(0u64, |count, (_, metadata)| count.checked_add(metadata.df))
            .ok_or_else(|| SkeinError::Storage("dictionary posting count overflows".to_string()))?;
        let descriptor = Descriptor {
            min_term: first.0.clone(),
            max_term: last.0.clone(),
            offset: self.bytes,
            length: bytes.len() as u64,
            checksum: checksum(&bytes),
            term_count: entries.len() as u64,
            posting_offset: first.1.posting_offset,
            posting_bytes: end.checked_sub(first.1.posting_offset).ok_or_else(|| {
                SkeinError::Storage("dictionary postings are unordered".to_string())
            })?,
            posting_count,
        };
        self.directory.admit(
            &mut self.descriptors,
            descriptor
                .min_term
                .capacity()
                .saturating_add(descriptor.max_term.capacity()),
        )?;
        self.spill.charge(bytes.len() as u64)?;
        self.file.write_all(&bytes)?;
        self.bytes += bytes.len() as u64;
        self.descriptors.push(descriptor);
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        writer: &mut impl Write,
        offset: &mut u64,
    ) -> Result<Vec<Descriptor>> {
        self.flush()?;
        self.file.seek(SeekFrom::Start(0))?;
        let mut remaining = self.bytes;
        let mut buffer = [0u8; 8192];
        let start = *offset;
        while remaining > 0 {
            let count = remaining.min(buffer.len() as u64) as usize;
            self.file.read_exact(&mut buffer[..count])?;
            writer.write_all(&buffer[..count])?;
            remaining -= count as u64;
        }
        *offset = offset
            .checked_add(self.bytes)
            .ok_or_else(|| SkeinError::Storage("lexical artifact extent overflows".to_string()))?;
        for descriptor in &mut self.descriptors {
            descriptor.offset += start;
        }
        Ok(self.descriptors)
    }
}
