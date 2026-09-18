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

use super::{
    encode_relational_key, IndexLeafEntry, RelationalIndexShadowConfig, RelationalIndexShadowError,
    TreeWriter,
};
use crate::relational::{
    column_positions, index_includes_key, row_key, RelationalIndexDefinition, RelationalIndexRole,
    RelationalIndexRowSource, RelationalTableSchema,
};
use hawdb_integrity::Crc32cHasher;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};

const RUN_HEADER: &[u8; 8] = b"SKRIDXR1";
const RUN_ENTRY_FIXED_BYTES: u64 = 8 + 4;
const MAX_RUN_IO_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct IndexSortReport {
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_memory_bytes: usize,
}

pub(super) struct IndexBuildInput<'a> {
    pub source: &'a dyn RelationalIndexRowSource,
    pub table: &'a str,
    pub schema: &'a RelationalTableSchema,
    pub definition: &'a RelationalIndexDefinition,
    pub spill_prefix: &'a Path,
    pub generation: u64,
    pub root_ordinal: usize,
    pub config: RelationalIndexShadowConfig,
    pub max_spill_bytes: u64,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EncodedIndexEntry {
    index_key: Vec<u8>,
    row_id: Vec<u8>,
}

impl EncodedIndexEntry {
    fn resident_bytes(&self) -> Result<usize, RelationalIndexShadowError> {
        size_of::<Self>()
            .checked_add(self.index_key.len())
            .and_then(|bytes| bytes.checked_add(self.row_id.len()))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "relational index sort resident-byte accounting overflow".to_string(),
                )
            })
    }

    fn run_encoded_len(&self) -> Result<u64, RelationalIndexShadowError> {
        RUN_ENTRY_FIXED_BYTES
            .checked_add(self.index_key.len() as u64)
            .and_then(|bytes| bytes.checked_add(self.row_id.len() as u64))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "relational index spill entry length overflow".to_string(),
                )
            })
    }
}

pub(super) fn write_index_from_rows(
    tree: &mut TreeWriter<'_>,
    input: IndexBuildInput<'_>,
) -> Result<IndexSortReport, RelationalIndexShadowError> {
    let IndexBuildInput {
        source,
        table,
        schema,
        definition,
        spill_prefix,
        generation,
        root_ordinal,
        config,
        max_spill_bytes,
    } = input;
    debug_assert_ne!(definition.role, RelationalIndexRole::Primary);
    let positions = column_positions(schema, &definition.columns).map_err(|error| {
        RelationalIndexShadowError::Corrupt(format!(
            "cannot resolve columns for relational index {table}.{}: {error}",
            definition.name
        ))
    })?;
    let mut sorter = IndexEntrySorter::new(
        spill_prefix,
        generation,
        root_ordinal,
        config,
        max_spill_bytes,
    );
    source.visit_rows(table, &mut |primary_key, row| {
        let index_key = row_key(row, &positions);
        if !index_includes_key(definition, &index_key) {
            return Ok(());
        }
        sorter.push(EncodedIndexEntry {
            index_key: encode_relational_key(&index_key)?,
            row_id: encode_relational_key(primary_key)?,
        })
    })?;
    sorter.finish()?.write(tree, definition.role)
}

struct IndexEntrySorter {
    config: RelationalIndexShadowConfig,
    chunk: Vec<EncodedIndexEntry>,
    chunk_bytes: usize,
    peak_memory_bytes: usize,
    runs: SpillRuns,
}

impl IndexEntrySorter {
    fn new(
        spill_prefix: &Path,
        generation: u64,
        root_ordinal: usize,
        config: RelationalIndexShadowConfig,
        max_spill_bytes: u64,
    ) -> Self {
        Self {
            config,
            chunk: Vec::new(),
            chunk_bytes: 0,
            peak_memory_bytes: 0,
            runs: SpillRuns::new(
                spill_prefix,
                generation,
                root_ordinal,
                config,
                max_spill_bytes,
            ),
        }
    }

    fn push(&mut self, entry: EncodedIndexEntry) -> Result<(), RelationalIndexShadowError> {
        validate_entry(&entry, self.config)?;
        let entry_bytes = entry.resident_bytes()?;
        let io_buffer_bytes = spill_writer_buffer_bytes(self.config);
        let entry_budget = self
            .config
            .max_sort_memory_bytes
            .get()
            .saturating_sub(io_buffer_bytes)
            / 2;
        if entry_bytes > entry_budget {
            return Err(RelationalIndexShadowError::Admission(format!(
                "one relational index sort entry uses {entry_bytes} resident bytes, exceeding limit {}",
                entry_budget
            )));
        }
        if !self.chunk.is_empty() && self.chunk_bytes.saturating_add(entry_bytes) > entry_budget {
            self.runs.spill(&mut self.chunk)?;
            self.chunk_bytes = 0;
        }
        self.chunk_bytes = self.chunk_bytes.checked_add(entry_bytes).ok_or_else(|| {
            RelationalIndexShadowError::Admission(
                "relational index sort resident-byte accounting overflow".to_string(),
            )
        })?;
        self.peak_memory_bytes = self.peak_memory_bytes.max(self.chunk_bytes);
        self.chunk.push(entry);
        Ok(())
    }

    fn finish(mut self) -> Result<IndexEntrySource, RelationalIndexShadowError> {
        if self.runs.paths.is_empty() {
            self.chunk.sort_unstable();
            return Ok(IndexEntrySource::Memory {
                entries: self.chunk,
                report: IndexSortReport {
                    peak_memory_bytes: self.peak_memory_bytes.saturating_mul(2),
                    ..IndexSortReport::default()
                },
            });
        }
        if !self.chunk.is_empty() {
            self.runs.spill(&mut self.chunk)?;
        }
        self.runs.compact_to_one()?;
        let report = IndexSortReport {
            spill_run_count: self.runs.next_run_sequence,
            spill_bytes: self.runs.spill_bytes,
            peak_memory_bytes: self
                .peak_memory_bytes
                .saturating_mul(2)
                .saturating_add(spill_writer_buffer_bytes(self.config))
                .max(self.runs.peak_merge_memory_bytes),
        };
        Ok(IndexEntrySource::Spilled {
            runs: self.runs,
            report,
        })
    }
}

enum IndexEntrySource {
    Memory {
        entries: Vec<EncodedIndexEntry>,
        report: IndexSortReport,
    },
    Spilled {
        runs: SpillRuns,
        report: IndexSortReport,
    },
}

impl IndexEntrySource {
    fn write(
        self,
        tree: &mut TreeWriter<'_>,
        role: RelationalIndexRole,
    ) -> Result<IndexSortReport, RelationalIndexShadowError> {
        match self {
            Self::Memory { entries, report } => {
                let mut cursor = MemoryCursor {
                    entries: entries.into_iter(),
                };
                write_sorted_entries(tree, role, &mut cursor)?;
                Ok(report)
            }
            Self::Spilled { runs, report } => {
                let path = runs.single_path()?;
                let mut cursor =
                    RunReader::open(path, runs.config, final_reader_buffer_bytes(runs.config))?;
                write_sorted_entries(tree, role, &mut cursor)?;
                Ok(report)
            }
        }
    }
}

trait SortedEntryCursor {
    fn next_entry(&mut self) -> Result<Option<EncodedIndexEntry>, RelationalIndexShadowError>;
}

struct MemoryCursor {
    entries: std::vec::IntoIter<EncodedIndexEntry>,
}

impl SortedEntryCursor for MemoryCursor {
    fn next_entry(&mut self) -> Result<Option<EncodedIndexEntry>, RelationalIndexShadowError> {
        Ok(self.entries.next())
    }
}

fn write_sorted_entries(
    tree: &mut TreeWriter<'_>,
    role: RelationalIndexRole,
    cursor: &mut dyn SortedEntryCursor,
) -> Result<(), RelationalIndexShadowError> {
    let mut pending = cursor.next_entry()?;
    while let Some(first) = pending.take() {
        let index_key = first.index_key;
        let mut first_row_id = Some(first.row_id);
        let mut cursor_failed = false;
        let posting = tree.write_encoded_postings(
            std::iter::from_fn(|| {
                if let Some(row_id) = first_row_id.take() {
                    return Some(Ok(row_id));
                }
                if cursor_failed {
                    return None;
                }
                match cursor.next_entry() {
                    Ok(Some(entry)) if entry.index_key == index_key => Some(Ok(entry.row_id)),
                    Ok(Some(entry)) => {
                        pending = Some(entry);
                        None
                    }
                    Ok(None) => None,
                    Err(error) => {
                        cursor_failed = true;
                        Some(Err(error))
                    }
                }
            }),
            role.is_unique(),
        )?;
        tree.push(IndexLeafEntry {
            key: index_key,
            posting,
        })?;
    }
    Ok(())
}

struct SpillRuns {
    prefix: PathBuf,
    generation: u64,
    root_ordinal: usize,
    config: RelationalIndexShadowConfig,
    paths: Vec<PathBuf>,
    spill_bytes: u64,
    next_run_sequence: usize,
    peak_merge_memory_bytes: usize,
    max_spill_bytes: u64,
}

impl SpillRuns {
    fn new(
        prefix: &Path,
        generation: u64,
        root_ordinal: usize,
        config: RelationalIndexShadowConfig,
        max_spill_bytes: u64,
    ) -> Self {
        Self {
            prefix: prefix.to_path_buf(),
            generation,
            root_ordinal,
            config,
            paths: Vec::new(),
            spill_bytes: 0,
            next_run_sequence: 0,
            peak_merge_memory_bytes: 0,
            max_spill_bytes,
        }
    }

    fn spill(
        &mut self,
        entries: &mut Vec<EncodedIndexEntry>,
    ) -> Result<(), RelationalIndexShadowError> {
        let required_runs = self.paths.len().saturating_add(1);
        if required_runs > self.config.max_sort_runs.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "relational index sort needs {required_runs} spill runs, exceeding limit {}",
                self.config.max_sort_runs
            )));
        }
        entries.sort_unstable();
        reject_duplicate_pairs(entries)?;
        let run_bytes = entries
            .iter()
            .try_fold(RUN_HEADER.len() as u64, |bytes, entry| {
                bytes.checked_add(entry.run_encoded_len()?).ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "relational index spill run length overflow".to_string(),
                    )
                })
            })?;
        self.admit_spill_bytes(run_bytes)?;
        let path = self.next_path();
        let result = write_run(&path, entries, spill_writer_buffer_bytes(self.config));
        if let Err(error) = result {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        self.paths.push(path);
        entries.clear();
        Ok(())
    }

    fn compact_to_one(&mut self) -> Result<(), RelationalIndexShadowError> {
        let fan_in = self.config.max_sort_merge_fan_in.get();
        if fan_in < 2 {
            return Err(RelationalIndexShadowError::Admission(
                "relational index sort merge fan-in must be at least two".to_string(),
            ));
        }
        while self.paths.len() > 1 {
            let old_paths = std::mem::take(&mut self.paths);
            let mut merged_paths = Vec::with_capacity(old_paths.len().div_ceil(fan_in));
            for group in old_paths.chunks(fan_in) {
                let expected_bytes = match merged_run_bytes(group) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        cleanup_paths(old_paths.iter().chain(merged_paths.iter()));
                        return Err(error);
                    }
                };
                if let Err(error) = self.admit_spill_bytes(expected_bytes) {
                    cleanup_paths(old_paths.iter().chain(merged_paths.iter()));
                    return Err(error);
                }
                let path = self.next_path();
                let result = merge_run_group(group, &path, self.config);
                let (bytes, peak_memory_bytes) = match result {
                    Ok(report) => report,
                    Err(error) => {
                        let _ = fs::remove_file(&path);
                        cleanup_paths(old_paths.iter().chain(merged_paths.iter()));
                        return Err(error);
                    }
                };
                if bytes != expected_bytes {
                    let _ = fs::remove_file(&path);
                    cleanup_paths(old_paths.iter().chain(merged_paths.iter()));
                    return Err(RelationalIndexShadowError::Corrupt(format!(
                        "relational index spill merge wrote {bytes} bytes, expected {expected_bytes}"
                    )));
                }
                self.peak_merge_memory_bytes = self.peak_merge_memory_bytes.max(peak_memory_bytes);
                merged_paths.push(path);
                for source in group {
                    if let Err(error) = fs::remove_file(source) {
                        cleanup_paths(old_paths.iter().chain(merged_paths.iter()));
                        return Err(RelationalIndexShadowError::Durability(format!(
                            "failed to remove relational index spill run {}: {error}",
                            source.display()
                        )));
                    }
                }
            }
            self.paths = merged_paths;
        }
        Ok(())
    }

    fn admit_spill_bytes(&mut self, additional: u64) -> Result<(), RelationalIndexShadowError> {
        let required = self.spill_bytes.checked_add(additional).ok_or_else(|| {
            RelationalIndexShadowError::Admission(
                "relational index spill byte accounting overflow".to_string(),
            )
        })?;
        if required > self.max_spill_bytes {
            return Err(RelationalIndexShadowError::Admission(format!(
                "relational index sort needs {required} spill bytes, exceeding limit {}",
                self.max_spill_bytes
            )));
        }
        self.spill_bytes = required;
        Ok(())
    }

    fn single_path(&self) -> Result<&Path, RelationalIndexShadowError> {
        match self.paths.as_slice() {
            [path] => Ok(path),
            _ => Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index sort finished with {} spill runs",
                self.paths.len()
            ))),
        }
    }

    fn next_path(&mut self) -> PathBuf {
        let sequence = self.next_run_sequence;
        self.next_run_sequence = self.next_run_sequence.saturating_add(1);
        self.prefix.with_file_name(format!(
            ".relational-index.{}.{}.run.{sequence}.tmp",
            self.generation, self.root_ordinal
        ))
    }
}

fn merged_run_bytes(sources: &[PathBuf]) -> Result<u64, RelationalIndexShadowError> {
    sources
        .iter()
        .try_fold(RUN_HEADER.len() as u64, |bytes, source| {
            let source_bytes = fs::metadata(source)
                .map_err(|error| {
                    RelationalIndexShadowError::Durability(format!(
                        "failed to inspect relational index spill run {}: {error}",
                        source.display()
                    ))
                })?
                .len();
            let payload_bytes = source_bytes
                .checked_sub(RUN_HEADER.len() as u64)
                .ok_or_else(|| {
                    RelationalIndexShadowError::Corrupt(format!(
                        "relational index spill run {} is shorter than its header",
                        source.display()
                    ))
                })?;
            bytes.checked_add(payload_bytes).ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "relational index merged run length overflow".to_string(),
                )
            })
        })
}

impl Drop for SpillRuns {
    fn drop(&mut self) {
        cleanup_paths(self.paths.iter());
    }
}

fn cleanup_paths<'a>(paths: impl Iterator<Item = &'a PathBuf>) {
    for path in paths {
        let _ = fs::remove_file(path);
    }
}

struct RunReader {
    reader: BufReader<File>,
    config: RelationalIndexShadowConfig,
}

impl RunReader {
    fn open(
        path: &Path,
        config: RelationalIndexShadowConfig,
        buffer_bytes: usize,
    ) -> Result<Self, RelationalIndexShadowError> {
        let mut reader = BufReader::with_capacity(
            buffer_bytes,
            File::open(path).map_err(|error| {
                RelationalIndexShadowError::Durability(format!(
                    "failed to open relational index spill run {}: {error}",
                    path.display()
                ))
            })?,
        );
        let mut header = [0u8; RUN_HEADER.len()];
        reader.read_exact(&mut header).map_err(spill_read_error)?;
        if &header != RUN_HEADER {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index spill run has an invalid header".to_string(),
            ));
        }
        Ok(Self { reader, config })
    }
}

impl SortedEntryCursor for RunReader {
    fn next_entry(&mut self) -> Result<Option<EncodedIndexEntry>, RelationalIndexShadowError> {
        let Some(index_key_len) = read_optional_u32(&mut self.reader)? else {
            return Ok(None);
        };
        let row_id_len = read_u32(&mut self.reader)?;
        let index_key_len = index_key_len as usize;
        let row_id_len = row_id_len as usize;
        if index_key_len == 0 || index_key_len > self.config.page_limits.max_key_bytes.get() {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index spill key length {index_key_len} exceeds limit {}",
                self.config.page_limits.max_key_bytes
            )));
        }
        if row_id_len == 0 || row_id_len > self.config.page_limits.max_row_id_bytes.get() {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index spill row-id length {row_id_len} exceeds limit {}",
                self.config.page_limits.max_row_id_bytes
            )));
        }
        let mut index_key = vec![0; index_key_len];
        let mut row_id = vec![0; row_id_len];
        self.reader
            .read_exact(&mut index_key)
            .map_err(spill_read_error)?;
        self.reader
            .read_exact(&mut row_id)
            .map_err(spill_read_error)?;
        let stored_crc32c = read_u32(&mut self.reader)?;
        let entry = EncodedIndexEntry { index_key, row_id };
        if entry_crc32c(&entry)? != stored_crc32c {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index spill entry checksum mismatch".to_string(),
            ));
        }
        Ok(Some(entry))
    }
}

fn merge_run_group(
    sources: &[PathBuf],
    destination: &Path,
    config: RelationalIndexShadowConfig,
) -> Result<(u64, usize), RelationalIndexShadowError> {
    let participant_count = sources.len().saturating_add(1);
    let io_buffer_bytes = merge_io_buffer_bytes(config, participant_count);
    let fixed_io_bytes = io_buffer_bytes
        .checked_mul(participant_count)
        .ok_or_else(|| {
            RelationalIndexShadowError::Admission(
                "relational index merge I/O buffer accounting overflow".to_string(),
            )
        })?;
    admit_merge_memory(fixed_io_bytes, config)?;
    let mut readers = sources
        .iter()
        .map(|path| RunReader::open(path, config, io_buffer_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    let mut heap_entry_bytes = 0usize;
    let mut peak_memory_bytes = fixed_io_bytes;
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = reader.next_entry()? {
            heap_entry_bytes = heap_entry_bytes
                .checked_add(entry.resident_bytes()?)
                .ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "relational index merge memory accounting overflow".to_string(),
                    )
                })?;
            heap.push(Reverse((entry, run_index)));
        }
    }
    let accounted_memory_bytes = merge_accounted_memory(fixed_io_bytes, heap_entry_bytes)?;
    admit_merge_memory(accounted_memory_bytes, config)?;
    peak_memory_bytes = peak_memory_bytes.max(accounted_memory_bytes);
    let mut writer = BufWriter::with_capacity(
        io_buffer_bytes,
        File::create(destination).map_err(|error| {
            RelationalIndexShadowError::Durability(format!(
                "failed to create relational index spill run {}: {error}",
                destination.display()
            ))
        })?,
    );
    writer.write_all(RUN_HEADER).map_err(spill_write_error)?;
    let mut written_bytes = RUN_HEADER.len() as u64;
    let mut previous: Option<(Vec<u8>, Vec<u8>)> = None;
    while let Some(Reverse((entry, run_index))) = heap.pop() {
        heap_entry_bytes = heap_entry_bytes.saturating_sub(entry.resident_bytes()?);
        if previous.as_ref().is_some_and(|(key, row_id)| {
            (key.as_slice(), row_id.as_slice())
                >= (entry.index_key.as_slice(), entry.row_id.as_slice())
        }) {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index spill merge encountered duplicate or unordered entries"
                    .to_string(),
            ));
        }
        write_entry(&mut writer, &entry)?;
        written_bytes = written_bytes
            .checked_add(entry.run_encoded_len()?)
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "relational index merged run length overflow".to_string(),
                )
            })?;
        previous = Some((entry.index_key, entry.row_id));
        if let Some(next) = readers[run_index].next_entry()? {
            heap_entry_bytes = heap_entry_bytes
                .checked_add(next.resident_bytes()?)
                .ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "relational index merge memory accounting overflow".to_string(),
                    )
                })?;
            let accounted_memory_bytes = merge_accounted_memory(fixed_io_bytes, heap_entry_bytes)?;
            admit_merge_memory(accounted_memory_bytes, config)?;
            peak_memory_bytes = peak_memory_bytes.max(accounted_memory_bytes);
            heap.push(Reverse((next, run_index)));
        }
    }
    writer.flush().map_err(spill_write_error)?;
    Ok((written_bytes, peak_memory_bytes))
}

fn admit_merge_memory(
    required: usize,
    config: RelationalIndexShadowConfig,
) -> Result<(), RelationalIndexShadowError> {
    if required > config.max_sort_memory_bytes.get() {
        return Err(RelationalIndexShadowError::Admission(format!(
            "relational index spill merge needs {required} resident bytes, exceeding limit {}",
            config.max_sort_memory_bytes
        )));
    }
    Ok(())
}

fn merge_accounted_memory(
    fixed_io_bytes: usize,
    heap_entry_bytes: usize,
) -> Result<usize, RelationalIndexShadowError> {
    heap_entry_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(fixed_io_bytes))
        .ok_or_else(|| {
            RelationalIndexShadowError::Admission(
                "relational index merge memory accounting overflow".to_string(),
            )
        })
}

fn write_run(
    path: &Path,
    entries: &[EncodedIndexEntry],
    buffer_bytes: usize,
) -> Result<(), RelationalIndexShadowError> {
    let mut writer = BufWriter::with_capacity(
        buffer_bytes,
        File::create(path).map_err(|error| {
            RelationalIndexShadowError::Durability(format!(
                "failed to create relational index spill run {}: {error}",
                path.display()
            ))
        })?,
    );
    writer.write_all(RUN_HEADER).map_err(spill_write_error)?;
    for entry in entries {
        write_entry(&mut writer, entry)?;
    }
    writer.flush().map_err(spill_write_error)
}

fn write_entry(
    writer: &mut impl Write,
    entry: &EncodedIndexEntry,
) -> Result<(), RelationalIndexShadowError> {
    let key_len = u32::try_from(entry.index_key.len()).map_err(|_| {
        RelationalIndexShadowError::Admission(
            "relational index spill key length does not fit u32".to_string(),
        )
    })?;
    let row_id_len = u32::try_from(entry.row_id.len()).map_err(|_| {
        RelationalIndexShadowError::Admission(
            "relational index spill row-id length does not fit u32".to_string(),
        )
    })?;
    writer
        .write_all(&key_len.to_le_bytes())
        .map_err(spill_write_error)?;
    writer
        .write_all(&row_id_len.to_le_bytes())
        .map_err(spill_write_error)?;
    writer
        .write_all(&entry.index_key)
        .map_err(spill_write_error)?;
    writer.write_all(&entry.row_id).map_err(spill_write_error)?;
    writer
        .write_all(&entry_crc32c(entry)?.to_le_bytes())
        .map_err(spill_write_error)
}

fn entry_crc32c(entry: &EncodedIndexEntry) -> Result<u32, RelationalIndexShadowError> {
    let key_len = u32::try_from(entry.index_key.len()).map_err(|_| {
        RelationalIndexShadowError::Admission(
            "relational index spill key length does not fit u32".to_string(),
        )
    })?;
    let row_id_len = u32::try_from(entry.row_id.len()).map_err(|_| {
        RelationalIndexShadowError::Admission(
            "relational index spill row-id length does not fit u32".to_string(),
        )
    })?;
    let mut hasher = Crc32cHasher::new();
    hasher.update(&key_len.to_le_bytes());
    hasher.update(&row_id_len.to_le_bytes());
    hasher.update(&entry.index_key);
    hasher.update(&entry.row_id);
    Ok(hasher.finish_u32())
}

fn validate_entry(
    entry: &EncodedIndexEntry,
    config: RelationalIndexShadowConfig,
) -> Result<(), RelationalIndexShadowError> {
    if entry.index_key.is_empty() || entry.index_key.len() > config.page_limits.max_key_bytes.get()
    {
        return Err(RelationalIndexShadowError::Admission(format!(
            "encoded relational index key contains {} bytes, exceeding limit {}",
            entry.index_key.len(),
            config.page_limits.max_key_bytes
        )));
    }
    if entry.row_id.is_empty() || entry.row_id.len() > config.page_limits.max_row_id_bytes.get() {
        return Err(RelationalIndexShadowError::Admission(format!(
            "encoded relational index row id contains {} bytes, exceeding limit {}",
            entry.row_id.len(),
            config.page_limits.max_row_id_bytes
        )));
    }
    Ok(())
}

fn spill_writer_buffer_bytes(config: RelationalIndexShadowConfig) -> usize {
    (config.max_sort_memory_bytes.get() / 4).clamp(1, MAX_RUN_IO_BUFFER_BYTES)
}

fn final_reader_buffer_bytes(config: RelationalIndexShadowConfig) -> usize {
    (config.max_sort_memory_bytes.get() / 2).clamp(1, MAX_RUN_IO_BUFFER_BYTES)
}

fn merge_io_buffer_bytes(config: RelationalIndexShadowConfig, participant_count: usize) -> usize {
    (config.max_sort_memory_bytes.get() / participant_count.saturating_mul(2).max(1))
        .clamp(1, MAX_RUN_IO_BUFFER_BYTES)
}

fn reject_duplicate_pairs(entries: &[EncodedIndexEntry]) -> Result<(), RelationalIndexShadowError> {
    if entries.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(RelationalIndexShadowError::Corrupt(
            "relational index sort encountered duplicate entries".to_string(),
        ));
    }
    Ok(())
}

fn read_optional_u32(reader: &mut impl Read) -> Result<Option<u32>, RelationalIndexShadowError> {
    let mut bytes = [0; 4];
    match reader.read(&mut bytes[..1]).map_err(spill_read_error)? {
        0 => return Ok(None),
        1 => {}
        _ => unreachable!("one-byte spill prefix read"),
    }
    reader
        .read_exact(&mut bytes[1..])
        .map_err(spill_read_error)?;
    Ok(Some(u32::from_le_bytes(bytes)))
}

fn read_u32(reader: &mut impl Read) -> Result<u32, RelationalIndexShadowError> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes).map_err(spill_read_error)?;
    Ok(u32::from_le_bytes(bytes))
}

fn spill_read_error(error: std::io::Error) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Corrupt(format!(
        "relational index spill run could not be read: {error}"
    ))
}

fn spill_write_error(error: std::io::Error) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Durability(format!(
        "relational index spill run could not be written: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};

    #[test]
    fn spill_entry_checksum_rejects_transient_corruption() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-relational-index-spill-corruption-{}-{nonce}.tmp",
            std::process::id()
        ));
        let config = RelationalIndexShadowConfig::default();
        write_run(
            &path,
            &[EncodedIndexEntry {
                index_key: vec![1, 2],
                row_id: vec![3, 4],
            }],
            spill_writer_buffer_bytes(config),
        )
        .expect("write relational index spill fixture");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open relational index spill fixture");
        file.seek(SeekFrom::Start((RUN_HEADER.len() + 8) as u64))
            .expect("seek relational index spill payload");
        file.write_all(&[9])
            .expect("corrupt relational index spill payload");
        file.sync_all()
            .expect("sync relational index spill corruption");
        drop(file);

        let mut reader = RunReader::open(&path, config, final_reader_buffer_bytes(config))
            .expect("open corrupted relational index spill fixture");
        assert!(matches!(
            reader.next_entry(),
            Err(RelationalIndexShadowError::Corrupt(message))
                if message.contains("checksum mismatch")
        ));
        std::fs::remove_file(path).expect("remove relational index spill fixture");
    }

    #[test]
    fn merge_spill_budget_is_admitted_before_destination_creation() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "hawdb-relational-index-spill-admission-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("create relational index spill fixture");
        let prefix = directory.join("candidate.tmp");
        let config = RelationalIndexShadowConfig {
            max_sort_merge_fan_in: std::num::NonZeroUsize::new(2).unwrap(),
            ..RelationalIndexShadowConfig::default()
        };
        let entry = |byte| EncodedIndexEntry {
            index_key: vec![byte],
            row_id: vec![byte],
        };
        let source_run_bytes = RUN_HEADER.len() as u64 + entry(0).run_encoded_len().unwrap();
        let merged_bytes = RUN_HEADER.len() as u64 + 2 * entry(0).run_encoded_len().unwrap();
        let max_spill_bytes = source_run_bytes * 2 + merged_bytes - 1;
        let mut runs = SpillRuns::new(&prefix, 1, 0, config, max_spill_bytes);
        runs.spill(&mut vec![entry(1)]).expect("spill first run");
        runs.spill(&mut vec![entry(2)]).expect("spill second run");

        let error = runs
            .compact_to_one()
            .expect_err("merge output must be admitted before it is created");
        assert!(matches!(
            error,
            RelationalIndexShadowError::Admission(message)
                if message.contains("spill bytes")
        ));
        assert!(!directory.join(".relational-index.1.0.run.2.tmp").exists());
        assert!(std::fs::read_dir(&directory)
            .expect("list relational index spill fixture")
            .next()
            .is_none());
        std::fs::remove_dir_all(directory).expect("remove relational index spill fixture");
    }
}
