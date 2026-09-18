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

//! Bounded external sorting for exact relational overflow root closure.

use super::publication::RelationalOverflowPublicationError;
use super::RelationalOverflowRef;
use crate::relational::RelationalScalarType;
use hawdb_integrity::{Crc32cHasher, Sha256Digest};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

const RUN_HEADER: &[u8; 8] = b"SKOVRFR1";
const RUN_RECORD_BYTES: u64 = 32 + 1 + 7 + 8 + 8 + 4;
const MAX_RUN_IO_BUFFER_BYTES: usize = 8 * 1024;

pub const DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SORT_MEMORY_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SPILL_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_RUNS: usize = 32;
pub const DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_OCCURRENCES: u64 = 100_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowReferenceSortConfig {
    pub max_memory_bytes: NonZeroUsize,
    pub max_spill_bytes: NonZeroU64,
    pub max_runs: NonZeroUsize,
    pub max_reference_occurrences: NonZeroU64,
}

impl Default for RelationalOverflowReferenceSortConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: NonZeroUsize::new(
                DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SORT_MEMORY_BYTES,
            )
            .expect("default overflow reference sort memory is non-zero"),
            max_spill_bytes: NonZeroU64::new(DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SPILL_BYTES)
                .expect("default overflow reference spill budget is non-zero"),
            max_runs: NonZeroUsize::new(DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_RUNS)
                .expect("default overflow reference run limit is non-zero"),
            max_reference_occurrences: NonZeroU64::new(
                DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_OCCURRENCES,
            )
            .expect("default overflow reference occurrence limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalOverflowReferenceSortReport {
    pub reference_occurrences: u64,
    pub unique_references: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_memory_bytes: usize,
}

pub struct RelationalOverflowReferenceSetBuilder {
    config: RelationalOverflowReferenceSortConfig,
    prefix: PathBuf,
    chunk: Vec<RelationalOverflowRef>,
    chunk_bytes: usize,
    reference_occurrences: u64,
    peak_memory_bytes: usize,
    runs: Vec<PathBuf>,
    spill_bytes: u64,
}

impl RelationalOverflowReferenceSetBuilder {
    pub fn new(
        directory: &Path,
        generation: u64,
        config: RelationalOverflowReferenceSortConfig,
    ) -> Result<Self, RelationalOverflowPublicationError> {
        if generation == 0 {
            return Err(RelationalOverflowPublicationError::Admission(
                "overflow reference sort generation must be non-zero".to_string(),
            ));
        }
        let minimum_memory = size_of::<RelationalOverflowRef>()
            .saturating_mul(4)
            .saturating_add(MAX_RUN_IO_BUFFER_BYTES);
        if config.max_memory_bytes.get() < minimum_memory {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "overflow reference sort needs at least {minimum_memory} memory bytes, got {}",
                config.max_memory_bytes
            )));
        }
        fs::create_dir_all(directory).map_err(durability("create overflow sort directory"))?;
        let prefix = directory.join(format!(".relational-overflow-gc-{generation}"));
        cleanup_stale_runs(directory, generation)?;
        Ok(Self {
            config,
            prefix,
            chunk: Vec::new(),
            chunk_bytes: 0,
            reference_occurrences: 0,
            peak_memory_bytes: 0,
            runs: Vec::new(),
            spill_bytes: 0,
        })
    }

    pub fn push(
        &mut self,
        reference: RelationalOverflowRef,
    ) -> Result<(), RelationalOverflowPublicationError> {
        validate_reference(reference)?;
        self.reference_occurrences =
            self.reference_occurrences.checked_add(1).ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "overflow reference occurrence count overflow".to_string(),
                )
            })?;
        if self.reference_occurrences > self.config.max_reference_occurrences.get() {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "overflow closure contains {} reference occurrences, exceeding limit {}",
                self.reference_occurrences, self.config.max_reference_occurrences
            )));
        }
        let entry_bytes = size_of::<RelationalOverflowRef>()
            .checked_mul(2)
            .ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "overflow reference memory accounting overflow".to_string(),
                )
            })?;
        let chunk_budget = self
            .config
            .max_memory_bytes
            .get()
            .saturating_sub(MAX_RUN_IO_BUFFER_BYTES);
        if !self.chunk.is_empty() && self.chunk_bytes.saturating_add(entry_bytes) > chunk_budget {
            self.spill_chunk()?;
        }
        self.chunk_bytes = self.chunk_bytes.checked_add(entry_bytes).ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "overflow reference memory accounting overflow".to_string(),
            )
        })?;
        self.peak_memory_bytes = self.peak_memory_bytes.max(self.chunk_bytes);
        self.chunk.push(reference);
        Ok(())
    }

    pub fn finish(
        mut self,
    ) -> Result<RelationalOverflowReferenceSet, RelationalOverflowPublicationError> {
        if self.runs.is_empty() {
            sort_and_deduplicate(&mut self.chunk)?;
            let unique_references = self.chunk.len() as u64;
            return Ok(RelationalOverflowReferenceSet {
                source: ReferenceSetSource::Memory(std::mem::take(&mut self.chunk)),
                report: RelationalOverflowReferenceSortReport {
                    reference_occurrences: self.reference_occurrences,
                    unique_references,
                    peak_memory_bytes: self.peak_memory_bytes,
                    ..RelationalOverflowReferenceSortReport::default()
                },
            });
        }
        if !self.chunk.is_empty() {
            self.spill_chunk()?;
        }
        drop(std::mem::take(&mut self.chunk));
        self.chunk_bytes = 0;
        let spill_run_count = self.runs.len();
        let per_run_memory_bytes = run_reader_buffer_bytes(self.config)
            .checked_add(size_of::<RunReader>())
            .and_then(|bytes| {
                bytes.checked_add(size_of::<RelationalOverflowRef>().saturating_mul(2))
            })
            .ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "overflow reference merge memory count overflow".to_string(),
                )
            })?;
        let merge_memory_bytes = per_run_memory_bytes
            .checked_mul(spill_run_count)
            .ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "overflow reference merge memory count overflow".to_string(),
                )
            })?;
        if merge_memory_bytes > self.config.max_memory_bytes.get() {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "overflow reference merge needs {merge_memory_bytes} memory bytes, exceeding limit {}",
                self.config.max_memory_bytes
            )));
        }
        let mut set = RelationalOverflowReferenceSet {
            source: ReferenceSetSource::Spilled {
                paths: std::mem::take(&mut self.runs),
                config: self.config,
            },
            report: RelationalOverflowReferenceSortReport {
                reference_occurrences: self.reference_occurrences,
                spill_run_count,
                spill_bytes: self.spill_bytes,
                peak_memory_bytes: self.peak_memory_bytes.max(merge_memory_bytes),
                ..RelationalOverflowReferenceSortReport::default()
            },
        };
        let mut unique_references = 0u64;
        set.visit(&mut |_| {
            unique_references = unique_references.checked_add(1).ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "overflow unique reference count overflow".to_string(),
                )
            })?;
            Ok(true)
        })?;
        set.report.unique_references = unique_references;
        Ok(set)
    }

    fn spill_chunk(&mut self) -> Result<(), RelationalOverflowPublicationError> {
        if self.runs.len() >= self.config.max_runs.get() {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "overflow reference sort needs more than {} spill runs",
                self.config.max_runs
            )));
        }
        sort_and_deduplicate(&mut self.chunk)?;
        let run_bytes = (RUN_HEADER.len() as u64)
            .checked_add(
                (self.chunk.len() as u64)
                    .checked_mul(RUN_RECORD_BYTES)
                    .ok_or_else(|| {
                        RelationalOverflowPublicationError::Admission(
                            "overflow reference run length overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                RelationalOverflowPublicationError::Admission(
                    "overflow reference run length overflow".to_string(),
                )
            })?;
        let next_spill_bytes = self.spill_bytes.checked_add(run_bytes).ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "overflow reference spill byte count overflow".to_string(),
            )
        })?;
        if next_spill_bytes > self.config.max_spill_bytes.get() {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "overflow reference sort needs {next_spill_bytes} spill bytes, exceeding limit {}",
                self.config.max_spill_bytes
            )));
        }
        let path = run_path(&self.prefix, self.runs.len());
        let result = write_run(&path, &self.chunk, run_writer_buffer_bytes(self.config));
        if let Err(error) = result {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        self.runs.push(path);
        self.spill_bytes = next_spill_bytes;
        self.chunk.clear();
        self.chunk_bytes = 0;
        Ok(())
    }
}

impl Drop for RelationalOverflowReferenceSetBuilder {
    fn drop(&mut self) {
        cleanup_paths(&self.runs);
    }
}

pub struct RelationalOverflowReferenceSet {
    source: ReferenceSetSource,
    report: RelationalOverflowReferenceSortReport,
}

impl RelationalOverflowReferenceSet {
    pub const fn report(&self) -> RelationalOverflowReferenceSortReport {
        self.report
    }

    pub fn visit(
        &self,
        visit: &mut dyn FnMut(
            RelationalOverflowRef,
        ) -> Result<bool, RelationalOverflowPublicationError>,
    ) -> Result<(), RelationalOverflowPublicationError> {
        match &self.source {
            ReferenceSetSource::Memory(references) => {
                for reference in references {
                    if !visit(*reference)? {
                        break;
                    }
                }
                Ok(())
            }
            ReferenceSetSource::Spilled { paths, config } => {
                visit_merged_runs(paths, *config, visit)
            }
        }
    }
}

enum ReferenceSetSource {
    Memory(Vec<RelationalOverflowRef>),
    Spilled {
        paths: Vec<PathBuf>,
        config: RelationalOverflowReferenceSortConfig,
    },
}

impl Drop for ReferenceSetSource {
    fn drop(&mut self) {
        if let Self::Spilled { paths, .. } = self {
            cleanup_paths(paths);
        }
    }
}

struct RunReader {
    reader: BufReader<File>,
}

impl RunReader {
    fn open(path: &Path, buffer_bytes: usize) -> Result<Self, RelationalOverflowPublicationError> {
        let mut reader = BufReader::with_capacity(
            buffer_bytes,
            File::open(path).map_err(durability("open overflow reference spill run"))?,
        );
        let mut header = [0u8; RUN_HEADER.len()];
        reader
            .read_exact(&mut header)
            .map_err(corrupt_read("read overflow reference spill header"))?;
        if &header != RUN_HEADER {
            return Err(RelationalOverflowPublicationError::Corrupt(
                "overflow reference spill run has an invalid header".to_string(),
            ));
        }
        Ok(Self { reader })
    }

    fn next_reference(
        &mut self,
    ) -> Result<Option<RelationalOverflowRef>, RelationalOverflowPublicationError> {
        let mut encoded = [0u8; RUN_RECORD_BYTES as usize];
        match self
            .reader
            .read(&mut encoded[..1])
            .map_err(corrupt_read("read overflow reference spill record"))?
        {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one-byte overflow spill prefix read"),
        }
        self.reader
            .read_exact(&mut encoded[1..])
            .map_err(corrupt_read("read overflow reference spill record"))?;
        decode_reference(&encoded).map(Some)
    }
}

fn visit_merged_runs(
    paths: &[PathBuf],
    config: RelationalOverflowReferenceSortConfig,
    visit: &mut dyn FnMut(
        RelationalOverflowRef,
    ) -> Result<bool, RelationalOverflowPublicationError>,
) -> Result<(), RelationalOverflowPublicationError> {
    let buffer_bytes = run_reader_buffer_bytes(config);
    let mut readers = paths
        .iter()
        .map(|path| RunReader::open(path, buffer_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (run, reader) in readers.iter_mut().enumerate() {
        if let Some(reference) = reader.next_reference()? {
            heap.push(Reverse((reference, run)));
        }
    }
    let mut previous: Option<RelationalOverflowRef> = None;
    while let Some(Reverse((reference, run))) = heap.pop() {
        match previous {
            Some(previous_reference)
                if previous_reference.digest == reference.digest
                    && previous_reference != reference =>
            {
                return Err(RelationalOverflowPublicationError::Corrupt(format!(
                    "overflow digest {} has conflicting reference metadata",
                    reference.digest
                )));
            }
            Some(previous_reference) if previous_reference == reference => {}
            _ => {
                if !visit(reference)? {
                    return Ok(());
                }
                previous = Some(reference);
            }
        }
        if let Some(next) = readers[run].next_reference()? {
            heap.push(Reverse((next, run)));
        }
    }
    Ok(())
}

fn sort_and_deduplicate(
    references: &mut Vec<RelationalOverflowRef>,
) -> Result<(), RelationalOverflowPublicationError> {
    references.sort_unstable();
    for pair in references.windows(2) {
        if pair[0].digest == pair[1].digest && pair[0] != pair[1] {
            return Err(RelationalOverflowPublicationError::Corrupt(format!(
                "overflow digest {} has conflicting reference metadata",
                pair[0].digest
            )));
        }
    }
    references.dedup();
    Ok(())
}

fn write_run(
    path: &Path,
    references: &[RelationalOverflowRef],
    buffer_bytes: usize,
) -> Result<(), RelationalOverflowPublicationError> {
    let mut writer = BufWriter::with_capacity(
        buffer_bytes,
        File::create(path).map_err(durability("create overflow reference spill run"))?,
    );
    writer
        .write_all(RUN_HEADER)
        .map_err(durability("write overflow reference spill header"))?;
    for reference in references {
        writer
            .write_all(&encode_reference(*reference)?)
            .map_err(durability("write overflow reference spill record"))?;
    }
    writer
        .flush()
        .map_err(durability("flush overflow reference spill run"))
}

fn encode_reference(
    reference: RelationalOverflowRef,
) -> Result<[u8; RUN_RECORD_BYTES as usize], RelationalOverflowPublicationError> {
    validate_reference(reference)?;
    let mut encoded = [0u8; RUN_RECORD_BYTES as usize];
    encoded[..32].copy_from_slice(reference.digest.as_bytes());
    encoded[32] = scalar_type_tag(reference.scalar_type)?;
    encoded[40..48].copy_from_slice(&reference.compressed_bytes.to_le_bytes());
    encoded[48..56].copy_from_slice(&reference.uncompressed_bytes.to_le_bytes());
    let mut hasher = Crc32cHasher::new();
    hasher.update(&encoded[..56]);
    encoded[56..60].copy_from_slice(&hasher.finish_u32().to_le_bytes());
    Ok(encoded)
}

fn decode_reference(
    encoded: &[u8; RUN_RECORD_BYTES as usize],
) -> Result<RelationalOverflowRef, RelationalOverflowPublicationError> {
    if encoded[33..40] != [0u8; 7] {
        return Err(RelationalOverflowPublicationError::Corrupt(
            "overflow reference spill record has non-zero reserved bytes".to_string(),
        ));
    }
    let mut hasher = Crc32cHasher::new();
    hasher.update(&encoded[..56]);
    let stored_crc32c = u32::from_le_bytes(
        encoded[56..60]
            .try_into()
            .expect("overflow spill CRC has a fixed length"),
    );
    if hasher.finish_u32() != stored_crc32c {
        return Err(RelationalOverflowPublicationError::Corrupt(
            "overflow reference spill record checksum mismatch".to_string(),
        ));
    }
    let reference = RelationalOverflowRef {
        digest: Sha256Digest::from_bytes(
            encoded[..32]
                .try_into()
                .expect("overflow spill digest has a fixed length"),
        ),
        scalar_type: scalar_type_from_tag(encoded[32])?,
        compressed_bytes: u64::from_le_bytes(
            encoded[40..48]
                .try_into()
                .expect("overflow spill compressed length has a fixed length"),
        ),
        uncompressed_bytes: u64::from_le_bytes(
            encoded[48..56]
                .try_into()
                .expect("overflow spill uncompressed length has a fixed length"),
        ),
    };
    validate_reference(reference)?;
    Ok(reference)
}

fn validate_reference(
    reference: RelationalOverflowRef,
) -> Result<(), RelationalOverflowPublicationError> {
    if reference.compressed_bytes == 0 || reference.uncompressed_bytes == 0 {
        return Err(RelationalOverflowPublicationError::Corrupt(format!(
            "overflow reference {} contains a zero length",
            reference.digest
        )));
    }
    scalar_type_tag(reference.scalar_type).map(|_| ())
}

fn scalar_type_tag(
    scalar_type: RelationalScalarType,
) -> Result<u8, RelationalOverflowPublicationError> {
    match scalar_type {
        RelationalScalarType::Text => Ok(1),
        RelationalScalarType::Bytea => Ok(2),
        _ => Err(RelationalOverflowPublicationError::Corrupt(
            "only TEXT and BYTEA may appear in an overflow reference set".to_string(),
        )),
    }
}

fn scalar_type_from_tag(
    tag: u8,
) -> Result<RelationalScalarType, RelationalOverflowPublicationError> {
    match tag {
        1 => Ok(RelationalScalarType::Text),
        2 => Ok(RelationalScalarType::Bytea),
        _ => Err(RelationalOverflowPublicationError::Corrupt(format!(
            "overflow reference spill record has invalid scalar type {tag}"
        ))),
    }
}

fn run_writer_buffer_bytes(config: RelationalOverflowReferenceSortConfig) -> usize {
    (config.max_memory_bytes.get() / 4).clamp(1, MAX_RUN_IO_BUFFER_BYTES)
}

fn run_reader_buffer_bytes(config: RelationalOverflowReferenceSortConfig) -> usize {
    (config.max_memory_bytes.get() / config.max_runs.get().saturating_mul(4).max(1))
        .clamp(1, MAX_RUN_IO_BUFFER_BYTES)
}

fn run_path(prefix: &Path, sequence: usize) -> PathBuf {
    prefix.with_extension(format!("run-{sequence}.tmp"))
}

fn cleanup_paths(paths: &[PathBuf]) {
    for path in paths {
        let _ = fs::remove_file(path);
    }
}

fn cleanup_stale_runs(
    directory: &Path,
    generation: u64,
) -> Result<(), RelationalOverflowPublicationError> {
    let prefix = format!(".relational-overflow-gc-{generation}.run-");
    for entry in fs::read_dir(directory).map_err(durability("list overflow sort directory"))? {
        let entry = entry.map_err(durability("read overflow sort directory entry"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(sequence) = name
            .strip_prefix(&prefix)
            .and_then(|name| name.strip_suffix(".tmp"))
        else {
            continue;
        };
        if sequence.is_empty() || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        fs::remove_file(entry.path())
            .map_err(durability("remove stale overflow reference spill run"))?;
    }
    Ok(())
}

fn durability(
    operation: &'static str,
) -> impl FnOnce(std::io::Error) -> RelationalOverflowPublicationError {
    move |error| RelationalOverflowPublicationError::Durability(format!("{operation}: {error}"))
}

fn corrupt_read(
    operation: &'static str,
) -> impl FnOnce(std::io::Error) -> RelationalOverflowPublicationError {
    move |error| RelationalOverflowPublicationError::Corrupt(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    fn unique_directory(name: &str) -> PathBuf {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "hawdb-overflow-reference-{name}-{}-{sequence}",
            std::process::id()
        ))
    }

    fn reference(seed: u8) -> RelationalOverflowRef {
        RelationalOverflowRef {
            digest: Sha256Digest::from_bytes([seed; 32]),
            scalar_type: RelationalScalarType::Text,
            compressed_bytes: seed as u64 + 1,
            uncompressed_bytes: seed as u64 + 2,
        }
    }

    fn distinct_reference(seed: u16) -> RelationalOverflowRef {
        let mut digest = [0u8; 32];
        digest[..2].copy_from_slice(&seed.to_le_bytes());
        RelationalOverflowRef {
            digest: Sha256Digest::from_bytes(digest),
            scalar_type: RelationalScalarType::Text,
            compressed_bytes: u64::from(seed) + 1,
            uncompressed_bytes: u64::from(seed) + 2,
        }
    }

    #[test]
    fn spilled_reference_set_is_repeatable_sorted_and_deduplicated() {
        let directory = unique_directory("repeatable");
        let mut builder = RelationalOverflowReferenceSetBuilder::new(
            &directory,
            7,
            RelationalOverflowReferenceSortConfig {
                max_memory_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
                max_spill_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
                max_runs: NonZeroUsize::new(8).unwrap(),
                max_reference_occurrences: NonZeroU64::new(1_000).unwrap(),
            },
        )
        .unwrap();
        for seed in (1..=200).rev().chain(1..=200) {
            builder.push(reference(seed)).unwrap();
        }
        let set = builder.finish().unwrap();
        assert!(set.report().spill_run_count > 1);
        assert_eq!(set.report().reference_occurrences, 400);
        assert_eq!(set.report().unique_references, 200);
        for _ in 0..2 {
            let mut actual = Vec::new();
            set.visit(&mut |reference| {
                actual.push(reference);
                Ok(true)
            })
            .unwrap();
            assert_eq!(actual, (1..=200).map(reference).collect::<Vec<_>>());
        }
        drop(set);
        assert_eq!(
            fs::read_dir(&directory).unwrap().count(),
            0,
            "spill runs must be removed when the exact set is dropped"
        );
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn conflicting_metadata_for_one_digest_fails_closed() {
        let directory = unique_directory("conflict");
        let mut builder = RelationalOverflowReferenceSetBuilder::new(
            &directory,
            9,
            RelationalOverflowReferenceSortConfig::default(),
        )
        .unwrap();
        let first = reference(3);
        let mut conflict = first;
        conflict.uncompressed_bytes += 1;
        builder.push(first).unwrap();
        builder.push(conflict).unwrap();
        assert!(matches!(
            builder.finish(),
            Err(RelationalOverflowPublicationError::Corrupt(_))
        ));
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn stale_runs_are_removed_independently_of_the_current_run_limit() {
        let directory = unique_directory("stale-runs");
        fs::create_dir_all(&directory).unwrap();
        let stale = directory.join(".relational-overflow-gc-11.run-99.tmp");
        let unrelated = directory.join(".relational-overflow-gc-12.run-99.tmp");
        fs::write(&stale, b"stale").unwrap();
        fs::write(&unrelated, b"other generation").unwrap();

        let builder = RelationalOverflowReferenceSetBuilder::new(
            &directory,
            11,
            RelationalOverflowReferenceSortConfig {
                max_runs: NonZeroUsize::new(1).unwrap(),
                ..RelationalOverflowReferenceSortConfig::default()
            },
        )
        .unwrap();
        assert!(!stale.exists());
        assert!(unrelated.exists());
        drop(builder);

        fs::remove_file(unrelated).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn merge_memory_is_admitted_before_opening_all_runs() {
        let directory = unique_directory("merge-memory");
        let mut builder = RelationalOverflowReferenceSetBuilder::new(
            &directory,
            13,
            RelationalOverflowReferenceSortConfig {
                max_memory_bytes: NonZeroUsize::new(9_000).unwrap(),
                max_spill_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
                max_runs: NonZeroUsize::new(64).unwrap(),
                max_reference_occurrences: NonZeroU64::new(1_000).unwrap(),
            },
        )
        .unwrap();
        for seed in 0..400 {
            builder.push(distinct_reference(seed)).unwrap();
        }
        let error = match builder.finish() {
            Ok(_) => panic!("merge memory must be admitted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("merge needs"));
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        fs::remove_dir(directory).unwrap();
    }
}
