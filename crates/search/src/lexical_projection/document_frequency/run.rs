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

//! Checksummed, staging-only field summaries. These runs are never persisted
//! in a published generation or interpreted by a public reader.

use super::*;
use crate::build_memory::reserved::Grant;
#[cfg(test)]
use crate::build_memory::reserved::ReservedMemory;

const HEADER: &[u8; 8] = b"SKNDOCF1";
const FOOTER_BYTES: u64 = 16;

pub(in crate::lexical_projection) struct FrequencyRun {
    pub(super) guard: RemoveOnDrop,
    pub(super) control: SpillControl,
    pub(super) posting_size: PostingSize,
}

/// Exact output size after combining fields of the same term. This metadata is
/// private to the live run owner; the on-disk format and reader stay unchanged.
#[derive(Default)]
pub(super) struct PostingSize {
    terms: u64,
    term_bytes: u64,
}

impl PostingSize {
    fn include(&mut self, term: &str) -> Result<()> {
        self.terms = self.terms.checked_add(1).ok_or_else(Self::overflow)?;
        self.term_bytes = self
            .term_bytes
            .checked_add(term.len() as u64)
            .ok_or_else(Self::overflow)?;
        Ok(())
    }

    pub(super) fn encoded_bytes(&self) -> Result<u64> {
        self.terms
            .checked_mul(Posting::encoded_parts_len(0))
            .and_then(|bytes| bytes.checked_add(self.term_bytes))
            .and_then(|bytes| bytes.checked_add(RUN_HEADER.len() as u64))
            .ok_or_else(Self::overflow)
    }

    fn overflow() -> HawDBError {
        HawDBError::Storage("lexical spill bytes overflow".into())
    }
}

struct HashWriter<W> {
    inner: W,
    digest: Digest,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn validate(record: &FrequencyRecord, config: LexicalProjectionConfig) -> Result<()> {
    if record.term.is_empty()
        || record.term.len() as u64 > config.max_term_bytes.get()
        || record.field >= 6
        || record
            .summary
            .first_event
            .is_none_or(|(_, weight)| weight > 2)
        || record.summary.frequency()? == 0
    {
        return Err(HawDBError::Storage(
            "invalid document frequency spill record".into(),
        ));
    }
    Ok(())
}

struct RunWriter<W> {
    writer: HashWriter<W>,
    total_bytes: u64,
    records: u64,
    config: LexicalProjectionConfig,
    _buffer_memory: Option<Grant>,
    control: SpillControl,
}

impl<W: Write> RunWriter<W> {
    fn push(&mut self, record: &FrequencyRecord) -> Result<()> {
        validate(record, self.config)?;
        let bytes = checked_spill_bytes(
            self.total_bytes,
            record.encoded_len(),
            self.config.max_spill_bytes,
        )?;
        let records = self
            .records
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("document frequency run count overflow".into()))?;
        let (ordinal, unique_weight) = record.summary.first_event.expect("validated first event");
        let mut writer = spill_control::RecordWriter::new(&mut self.writer, &self.control)?;
        write_string(&mut writer, &record.term)?;
        writer.write_all(&[record.field])?;
        writer.write_all(&record.summary.repeated_weight.to_le_bytes())?;
        writer.write_all(&ordinal.to_le_bytes())?;
        writer.write_all(&unique_weight.to_le_bytes())?;
        self.total_bytes = bytes;
        self.records = records;
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        // The footer is reserved before creating the file, not after emitting
        // the last record. It commits both count and payload integrity.
        let mut output =
            crate::build_control::CheckedWriter::new(&mut self.writer.inner, self.control.task());
        output.write_all(&self.records.to_le_bytes())?;
        output.write_all(&self.writer.digest.finish().to_le_bytes())?;
        output.flush()?;
        Ok(self.total_bytes)
    }
}

pub(super) fn write_run(
    records: impl IntoIterator<Item = Result<FrequencyRecord>>,
    pool: &mut SpillRuns,
    io: &mut impl SpillIo,
) -> Result<FrequencyRun> {
    let total_bytes = checked_spill_bytes(
        pool.bytes,
        HEADER.len() as u64 + FOOTER_BYTES,
        pool.config.max_spill_bytes,
    )?;
    let mut guard = pool.next_guard()?;
    let buffer_memory = pool.control.reserve(SPILL_IO_BUFFER_BYTES)?;
    let mut writer = RunWriter {
        writer: HashWriter {
            inner: pool
                .control
                .with_path(&guard.path, || io.create(&guard.path))?,
            digest: Digest::new(),
        },
        total_bytes,
        records: 0,
        config: pool.config,
        _buffer_memory: buffer_memory,
        control: pool.control.clone(),
    };
    pool.check()?;
    writer.writer.write_all(HEADER)?;
    let mut pending: Option<FrequencyRecord> = None;
    let mut posting_size = PostingSize::default();
    for record in records {
        pool.check()?;
        let record = record?;
        validate(&record, pool.config)?;
        if let Some(previous) = pending.as_mut() {
            match previous.key().cmp(&record.key()) {
                std::cmp::Ordering::Greater => {
                    return Err(HawDBError::Storage(
                        "unordered document frequency run input".into(),
                    ))
                }
                std::cmp::Ordering::Equal => {
                    previous.summary.merge(record.summary)?;
                    continue;
                }
                std::cmp::Ordering::Less => {
                    if previous.term != record.term {
                        posting_size.include(&previous.term)?;
                    }
                    writer.push(previous)?;
                }
            }
        }
        pending = Some(record);
    }
    if let Some(record) = pending {
        posting_size.include(&record.term)?;
        writer.push(&record)?;
    }
    pool.bytes = writer.finish()?;
    if let Some(memory) = guard._memory.as_mut() {
        if guard.path.capacity() > memory.bytes() {
            return Err(HawDBError::Execution(
                "search spill path allocation exceeded its admitted capacity".into(),
            ));
        }
        memory.shrink(memory.bytes() - guard.path.capacity());
    }
    Ok(FrequencyRun {
        guard,
        control: pool.control.clone(),
        posting_size,
    })
}

struct HashRead<'a> {
    reader: &'a mut BufReader<File>,
    remaining: &'a mut u64,
    digest: &'a mut Digest,
}

impl Read for HashRead<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let limit = bytes
            .len()
            .min(usize::try_from(*self.remaining).unwrap_or(usize::MAX));
        let read = self.reader.read(&mut bytes[..limit])?;
        *self.remaining -= read as u64;
        self.digest.update(&bytes[..read]);
        Ok(read)
    }
}

pub(super) struct FrequencyRunReader {
    reader: BufReader<File>,
    remaining: u64,
    digest: Digest,
    records: u64,
    finished: bool,
    previous: Option<(Term, u8)>,
    config: LexicalProjectionConfig,
    _buffer_memory: Option<Grant>,
    control: SpillControl,
}

impl FrequencyRunReader {
    #[cfg(test)]
    pub(super) fn open(path: &Path, config: LexicalProjectionConfig) -> Result<Self> {
        Self::open_with_progress(path, config, None)
    }

    #[cfg(test)]
    pub(super) fn open_with_progress(
        path: &Path,
        config: LexicalProjectionConfig,
        progress: Option<&ReservedMemory>,
    ) -> Result<Self> {
        Self::open_with_control(path, config, &SpillControl::fixture(progress, None))
    }

    pub(super) fn open_with_control(
        path: &Path,
        config: LexicalProjectionConfig,
        control: &SpillControl,
    ) -> Result<Self> {
        control.check()?;
        let file = control.with_path(path, || Ok(File::open(path)?))?;
        let length = file.metadata()?.len();
        if length < HEADER.len() as u64 + FOOTER_BYTES || length > config.max_spill_bytes.get() {
            return Err(HawDBError::Storage(
                "invalid document frequency spill length".into(),
            ));
        }
        let buffer_memory = control.reserve(SPILL_IO_BUFFER_BYTES)?;
        let mut reader = BufReader::with_capacity(SPILL_IO_BUFFER_BYTES, file);
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        if &header != HEADER {
            return Err(HawDBError::Storage(
                "document frequency spill header mismatch".into(),
            ));
        }
        let mut digest = Digest::new();
        digest.update(&header);
        Ok(Self {
            reader,
            remaining: length - HEADER.len() as u64 - FOOTER_BYTES,
            digest,
            records: 0,
            finished: false,
            previous: None,
            config,
            _buffer_memory: buffer_memory,
            control: control.clone(),
        })
    }

    pub(super) fn next(&mut self) -> Result<Option<FrequencyRecord>> {
        self.control.check()?;
        if self.finished {
            return Ok(None);
        }
        if self.remaining == 0 {
            let records = read_u64(&mut self.reader)?;
            let digest = read_u64(&mut self.reader)?;
            if records != self.records || digest != self.digest.finish() {
                return Err(HawDBError::Storage(
                    "document frequency spill footer mismatch".into(),
                ));
            }
            self.finished = true;
            return Ok(None);
        }
        let mut reader = HashRead {
            reader: &mut self.reader,
            remaining: &mut self.remaining,
            digest: &mut self.digest,
        };
        let length = read_u32(&mut reader)? as usize;
        if length as u64 > self.config.max_term_bytes.get() {
            return Err(HawDBError::Storage(
                "lexical spill string exceeds its limit".into(),
            ));
        }
        let term = self.control.build_term(length, || {
            spill_memory::read_text(&mut reader, length, &self.control)
        })?;
        let mut field = [0u8; 1];
        reader.read_exact(&mut field)?;
        let repeated_weight = read_u64(&mut reader)?;
        let ordinal = read_u64(&mut reader)?;
        let unique_weight = read_u64(&mut reader)?;
        let record = FrequencyRecord {
            term,
            field: field[0],
            summary: PartialFieldFrequency {
                repeated_weight,
                first_event: Some((ordinal, unique_weight)),
            },
        };
        validate(&record, self.config)?;
        if self
            .previous
            .as_ref()
            .is_some_and(|(term, field)| (term.as_str(), *field) >= record.key())
        {
            return Err(HawDBError::Storage(
                "unordered document frequency spill records".into(),
            ));
        }
        self.previous = Some((record.term.clone(), record.field));
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("document frequency run count overflow".into()))?;
        Ok(Some(record))
    }
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

pub(super) fn merge_pair(
    mut left: FrequencyRun,
    mut right: FrequencyRun,
    pool: &mut SpillRuns,
) -> Result<FrequencyRun> {
    let merged = {
        let mut left_reader =
            FrequencyRunReader::open_with_control(&left.guard.path, pool.config, &pool.control)?;
        let mut right_reader =
            FrequencyRunReader::open_with_control(&right.guard.path, pool.config, &pool.control)?;
        let mut left_next = left_reader.next()?;
        let mut right_next = right_reader.next()?;
        let mut failed = false;
        let records = std::iter::from_fn(|| {
            if failed {
                return None;
            }
            let take_left = match (&left_next, &right_next) {
                (Some(left), Some(right)) => left.key() <= right.key(),
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => return None,
            };
            let (slot, reader) = if take_left {
                (&mut left_next, &mut left_reader)
            } else {
                (&mut right_next, &mut right_reader)
            };
            let record = slot.take().expect("selected nonempty input");
            match reader.next() {
                Ok(next) => {
                    *slot = next;
                    Some(Ok(record))
                }
                Err(error) => {
                    failed = true;
                    Some(Err(error))
                }
            }
        });
        write_run(records, pool, &mut FileSpillIo)?
    };
    // The reader scope has ended before unlink, including on Windows. Local
    // guards own both sources and the destination if either removal fails.
    left.guard.remove()?;
    right.guard.remove()?;
    Ok(merged)
}
