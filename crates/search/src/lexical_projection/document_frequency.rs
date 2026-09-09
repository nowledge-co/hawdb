//! Document-local analysis with an in-memory fast path and private sorted runs.

use super::*;

mod run;
use run::{merge_pair, write_run, FrequencyRun, FrequencyRunReader};

#[cfg(test)]
mod proof;

#[cfg(test)]
mod tests;

/// The first event of either kind suppresses all later unique-in-field events.
/// Partial sums alone cannot preserve this rule across physical spill boundaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PartialFieldFrequency {
    repeated_weight: u64,
    first_event: Option<(u64, u64)>,
}

impl PartialFieldFrequency {
    fn push(&mut self, ordinal: u64, occurrence: TokenOccurrence, weight: u64) -> Result<()> {
        self.merge(Self {
            repeated_weight: if occurrence == TokenOccurrence::Repeated {
                weight
            } else {
                0
            },
            first_event: Some((
                ordinal,
                if occurrence == TokenOccurrence::UniqueInField {
                    weight
                } else {
                    0
                },
            )),
        })
    }

    fn merge(&mut self, incoming: Self) -> Result<()> {
        let repeated_weight = self
            .repeated_weight
            .checked_add(incoming.repeated_weight)
            .ok_or_else(|| SkeinError::Storage("document term frequency overflow".into()))?;
        if let Some(first) = incoming
            .first_event
            .filter(|first| self.first_event.is_none_or(|current| first.0 < current.0))
        {
            self.first_event = Some(first);
        }
        self.repeated_weight = repeated_weight;
        Ok(())
    }

    fn frequency(self) -> Result<u64> {
        self.repeated_weight
            .checked_add(self.first_event.map_or(0, |(_, weight)| weight))
            .ok_or_else(|| SkeinError::Storage("document term frequency overflow".into()))
    }
}

#[derive(Debug)]
struct FrequencyRecord {
    term: String,
    field: u8,
    summary: PartialFieldFrequency,
}

impl FrequencyRecord {
    fn key(&self) -> (&str, u8) {
        (&self.term, self.field)
    }
    fn encoded_len(&self) -> u64 {
        self.term.len() as u64 + 29
    }
}

// Binary carries keep at most one live run per level, instead of retaining
// one path for every flushed chunk of a large document.
struct DocumentRuns {
    levels: [Option<FrequencyRun>; usize::BITS as usize],
}

impl Default for DocumentRuns {
    fn default() -> Self {
        Self {
            levels: std::array::from_fn(|_| None),
        }
    }
}

impl DocumentRuns {
    fn insert(&mut self, mut incoming: FrequencyRun, pool: &mut SpillRuns) -> Result<()> {
        for level in &mut self.levels {
            let Some(previous) = level.take() else {
                *level = Some(incoming);
                return Ok(());
            };
            incoming = merge_pair(previous, incoming, pool)?;
        }
        Err(SkeinError::Storage(
            "document frequency spill levels exhausted".into(),
        ))
    }

    fn finish(self, pool: &mut SpillRuns) -> Result<FrequencyRun> {
        let mut merged = None;
        for run in self.levels.into_iter().flatten() {
            merged = Some(match merged {
                Some(previous) => merge_pair(previous, run, pool)?,
                None => run,
            });
        }
        merged.ok_or_else(|| SkeinError::Storage("document frequency spill has no runs".into()))
    }
}

struct SpillingAnalysis {
    records: Vec<FrequencyRecord>,
    string_bytes: u64,
    buffer_limit: u64,
    lower_bound: u64,
    runs: DocumentRuns,
}

impl SpillingAnalysis {
    fn push(&mut self, record: FrequencyRecord, pool: &mut SpillRuns) -> Result<()> {
        let lower_bound = self
            .lower_bound
            .checked_add(record.summary.repeated_weight)
            .ok_or_else(|| SkeinError::Storage("lexical document length overflow".into()))?;
        admit_document_len(lower_bound, pool.config)?;
        let string_bytes = record.term.capacity() as u64;
        let slot_bytes = std::mem::size_of::<FrequencyRecord>() as u64;
        let mut capacity = self.records.capacity();
        if self.records.len() == capacity {
            capacity = capacity.saturating_mul(2).max(1);
        }
        let required = self
            .string_bytes
            .saturating_add(string_bytes)
            .saturating_add((capacity as u64).saturating_mul(slot_bytes));
        if required > self.buffer_limit {
            self.flush(pool)?;
            capacity = 1;
        }
        if string_bytes.saturating_add(slot_bytes.saturating_mul(capacity as u64))
            > self.buffer_limit
        {
            return Err(SkeinError::Storage(
                "one document frequency record exceeds analyzer bytes".into(),
            ));
        }
        if capacity > self.records.capacity() {
            self.records
                .try_reserve_exact(capacity - self.records.len())
                .map_err(|error| {
                    SkeinError::Storage(format!("reserve document frequency records: {error}"))
                })?;
        }
        self.string_bytes = self.string_bytes.saturating_add(string_bytes);
        self.records.push(record);
        self.lower_bound = lower_bound;
        Ok(())
    }

    fn flush(&mut self, pool: &mut SpillRuns) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records
            .sort_unstable_by(|left, right| left.key().cmp(&right.key()));
        let records = std::mem::take(&mut self.records);
        self.string_bytes = 0;
        let run = write_run(records.into_iter().map(Ok), pool, &mut FileSpillIo)?;
        self.runs.insert(run, pool)
    }

    fn finish(mut self, pool: &mut SpillRuns) -> Result<FrequencyRun> {
        self.flush(pool)?;
        self.runs.finish(pool)
    }
}

pub(super) enum AnalyzedDocument<'a> {
    Resident(DocumentAnalysis<'a>),
    Spilled {
        run: FrequencyRun,
        document_len: u32,
        read_memory_bytes: u64,
    },
}

impl AnalyzedDocument<'_> {
    pub(super) fn document_len(&self) -> u32 {
        match self {
            Self::Resident(analysis) => analysis.document_len,
            Self::Spilled { document_len, .. } => *document_len,
        }
    }

    /// The callback receives the analysis state that still coexists with the
    /// corpus posting buffer. The resident map uses the existing estimated
    /// charge; disk reads have a conservative fixed progress reservation.
    pub(super) fn visit(
        self,
        config: LexicalProjectionConfig,
        mut emit: impl FnMut(String, u32, u64) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Resident(analysis) => {
                let mut resident = analysis
                    .required_map_bytes(analysis.resident_bytes, analysis.frequencies.len());
                for (term, entry) in analysis.frequencies {
                    let marker_bytes =
                        (std::mem::size_of::<AnalyzedTerm>() - std::mem::size_of::<u32>()) as u64;
                    resident = resident.saturating_sub(term.len() as u64 + 32 + marker_bytes);
                    emit(term, entry.frequency, resident)?;
                }
                Ok(())
            }
            Self::Spilled {
                run,
                read_memory_bytes,
                ..
            } => visit_frequencies(&run, config, |term, frequency| {
                emit(term, frequency, read_memory_bytes)
            }),
        }
    }
}

fn admit_document_len(length: u64, config: LexicalProjectionConfig) -> Result<u32> {
    if length > config.max_document_tokens.get() as u64 {
        return Err(SkeinError::Storage(format!(
            "lexical document produced at least {length} tokens, exceeding {}",
            config.max_document_tokens
        )));
    }
    u32::try_from(length)
        .map_err(|_| SkeinError::Storage("lexical document length exceeds u32".into()))
}

fn progress_memory(pool: &SpillRuns, document_id: &str) -> u64 {
    let levels = u64::from(usize::BITS - pool.config.max_spill_runs.get().leading_zeros());
    let paths = levels.saturating_mul(pool.root.as_os_str().len() as u64 + 96);
    // Three fixed I/O buffers, current/read-order/reduction keys, run metadata,
    // and one final posting. Source and opaque analyzer scratch remain separate.
    (3u64 * SPILL_IO_BUFFER_BYTES as u64)
        .saturating_add(
            8u64.saturating_mul(
                pool.config
                    .max_term_bytes
                    .get()
                    .saturating_add(std::mem::size_of::<FrequencyRecord>() as u64),
            ),
        )
        .saturating_add(paths)
        .saturating_add(std::mem::size_of::<DocumentRuns>() as u64)
        .saturating_add(document_id.len() as u64)
        .saturating_add(128)
}

pub(super) fn analyze<'a>(
    document: &'a SearchDocument,
    analyzer: &SearchAnalyzerLexicon,
    pool: &mut SpillRuns,
    pending: &mut Vec<Posting>,
    pending_bytes: &mut u64,
) -> Result<AnalyzedDocument<'a>> {
    let config = pool.config;
    admit_document_source(document, config)?;
    let progress = progress_memory(pool, &document.id);
    let base = document.id.len() as u64 + 64;
    let spill_buffer = config
        .build_memory_bytes
        .get()
        .checked_sub(progress)
        .filter(|bytes| *bytes >= base);
    let map_limit = spill_buffer.unwrap_or(config.build_memory_bytes.get());
    if base.saturating_add(*pending_bytes) > map_limit {
        flush_pending(pool, pending, pending_bytes)?;
    }
    let mut resident = Some(DocumentAnalysis::new(
        &document.id,
        LexicalProjectionConfig {
            build_memory_bytes: NonZeroU64::new(map_limit).unwrap(),
            ..config
        },
    )?);
    let mut spilled: Option<SpillingAnalysis> = None;
    let mut ordinal = 0u64;
    for (field, (text, weight)) in document_token_fields(document).enumerate() {
        let field = u8::try_from(field).expect("at most six analysis fields");
        visit_token_list(text, analyzer, |term, occurrence| {
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| SkeinError::Storage("document token ordinal overflow".into()))?;
            if term.len() as u64 > config.max_term_bytes.get() {
                return Err(SkeinError::Storage(format!(
                    "lexical term uses {} bytes, exceeding {}",
                    term.len(),
                    config.max_term_bytes
                )));
            }
            if let Some(analysis) = resident.as_ref() {
                let new_term = !analysis.frequencies.contains_key(&term);
                let required = analysis.required_map_bytes(
                    analysis.resident_bytes.saturating_add(if new_term {
                        (term.len() as u64).saturating_add(32)
                    } else {
                        0
                    }),
                    analysis
                        .frequencies
                        .len()
                        .saturating_add(usize::from(new_term)),
                );
                if required.saturating_add(*pending_bytes) > map_limit {
                    flush_pending(pool, pending, pending_bytes)?;
                }
                if required > map_limit {
                    let Some(buffer_limit) = spill_buffer else {
                        return Err(SkeinError::Storage(format!("document frequency spill needs at least {} analyzer bytes for progress", progress.saturating_add(base))));
                    };
                    if config.max_merge_fan_in.get() < 2 {
                        return Err(SkeinError::Storage(
                            "lexical merge fan-in must be at least two".into(),
                        ));
                    }
                    let analysis = resident.take().expect("resident accumulator");
                    let mut external = SpillingAnalysis {
                        records: Vec::new(),
                        string_bytes: 0,
                        buffer_limit,
                        lower_bound: u64::from(analysis.document_len),
                        runs: DocumentRuns::default(),
                    };
                    if !analysis.frequencies.is_empty() {
                        let prefix = analysis.frequencies.into_iter().map(|(term, entry)| {
                            Ok(FrequencyRecord {
                                term,
                                field: entry.last_field,
                                summary: PartialFieldFrequency {
                                    repeated_weight: u64::from(entry.frequency),
                                    first_event: Some((0, 0)),
                                },
                            })
                        });
                        let run = write_run(prefix, pool, &mut FileSpillIo)?;
                        external.runs.insert(run, pool)?;
                    }
                    spilled = Some(external);
                }
            }
            if let Some(analysis) = resident.as_mut() {
                analysis.push(term, occurrence, field, weight)
            } else {
                let mut summary = PartialFieldFrequency::default();
                summary.push(ordinal, occurrence, weight as u64)?;
                spilled.as_mut().expect("spilling accumulator").push(
                    FrequencyRecord {
                        term,
                        field,
                        summary,
                    },
                    pool,
                )
            }
        })?;
    }
    if let Some(analysis) = resident {
        return Ok(AnalyzedDocument::Resident(analysis));
    }
    let run = spilled.expect("spilling accumulator").finish(pool)?;
    let mut document_len = 0u64;
    visit_frequencies(&run, config, |_, frequency| {
        document_len = document_len
            .checked_add(u64::from(frequency))
            .ok_or_else(|| SkeinError::Storage("lexical document length overflow".into()))?;
        admit_document_len(document_len, config)?;
        Ok(())
    })?;
    Ok(AnalyzedDocument::Spilled {
        run,
        document_len: admit_document_len(document_len, config)?,
        read_memory_bytes: progress,
    })
}

fn visit_frequencies(
    run: &FrequencyRun,
    config: LexicalProjectionConfig,
    mut emit: impl FnMut(String, u32) -> Result<()>,
) -> Result<()> {
    let mut reader = FrequencyRunReader::open(&run.guard.path, config)?;
    let mut current: Option<(String, u64)> = None;
    while let Some(record) = reader.next()? {
        let frequency = record.summary.frequency()?;
        match current.as_mut() {
            Some((term, total)) if term == &record.term => {
                *total = total.checked_add(frequency).ok_or_else(|| {
                    SkeinError::Storage("document term frequency overflow".into())
                })?;
            }
            _ => {
                if let Some((term, total)) = current.take() {
                    emit(term, admit_document_len(total, config)?)?;
                }
                current = Some((record.term, frequency));
            }
        }
    }
    if let Some((term, total)) = current {
        emit(term, admit_document_len(total, config)?)?;
    }
    Ok(())
}

pub(super) fn flush_pending(
    pool: &mut SpillRuns,
    pending: &mut Vec<Posting>,
    bytes: &mut u64,
) -> Result<()> {
    if !pending.is_empty() {
        pool.spill(pending)?;
    }
    // Capacity must not stay resident beside the document-local accumulator.
    *pending = Vec::new();
    *bytes = 0;
    Ok(())
}
