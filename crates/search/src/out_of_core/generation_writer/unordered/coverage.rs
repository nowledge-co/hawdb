// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::cmp::Ordering;

/// Compares a closed delta's complete resulting set with an independently
/// enumerated full source. Both streams share the original admission and stage;
/// callers cannot substitute a candidate after comparison. This proves content,
/// not source enumeration, source identity, or a host binding contract.
pub struct SearchOutOfCoreDeltaComparison<'reader> {
    original: Option<Run>,
    mutations: Cursor,
    expected: Cursor,
    writer: SearchOutOfCoreUnorderedMutationWriter<'reader>,
    limits: SearchBaselineComparisonLimits,
    report: SearchBaselineComparisonReport,
    resulting_documents: usize,
    started: bool,
    complete: bool,
    failed: bool,
    _memory: QueryMemoryLease,
}

#[derive(Default)]
struct Cursor {
    reader: Option<Reader>,
    pending: Option<run::Entry>,
    ended: bool,
}
impl Cursor {
    fn fill(&mut self, control: &mut Control) -> Result<bool> {
        if self.pending.is_some() || self.ended {
            return Ok(true);
        }
        let Some(reader) = &mut self.reader else {
            self.ended = true;
            return Ok(true);
        };
        let Some(entry) = reader.next_in_step(control)? else {
            return Ok(false);
        };
        self.ended = entry.is_none();
        self.pending = entry;
        Ok(true)
    }
}

impl<'reader> SearchOutOfCoreUnorderedMutationWriter<'reader> {
    /// Begin independent unique full-source intake after closing the delta.
    /// Cumulative ordering/I/O admission is retained across both phases.
    pub fn compare_complete_delta(
        mut self,
        limits: SearchBaselineComparisonLimits,
    ) -> Result<SearchOutOfCoreDeltaComparison<'reader>> {
        if self.failed
            || !self.ordering_complete()
            || self.inspection.started
            || self.control.unique_upserts
        {
            return Err(unusable());
        }
        let memory = self.reserve_session_metadata(std::mem::size_of::<
            SearchOutOfCoreDeltaComparison<'reader>,
        >())?;
        self.inner
            .writer
            .stage
            .reserve_additional_disk(limits.expanded_segment_bytes.get())?;
        let original = self.ordering.take_reduced();
        self.ordering = ordering::Ordering::new();
        self.control.unique_upserts = true;
        let resulting_documents = self.inner.base_reader().document_count();
        Ok(SearchOutOfCoreDeltaComparison {
            original,
            mutations: Cursor::default(),
            expected: Cursor::default(),
            writer: self,
            limits,
            report: Default::default(),
            resulting_documents,
            started: false,
            complete: false,
            failed: false,
            _memory: memory,
        })
    }
}

impl SearchOutOfCoreDeltaComparison<'_> {
    pub fn reserve_session_metadata(&self, bytes: usize) -> Result<QueryMemoryLease> {
        self.writer.reserve_session_metadata(bytes)
    }
    pub fn task_context(&self) -> &RuntimeTaskContext {
        self.writer.task_context()
    }
    pub fn ready_for_input(&self) -> bool {
        !self.failed && !self.started && self.writer.ready_for_input()
    }
    pub fn queue_expected(&mut self, document: SearchDocument) -> Result<()> {
        if self.failed || self.started {
            self.failed = true;
            return Err(unusable());
        }
        self.failed = true;
        self.writer.queue_upsert(document)?;
        self.failed = false;
        Ok(())
    }
    pub fn finish_input(&mut self) -> Result<()> {
        if self.failed || self.started {
            self.failed = true;
            return Err(unusable());
        }
        self.failed = true;
        self.writer.finish_input()?;
        self.failed = false;
        Ok(())
    }
    pub fn ordering_complete(&self) -> bool {
        !self.failed && self.writer.ordering_complete()
    }
    pub fn advance_ordering(
        &mut self,
        limits: SearchMutationSortStepLimits,
    ) -> Result<SearchMutationSortStep> {
        if self.failed || self.started {
            self.failed = true;
            return Err(unusable());
        }
        self.failed = true;
        let step = self.writer.advance_ordering(limits)?;
        self.failed = false;
        Ok(step)
    }
    pub fn report(&self) -> SearchBaselineComparisonReport {
        self.report
    }
    pub fn advance(
        &mut self,
        limits: SearchMutationSortStepLimits,
    ) -> Result<SearchMutationInspectionStep> {
        if self.failed {
            return Err(unusable());
        }
        self.failed = true;
        self.writer.control.begin_step(limits);
        let result = self.advance_inner();
        let usage = self.writer.control.end_step();
        let visited = result?;
        self.failed = false;
        Ok(SearchMutationInspectionStep {
            usage,
            visited,
            complete: self.complete,
        })
    }
    fn advance_inner(&mut self) -> Result<bool> {
        checkpoint(self.writer.task_context())?;
        if !self.writer.ordering_complete() {
            return Err(unusable());
        }
        if self.complete {
            return Ok(false);
        }
        if !self.started {
            if !self
                .writer
                .control
                .allow(1, 2 * spool::SPOOL_HEADER.len() as u64, 0)?
            {
                return Ok(false);
            }
            self.mutations.reader = self
                .original
                .take()
                .map(|run| Reader::open(run, &mut self.writer.control, &self.writer.inner.writer))
                .transpose()?;
            self.expected.reader = self
                .writer
                .ordering
                .take_reduced()
                .map(|run| Reader::open(run, &mut self.writer.control, &self.writer.inner.writer))
                .transpose()?;
            self.started = true;
        }
        if !self.mutations.fill(&mut self.writer.control)?
            || !self.expected.fill(&mut self.writer.control)?
        {
            return Ok(false);
        }
        let mutation = self.mutations.pending.as_ref();
        let expected = self.expected.pending.as_ref();
        let work = mutation
            .zip(expected)
            .map_or(1, |(m, e)| 1 + m.id().len().min(e.id().len()));
        // Reserve comparison before inspecting keys; retained records carry
        // their admitted lengths across a quantum yield.
        let bytes = [mutation, expected]
            .into_iter()
            .flatten()
            .try_fold(0u64, |sum, entry| {
                sum.checked_add(match entry {
                    run::Entry::Upsert(record, _) => record.encoded_bytes,
                    _ => 0,
                })
                .ok_or_else(unusable)
            })?;
        if !self.writer.control.allow(work, bytes, 0)? {
            return Ok(false);
        }
        let order = match (mutation, expected) {
            (Some(m), Some(e)) => m.id().cmp(e.id()),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => {
                if self.report.compared_documents != self.resulting_documents {
                    return Err(HawDBError::Execution(
                        "complete delta retains unexpected base documents".into(),
                    ));
                }
                self.complete = true;
                return Ok(true);
            }
        };
        let bytes = match (order, mutation, expected) {
            (Ordering::Equal, Some(run::Entry::Upsert(m, _)), Some(run::Entry::Upsert(e, _))) => m
                .encoded_bytes
                .checked_add(e.encoded_bytes)
                .ok_or_else(unusable)?,
            (Ordering::Greater, _, Some(run::Entry::Upsert(e, _))) => e.encoded_bytes,
            _ => 0,
        };
        let base = self.writer.inner.base_reader();
        let memory = self.writer.inner.writer.memory.clone();
        let task = self.writer.task_context().clone();
        match order {
            Ordering::Less => {
                let Some(run::Entry::Delete(id)) = mutation else {
                    return Err(HawDBError::Execution(
                        "delta upsert is absent from the complete source".into(),
                    ));
                };
                baseline::reserve_lookup(&mut self.report, self.limits)?;
                if base.segment_for_document(id)?.is_some() {
                    self.resulting_documents = self
                        .resulting_documents
                        .checked_sub(1)
                        .ok_or_else(unusable)?;
                }
            }
            Ordering::Equal => {
                let (Some(run::Entry::Upsert(actual, _)), Some(run::Entry::Upsert(expected, _))) =
                    (mutation, expected)
                else {
                    return Err(HawDBError::Execution(
                        "delta deletes a complete-source document".into(),
                    ));
                };
                self.writer.control.read(bytes)?;
                if !actual.encoded_eq(expected, &memory, &task)? {
                    return Err(HawDBError::Execution(
                        "delta document differs from the complete source".into(),
                    ));
                }
                baseline::reserve_lookup(&mut self.report, self.limits)?;
                if base.segment_for_document(&actual.header.id)?.is_none() {
                    self.resulting_documents = self
                        .resulting_documents
                        .checked_add(1)
                        .ok_or_else(unusable)?;
                }
            }
            Ordering::Greater => {
                let Some(run::Entry::Upsert(expected, _)) = expected else {
                    return Err(unusable());
                };
                let path = io::GenerationIo::new(&memory, &task).path(
                    &self.writer.inner.writer.stage.path,
                    Path::new("baseline-source.body"),
                )?;
                baseline::compare_record(
                    base,
                    expected,
                    &mut self.writer.control,
                    self.limits,
                    &mut self.report,
                    (&memory, &task, &path),
                )?;
            }
        }
        if order != Ordering::Greater {
            self.mutations.pending = None;
        }
        if order != Ordering::Less {
            self.expected.pending = None;
            self.report.compared_documents = self
                .report
                .compared_documents
                .checked_add(1)
                .ok_or_else(unusable)?;
        }
        Ok(true)
    }
}

impl<'reader> SearchOutOfCoreDeltaComparison<'reader> {
    /// Restores only the original, now compared delta. Its expected selector,
    /// source epoch and admission cannot be replaced by the caller.
    pub fn finish(
        mut self,
    ) -> Result<(SearchUnorderedMutationReport, SearchOutOfCorePreparedUpdate)> {
        if self.failed || !self.complete {
            return Err(unusable());
        }
        checkpoint(self.writer.task_context())?;
        self.writer
            .ordering
            .restore_reduced(self.mutations.reader.take().map(Reader::into_run));
        self.writer.control.unique_upserts = false;
        if let Some(reader) = self.expected.reader.take() {
            reader.remove(&self.writer.control)?;
        }
        self.writer.prepare_publication()
    }
}
