// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

/// Artifact ordering limits, not source/capture authority. Phase zero is C;
/// larger ordinals supersede it. The facade must bind actual native phase Ends.
#[derive(Debug, Clone, Copy)]
pub struct SearchOwnerOverlayLimits {
    pub max_owner_key_bytes: NonZeroUsize,
    pub max_phases: NonZeroU64,
    /// Cumulative across owner intake, all intermediate runs, transfer and final
    /// document ordering. Input operations include transferred final documents.
    pub ordering: SearchUnorderedMutationLimits,
}
impl Default for SearchOwnerOverlayLimits {
    fn default() -> Self {
        Self {
            max_owner_key_bytes: NonZeroUsize::new(4096).unwrap(),
            max_phases: NonZeroU64::new(64).unwrap(),
            ordering: Default::default(),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchOwnerOverlayReport {
    /// Includes repeated phase states and explicit None inputs.
    pub owner_inputs: usize,
    /// Includes owners whose final state is None.
    pub reduced_owners: usize,
    pub documents: usize,
    /// Shared cumulative counters: input operations include owner inputs and
    /// transferred documents; output operations include reduced owners and
    /// reduced documents. These counters are not source coverage receipts.
    pub sorting: SearchUnorderedMutationReport,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Input,
    OwnerSort,
    Transfer,
    DocumentSort,
    Complete,
}

/// Private artifact builder: exact owner/phase ordering precedes strict final
/// document-ID ordering. None is an owner tombstone; equal-phase conflicting
/// states refuse even if a later phase supersedes them. One stage, task, memory
/// ledger and actual governor permit cover both sorts and the prepared result.
///
/// Owner keys and phase numbers supplied here do NOT certify canonical identity,
/// complete capture, mapping, baseline or final-E dependencies. Native facade
/// coverage must supply those premises before activation. Artifact construction
/// after ordering remains blocking. Business document IDs are never rewritten.
pub struct SearchOutOfCoreOwnerReplacementWriter {
    // Close the retained owner file before the enclosing stage cleanup owner.
    source: Option<Reader>,
    inner: SearchOutOfCoreUnorderedMutationWriter<'static>,
    limits: SearchOwnerOverlayLimits,
    phase: Phase,
    owner_inputs: usize,
    reduced_owners: usize,
    _memory: QueryMemoryLease,
}

pub(in crate::out_of_core::generation_writer) fn from_inner(
    inner: SearchOutOfCoreMutationWriter<'_>,
    limits: SearchOwnerOverlayLimits,
    max_id_bytes: usize,
) -> Result<SearchOutOfCoreOwnerReplacementWriter> {
    let inner = inner.into_owned_replacement()?;
    let mut inner = super::from_inner(inner, limits.ordering, max_id_bytes)?;
    inner.control.owner_order = true;
    inner.control.max_owner_bytes = limits.max_owner_key_bytes.get();
    let memory = inner
        .reserve_session_metadata(std::mem::size_of::<SearchOutOfCoreOwnerReplacementWriter>())?;
    Ok(SearchOutOfCoreOwnerReplacementWriter {
        source: None,
        inner,
        limits,
        phase: Phase::Input,
        owner_inputs: 0,
        reduced_owners: 0,
        _memory: memory,
    })
}
impl SearchOutOfCoreOwnerReplacementWriter {
    pub fn task_context(&self) -> &RuntimeTaskContext {
        self.inner.task_context()
    }
    pub fn report(&self) -> SearchOwnerOverlayReport {
        SearchOwnerOverlayReport {
            owner_inputs: self.owner_inputs,
            reduced_owners: self.reduced_owners,
            documents: if self.phase == Phase::Complete {
                self.inner.control.report.output_operations - self.reduced_owners
            } else {
                0
            },
            sorting: self.inner.report(),
        }
    }
    pub fn ready_for_input(&self) -> bool {
        self.phase == Phase::Input && self.inner.ready_for_input()
    }
    pub fn ordering_complete(&self) -> bool {
        self.phase == Phase::Complete && !self.inner.failed
    }
    pub fn queue_owner_state(
        &mut self,
        owner: &str,
        phase: u64,
        document: Option<SearchDocument>,
    ) -> Result<()> {
        if self.inner.failed {
            return Err(unusable());
        }
        self.inner.failed = true;
        checkpoint(self.task_context())?;
        if self.phase != Phase::Input
            || !self.inner.ordering.ready_for_input()
            || owner.is_empty()
            || owner.len() > self.limits.max_owner_key_bytes.get()
            || phase >= self.limits.max_phases.get()
            || self.inner.control.report.input_operations
                >= self.limits.ordering.max_input_operations.get()
        {
            return Err(unusable());
        }
        let mut bytes = checked_add(owner.len(), Term::tracking_overhead())?;
        let key = Term::copy(owner, Some(&self.inner.inner.writer.memory))?;
        let body = if let Some(document) = document {
            self.inner.control.check_input(&document.id)?;
            if document.metadata.len() > self.inner.inner.writer.options.max_metadata_fields.get() {
                return Err(unusable());
            }
            bytes = checked_add(bytes, document_bytes(&document)?)?;
            let admitted = self.inner.inner.writer.memory.admit_document(document)?;
            let encoded = DocumentEncoding::new_with_context(&admitted, Some(self.task_context()))?
                .len() as u64;
            Some((admitted, encoded))
        } else {
            None
        };
        self.inner
            .enqueue(Buffered::Owner(OwnerStamp { key, phase }, body), bytes)?;
        self.owner_inputs += 1;
        self.inner.failed = false;
        Ok(())
    }
    /// Close all artifact input and bind its final epoch. This numeric tag is
    /// not native evidence that any source interval or mapping was complete.
    pub fn finish_input(&mut self, source_epoch: u64) -> Result<()> {
        if self.inner.failed {
            return Err(unusable());
        }
        self.inner.failed = true;
        checkpoint(self.task_context())?;
        if self.phase != Phase::Input {
            return Err(unusable());
        }
        self.inner
            .inner
            .writer
            .options
            .bind_replacement_epoch(source_epoch)?;
        self.inner.ordering.finish_input();
        self.phase = Phase::OwnerSort;
        self.inner.failed = false;
        Ok(())
    }
    fn advance_inner(&mut self) -> Result<()> {
        self.inner.ordering.advance(
            &mut self.inner.buffer,
            &mut self.inner.slots,
            &mut self.inner.levels,
            &mut self.inner.payload,
            &mut self.inner.control,
            &mut self.inner.inner.writer,
        )
    }
    fn advance(&mut self) -> Result<()> {
        loop {
            match self.phase {
                Phase::Input => {
                    self.advance_inner()?;
                    return Ok(());
                }
                Phase::OwnerSort => {
                    self.advance_inner()?;
                    if !self.inner.ordering.complete() {
                        return Ok(());
                    }
                    if !self.inner.control.allow(1, 8, 0)? {
                        return Ok(());
                    }
                    self.reduced_owners = self.inner.control.report.output_operations;
                    let run = self.inner.ordering.take_reduced();
                    self.source = run
                        .map(|run| {
                            Reader::open(run, &mut self.inner.control, &self.inner.inner.writer)
                        })
                        .transpose()?;
                    self.inner.ordering = ordering::Ordering::new();
                    self.inner.control.owner_order = false;
                    self.inner.control.unique_upserts = true;
                    if self.source.is_some() {
                        self.phase = Phase::Transfer;
                    } else {
                        self.inner.ordering.finish_input();
                        self.phase = Phase::DocumentSort;
                    }
                }
                Phase::Transfer => {
                    if !self.inner.ordering.ready_for_input() {
                        self.advance_inner()?;
                        if !self.inner.ordering.ready_for_input() {
                            return Ok(());
                        }
                    }
                    let Some(next) = self
                        .source
                        .as_mut()
                        .unwrap()
                        .next_in_step(&mut self.inner.control)?
                    else {
                        return Ok(());
                    };
                    match next {
                        Some(run::Entry::Owner(_, body)) => {
                            if let Some((record, flag)) = body {
                                self.inner.control.check_input(&record.header.id)?;
                                let bytes = record.retained_bytes()?;
                                self.inner.enqueue(Buffered::Spool(record, flag), bytes)?;
                            }
                        }
                        None => {
                            self.inner.ordering.finish_input();
                            self.phase = Phase::DocumentSort;
                        }
                        _ => return Err(unusable()),
                    }
                }
                Phase::DocumentSort => {
                    self.advance_inner()?;
                    if !self.inner.ordering.complete() {
                        return Ok(());
                    }
                    if !self.inner.control.allow(1, 0, 0)? {
                        return Ok(());
                    }
                    if let Some(source) = self.source.take() {
                        source.remove(&self.inner.control)?;
                    }
                    self.phase = Phase::Complete;
                }
                Phase::Complete => return Ok(()),
            }
        }
    }
    pub fn advance_ordering(
        &mut self,
        limits: SearchMutationSortStepLimits,
    ) -> Result<SearchMutationSortStep> {
        if self.inner.failed {
            return Err(unusable());
        }
        self.inner.failed = true;
        checkpoint(self.task_context())?;
        self.inner.control.begin_step(limits);
        let result = self.advance();
        let usage = self.inner.control.end_step();
        result?;
        self.inner.failed = false;
        Ok(SearchMutationSortStep {
            usage,
            ready_for_input: self.ready_for_input(),
            ordering_complete: self.ordering_complete(),
        })
    }
    /// Consumes completed ordering only. Source/capture/binding validation must
    /// still be performed by the facade that holds publication authority.
    pub fn prepare_publication(
        mut self,
    ) -> Result<(SearchOwnerOverlayReport, SearchOutOfCorePreparedGeneration)> {
        if !self.ordering_complete() {
            return Err(unusable());
        }
        self.inner.failed = true;
        checkpoint(self.task_context())?;
        if let Some(run) = self.inner.ordering.take_reduced() {
            let mut source = Reader::open(run, &mut self.inner.control, &self.inner.inner.writer)?;
            while let Some(entry) = source.next(&mut self.inner.control)? {
                let run::Entry::Upsert(record, flag) = entry else {
                    return Err(unusable());
                };
                self.inner.control.read(record.encoded_bytes)?;
                self.inner.inner.writer.push_spool_record(*record, flag)?;
            }
            source.remove(&self.inner.control)?;
        }
        let report = self.report();
        Ok((report, self.inner.inner.writer.prepare_publication()?))
    }
}

#[cfg(test)]
mod tests;
