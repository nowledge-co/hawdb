// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Governed external ordering for fixed-snapshot projection mutations.

use super::*;
use crate::build_memory::document_bytes;
use crate::build_term::Term;
use crate::out_of_core::mutation_run::SearchMutationOperation;
use crate::SearchOutOfCoreReader;
use std::num::{NonZeroU64, NonZeroUsize};

mod baseline;
pub(super) mod replacement;
pub use baseline::{
    SearchBaselineComparisonLimits, SearchBaselineComparisonReport, SearchMatchedBaseline,
    SearchOutOfCoreBaselineComparison,
};
pub use replacement::SearchOutOfCoreUnorderedReplacementWriter;
pub(super) mod owner;
pub use owner::{
    SearchOutOfCoreOwnerReplacementWriter, SearchOwnerOverlayLimits, SearchOwnerOverlayReport,
};
mod heap;
mod inspection;
pub use inspection::{SearchMutationInspectionInput, SearchMutationInspectionStep};
mod ordering;
mod run;
use run::{Control, Reader, Run};

/// Private run admission. Spill/read bytes and merge visits are cumulative,
/// including duplicates and intermediate runs. The payload buffer is separate
/// from the task's shared hard memory ledger, which also owns slots/paths/I/O.
#[derive(Debug, Clone, Copy)]
pub struct SearchUnorderedMutationLimits {
    pub max_buffer_records: NonZeroUsize,
    pub max_buffer_payload_bytes: NonZeroUsize,
    pub max_input_operations: NonZeroUsize,
    pub max_spill_bytes: NonZeroU64,
    pub max_read_bytes: NonZeroU64,
    pub max_merge_visits: NonZeroUsize,
    pub max_sort_work: NonZeroUsize,
}
impl Default for SearchUnorderedMutationLimits {
    fn default() -> Self {
        Self {
            max_buffer_records: NonZeroUsize::new(1024).unwrap(),
            max_buffer_payload_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
            max_input_operations: NonZeroUsize::new(1_000_000).unwrap(),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024).unwrap(),
            max_read_bytes: NonZeroU64::new(16 * 1024 * 1024 * 1024).unwrap(),
            max_merge_visits: NonZeroUsize::new(64_000_000).unwrap(),
            max_sort_work: NonZeroUsize::new(1_000_000_000).unwrap(),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchUnorderedMutationReport {
    pub input_operations: usize,
    pub output_operations: usize,
    pub merged_records: usize,
    pub reserved_spill_bytes: u64,
    pub reserved_read_bytes: u64,
    pub peak_buffer_payload_bytes: usize,
    pub runs_created: u64,
    pub sort_work: usize,
}

/// One ordering quantum. Read/spill bytes are admitted logical I/O; a buffered
/// flush or filesystem operation does not provide a wall-clock latency bound.
/// A complete record/comparison must fit one quantum or the attempt refuses.
#[derive(Debug, Clone, Copy)]
pub struct SearchMutationSortStepLimits {
    pub work: NonZeroUsize,
    pub read_bytes: NonZeroU64,
    pub spill_bytes: NonZeroU64,
}
impl Default for SearchMutationSortStepLimits {
    fn default() -> Self {
        Self {
            work: NonZeroUsize::new(65_536).unwrap(),
            read_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            spill_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchMutationSortStepUsage {
    pub work: usize,
    pub read_bytes: u64,
    pub spill_bytes: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchMutationSortStep {
    pub usage: SearchMutationSortStepUsage,
    pub ready_for_input: bool,
    pub ordering_complete: bool,
}

enum Buffered {
    Upsert(AdmittedDocument, u64),
    Delete(Term),
    Owner(OwnerStamp, Option<(AdmittedDocument, u64)>),
    Spool(Box<spool::SpoolRecord>, bool),
}
struct OwnerStamp {
    key: Term,
    phase: u64,
}
impl Buffered {
    fn id(&self) -> &str {
        match self {
            Self::Upsert(doc, _) => &doc.id,
            Self::Delete(id) => id,
            Self::Owner(owner, _) => &owner.key,
            Self::Spool(record, _) => &record.header.id,
        }
    }
    fn key(&self) -> (&str, u64) {
        (
            self.id(),
            match self {
                Self::Owner(owner, _) => owner.phase,
                _ => 0,
            },
        )
    }
    fn copy_cost(&self) -> Result<(u64, u64)> {
        let (read, spill) = match self {
            Self::Delete(id) => (0, id.len() as u64 + 17),
            Self::Upsert(_, bytes) => (0, bytes.checked_add(18).ok_or_else(unusable)?),
            Self::Spool(record, _) => (
                record.encoded_bytes,
                record.encoded_bytes.checked_add(18).ok_or_else(unusable)?,
            ),
            Self::Owner(stamp, body) => (
                0,
                (stamp.key.len() as u64)
                    .checked_add(26)
                    .and_then(|n| {
                        body.as_ref()
                            .map_or(Some(n), |(_, bytes)| n.checked_add(17)?.checked_add(*bytes))
                    })
                    .ok_or_else(unusable)?,
            ),
        };
        Ok((read, spill))
    }
}

/// Sorts arbitrary input order into one invisible expected-base update.
/// All upserts must describe the same source snapshot: identical encoded
/// documents coalesce, conflicting upserts fail, and upserts win over deletes.
/// This is an artifact builder, not a graph/host batch coverage receipt.
/// Queue one owned input, advance ordering in bounded quanta, then close intake
/// and finish the private reduced run. The blocking convenience methods drive
/// those same states. Artifact construction after ordering remains blocking.
/// A finite shared memory bound requires a task memory reservation. Caller-owned
/// input construction precedes admission; queue-time structural measurement is
/// bounded by the admitted document/header configuration, not the sort quantum.
pub struct SearchOutOfCoreUnorderedMutationWriter<'a> {
    buffer: Vec<Buffered>,
    slots: QueryMemoryLease,
    levels: [Option<Run>; usize::BITS as usize],
    // Close pending files before the inner stage cleanup owner drops.
    ordering: ordering::Ordering,
    inspection: inspection::Inspection,
    inner: SearchOutOfCoreMutationWriter<'a>,
    control: Control,
    payload: usize,
    failed: bool,
    _memory: QueryMemoryLease,
}
impl SearchOutOfCoreGenerationWriter {
    pub fn prepare_unordered_delta<'a>(
        reader: &'a SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
        limits: SearchUnorderedMutationLimits,
        task: RuntimeTaskContext,
    ) -> Result<SearchOutOfCoreUnorderedMutationWriter<'a>> {
        let inner = Self::prepare_streamed_delta_with_context(reader, options, task)?;
        from_inner(
            inner,
            limits,
            reader.config().max_document_header_bytes.get(),
        )
    }
}

pub(super) fn from_inner<'a>(
    inner: SearchOutOfCoreMutationWriter<'a>,
    limits: SearchUnorderedMutationLimits,
    max_id_bytes: usize,
) -> Result<SearchOutOfCoreUnorderedMutationWriter<'a>> {
    let memory = inner.writer.memory.clone();
    let owner = memory.retained.reserve(std::mem::size_of::<
        SearchOutOfCoreUnorderedMutationWriter<'a>,
    >())?;
    let slots = memory.retained.reserve(0)?;
    let control = Control::new(
        limits,
        memory,
        inner.writer.task_context.clone(),
        max_id_bytes,
    );
    Ok(SearchOutOfCoreUnorderedMutationWriter {
        buffer: Vec::new(),
        slots,
        levels: std::array::from_fn(|_| None),
        ordering: ordering::Ordering::new(),
        inspection: inspection::Inspection::new(),
        inner,
        control,
        payload: 0,
        failed: false,
        _memory: owner,
    })
}
impl SearchOutOfCoreUnorderedMutationWriter<'_> {
    /// Carries this operation's admitted CPU, memory and I/O ceilings to source reads.
    #[doc(hidden)]
    pub fn task_context(&self) -> &RuntimeTaskContext {
        &self.inner.writer.task_context
    }
    /// Shares the stage's retained-memory ledger with its owning facade session.
    /// This does not reserve source snapshots or arbitrary host allocations.
    #[doc(hidden)]
    pub fn reserve_session_metadata(&self, bytes: usize) -> Result<QueryMemoryLease> {
        self.inner.writer.memory.retained.reserve(bytes)
    }
    /// Full-source mapping must supply each document once. Enable before input;
    /// identical duplicate bodies are rejected too. This cannot be disabled.
    #[doc(hidden)]
    pub fn require_unique_upserts(&mut self) -> Result<()> {
        if self.failed || self.control.report.input_operations != 0 || !self.ready_for_input() {
            self.failed = true;
            return Err(unusable());
        }
        self.control.unique_upserts = true;
        Ok(())
    }
    pub fn report(&self) -> SearchUnorderedMutationReport {
        self.control.report
    }
    /// Admits one input without running sort/merge I/O. When false, advance
    /// ordering before offering another input. Calling while busy poisons the
    /// attempt instead of silently dropping an input.
    pub fn ready_for_input(&self) -> bool {
        !self.failed && self.ordering.ready_for_input()
    }
    pub fn ordering_complete(&self) -> bool {
        !self.failed && self.ordering.complete()
    }
    pub fn queue_upsert(&mut self, document: SearchDocument) -> Result<()> {
        if self.failed {
            return Err(unusable());
        }
        self.failed = true;
        let result = (|| {
            if !self.ordering.ready_for_input() {
                return Err(unusable());
            }
            self.control.check_input(&document.id)?;
            if document.metadata.len() > self.inner.writer.options.max_metadata_fields.get() {
                return Err(HawDBError::Storage(
                    "unordered mutation metadata fields exceed admission".into(),
                ));
            }
            let bytes = document_bytes(&document)?;
            let admitted = self.inner.writer.memory.admit_document(document)?;
            let encoded = DocumentEncoding::new_with_context(
                &admitted,
                Some(&self.inner.writer.task_context),
            )?
            .len() as u64;
            self.enqueue(Buffered::Upsert(admitted, encoded), bytes)
        })();
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    pub fn queue_delete(&mut self, id: &str) -> Result<()> {
        if self.failed {
            return Err(unusable());
        }
        self.failed = true;
        let result = (|| {
            if !self.ordering.ready_for_input() {
                return Err(unusable());
            }
            self.control.check_input(id)?;
            let bytes = checked_add(id.len(), Term::tracking_overhead())?;
            let id = Term::copy(id, Some(&self.inner.writer.memory))?;
            self.enqueue(Buffered::Delete(id), bytes)
        })();
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    fn enqueue(&mut self, entry: Buffered, bytes: usize) -> Result<()> {
        if bytes > self.control.limits.max_buffer_payload_bytes.get() {
            return Err(HawDBError::Execution(
                "unordered mutation payload cannot fit its buffer admission".into(),
            ));
        }
        if self.buffer.len() == self.control.limits.max_buffer_records.get()
            || bytes > self.control.limits.max_buffer_payload_bytes.get() - self.payload
        {
            self.ordering.queue(entry, bytes, self.buffer.len());
        } else {
            self.push(entry, bytes)?;
        }
        self.control.report.input_operations += 1;
        Ok(())
    }
    /// Blocking convenience; use queue_upsert/advance_ordering for quanta.
    pub fn upsert(&mut self, document: SearchDocument) -> Result<()> {
        self.queue_upsert(document)?;
        self.drain_input_work()
    }
    /// Blocking convenience; use queue_delete/advance_ordering for quanta.
    pub fn delete(&mut self, id: &str) -> Result<()> {
        self.queue_delete(id)?;
        self.drain_input_work()
    }
    fn drain_input_work(&mut self) -> Result<()> {
        while !self.ready_for_input() {
            self.advance_ordering(unlimited_step())?;
        }
        Ok(())
    }
    /// Closes intake without building or publishing an artifact. Further
    /// advance_ordering calls prepare a private, completely reduced run.
    pub fn finish_input(&mut self) -> Result<()> {
        if self.failed {
            return Err(unusable());
        }
        self.ordering.finish_input();
        Ok(())
    }
    pub fn advance_ordering(
        &mut self,
        limits: SearchMutationSortStepLimits,
    ) -> Result<SearchMutationSortStep> {
        if self.failed {
            return Err(unusable());
        }
        self.failed = true;
        checkpoint(&self.inner.writer.task_context)?;
        self.control.begin_step(limits);
        let result = self.ordering.advance(
            &mut self.buffer,
            &mut self.slots,
            &mut self.levels,
            &mut self.payload,
            &mut self.control,
            &mut self.inner.writer,
        );
        let usage = self.control.end_step();
        result?;
        self.failed = false;
        Ok(SearchMutationSortStep {
            usage,
            ready_for_input: self.ready_for_input(),
            ordering_complete: self.ordering_complete(),
        })
    }
    fn push(&mut self, value: Buffered, bytes: usize) -> Result<()> {
        if self.buffer.len() == self.buffer.capacity() {
            let capacity = self
                .buffer
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(self.control.limits.max_buffer_records.get());
            crate::build_memory::reserve_capacity(&mut self.buffer, capacity, &mut self.slots)?;
        }
        self.buffer.push(value);
        self.payload += bytes;
        self.control.report.peak_buffer_payload_bytes = self
            .control
            .report
            .peak_buffer_payload_bytes
            .max(self.payload);
        Ok(())
    }
    #[cfg(test)]
    fn flush(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.ordering.flush(self.buffer.len());
        }
        self.drain_input_work()
    }
    /// Blocking artifact construction follows resumable ordering. This method
    /// still performs base retraction reads, index construction and sealing;
    /// advance_ordering's quantum does not bound those later phases.
    pub fn prepare_publication(
        mut self,
    ) -> Result<(SearchUnorderedMutationReport, SearchOutOfCorePreparedUpdate)> {
        if self.failed {
            return Err(unusable());
        }
        if self.inspection.started && !self.inspection.complete {
            return Err(HawDBError::Storage(
                "reduced mutation inspection is incomplete".into(),
            ));
        }
        self.finish_input()?;
        while !self.ordering_complete() {
            self.advance_ordering(unlimited_step())?;
        }
        self.failed = true;
        if let Some(run) = self.ordering.take_reduced() {
            let mut source = Reader::open(run, &mut self.control, &self.inner.writer)?;
            while let Some(entry) = source.next(&mut self.control)? {
                self.emit(entry)?;
            }
            source.remove(&self.control)?;
        }
        let report = self.control.report;
        Ok((report, self.inner.prepare_publication()?))
    }
    fn emit(&mut self, entry: run::Entry) -> Result<()> {
        match entry {
            run::Entry::Delete(id) => self.inner.delete(&id)?,
            run::Entry::Upsert(record, needs_chinese) => {
                self.control.read(record.encoded_bytes)?;
                self.inner
                    .target(&record.header.id, SearchMutationOperation::Replace)?;
                self.inner
                    .writer
                    .push_spool_record(*record, needs_chinese)?;
            }
            run::Entry::Owner(..) => return Err(unusable()),
        }
        Ok(())
    }
}
fn unlimited_step() -> SearchMutationSortStepLimits {
    SearchMutationSortStepLimits {
        work: NonZeroUsize::new(usize::MAX).unwrap(),
        read_bytes: NonZeroU64::new(u64::MAX).unwrap(),
        spill_bytes: NonZeroU64::new(u64::MAX).unwrap(),
    }
}
fn unusable() -> HawDBError {
    HawDBError::Storage("unordered mutation writer is unusable after an input failure".into())
}

#[cfg(test)]
mod quantum_tests;
#[cfg(test)]
mod tests;
