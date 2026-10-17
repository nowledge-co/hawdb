// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use run::{Entry, Merge, Output};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Input,
    Sort,
    OpenBuffer,
    WriteBuffer,
    Carry,
    Seal,
    OpenSource,
    OpenReduced,
    Load,
    Classify,
    Equal,
    Emit,
    EmitLast,
    FinishReduced,
    RemoveSource,
    Ready,
}

/// Drop readers, pending entries and output buffers before the enclosing stage
/// owner. No continuation is cloneable or caller-constructible.
pub(super) struct Ordering {
    phase: Phase,
    finishing: bool,
    incoming: Option<(Buffered, usize)>,
    heap: Option<heap::Heap>,
    output: Option<Output>,
    run: Option<Run>,
    merging: Option<Box<Merge>>,
    level: usize,
    source: Option<Reader>,
    winner: Option<Entry>,
    current: Option<Entry>,
    reduced: Option<Run>,
}
impl Ordering {
    pub(super) fn new() -> Self {
        Self {
            phase: Phase::Input,
            finishing: false,
            incoming: None,
            heap: None,
            output: None,
            run: None,
            merging: None,
            level: 0,
            source: None,
            winner: None,
            current: None,
            reduced: None,
        }
    }
    pub(super) fn ready_for_input(&self) -> bool {
        self.phase == Phase::Input && !self.finishing
    }
    pub(super) fn complete(&self) -> bool {
        self.phase == Phase::Ready
    }
    pub(super) fn finish_input(&mut self) {
        self.finishing = true;
    }
    pub(super) fn restore_reduced(&mut self, run: Option<Run>) {
        assert!(self.complete() && self.reduced.is_none());
        self.reduced = run;
    }
    pub(super) fn take_reduced(&mut self) -> Option<Run> {
        self.reduced.take()
    }
    pub(super) fn queue(&mut self, entry: Buffered, bytes: usize, length: usize) {
        self.incoming = Some((entry, bytes));
        self.flush(length);
    }
    pub(super) fn flush(&mut self, length: usize) {
        self.heap = Some(heap::Heap::new(length));
        self.phase = Phase::Sort;
    }
    fn merge(
        &mut self,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<bool> {
        if let Some(merging) = &mut self.merging {
            if !merging.advance(control, writer)? {
                return Ok(false);
            }
            self.run = Some(self.merging.take().unwrap().finish());
        }
        Ok(true)
    }
    pub(super) fn advance(
        &mut self,
        buffer: &mut Vec<Buffered>,
        slots: &mut QueryMemoryLease,
        levels: &mut [Option<Run>; usize::BITS as usize],
        payload: &mut usize,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<()> {
        loop {
            match self.phase {
                Phase::Input => {
                    if !self.finishing {
                        return Ok(());
                    }
                    if buffer.is_empty() {
                        self.level = 0;
                        self.phase = Phase::Seal;
                    } else {
                        self.flush(buffer.len());
                    }
                }
                Phase::Sort => {
                    if !self.heap.as_mut().unwrap().advance(buffer, control)? {
                        return Ok(());
                    }
                    self.heap = None;
                    self.phase = Phase::OpenBuffer;
                }
                Phase::OpenBuffer => {
                    if !control.allow(1, 0, 8)? {
                        return Ok(());
                    }
                    self.output = Some(control.output(writer)?);
                    self.phase = Phase::WriteBuffer;
                }
                Phase::WriteBuffer => {
                    if let Some(entry) = buffer.last() {
                        let (read, bytes) = entry.copy_cost()?;
                        if !control.allow(1, read, bytes)? {
                            return Ok(());
                        }
                        self.output.as_mut().unwrap().buffered(
                            buffer.pop().unwrap(),
                            control,
                            writer,
                        )?;
                    } else {
                        if !control.allow(1, 0, 0)? {
                            return Ok(());
                        }
                        self.run = Some(self.output.take().unwrap().finish()?);
                        *payload = 0;
                        self.level = 0;
                        self.phase = Phase::Carry;
                    }
                }
                Phase::Carry => {
                    if self.merging.is_some() {
                        if !self.merge(control, writer)? {
                            return Ok(());
                        }
                        self.level += 1;
                    }
                    if !control.allow(1, 0, 0)? {
                        return Ok(());
                    }
                    if self.level == levels.len() {
                        return Err(HawDBError::Storage(
                            "unordered mutation run levels exhausted".into(),
                        ));
                    }
                    if let Some(previous) = levels[self.level].take() {
                        self.merging =
                            Some(Merge::new(previous, self.run.take().unwrap(), control)?);
                    } else {
                        levels[self.level] = self.run.take();
                        if let Some((entry, bytes)) = self.incoming.take() {
                            push_buffer(buffer, slots, payload, entry, bytes, control)?;
                        }
                        self.phase = Phase::Input;
                    }
                }
                Phase::Seal => {
                    if !self.merge(control, writer)? {
                        return Ok(());
                    }
                    if !control.allow(1, 0, 0)? {
                        return Ok(());
                    }
                    if self.level == levels.len() {
                        *buffer = Vec::new();
                        slots.shrink(slots.bytes());
                        self.phase = if self.run.is_some() {
                            Phase::OpenSource
                        } else {
                            Phase::Ready
                        };
                    } else {
                        if let Some(next) = levels[self.level].take() {
                            if let Some(previous) = self.run.take() {
                                self.merging = Some(Merge::new(previous, next, control)?);
                            } else {
                                self.run = Some(next);
                            }
                        }
                        self.level += 1;
                    }
                }
                Phase::OpenSource => {
                    if !control.allow(1, 8, 0)? {
                        return Ok(());
                    }
                    self.source = Some(Reader::open(self.run.take().unwrap(), control, writer)?);
                    self.phase = Phase::OpenReduced;
                }
                Phase::OpenReduced => {
                    if !control.allow(1, 0, 8)? {
                        return Ok(());
                    }
                    self.output = Some(control.output(writer)?);
                    self.phase = Phase::Load;
                }
                Phase::Load => {
                    let Some(next) = self.source.as_mut().unwrap().next_in_step(control)? else {
                        return Ok(());
                    };
                    self.current = next;
                    self.phase = if self.current.is_some() {
                        Phase::Classify
                    } else {
                        Phase::EmitLast
                    };
                }
                Phase::Classify => {
                    let work = self
                        .winner
                        .as_ref()
                        .zip(self.current.as_ref())
                        .map_or(1, |(a, b)| 1 + a.id().len().min(b.id().len()));
                    if !control.allow(work, 0, 0)? {
                        return Ok(());
                    }
                    if let Some(winner) = &self.winner {
                        self.phase = if winner.id() == self.current.as_ref().unwrap().id() {
                            Phase::Equal
                        } else {
                            Phase::Emit
                        };
                    } else {
                        self.winner = self.current.take();
                        self.phase = Phase::Load;
                    }
                }
                Phase::Equal => {
                    let read = run::comparison_cost(
                        self.winner.as_ref().unwrap(),
                        self.current.as_ref().unwrap(),
                    )?;
                    if !control.allow(1, read, 0)? {
                        return Ok(());
                    }
                    self.winner = Some(run::reduce(
                        self.winner.take().unwrap(),
                        self.current.take().unwrap(),
                        control,
                    )?);
                    self.phase = Phase::Load;
                }
                Phase::Emit | Phase::EmitLast => {
                    if let Some(winner) = &self.winner {
                        let (read, spill) = winner.copy_cost();
                        if !control.allow(1, read, spill)? {
                            return Ok(());
                        }
                        self.output
                            .as_mut()
                            .unwrap()
                            .copied(winner, control, writer)?;
                        self.winner = None;
                        control.report.output_operations += 1;
                    }
                    if self.phase == Phase::Emit {
                        self.winner = self.current.take();
                        self.phase = Phase::Load;
                    } else {
                        self.phase = Phase::FinishReduced;
                    }
                }
                Phase::FinishReduced => {
                    if !control.allow(1, 0, 0)? {
                        return Ok(());
                    }
                    self.reduced = Some(self.output.take().unwrap().finish()?);
                    self.phase = Phase::RemoveSource;
                }
                Phase::RemoveSource => {
                    if !control.allow(1, 0, 0)? {
                        return Ok(());
                    }
                    self.source.take().unwrap().remove(control)?;
                    self.phase = Phase::Ready;
                }
                Phase::Ready => return Ok(()),
            }
        }
    }
}

pub(super) fn push_buffer(
    buffer: &mut Vec<Buffered>,
    slots: &mut QueryMemoryLease,
    payload: &mut usize,
    entry: Buffered,
    bytes: usize,
    control: &mut Control,
) -> Result<()> {
    if buffer.len() == buffer.capacity() {
        let capacity = buffer
            .capacity()
            .saturating_mul(2)
            .max(1)
            .min(control.limits.max_buffer_records.get());
        crate::build_memory::reserve_capacity(buffer, capacity, slots)?;
    }
    buffer.push(entry);
    *payload += bytes;
    control.report.peak_buffer_payload_bytes =
        control.report.peak_buffer_payload_bytes.max(*payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{document, fixture, limits};
    use super::*;
    #[test]
    fn cancellation_at_retained_run_and_reducer_phases_refuses_without_visibility() {
        for target in [
            Phase::Carry,
            Phase::Seal,
            Phase::Load,
            Phase::Equal,
            Phase::Emit,
            Phase::RemoveSource,
        ] {
            let (root, reader) = fixture("ordering-phase-cancel");
            let task = RuntimeTaskContext::default();
            let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                &reader,
                Default::default(),
                limits(1),
                task.clone(),
            )
            .unwrap();
            let memory = writer.inner.writer.memory.clone();
            writer.upsert(document("a", "new")).unwrap();
            writer.upsert(document("a", "new")).unwrap();
            writer.delete("b").unwrap();
            writer.upsert(document("z", "new")).unwrap();
            writer.finish_input().unwrap();
            let mut reached = false;
            for _ in 0..1000 {
                if writer.ordering.phase == target {
                    reached = true;
                    break;
                }
                if writer.ordering_complete() {
                    break;
                }
                writer
                    .advance_ordering(SearchMutationSortStepLimits {
                        work: NonZeroUsize::new(2).unwrap(),
                        read_bytes: NonZeroU64::new(90).unwrap(),
                        spill_bytes: NonZeroU64::new(90).unwrap(),
                    })
                    .unwrap();
            }
            assert!(reached, "target phase {} was not exercised", target as u8);
            task.cancellation().cancel();
            assert!(writer.advance_ordering(unlimited_step()).is_err());
            assert!(writer.prepare_publication().is_err());
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                reader.generation()
            );
            assert!(!std::fs::read_dir(&root).unwrap().any(|e| e
                .unwrap()
                .file_type()
                .unwrap()
                .is_dir()));
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
