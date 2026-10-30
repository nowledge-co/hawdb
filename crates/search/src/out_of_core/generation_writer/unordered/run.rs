// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::build_control::CheckedWriter;
use crate::build_memory::path::OwnedPath;
use crate::lexical_projection::DocumentsDigest;
use hawdb_integrity::Crc32cHasher;
use std::io::{BufWriter, Write};

pub(super) struct Control {
    pub(super) limits: SearchUnorderedMutationLimits,
    pub(super) report: SearchUnorderedMutationReport,
    pub(super) unique_upserts: bool,
    memory: BuildMemory,
    task: RuntimeTaskContext,
    max_id_bytes: usize,
    step: Option<(SearchMutationSortStepLimits, SearchMutationSortStepUsage)>,
}
impl Control {
    pub(super) fn rebind_execution(
        &mut self,
        task: &RuntimeTaskContext,
        permit: Option<std::sync::Arc<hawdb_qos::RuntimePermit>>,
    ) {
        self.task = task.clone();
        self.memory.host_admission = permit;
    }

    pub(super) fn new(
        limits: SearchUnorderedMutationLimits,
        memory: BuildMemory,
        task: RuntimeTaskContext,
        max_id_bytes: usize,
    ) -> Self {
        Self {
            limits,
            report: Default::default(),
            unique_upserts: false,
            memory,
            task,
            max_id_bytes,
            step: None,
        }
    }
    pub(super) fn require_document(
        &mut self,
        entry: &Entry,
        expected: &SearchDocument,
    ) -> Result<()> {
        let Entry::Upsert(record, _) = entry else {
            return Err(invalid("expected upsert during inspection"));
        };
        self.require_record_document(record, expected)
    }
    fn require_record_document(
        &mut self,
        record: &spool::SpoolRecord,
        expected: &SearchDocument,
    ) -> Result<()> {
        self.read(record.encoded_bytes)?;
        record.require_document(expected, &self.memory, &self.task)
    }
    pub(super) fn check_input(&self, id: &str) -> Result<()> {
        checkpoint(&self.task)?;
        validate_id(id, self.max_id_bytes)?;
        if self.report.input_operations >= self.limits.max_input_operations.get() {
            return Err(invalid("input operation admission exhausted"));
        }
        Ok(())
    }
    pub(super) fn read(&mut self, bytes: u64) -> Result<()> {
        checkpoint(&self.task)?;
        let next = self
            .report
            .reserved_read_bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.limits.max_read_bytes.get())
            .ok_or_else(|| invalid("read byte admission exhausted"))?;
        if let Some((limits, used)) = &mut self.step {
            used.read_bytes = used
                .read_bytes
                .checked_add(bytes)
                .filter(|value| *value <= limits.read_bytes.get())
                .ok_or_else(|| invalid("step read preflight was incomplete"))?;
        }
        self.report.reserved_read_bytes = next;
        Ok(())
    }
    fn write(&mut self, writer: &mut SearchOutOfCoreGenerationWriter, bytes: u64) -> Result<()> {
        checkpoint(&self.task)?;
        let next = self
            .report
            .reserved_spill_bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.limits.max_spill_bytes.get())
            .ok_or_else(|| invalid("spill byte admission exhausted"))?;
        if let Some((limits, used)) = &mut self.step {
            used.spill_bytes = used
                .spill_bytes
                .checked_add(bytes)
                .filter(|value| *value <= limits.spill_bytes.get())
                .ok_or_else(|| invalid("step spill preflight was incomplete"))?;
        }
        writer.stage.reserve_additional_disk(bytes)?;
        self.report.reserved_spill_bytes = next;
        Ok(())
    }
    pub(super) fn begin_step(&mut self, limits: SearchMutationSortStepLimits) {
        self.step = Some((limits, Default::default()));
    }
    pub(super) fn end_step(&mut self) -> SearchMutationSortStepUsage {
        self.step
            .take()
            .map_or(Default::default(), |(_, usage)| usage)
    }
    /// Charge an atomic transition only after its complete conservative byte
    /// preflight fits the remaining quantum. Yield never refunds total work.
    pub(super) fn allow(&mut self, work: usize, read: u64, spill: u64) -> Result<bool> {
        checkpoint(&self.task)?;
        if let Some((limits, used)) = &self.step {
            if work > limits.work.get()
                || read > limits.read_bytes.get()
                || spill > limits.spill_bytes.get()
            {
                return Err(invalid(
                    "one ordering operation cannot fit the step admission",
                ));
            }
            if work > limits.work.get() - used.work
                || read > limits.read_bytes.get() - used.read_bytes
                || spill > limits.spill_bytes.get() - used.spill_bytes
            {
                return Ok(false);
            }
        }
        self.report.sort_work = self
            .report
            .sort_work
            .checked_add(work)
            .filter(|value| *value <= self.limits.max_sort_work.get())
            .ok_or_else(|| invalid("cumulative sort work admission exhausted"))?;
        if let Some((_, used)) = &mut self.step {
            used.work += work;
        }
        Ok(true)
    }
    fn visit(&mut self) -> Result<()> {
        checkpoint(&self.task)?;
        if self.report.merged_records >= self.limits.max_merge_visits.get() {
            return Err(invalid("merge visit admission exhausted"));
        }
        self.report.merged_records += 1;
        Ok(())
    }
    pub(super) fn output(
        &mut self,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<Output> {
        let sequence = self.report.runs_created;
        self.report.runs_created = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("run identity overflow"))?;
        let _name = self.memory.retained.reserve(64)?;
        let name = format!("unordered-{sequence}.run");
        if name.capacity() > 64 {
            return Err(invalid("run name exceeds admission"));
        }
        let io = io::GenerationIo::new(&self.memory, &self.task);
        let path = io.path(&writer.stage.path, Path::new(&name))?;
        let buffer = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let scratch = writer.stage.scratch_directory();
        let wave = crate::out_of_core::scratch_directory::io_scope(
            &self.task,
            self.memory.host_admission.is_some()
                || scratch.as_ref().is_some_and(|scratch| scratch.governed()),
        )?;
        let file = match &scratch {
            Some(scratch) => scratch.create(&path)?,
            None => io.native(&[&path], || {
                OpenOptions::new().create_new(true).write(true).open(&*path)
            })??,
        };
        let file = BufWriter::with_capacity(SPOOL_BUFFER_BYTES, file);
        if file.capacity() != SPOOL_BUFFER_BYTES {
            return Err(invalid("run buffer exceeds admission"));
        }
        let mut file = io_phase::Spool::new(file);
        self.write(writer, spool::SPOOL_HEADER.len() as u64)?;
        file.write_all(spool::SPOOL_HEADER)?;
        Ok(Output {
            file,
            path,
            io_task: wave.is_some().then(|| self.task.clone()),
            scratch,
            memory: self.memory.clone(),
            records: 0,
            bytes: spool::SPOOL_HEADER.len() as u64,
            digest: Default::default(),
            _buffer: buffer,
        })
    }
}
fn invalid(message: &str) -> HawDBError {
    HawDBError::Storage(format!("unordered mutation {message}"))
}
fn validate_id(id: &str, max: usize) -> Result<()> {
    if id.is_empty() || id.len() > max {
        Err(invalid("ID is empty or exceeds admission"))
    } else {
        Ok(())
    }
}
fn evidence(digest: &mut DocumentsDigest, record: u64, bytes: u64, tag: u64) {
    digest.add_record(record ^ (tag << 56), bytes);
}

pub(super) struct Run {
    path: OwnedPath,
    held: Option<spool::HeldSpool>,
    scratch: Option<std::sync::Arc<crate::out_of_core::ScratchDirectory>>,
    records: usize,
    bytes: u64,
    digest: DocumentsDigest,
}
impl Run {
    pub(super) fn rebind_task(&self, task: &RuntimeTaskContext) -> Result<()> {
        let held = self
            .held
            .as_ref()
            .ok_or_else(|| invalid("Consumer run lacks native custody"))?;
        held.rebind_task(task);
        Ok(())
    }
}
pub(super) struct Output {
    file: io_phase::Spool,
    path: OwnedPath,
    io_task: Option<RuntimeTaskContext>,
    scratch: Option<std::sync::Arc<crate::out_of_core::ScratchDirectory>>,
    memory: BuildMemory,
    records: usize,
    bytes: u64,
    digest: DocumentsDigest,
    _buffer: QueryMemoryLease,
}
impl Output {
    fn reserve(
        &mut self,
        bytes: u64,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<()> {
        control.write(writer, bytes)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("run extent overflow"))?;
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| invalid("run record count overflow"))?;
        Ok(())
    }
    pub(super) fn buffered(
        &mut self,
        entry: Buffered,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<()> {
        match entry {
            Buffered::Delete(id) => self.delete(&id, control, writer),
            Buffered::Upsert(document, _) => {
                let _io = self
                    .io_task
                    .as_ref()
                    .map(|task| crate::out_of_core::scratch_directory::io_scope(task, true))
                    .transpose()?;
                let encoding = DocumentEncoding::new_with_context(&document, Some(&control.task))?;
                let bytes = encoding.len() as u64;
                if bytes > writer.options.max_record_bytes.get() {
                    return Err(invalid("record exceeds encoded admission"));
                }
                let chinese = crate::analyzer_workspace::document_needs_workspace(&document);
                let extent = bytes
                    .checked_add(18)
                    .ok_or_else(|| invalid("record extent overflow"))?;
                self.reserve(extent, control, writer)?;
                self.file.write_all(&[1, u8::from(chinese)])?;
                let mut record = DocumentsDigest::default();
                spool::write_frame_with_context(
                    &mut self.file,
                    &encoding,
                    &mut record,
                    &control.memory,
                    &control.task,
                )?;
                evidence(
                    &mut self.digest,
                    record.finish(),
                    extent,
                    2 + u64::from(chinese),
                );
                Ok(())
            }
        }
    }
    fn delete(
        &mut self,
        id: &str,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<()> {
        let length = id.len() as u64;
        let _io = self
            .io_task
            .as_ref()
            .map(|task| crate::out_of_core::scratch_directory::io_scope(task, true))
            .transpose()?;
        let extent = length
            .checked_add(17)
            .ok_or_else(|| invalid("delete extent overflow"))?;
        self.reserve(extent, control, writer)?;
        let mut digest = Crc32cHasher::new();
        digest.update(id.as_bytes());
        let checksum = digest.finish();
        let mut output = CheckedWriter::new(&mut self.file, Some(&control.task));
        output.write_all(&[0])?;
        output.write_all(&length.to_le_bytes())?;
        output.write_all(id.as_bytes())?;
        output.write_all(&checksum.to_le_bytes())?;
        let mut record = DocumentsDigest::default();
        record.add_record(checksum, length);
        evidence(&mut self.digest, record.finish(), extent, 0);
        Ok(())
    }
    pub(super) fn copied(
        &mut self,
        entry: &Entry,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<()> {
        match entry {
            Entry::Delete(id) => self.delete(id, control, writer),
            Entry::Upsert(record, chinese) => {
                let extent = record
                    .encoded_bytes
                    .checked_add(18)
                    .ok_or_else(|| invalid("record extent overflow"))?;
                control.read(record.encoded_bytes)?;
                self.reserve(extent, control, writer)?;
                {
                    let _io = self
                        .io_task
                        .as_ref()
                        .map(|task| crate::out_of_core::scratch_directory::io_scope(task, true))
                        .transpose()?;
                    self.file.write_all(&[1, u8::from(*chinese)])?;
                    self.file.write_all(&record.encoded_bytes.to_le_bytes())?;
                    self.file.write_all(&record.checksum().to_le_bytes())?;
                }
                record.write_to(&mut self.file, &control.memory, &control.task)?;
                let mut value = DocumentsDigest::default();
                record.add_digest(&mut value);
                evidence(
                    &mut self.digest,
                    value.finish(),
                    extent,
                    2 + u64::from(*chinese),
                );
                Ok(())
            }
        }
    }
    pub(super) fn finish(mut self) -> Result<Run> {
        {
            let _io = self
                .io_task
                .as_ref()
                .map(|task| crate::out_of_core::scratch_directory::io_scope(task, true))
                .transpose()?;
            self.file.flush()?;
        }
        // Captured runs retain native custody. Ordinary runs retain only
        // paths while idle, preserving their existing descriptor bound.
        let held = match (self.scratch.is_some(), self.io_task) {
            (true, Some(task)) => Some(spool::HeldSpool::capture_governed(
                self.file.into_file(),
                self.bytes,
                &self.memory,
                task,
            )?),
            _ => None,
        };
        Ok(Run {
            path: self.path,
            held,
            scratch: self.scratch,
            records: self.records,
            bytes: self.bytes,
            digest: self.digest,
        })
    }
}

pub(super) enum Entry {
    Delete(Term),
    Upsert(Box<spool::SpoolRecord>, bool),
}
impl Entry {
    pub(super) fn copy_cost(&self) -> (u64, u64) {
        match self {
            Self::Delete(id) => (0, id.len() as u64 + 17),
            Self::Upsert(record, _) => (record.encoded_bytes, record.encoded_bytes + 18),
        }
    }
    pub(super) fn id(&self) -> &str {
        match self {
            Self::Delete(id) => id,
            Self::Upsert(record, _) => &record.header.id,
        }
    }
}
pub(super) struct Reader {
    cursor: spool::SpoolCursor,
    run: Run,
    position: u64,
    records: usize,
    digest: DocumentsDigest,
    previous: Option<Term>,
    inspected_read_bytes: Option<u64>,
    memory: BuildMemory,
    task: RuntimeTaskContext,
    _memory: QueryMemoryLease,
}
impl Reader {
    pub(super) fn open(
        run: Run,
        control: &mut Control,
        writer: &SearchOutOfCoreGenerationWriter,
    ) -> Result<Self> {
        control.read(spool::SPOOL_HEADER.len() as u64)?;
        let memory = control.memory.spool.reserve(std::mem::size_of::<Self>())?;
        let held = match &run.held {
            Some(held) => held.clone(),
            None => {
                let wave = crate::out_of_core::scratch_directory::io_scope(
                    &control.task,
                    control.memory.host_admission.is_some(),
                )?;
                let file = io::GenerationIo::new(&control.memory, &control.task)
                    .native(&[&run.path], || File::open(&*run.path))??;
                let held = spool::HeldSpool::capture(file, run.bytes, &control.memory)?;
                if wave.is_some() {
                    held.rebind_task(&control.task);
                }
                held
            }
        };
        let cursor = spool::SpoolSource {
            held: Some(held),
            path: &run.path,
            document_count: 0,
            max_record_bytes: writer.options.max_record_bytes.get(),
            max_metadata_fields: writer.options.max_metadata_fields.get(),
            memory: control.memory.clone(),
        }
        .cursor(&control.task)?;
        if cursor.length() != run.bytes {
            return Err(invalid("run length changed"));
        }
        Ok(Self {
            cursor,
            run,
            position: spool::SPOOL_HEADER.len() as u64,
            records: 0,
            digest: Default::default(),
            previous: None,
            inspected_read_bytes: None,
            memory: control.memory.clone(),
            task: control.task.clone(),
            _memory: memory,
        })
    }
    fn raw(&self, offset: u64, bytes: &mut [u8], control: &mut Control) -> Result<()> {
        control.read(bytes.len() as u64)?;
        self.cursor.read_exact_at(offset, bytes)
    }
    /// Read only the bounded frame prefix. Cache its conservative full-read
    /// cost across quantum boundaries; the existing decoder revalidates it.
    fn inspect(&mut self, control: &mut Control) -> Result<()> {
        let mut tag = [0];
        self.raw(self.position, &mut tag, control)?;
        let cost = match tag[0] {
            0 => {
                let mut length = [0; 8];
                self.raw(self.position + 1, &mut length, control)?;
                let length = u64::from_le_bytes(length);
                if length == 0 || length > control.max_id_bytes as u64 {
                    return Err(invalid("ID exceeds admission"));
                }
                length.checked_add(17)
            }
            1 => {
                let mut frame = [0; 17];
                self.raw(self.position + 1, &mut frame, control)?;
                if frame[0] > 1 {
                    return Err(invalid("invalid analyzer marker"));
                }
                let length = u64::from_le_bytes(frame[1..9].try_into().unwrap());
                length.checked_add(34)
            }
            _ => return Err(invalid("invalid operation tag")),
        }
        .ok_or_else(|| invalid("record read preflight overflow"))?;
        self.inspected_read_bytes = Some(cost);
        Ok(())
    }
    /// Returns false only when the current quantum must yield. A record that
    /// cannot fit any quantum with these limits is a permanent refusal.
    pub(super) fn next_in_step(&mut self, control: &mut Control) -> Result<Option<Option<Entry>>> {
        self.next_admitted(control, false)
    }
    /// Admit the full decode plus a possible exact document comparison before
    /// invoking host verification. Callback I/O must fit the same quantum.
    pub(super) fn next_for_inspection(
        &mut self,
        control: &mut Control,
    ) -> Result<Option<Option<Entry>>> {
        self.next_admitted(control, true)
    }
    fn next_admitted(
        &mut self,
        control: &mut Control,
        comparison: bool,
    ) -> Result<Option<Option<Entry>>> {
        if self.records == self.run.records {
            return if control.allow(1, 0, 0)? {
                self.next(control).map(Some)
            } else {
                Ok(None)
            };
        }
        if self.inspected_read_bytes.is_none() {
            if !control.allow(1, 18, 0)? {
                return Ok(None);
            }
            self.inspect(control)?;
        }
        let read = self
            .inspected_read_bytes
            .unwrap()
            .checked_mul(if comparison { 2 } else { 1 })
            .ok_or_else(|| invalid("inspection read preflight overflow"))?;
        if !control.allow(1, read, 0)? {
            return Ok(None);
        }
        let value = self.next(control)?;
        self.inspected_read_bytes = None;
        Ok(Some(value))
    }
    pub(super) fn next(&mut self, control: &mut Control) -> Result<Option<Entry>> {
        if self.records == self.run.records {
            if self.position != self.run.bytes || self.digest != self.run.digest {
                return Err(invalid("run is incomplete, reordered or corrupted"));
            }
            return Ok(None);
        }
        control.visit()?;
        let start = self.position;
        let mut tag = [0];
        self.raw(start, &mut tag, control)?;
        let (entry, end) = match tag[0] {
            0 => {
                let mut length = [0; 8];
                self.raw(start + 1, &mut length, control)?;
                let length = u64::from_le_bytes(length);
                let size =
                    usize::try_from(length).map_err(|_| invalid("ID length exceeds usize"))?;
                if size == 0 || size > control.max_id_bytes {
                    return Err(invalid("ID exceeds admission"));
                }
                let end = start
                    .checked_add(17)
                    .and_then(|n| n.checked_add(length))
                    .filter(|n| *n <= self.run.bytes)
                    .ok_or_else(|| invalid("delete is truncated"))?;
                let _scratch = self.memory.spool.reserve(size)?;
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(size)
                    .map_err(|_| invalid("cannot allocate ID input"))?;
                if bytes.capacity() != size {
                    return Err(invalid("ID input exceeds admission"));
                }
                bytes.resize(size, 0);
                self.raw(start + 9, &mut bytes, control)?;
                let mut checksum = [0; 8];
                self.raw(start + 9 + length, &mut checksum, control)?;
                let mut digest = Crc32cHasher::new();
                digest.update(&bytes);
                if digest.finish() != u64::from_le_bytes(checksum) {
                    return Err(invalid("delete checksum mismatch"));
                }
                let id = std::str::from_utf8(&bytes).map_err(|_| invalid("ID is not UTF-8"))?;
                let id = Term::copy(id, Some(&self.memory))?;
                let mut record = DocumentsDigest::default();
                record.add_record(digest.finish(), length);
                evidence(&mut self.digest, record.finish(), end - start, 0);
                (Entry::Delete(id), end)
            }
            1 => {
                let mut flag = [0];
                self.raw(start + 1, &mut flag, control)?;
                if flag[0] > 1 {
                    return Err(invalid("invalid analyzer marker"));
                }
                let mut frame = [0; 16];
                self.raw(start + 2, &mut frame, control)?;
                let length = u64::from_le_bytes(frame[..8].try_into().unwrap());
                let end = start
                    .checked_add(18)
                    .and_then(|n| n.checked_add(length))
                    .filter(|n| *n <= self.run.bytes)
                    .ok_or_else(|| invalid("upsert is truncated"))?;
                control.read(
                    length
                        .checked_add(16)
                        .ok_or_else(|| invalid("read extent overflow"))?,
                )?;
                let record = self.cursor.record_at(start + 2, end)?;
                let mut value = DocumentsDigest::default();
                record.add_digest(&mut value);
                evidence(
                    &mut self.digest,
                    value.finish(),
                    end - start,
                    2 + u64::from(flag[0]),
                );
                (Entry::Upsert(Box::new(record), flag[0] == 1), end)
            }
            _ => return Err(invalid("invalid operation tag")),
        };
        validate_id(entry.id(), control.max_id_bytes)?;
        if self
            .previous
            .as_ref()
            .is_some_and(|previous| &**previous > entry.id())
        {
            return Err(invalid("run IDs are not ordered"));
        }
        self.previous = Some(Term::copy(entry.id(), Some(&self.memory))?);
        self.position = end;
        self.records += 1;
        Ok(Some(entry))
    }
    pub(super) fn into_run(self) -> Run {
        self.run
    }
    pub(super) fn remove(self, control: &Control) -> Result<()> {
        let path = self.run.path;
        drop(self.cursor);
        drop(self.run.held);
        let _wave = crate::out_of_core::scratch_directory::io_scope(
            &self.task,
            control.memory.host_admission.is_some()
                || self
                    .run
                    .scratch
                    .as_ref()
                    .is_some_and(|scratch| scratch.governed()),
        )?;
        match self.run.scratch {
            Some(scratch) => {
                scratch.remove(&path)?;
            }
            None => io::GenerationIo::new(&control.memory, &self.task)
                .native(&[&path], || fs::remove_file(&*path))??,
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum MergePhase {
    OpenLeft,
    OpenRight,
    OpenOutput,
    FillLeft,
    FillRight,
    Choose,
    Copy,
    Flush,
    RemoveLeft,
    RemoveRight,
    Done,
}

/// Readers and pending entries survive yields; complete input drains and both
/// removals precede handing the replacement run back to its owner.
pub(super) struct Merge {
    left_run: Option<Run>,
    right_run: Option<Run>,
    left: Option<Reader>,
    right: Option<Reader>,
    output: Option<Output>,
    a: Option<Entry>,
    b: Option<Entry>,
    left_done: bool,
    right_done: bool,
    from_left: bool,
    result: Option<Run>,
    phase: MergePhase,
    _memory: QueryMemoryLease,
}
impl Merge {
    pub(super) fn new(left: Run, right: Run, control: &Control) -> Result<Box<Self>> {
        let memory = control
            .memory
            .retained
            .reserve(std::mem::size_of::<Self>())?;
        Ok(Box::new(Self {
            left_run: Some(left),
            right_run: Some(right),
            left: None,
            right: None,
            output: None,
            a: None,
            b: None,
            left_done: false,
            right_done: false,
            from_left: false,
            result: None,
            phase: MergePhase::OpenLeft,
            _memory: memory,
        }))
    }
    pub(super) fn advance(
        &mut self,
        control: &mut Control,
        writer: &mut SearchOutOfCoreGenerationWriter,
    ) -> Result<bool> {
        loop {
            match self.phase {
                MergePhase::OpenLeft => {
                    if !control.allow(1, 8, 0)? {
                        return Ok(false);
                    }
                    self.left = Some(Reader::open(
                        self.left_run.take().unwrap(),
                        control,
                        writer,
                    )?);
                    self.phase = MergePhase::OpenRight;
                }
                MergePhase::OpenRight => {
                    if !control.allow(1, 8, 0)? {
                        return Ok(false);
                    }
                    self.right = Some(Reader::open(
                        self.right_run.take().unwrap(),
                        control,
                        writer,
                    )?);
                    self.phase = MergePhase::OpenOutput;
                }
                MergePhase::OpenOutput => {
                    if !control.allow(1, 0, 8)? {
                        return Ok(false);
                    }
                    self.output = Some(control.output(writer)?);
                    self.phase = MergePhase::FillLeft;
                }
                MergePhase::FillLeft => {
                    if self.a.is_none() && !self.left_done {
                        let Some(value) = self.left.as_mut().unwrap().next_in_step(control)? else {
                            return Ok(false);
                        };
                        self.left_done = value.is_none();
                        self.a = value;
                    }
                    self.phase = MergePhase::FillRight;
                }
                MergePhase::FillRight => {
                    if self.b.is_none() && !self.right_done {
                        let Some(value) = self.right.as_mut().unwrap().next_in_step(control)?
                        else {
                            return Ok(false);
                        };
                        self.right_done = value.is_none();
                        self.b = value;
                    }
                    self.phase = MergePhase::Choose;
                }
                MergePhase::Choose => {
                    let comparison = self
                        .a
                        .as_ref()
                        .zip(self.b.as_ref())
                        .map_or(0, |(a, b)| a.id().len().min(b.id().len()));
                    if !control.allow(1 + comparison, 0, 0)? {
                        return Ok(false);
                    }
                    self.from_left = match (&self.a, &self.b) {
                        (Some(a), Some(b)) => a.id() <= b.id(),
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        (None, None) => {
                            self.phase = MergePhase::Flush;
                            continue;
                        }
                    };
                    self.phase = MergePhase::Copy;
                }
                MergePhase::Copy => {
                    let entry = if self.from_left {
                        self.a.as_ref().unwrap()
                    } else {
                        self.b.as_ref().unwrap()
                    };
                    let (read, spill) = entry.copy_cost();
                    if !control.allow(1, read, spill)? {
                        return Ok(false);
                    }
                    self.output
                        .as_mut()
                        .unwrap()
                        .copied(entry, control, writer)?;
                    if self.from_left {
                        self.a = None;
                    } else {
                        self.b = None;
                    }
                    self.phase = MergePhase::FillLeft;
                }
                MergePhase::Flush => {
                    if !control.allow(1, 0, 0)? {
                        return Ok(false);
                    }
                    self.result = Some(self.output.take().unwrap().finish()?);
                    self.phase = MergePhase::RemoveLeft;
                }
                MergePhase::RemoveLeft => {
                    if !control.allow(1, 0, 0)? {
                        return Ok(false);
                    }
                    self.left.take().unwrap().remove(control)?;
                    self.phase = MergePhase::RemoveRight;
                }
                MergePhase::RemoveRight => {
                    if !control.allow(1, 0, 0)? {
                        return Ok(false);
                    }
                    self.right.take().unwrap().remove(control)?;
                    self.phase = MergePhase::Done;
                }
                MergePhase::Done => return Ok(true),
            }
        }
    }
    pub(super) fn finish(mut self: Box<Self>) -> Run {
        assert!(matches!(self.phase, MergePhase::Done));
        self.result.take().unwrap()
    }
}

pub(super) fn reduce(left: Entry, right: Entry, control: &mut Control) -> Result<Entry> {
    match (left, right) {
        (Entry::Delete(_), upsert @ Entry::Upsert(..))
        | (upsert @ Entry::Upsert(..), Entry::Delete(_)) => Ok(upsert),
        (Entry::Delete(left), Entry::Delete(_)) => Ok(Entry::Delete(left)),
        (Entry::Upsert(left, a), Entry::Upsert(right, b)) => {
            if control.unique_upserts {
                return Err(invalid("duplicate full-source document ID"));
            }
            control.read(
                left.encoded_bytes
                    .checked_add(right.encoded_bytes)
                    .ok_or_else(|| invalid("comparison extent overflow"))?,
            )?;
            if !left.encoded_eq(&right, &control.memory, &control.task)? {
                return Err(invalid("conflicting upserts for one document ID"));
            }
            Ok(Entry::Upsert(left, a || b))
        }
    }
}

#[cfg(test)]
impl Run {
    pub(super) fn path_for_test(&self) -> &std::path::Path {
        &self.path
    }
}
