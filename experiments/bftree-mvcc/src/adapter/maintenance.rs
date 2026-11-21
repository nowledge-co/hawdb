// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde::Serialize;
use std::ops::Bound;

#[cfg(test)]
mod tests;

pub const MAX_GC_WORK: usize = 128;
const MAX_CHECKPOINT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceStage {
    Durable,
    Removed(usize),
    Published,
}

#[derive(Debug, Serialize)]
pub struct CollectionReport {
    pub horizon: u64,
    pub examined: usize,
    pub removed: usize,
    pub complete_pass: bool,
    pub retained_versions: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct RecoveryReport {
    pub checkpoint_epoch: u64,
    pub restored_versions: usize,
    pub replayed_batches: usize,
}

#[derive(Debug, Serialize)]
pub struct CheckpointReport {
    pub epoch: u64,
    pub covered_end: u64,
    pub end: u64,
    pub restored_versions: usize,
}

#[derive(Serialize)]
pub struct StorageStats {
    pub epoch: u64,
    pub logical_keys: usize,
    pub retained_versions: usize,
    pub receipt_count: usize,
    /// Encoded payload size of distinct batches kept by current heads; this
    /// excludes allocator/container overhead and is not an RSS measurement.
    pub retained_receipt_payload_bytes: usize,
    pub journal_bytes: u64,
    pub active_snapshots: usize,
    pub index_generation: u64,
    pub recovery: RecoveryReport,
}

struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let (head, rest) = self
            .0
            .split_at_checked(count)
            .ok_or(Error::Invalid("truncated maintenance record"))?;
        self.0 = rest;
        Ok(head)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("u16")))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("u32")))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("u64")))
    }
    fn key(&mut self) -> Result<Vec<u8>> {
        let len = self.u16()? as usize;
        if len > MAX_KEY {
            return Err(Error::Budget("maintenance key bytes"));
        }
        Ok(self.take(len)?.to_vec())
    }
    fn finish(self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::Invalid("extra maintenance bytes"))
        }
    }
}

fn put_key(out: &mut Vec<u8>, key: &[u8]) {
    out.extend_from_slice(&(key.len() as u16).to_le_bytes());
    out.extend_from_slice(key);
}

struct GcPlan {
    epoch: u64,
    horizon: u64,
    entries: Vec<(Vec<u8>, u64)>,
}
impl GcPlan {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![1];
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.horizon.to_le_bytes());
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (key, epoch) in &self.entries {
            put_key(&mut out, key);
            out.extend_from_slice(&epoch.to_le_bytes());
        }
        out
    }
    fn decode(payload: &[u8]) -> Result<Self> {
        let mut input = Input(
            payload
                .strip_prefix(&[1])
                .ok_or(Error::Invalid("not a GC plan"))?,
        );
        let epoch = input.u64()?;
        let horizon = input.u64()?;
        let count = input.u32()? as usize;
        if count == 0 || count > MAX_GC_WORK {
            return Err(Error::Budget("GC plan entries"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push((input.key()?, input.u64()?));
        }
        input.finish()?;
        Ok(Self {
            epoch,
            horizon,
            entries,
        })
    }
    fn validate(&self, journal: &Journal, published: u64) -> Result<()> {
        if self.epoch != published || self.horizon > published {
            return Err(Error::Invalid("GC publication horizon mismatch"));
        }
        let mut previous: Option<&[u8]> = None;
        let mut removed = 0;
        for (key, epoch) in &self.entries {
            if previous.is_some_and(|p| p > key.as_slice()) {
                return Err(Error::Invalid("unordered GC plan"));
            }
            if previous != Some(key.as_slice()) {
                removed = 0;
            }
            let head = journal
                .latest
                .get(key.as_slice())
                .ok_or(Error::Invalid("unknown GC key"))?;
            if head.versions.get(removed) != Some(epoch)
                || !head
                    .versions
                    .get(removed + 1)
                    .is_some_and(|next| *next <= self.horizon)
            {
                return Err(Error::Invalid("GC would remove a retained anchor"));
            }
            removed += 1;
            previous = Some(key);
        }
        Ok(())
    }
    fn apply_metadata(&self, journal: &mut Journal) -> Result<()> {
        for (key, epoch) in &self.entries {
            let head = journal
                .latest
                .get_mut(key.as_slice())
                .ok_or(Error::Invalid("missing GC key"))?;
            if head.versions.pop_front() != Some(*epoch) {
                return Err(Error::Invalid("GC version mismatch"));
            }
            journal.retained_versions -= 1;
        }
        Ok(())
    }
}

struct SavedVersion {
    key: Vec<u8>,
    epoch: u64,
    value: Option<Vec<u8>>,
}
struct Snapshot {
    epoch: u64,
    covered_end: u64,
    rows: Vec<SavedVersion>,
}
impl Snapshot {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut out = vec![2];
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.covered_end.to_le_bytes());
        out.extend_from_slice(&(self.rows.len() as u32).to_le_bytes());
        for row in &self.rows {
            put_key(&mut out, &row.key);
            out.extend_from_slice(&row.epoch.to_le_bytes());
            out.extend_from_slice(
                &row.value
                    .as_ref()
                    .map_or(u32::MAX, |v| v.len() as u32)
                    .to_le_bytes(),
            );
            if let Some(value) = &row.value {
                out.extend_from_slice(value);
            }
            if out.len() > MAX_CHECKPOINT_BYTES {
                return Err(Error::Budget("checkpoint bytes"));
            }
        }
        Ok(out)
    }
    fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() > MAX_CHECKPOINT_BYTES {
            return Err(Error::Budget("checkpoint bytes"));
        }
        let mut input = Input(
            payload
                .strip_prefix(&[2])
                .ok_or(Error::Invalid("not a checkpoint"))?,
        );
        let epoch = input.u64()?;
        let covered_end = input.u64()?;
        let count = input.u32()? as usize;
        if count > MAX_SCAN_RECORDS || count > input.0.len() / 14 {
            return Err(Error::Budget("checkpoint rows"));
        }
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let key = input.key()?;
            let epoch = input.u64()?;
            let len = input.u32()?;
            let value = if len == u32::MAX {
                None
            } else {
                if len as usize > MAX_VALUE {
                    return Err(Error::Budget("checkpoint value bytes"));
                }
                Some(input.take(len as usize)?.to_vec())
            };
            rows.push(SavedVersion { key, epoch, value });
        }
        input.finish()?;
        Ok(Self {
            epoch,
            covered_end,
            rows,
        })
    }
    fn validate(
        &self,
        journal: &Journal,
        layout: Layout,
        epoch: u64,
        covered_end: u64,
    ) -> Result<()> {
        if self.epoch != epoch || self.covered_end != covered_end {
            return Err(Error::Invalid("checkpoint coverage mismatch"));
        }
        let mut rows = self.rows.iter();
        for (key, head) in &journal.latest {
            for version in &head.versions {
                if layout == Layout::Cow && *version != head.epoch {
                    continue;
                }
                let row = rows
                    .next()
                    .ok_or(Error::Invalid("checkpoint omits a retained version"))?;
                if row.key != *key || row.epoch != *version {
                    return Err(Error::Invalid("checkpoint version set mismatch"));
                }
                if *version == head.epoch && row.value != head.batch.writes[head.position].value {
                    return Err(Error::Invalid("checkpoint head value mismatch"));
                }
            }
        }
        if rows.next().is_some() {
            return Err(Error::Invalid("checkpoint has extra versions"));
        }
        Ok(())
    }
    fn restore<S: Storage>(&self, layout: Layout) -> Result<Index<S>> {
        let index = Index::new(layout)?;
        for (position, row) in self.rows.iter().enumerate() {
            let latest = self
                .rows
                .get(position + 1)
                .is_none_or(|next| next.key != row.key);
            index.restore_version(&row.key, row.epoch, &row.value, latest)?;
        }
        Ok(index)
    }
}

fn journal_reader(journal: &Journal, layout: Layout) -> Result<BinaryWalReader<WalFile>> {
    let mut file = journal.file.try_clone()?;
    file.seek(SeekFrom::Start(
        PREFIX + WAL_BINARY_FILE_HEADER_BYTES as u64,
    ))?;
    Ok(BinaryWalReader::new(
        WalFile(file),
        layout.id(),
        Some(MAX_CHECKPOINT_BYTES),
    ))
}

pub(super) fn recover<S: Storage>(
    journal: &mut Journal,
    layout: Layout,
) -> Result<(Index<S>, u64, RecoveryReport)> {
    // Validate the complete evidence stream first. This bounds receipt payload
    // residency and permits skipping covered index reconstruction, while never
    // treating a checkpoint as permission to ignore corrupt earlier evidence.
    let mut reader = journal_reader(journal, layout)?;
    let mut epoch = 0;
    let mut valid_end = WAL_BINARY_FILE_HEADER_BYTES as u64;
    let mut checkpoint = None;
    loop {
        match reader
            .next_event()
            .map_err(|e| Error::Journal(e.to_string()))?
        {
            BinaryWalReadEvent::Record {
                payload,
                start_offset,
                end_offset,
                ..
            } => {
                match payload.first() {
                    Some(0) => {
                        if payload.len() > MAX_BATCH {
                            return Err(Error::Budget("encoded batch"));
                        }
                        let batch = Batch::decode(&payload)?;
                        if batch.epoch != epoch + 1
                            || journal.receipts.contains_key(&batch.operation)
                        {
                            return Err(Error::Invalid(
                                "non-contiguous epoch or duplicate operation",
                            ));
                        }
                        epoch = batch.epoch;
                        journal.record(batch, start_offset, end_offset);
                    }
                    Some(1) => {
                        let plan = GcPlan::decode(&payload)?;
                        plan.validate(journal, epoch)?;
                        plan.apply_metadata(journal)?;
                    }
                    Some(2) => {
                        let snapshot = Snapshot::decode(&payload)?;
                        snapshot.validate(journal, layout, epoch, valid_end + PREFIX)?;
                        checkpoint = Some((snapshot, end_offset));
                    }
                    _ => return Err(Error::Invalid("unknown experiment record")),
                }
                valid_end = end_offset;
            }
            BinaryWalReadEvent::Eof => break,
            BinaryWalReadEvent::TornTail { reason, .. }
            | BinaryWalReadEvent::Corrupt { reason, .. } => return Err(Error::Journal(reason)),
        }
    }
    if valid_end + PREFIX != journal.len {
        return Err(Error::Invalid("unverified trailing journal bytes"));
    }
    let mut report = RecoveryReport::default();
    let (index, covered_end) = if let Some((snapshot, end)) = checkpoint {
        report.checkpoint_epoch = snapshot.epoch;
        report.restored_versions = snapshot.rows.len();
        (snapshot.restore(layout)?, end)
    } else {
        (Index::new(layout)?, WAL_BINARY_FILE_HEADER_BYTES as u64)
    };
    let published = AtomicU64::new(report.checkpoint_epoch);
    let mut reader = BinaryWalReader::range(
        WalFile(journal.file.try_clone()?),
        layout.id(),
        Some(MAX_CHECKPOINT_BYTES),
        covered_end,
        journal.len - PREFIX,
    )
    .map_err(|error| Error::Journal(error.to_string()))?;
    loop {
        match reader
            .next_event()
            .map_err(|e| Error::Journal(e.to_string()))?
        {
            BinaryWalReadEvent::Record {
                payload,
                end_offset,
                ..
            } => {
                if end_offset <= covered_end {
                    continue;
                }
                match payload.first() {
                    Some(0) => {
                        let batch = Batch::decode(&payload)?;
                        let mut previous = BTreeMap::new();
                        if layout == Layout::Hybrid {
                            for change in &batch.writes {
                                if let Some(head) = index.current_head(&change.key)? {
                                    previous.insert(change.key.clone(), head);
                                }
                            }
                        }
                        let prepared = index.prepare(
                            &batch,
                            |key| {
                                Ok(previous.get(key).map(|(epoch, value)| Previous {
                                    epoch: *epoch,
                                    value,
                                }))
                            },
                            |_| Ok(()),
                        )?;
                        index.publish(prepared, batch.epoch, &published)?;
                        report.replayed_batches += 1;
                    }
                    Some(1) => {
                        let plan = GcPlan::decode(&payload)?;
                        for (key, version) in &plan.entries {
                            index.remove_version(key, *version)?;
                        }
                    }
                    _ => return Err(Error::Invalid("unexpected post-checkpoint record")),
                }
            }
            BinaryWalReadEvent::Eof => break,
            BinaryWalReadEvent::TornTail { reason, .. }
            | BinaryWalReadEvent::Corrupt { reason, .. } => return Err(Error::Journal(reason)),
        }
    }
    if published.load(Ordering::Relaxed) != epoch {
        return Err(Error::Invalid("replay epoch mismatch"));
    }
    journal.file.seek(SeekFrom::End(0))?;
    Ok((index, epoch, report))
}

impl<S: Storage> Store<S> {
    pub fn storage_stats(&self) -> Result<StorageStats> {
        self.0.usable()?;
        let journal = self.0.journal.lock().map_err(|_| Error::NeedsRecovery)?;
        let snapshots = self.0.snapshots.lock().map_err(|_| Error::NeedsRecovery)?;
        let mut distinct = std::collections::BTreeSet::new();
        let payload_bytes = journal
            .latest
            .values()
            .filter(|head| distinct.insert(head.epoch))
            .map(|head| head.batch.encode().len())
            .sum();
        Ok(StorageStats {
            epoch: self.0.published.load(Ordering::Acquire),
            logical_keys: journal.latest.len(),
            retained_versions: journal.retained_versions,
            receipt_count: journal.receipts.len(),
            retained_receipt_payload_bytes: payload_bytes,
            journal_bytes: journal.len,
            active_snapshots: snapshots.values().sum(),
            index_generation: self.0.index_generation.load(Ordering::Acquire),
            recovery: RecoveryReport {
                checkpoint_epoch: self.0.recovery.checkpoint_epoch,
                restored_versions: self.0.recovery.restored_versions,
                replayed_batches: self.0.recovery.replayed_batches,
            },
        })
    }
    pub fn collect_garbage(&self, work_budget: usize) -> Result<CollectionReport> {
        self.collect_with_hook(work_budget, |_| Ok(()))
    }
    pub fn collect_with_hook(
        &self,
        work_budget: usize,
        mut hook: impl FnMut(MaintenanceStage) -> Result<()>,
    ) -> Result<CollectionReport> {
        if work_budget == 0 || work_budget > MAX_GC_WORK {
            return Err(Error::Budget("GC work"));
        }
        self.0.usable()?;
        let mut journal = self.0.journal.lock().map_err(|_| Error::NeedsRecovery)?;
        let snapshots = self.0.snapshots.lock().map_err(|_| Error::NeedsRecovery)?;
        self.0.usable()?;
        let epoch = self.0.published.load(Ordering::Acquire);
        let current_horizon = snapshots.keys().next().copied().unwrap_or(epoch);
        // A pass uses one fixed safe horizon so writes cannot keep restarting
        // its cursor. If leases disappear meanwhile, finish this pass, then
        // schedule another one before reporting collection caught up.
        let horizon = journal.gc_pass_horizon.unwrap_or(current_horizon);
        if horizon > current_horizon {
            return Err(Error::Invalid("collection horizon moved backwards"));
        }
        let mut entries = Vec::new();
        let mut work = 0;
        let mut next = None;
        let mut complete = true;
        for (key, head) in journal.latest.range::<[u8], _>((
            Bound::Included(journal.gc_cursor.as_deref().unwrap_or(&[])),
            Bound::Unbounded,
        )) {
            let mut offset = 0;
            while head
                .versions
                .get(offset + 1)
                .is_some_and(|version| *version <= horizon)
                && work < work_budget
            {
                entries.push((key.clone(), head.versions[offset]));
                offset += 1;
                work += 1;
            }
            if work == work_budget {
                next = Some(key.clone());
                complete = false;
                break;
            }
            work += 1;
            if work == work_budget {
                // Minimum lexicographic successor among bounded logical keys.
                let mut successor = key.clone();
                if successor.len() < MAX_KEY {
                    successor.push(0);
                    next = Some(successor);
                } else if let Some(pos) = successor.iter().rposition(|byte| *byte != 255) {
                    successor[pos] += 1;
                    successor.truncate(pos + 1);
                    next = Some(successor);
                }
                complete = next.is_none();
                break;
            }
        }
        let plan = GcPlan {
            epoch,
            horizon,
            entries,
        };
        if !plan.entries.is_empty() {
            plan.validate(&journal, epoch)?;
            let (frame, len) = journal.frame(&plan.encode(), self.0.layout.id())?;
            let mut uncertain = Uncertain {
                poisoned: &self.0.poisoned,
                armed: true,
            };
            let outcome: Result<()> = (|| {
                journal.write_frame(&frame)?;
                hook(MaintenanceStage::Durable)?;
                let index = self.0.index.read().map_err(|_| Error::NeedsRecovery)?;
                for (n, (key, version)) in plan.entries.iter().enumerate() {
                    index.remove_version(key, *version)?;
                    hook(MaintenanceStage::Removed(n + 1))?;
                }
                plan.apply_metadata(&mut journal)?;
                hook(MaintenanceStage::Published)?;
                Ok(())
            })();
            outcome.map_err(|e| Error::CommitUncertain(e.to_string()))?;
            journal.len = len;
            uncertain.armed = false;
        }
        journal.gc_cursor = next;
        journal.gc_pass_horizon = (!complete).then_some(horizon);
        Ok(CollectionReport {
            horizon,
            examined: work,
            removed: plan.entries.len(),
            complete_pass: complete && horizon == current_horizon,
            retained_versions: journal.retained_versions,
        })
    }
    pub fn checkpoint(&self) -> Result<CheckpointReport> {
        self.checkpoint_with_hook(|_| Ok(()))
    }
    pub fn checkpoint_with_hook(
        &self,
        mut hook: impl FnMut(MaintenanceStage) -> Result<()>,
    ) -> Result<CheckpointReport> {
        self.0.usable()?;
        let mut journal = self.0.journal.lock().map_err(|_| Error::NeedsRecovery)?;
        self.0.usable()?;
        let epoch = self.0.published.load(Ordering::Acquire);
        let mut snapshot = Snapshot {
            epoch,
            covered_end: journal.len,
            rows: Vec::new(),
        };
        {
            let index = self.0.index.read().map_err(|_| Error::NeedsRecovery)?;
            let mut bytes = 21;
            for (key, head) in &journal.latest {
                for version in &head.versions {
                    if self.0.layout == Layout::Cow && *version != head.epoch {
                        continue;
                    }
                    let value = index.retained_value(key, *version, *version == head.epoch)?;
                    bytes += key.len() + 14 + value.as_ref().map_or(0, Vec::len);
                    if bytes > MAX_CHECKPOINT_BYTES || snapshot.rows.len() == MAX_SCAN_RECORDS {
                        return Err(Error::Budget("checkpoint image"));
                    }
                    snapshot.rows.push(SavedVersion {
                        key: key.clone(),
                        epoch: *version,
                        value,
                    });
                }
            }
        }
        snapshot.validate(&journal, self.0.layout, epoch, journal.len)?;
        let payload = snapshot.encode()?;
        let (frame, end) = journal.frame(&payload, self.0.layout.id())?;
        let compact = snapshot.restore(self.0.layout)?;
        let mut uncertain = Uncertain {
            poisoned: &self.0.poisoned,
            armed: true,
        };
        let outcome: Result<()> = (|| {
            journal.write_frame(&frame)?;
            hook(MaintenanceStage::Durable)?;
            let retired = {
                let mut index = self.0.index.write().map_err(|_| Error::NeedsRecovery)?;
                std::mem::replace(&mut *index, compact)
            };
            self.0.index_generation.fetch_add(1, Ordering::AcqRel);
            hook(MaintenanceStage::Published)?;
            // Tree teardown can join a background worker. Keep it outside
            // the publication lock so new snapshot capture stays short.
            drop(retired);
            Ok(())
        })();
        outcome.map_err(|e| Error::CommitUncertain(e.to_string()))?;
        journal.len = end;
        uncertain.armed = false;
        Ok(CheckpointReport {
            epoch,
            covered_end: snapshot.covered_end,
            end,
            restored_versions: snapshot.rows.len(),
        })
    }
}
