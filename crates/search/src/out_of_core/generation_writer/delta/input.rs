//! Keep the admitted delta batch alive across row conversion and spool handoff.

use crate::build_control::checkpoint;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, document_bytes, projection_row_bytes, shared::Shared,
    AdmittedDocument, BuildMemory, MAP_ENTRY_BYTES,
};
use crate::{Result, SearchDocument, SearchProjectionDelta, SearchProjectionRow, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::collections::VecDeque;
use std::mem::size_of;

pub(super) struct Input {
    pub(super) upserts: VecDeque<SearchDocument>,
    pub(super) deletes: VecDeque<String>,
    memory: Shared<QueryMemoryLease>,
}

pub(super) struct Pending {
    rows: std::vec::IntoIter<SearchProjectionRow>,
    row_slots: usize,
    upserts: VecDeque<SearchDocument>,
    deletes: VecDeque<String>,
    memory: QueryMemoryLease,
}

impl Pending {
    pub(super) fn new(
        delta: SearchProjectionDelta,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let input_bytes = capacity_bytes(&delta, task)?;
        let row_slots = mul(delta.upserts.capacity(), size_of::<SearchProjectionRow>())?;
        let lease = memory.input.reserve(input_bytes)?;
        Ok(Self {
            rows: delta.upserts.into_iter(),
            row_slots,
            upserts: VecDeque::new(),
            deletes: VecDeque::from(delta.deletes),
            memory: lease,
        })
    }

    pub(super) fn convert(self, task: &RuntimeTaskContext) -> Result<Input> {
        checkpoint(task)?;
        let mut input = self;
        let count = input.rows.len();
        let slots = mul(count, size_of::<SearchDocument>())?;
        input.memory.grow(slots)?;
        input.upserts.try_reserve_exact(count).map_err(|error| {
            SkeinError::Execution(format!("cannot allocate search delta slots: {error}"))
        })?;
        if input.upserts.capacity() > count {
            return Err(SkeinError::Execution(
                "search delta slots exceeded admission".into(),
            ));
        }
        for row in input.rows.by_ref() {
            checkpoint(task)?;
            let old = projection_row_bytes(&row)? - size_of::<SearchProjectionRow>();
            let extra = conversion_bytes(&row)?;
            input.memory.grow(extra)?;
            // Reuse the public conversion's identity and metadata overwrite rules.
            let document = row.into_document();
            let actual = document_bytes(&document)? - size_of::<SearchDocument>();
            let covered = add(old, extra)?;
            if actual > covered {
                return Err(SkeinError::Execution(
                    "search delta conversion exceeded admission".into(),
                ));
            }
            input.upserts.push_back(document);
            input.memory.shrink(covered - actual);
        }
        // An exhausted IntoIter still owns its original vector allocation.
        input.rows = Vec::new().into_iter();
        input.memory.shrink(input.row_slots);
        sort(input.upserts.make_contiguous(), task, |left, right| {
            left.id.cmp(&right.id)
        })?;
        sort(input.deletes.make_contiguous(), task, Ord::cmp)?;
        // Shared's final-drop path frees its Arc control block before this lease.
        input.memory.grow(size_of::<QueryMemoryLease>() + 64)?;
        Ok(Input {
            upserts: input.upserts,
            deletes: input.deletes,
            memory: Shared::new(input.memory),
        })
    }
}

impl Input {
    #[cfg(test)]
    pub(super) fn new(
        delta: SearchProjectionDelta,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Pending::new(delta, memory, task)?.convert(task)
    }

    pub(super) fn pop_upsert(&mut self) -> AdmittedDocument {
        AdmittedDocument::from_batch(
            self.upserts.pop_front().expect("front was present"),
            self.memory.clone(),
        )
    }
}

fn capacity_bytes(delta: &SearchProjectionDelta, task: &RuntimeTaskContext) -> Result<usize> {
    let mut bytes = add(
        mul(delta.upserts.capacity(), size_of::<SearchProjectionRow>())?,
        mul(delta.deletes.capacity(), size_of::<String>())?,
    )?;
    for row in &delta.upserts {
        checkpoint(task)?;
        bytes = add(
            bytes,
            projection_row_bytes(row)? - size_of::<SearchProjectionRow>(),
        )?;
    }
    for id in &delta.deletes {
        checkpoint(task)?;
        bytes = add(bytes, id.capacity())?;
    }
    Ok(bytes)
}

fn conversion_bytes(row: &SearchProjectionRow) -> Result<usize> {
    let kind = row.kind.as_str();
    // format! may grow its initial literal estimate. Cover replacement overlap
    // as well as the final ID, without relying on an exact allocator growth.
    let id = add(add(kind.len(), 1)?, row.external_id.len())?;
    let identity = add(mul(id.max(8), 3)?, row.external_id.len())?;
    let metadata = add(
        3 * MAP_ENTRY_BYTES,
        "kindexternal_idsource_id".len() + kind.len(),
    )?;
    add(identity, metadata)
}

fn sort<T>(
    values: &mut [T],
    task: &RuntimeTaskContext,
    compare: impl Fn(&T, &T) -> std::cmp::Ordering,
) -> Result<()> {
    // In-place heapsort permits cancellation at each sift, without changing a
    // comparator's ordering after cancellation or allocating an auxiliary run.
    fn sift<T>(
        values: &mut [T],
        mut root: usize,
        task: &RuntimeTaskContext,
        compare: &impl Fn(&T, &T) -> std::cmp::Ordering,
    ) -> Result<()> {
        while root < values.len() / 2 {
            checkpoint(task)?;
            let mut child = root * 2 + 1;
            if child + 1 < values.len() && compare(&values[child], &values[child + 1]).is_lt() {
                child += 1;
            }
            if !compare(&values[root], &values[child]).is_lt() {
                break;
            }
            values.swap(root, child);
            root = child;
        }
        Ok(())
    }
    checkpoint(task)?;
    for root in (0..values.len() / 2).rev() {
        sift(values, root, task, &compare)?;
    }
    for end in (1..values.len()).rev() {
        checkpoint(task)?;
        values.swap(0, end);
        sift(&mut values[..end], 0, task, &compare)?;
    }
    checkpoint(task)
}

#[cfg(test)]
mod tests;
