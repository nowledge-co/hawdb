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

//! Allocation ownership for copies of pinned graph record pages.
//! Pinned std B-tree/Arc layouts and allocator latency are platform assumptions.
//! Record byte traversal, wide record cloning and other graph/schema
//! structures still require their complete preparation/replay qualification.

use super::*;
use crate::background::{CheckpointAllocationOwner, CheckpointWorkContext, CheckpointWorkError};
use std::mem::{align_of, size_of};
use std::sync::atomic::{AtomicUsize, Ordering};

type Result<T> = std::result::Result<T, CheckpointWorkError>;

/// Split descendants consume one shared preflight allowance. Copying the
/// actual B-tree creates a new allowance instead of duplicating these credits.
#[derive(Debug)]
pub(super) struct CheckpointGrowthBudget {
    remaining: AtomicUsize,
    ceiling: u64,
}

impl CheckpointGrowthBudget {
    pub(super) fn consume_insertion(&self) {
        // Frontend writes may outlive a controlled replay. Exhausted credits
        // stay exhausted; the next controlled preflight must admit more.
        let _ = self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            });
    }

    fn covers(&self, count: usize, work: &CheckpointWorkContext) -> bool {
        self.ceiling <= work.memory_ceiling() && self.remaining.load(Ordering::Relaxed) >= count
    }
}

pub(crate) trait CheckpointCopyBytes {
    fn checkpoint_copy_bytes(&self) -> usize;
}

impl CheckpointCopyBytes for NodeId {
    fn checkpoint_copy_bytes(&self) -> usize {
        size_of::<Self>()
    }
}

impl CheckpointCopyBytes for RelId {
    fn checkpoint_copy_bytes(&self) -> usize {
        size_of::<Self>()
    }
}

impl CheckpointCopyBytes for NodeRecord {
    fn checkpoint_copy_bytes(&self) -> usize {
        hawdb_core::ids::node_allocation_bytes(self)
    }
}

impl CheckpointCopyBytes for RelRecord {
    fn checkpoint_copy_bytes(&self) -> usize {
        hawdb_core::ids::relationship_allocation_bytes(self)
    }
}

fn overflow(work: &CheckpointWorkContext) -> CheckpointWorkError {
    work.record_failure(CheckpointWorkError::Allocation {
        bytes: u64::MAX,
        reason: "checkpoint COW copy capacity overflows usize".into(),
    })
}

fn ownership_bytes<T>() -> usize {
    size_of::<CowData<T>>() + 2 * size_of::<std::sync::atomic::AtomicUsize>()
}

fn tree_node_bytes<K, V>() -> usize {
    // Pinned Rust B=6: include the largest internal-node layout, padded fields
    // and a conservative node count that covers root and underfilled nodes.
    let alignment = align_of::<K>()
        .max(align_of::<V>())
        .max(align_of::<usize>());
    size_of::<Option<std::ptr::NonNull<u8>>>()
        + 2 * size_of::<u16>()
        + 11 * (size_of::<K>() + size_of::<V>())
        + 12 * size_of::<std::ptr::NonNull<u8>>()
        + 8 * (alignment - 1)
}

fn tree_bytes<K, V>(len: usize, work: &CheckpointWorkContext) -> Result<usize> {
    if len == 0 {
        return Ok(0);
    }
    (len / 4 + 2)
        .checked_mul(tree_node_bytes::<K, V>())
        .ok_or_else(|| overflow(work))
}

fn owned<T>(value: T, memory: CheckpointAllocationOwner) -> CowSegment<T> {
    CowSegment(Arc::new(CowData {
        value,
        growth: None,
        _memory: memory,
    }))
}

impl<K, V> CowSegmentedMap<K, V>
where
    K: Ord + Clone + CowPageWeight,
    V: Clone + CowPageWeight,
{
    pub(crate) fn prepare_checkpoint_copy_for_key(
        &mut self,
        key: &K,
        work: &CheckpointWorkContext,
    ) -> Result<()>
    where
        K: CheckpointCopyBytes,
        V: CheckpointCopyBytes,
    {
        self.prepare_checkpoint_page_copy(key, false, 0, work)
    }

    pub(crate) fn prepare_checkpoint_insert_copy_for_key(
        &mut self,
        key: &K,
        work: &CheckpointWorkContext,
    ) -> Result<()>
    where
        K: CheckpointCopyBytes,
        V: CheckpointCopyBytes,
    {
        self.prepare_checkpoint_insertions_for_key(key, 1, work)
    }

    pub(crate) fn prepare_checkpoint_insertions_for_key(
        &mut self,
        key: &K,
        planned_insertions: usize,
        work: &CheckpointWorkContext,
    ) -> Result<()>
    where
        K: CheckpointCopyBytes,
        V: CheckpointCopyBytes,
    {
        self.prepare_checkpoint_page_copy(key, true, planned_insertions.max(1), work)
    }

    fn prepare_checkpoint_page_copy(
        &mut self,
        key: &K,
        inserting: bool,
        planned_insertions: usize,
        work: &CheckpointWorkContext,
    ) -> Result<()>
    where
        K: CheckpointCopyBytes,
        V: CheckpointCopyBytes,
    {
        let Some(index) = self.segment_index(key) else {
            if inserting {
                return self.prepare_checkpoint_initial_page(planned_insertions, work);
            }
            return Ok(());
        };
        // Insertion detaches the target page even when the key is absent.
        // Missing-key updates/deletes leave the original page untouched.
        if !inserting && !self.segments[index].contains_key(key) {
            return Ok(());
        }
        let copy_directory = Arc::strong_count(&self.segments.0) > 1;
        let copy_page = copy_directory || Arc::strong_count(&self.segments[index].0) > 1;
        let grow_directory = inserting
            && self.segments.capacity()
                < self
                    .segments
                    .len()
                    .checked_add(planned_insertions)
                    .ok_or_else(|| overflow(work))?;
        if !copy_page && !inserting {
            return work.checkpoint();
        }
        // Declare inventories before temporary data so every failed build
        // destroys its buffers before refunding admission. Install both copies
        // only after the final check; denial preserves original shared pages.
        let mut directory_memory = CheckpointAllocationOwner::default();
        let mut page_memory = CheckpointAllocationOwner::default();
        let mut directory = None;
        if copy_directory || grow_directory {
            let capacity = self
                .segments
                .len()
                .checked_add(if inserting { planned_insertions } else { 0 })
                .ok_or_else(|| overflow(work))?;
            let bytes = capacity
                .checked_mul(size_of::<CowSegment<BTreeMap<K, V>>>())
                .and_then(|bytes| {
                    bytes.checked_add(ownership_bytes::<Vec<CowSegment<BTreeMap<K, V>>>>())
                })
                .ok_or_else(|| overflow(work))?;
            let unit = work.start_unit()?;
            directory_memory.reserve(bytes, work)?;
            let mut values = Vec::new();
            values.try_reserve_exact(capacity).map_err(|error| {
                work.record_failure(CheckpointWorkError::Allocation {
                    bytes: bytes as u64,
                    reason: error.to_string(),
                })
            })?;
            if values.capacity() != capacity {
                return Err(work.record_failure(CheckpointWorkError::Allocation {
                    bytes: bytes as u64,
                    reason: "COW directory capacity differs from admission".into(),
                }));
            }
            unit.finish();
            let count = (64 * 1024 / size_of::<CowSegment<BTreeMap<K, V>>>()).max(1);
            for group in self.segments.chunks(count) {
                let unit = work.start_unit()?;
                values.extend(group.iter().cloned());
                unit.finish();
            }
            directory = Some(values);
        }
        let source = &self.segments[index];
        if !copy_page {
            // Retain existing capacity ownership when adding admission to a
            // unique page. No record payload is copied in this case.
            page_memory = source.0._memory.clone();
        }
        let mut growth = if copy_page {
            None
        } else {
            source.0.growth.clone()
        };
        if inserting
            && !growth
                .as_ref()
                .is_some_and(|growth| growth.covers(planned_insertions, work))
        {
            let bytes = Self::checkpoint_growth_bytes(
                source.len(),
                planned_insertions,
                key.checkpoint_copy_bytes(),
                work,
            )?;
            let unit = work.start_unit()?;
            page_memory.reserve(bytes, work)?;
            growth = Some(Arc::new(CheckpointGrowthBudget {
                remaining: AtomicUsize::new(planned_insertions),
                ceiling: work.memory_ceiling(),
            }));
            unit.finish();
        }
        let mut values = None;
        if copy_page {
            let mut bytes = ownership_bytes::<BTreeMap<K, V>>()
                .checked_add(tree_bytes::<K, V>(source.len(), work)?)
                .ok_or_else(|| overflow(work))?;
            for (key, value) in source.iter() {
                let unit = work.start_unit()?;
                bytes = bytes
                    .checked_add(key.checkpoint_copy_bytes())
                    .and_then(|bytes| bytes.checked_add(value.checkpoint_copy_bytes()))
                    .ok_or_else(|| overflow(work))?;
                unit.finish();
            }
            let unit = work.start_unit()?;
            page_memory.reserve(bytes, work)?;
            unit.finish();
            let mut copied = BTreeMap::new();
            for (key, value) in source.iter() {
                let unit = work.start_unit()?;
                copied.insert(key.clone(), value.clone());
                unit.finish();
            }
            values = Some(copied);
        }
        let unit = work.start_unit()?;
        let mut page = values.map(|values| owned(values, std::mem::take(&mut page_memory)));
        if let Some(page) = &mut page {
            Arc::get_mut(&mut page.0)
                .expect("new page is unique")
                .growth = growth.clone();
        }
        let directory = directory.map(|mut values| {
            if let Some(page) = page.take() {
                values[index] = page;
            }
            owned(values, directory_memory)
        });
        unit.finish();
        work.checkpoint()?;
        if let Some(directory) = directory {
            self.segments = directory;
        } else if let Some(page) = page {
            self.segments[index] = page;
        }
        if !copy_page {
            // Every fallible admission/cancellation check completed before
            // replacing metadata. Dropping the old directory makes this page
            // unique again even when its pointer was moved into a larger one.
            let directory =
                Arc::get_mut(&mut self.segments.0).expect("prepared directory is unique");
            let page =
                Arc::get_mut(&mut directory.value[index].0).expect("prepared page is unique");
            page.growth = growth;
            page._memory = page_memory;
        }
        Ok(())
    }

    fn checkpoint_growth_bytes(
        source_len: usize,
        count: usize,
        key_bytes: usize,
        work: &CheckpointWorkContext,
    ) -> Result<usize> {
        let future_len = source_len
            .checked_add(count)
            .ok_or_else(|| overflow(work))?;
        let budget_bytes = size_of::<CheckpointGrowthBudget>() + 2 * size_of::<AtomicUsize>();
        // A one-record page cannot split. Its first leaf and allowance are
        // sufficient, including when an empty snapshot forces a fresh copy.
        if future_len == 1 {
            return tree_bytes::<K, V>(1, work)?
                .checked_add(budget_bytes)
                .ok_or_else(|| overflow(work));
        }
        let mut min_subtree = future_len.checked_add(1).ok_or_else(|| overflow(work))? / 2;
        let mut height = 0usize;
        while min_subtree >= 6 {
            min_subtree /= 6;
            height += 1;
        }
        // Steady B-tree capacity plus at most two underfilled border nodes
        // for every possible page split. Only one sequential insertion/split
        // pillar is transient at a time, so reserve its height bound once.
        let transient = (2 * height + 4)
            .checked_mul(tree_node_bytes::<K, V>())
            .ok_or_else(|| overflow(work))?;
        let per_split = 2usize
            .checked_mul(tree_node_bytes::<K, V>())
            .and_then(|bytes| bytes.checked_add(ownership_bytes::<BTreeMap<K, V>>()))
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .ok_or_else(|| overflow(work))?;
        let amortized = tree_bytes::<K, V>(future_len, work)?
            .checked_add(count.checked_mul(per_split).ok_or_else(|| overflow(work))?)
            .and_then(|bytes| bytes.checked_add(transient))
            .ok_or_else(|| overflow(work))?;
        // The independent per-insertion bound is tighter for small batches.
        let sequential = transient
            .checked_add(ownership_bytes::<BTreeMap<K, V>>())
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .and_then(|bytes| bytes.checked_mul(count))
            .ok_or_else(|| overflow(work))?;
        amortized
            .min(sequential)
            .checked_add(budget_bytes)
            .ok_or_else(|| overflow(work))
    }

    fn prepare_checkpoint_initial_page(
        &mut self,
        planned_insertions: usize,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        // The ordinary insert path can reuse this logically empty page. Admit
        // its first B-tree node, both Arc headers and the planned directory
        // before building any of them. Denial installs no physical root.
        let mut directory_memory = CheckpointAllocationOwner::default();
        let mut page_memory = CheckpointAllocationOwner::default();
        let directory_capacity = planned_insertions
            .checked_add(1)
            .ok_or_else(|| overflow(work))?;
        let directory_bytes = directory_capacity
            .checked_mul(size_of::<CowSegment<BTreeMap<K, V>>>())
            .and_then(|bytes| {
                bytes.checked_add(ownership_bytes::<Vec<CowSegment<BTreeMap<K, V>>>>())
            })
            .ok_or_else(|| overflow(work))?;
        let page_bytes = ownership_bytes::<BTreeMap<K, V>>()
            .checked_add(Self::checkpoint_growth_bytes(
                0,
                planned_insertions,
                size_of::<K>(),
                work,
            )?)
            .ok_or_else(|| overflow(work))?;
        let unit = work.start_unit()?;
        directory_memory.reserve(directory_bytes, work)?;
        unit.finish();
        let unit = work.start_unit()?;
        page_memory.reserve(page_bytes, work)?;
        unit.finish();
        let unit = work.start_unit()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(directory_capacity)
            .map_err(|error| {
                work.record_failure(CheckpointWorkError::Allocation {
                    bytes: directory_bytes as u64,
                    reason: error.to_string(),
                })
            })?;
        if values.capacity() != directory_capacity {
            return Err(work.record_failure(CheckpointWorkError::Allocation {
                bytes: directory_bytes as u64,
                reason: "initial COW directory capacity differs from admission".into(),
            }));
        }
        let mut page = owned(BTreeMap::new(), page_memory);
        Arc::get_mut(&mut page.0)
            .expect("new page is unique")
            .growth = Some(Arc::new(CheckpointGrowthBudget {
            remaining: AtomicUsize::new(planned_insertions),
            ceiling: work.memory_ceiling(),
        }));
        values.push(page);
        let directory = owned(values, directory_memory);
        unit.finish();
        work.checkpoint()?;
        self.segments = directory;
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
