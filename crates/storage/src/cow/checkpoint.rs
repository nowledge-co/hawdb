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
//! Record byte traversal, wide record cloning, growth and other graph/schema
//! structures still require their complete preparation/replay qualification.

use super::*;
use crate::background::{CheckpointAllocationOwner, CheckpointWorkContext, CheckpointWorkError};
use std::mem::{align_of, size_of};

type Result<T> = std::result::Result<T, CheckpointWorkError>;

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

fn tree_bytes<K, V>(len: usize, work: &CheckpointWorkContext) -> Result<usize> {
    if len == 0 {
        return Ok(0);
    }
    // Pinned Rust B=6: include the largest internal-node layout, padded fields
    // and a conservative node count that covers root and underfilled nodes.
    let alignment = align_of::<K>()
        .max(align_of::<V>())
        .max(align_of::<usize>());
    let node = size_of::<Option<std::ptr::NonNull<u8>>>()
        + 2 * size_of::<u16>()
        + 11 * (size_of::<K>() + size_of::<V>())
        + 12 * size_of::<std::ptr::NonNull<u8>>()
        + 8 * (alignment - 1);
    (len / 4 + 2)
        .checked_mul(node)
        .ok_or_else(|| overflow(work))
}

fn owned<T>(value: T, memory: CheckpointAllocationOwner) -> CowSegment<T> {
    CowSegment(Arc::new(CowData {
        value,
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
        let Some(index) = self.segment_index(key) else {
            return Ok(());
        };
        if !self.segments[index].contains_key(key) {
            return Ok(());
        }
        let copy_directory = Arc::strong_count(&self.segments.0) > 1;
        let copy_page = copy_directory || Arc::strong_count(&self.segments[index].0) > 1;
        if !copy_page {
            return work.checkpoint();
        }
        // Declare inventories before temporary data so every failed build
        // destroys its buffers before refunding admission. Install both copies
        // only after the final check; denial preserves original shared pages.
        let mut directory_memory = CheckpointAllocationOwner::default();
        let mut page_memory = CheckpointAllocationOwner::default();
        let mut directory = None;
        if copy_directory {
            let capacity = self.segments.len();
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
        let mut values = BTreeMap::new();
        for (key, value) in source.iter() {
            let unit = work.start_unit()?;
            values.insert(key.clone(), value.clone());
            unit.finish();
        }
        let unit = work.start_unit()?;
        let page = owned(values, page_memory);
        let directory = directory.map(|mut values| {
            values[index] = page.clone();
            owned(values, directory_memory)
        });
        unit.finish();
        work.checkpoint()?;
        if let Some(directory) = directory {
            self.segments = directory;
        } else {
            self.segments[index] = page;
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
