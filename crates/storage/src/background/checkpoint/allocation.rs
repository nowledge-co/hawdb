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

//! Retain decoded allocation leases through moves and COW snapshot ownership.
//! The inventory is persistent: cloning a captured owner does not copy cells.
//! Dropping a long unshared inventory does not recurse or allocate scratch.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Allocation {
    buffer: Mutex<Option<Box<dyn RuntimeMemoryPermit>>>,
    address: AtomicUsize,
    next: Option<Arc<Self>>,
    _metadata: Option<Box<dyn RuntimeMemoryPermit>>,
}

#[derive(Default, Clone)]
pub(crate) struct CheckpointAllocationOwner {
    head: Option<Arc<Allocation>>,
}

impl std::fmt::Debug for CheckpointAllocationOwner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointAllocationOwner")
            .field("has_retained_allocations", &self.head.is_some())
            .finish()
    }
}

pub(crate) struct CheckpointAllocationToken(Option<Arc<Allocation>>);

impl CheckpointAllocationToken {
    pub(crate) fn address(&self, address: usize) {
        if let Some(node) = &self.0 {
            node.address.store(address, Ordering::Relaxed);
        }
    }

    /// The caller must destroy the previous backing allocation first.
    /// Its inventory cell stays admitted until the inventory's last owner.
    pub(crate) fn release_buffer(self) {
        if let Some(node) = &self.0 {
            let permit = node
                .buffer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            drop(permit);
        }
    }
}

impl CheckpointAllocationOwner {
    pub(crate) const METADATA_BYTES: usize = std::mem::size_of::<Allocation>()
        + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>();

    /// The pinned Arc layout owns two reference counters. The governor also
    /// charges its concrete permit payload; allocator metadata/rounding and
    /// allocation latency remain platform assumptions.
    pub(crate) fn reserve(
        &mut self,
        bytes: usize,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointAllocationToken, CheckpointWorkError> {
        if bytes == 0 {
            return Ok(CheckpointAllocationToken(None));
        }
        let metadata = work.reserve_memory(Self::METADATA_BYTES)?;
        let buffer = work.reserve_memory(bytes)?;
        if metadata.is_none() && buffer.is_none() {
            return Ok(CheckpointAllocationToken(None));
        }
        let node = Arc::new(Allocation {
            buffer: Mutex::new(buffer),
            address: AtomicUsize::new(0),
            next: self.head.take(),
            _metadata: metadata,
        });
        self.head = Some(node.clone());
        Ok(CheckpointAllocationToken(Some(node)))
    }

    pub(crate) fn find(
        &self,
        address: usize,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointAllocationToken, CheckpointWorkError> {
        let mut current = self.head.as_ref();
        while let Some(node) = current {
            let unit = work.start_unit()?;
            let found = node.address.load(Ordering::Relaxed) == address;
            unit.finish();
            if found {
                return Ok(CheckpointAllocationToken(Some(node.clone())));
            }
            current = node.next.as_ref();
        }
        Ok(CheckpointAllocationToken(None))
    }

    /// Incoming decoded ownership is unique. Existing COW snapshots may share
    /// the target inventory. Change only the incoming tail after all checks;
    /// a cancellation leaves both owners intact.
    pub(crate) fn append(
        &mut self,
        incoming: &mut Self,
        work: &CheckpointWorkContext,
    ) -> Result<(), CheckpointWorkError> {
        let Some(mut current) = incoming.head.as_mut() else {
            return Ok(());
        };
        loop {
            let unit = work.start_unit()?;
            let node = Arc::get_mut(current).ok_or_else(|| {
                work.record_failure(CheckpointWorkError::Allocation {
                    bytes: 0,
                    reason: "decoded allocation transfer requires unique incoming ownership".into(),
                })
            })?;
            let last = node.next.is_none();
            unit.finish();
            if last {
                work.checkpoint()?;
                node.next = self.head.take();
                break;
            }
            current = node
                .next
                .as_mut()
                .expect("checked incoming allocation tail");
        }
        self.head = incoming.head.take();
        Ok(())
    }
}

impl Drop for CheckpointAllocationOwner {
    fn drop(&mut self) {
        let mut current = self.head.take();
        while let Some(node) = current {
            match Arc::try_unwrap(node) {
                Ok(mut node) => {
                    current = node.next.take();
                    drop(node);
                }
                Err(shared) => {
                    drop(shared);
                    break;
                }
            }
        }
    }
}
