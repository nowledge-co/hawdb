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

use super::*;
use crate::build_memory::{checked_add, checked_mul, document_bytes, MAP_ENTRY_BYTES};
use std::mem::size_of;

pub(super) struct Admission<'a> {
    lease: QueryMemoryLease,
    task: &'a RuntimeTaskContext,
    max_metadata_fields: usize,
}

impl<'a> Admission<'a> {
    pub(super) fn new(
        memory: &BuildMemory,
        max_metadata_fields: usize,
        task: &'a RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let lease = memory
            .input
            .reserve(size_of::<SearchDocument>() + MAP_ENTRY_BYTES)?;
        Ok(Self {
            lease,
            task,
            max_metadata_fields,
        })
    }

    pub(super) fn checkpoint(&self) -> Result<()> {
        checkpoint(self.task)
    }

    pub(super) fn reserve<T>(&mut self, values: &mut Vec<T>) -> Result<()> {
        if values.len() < values.capacity() {
            return Ok(());
        }
        self.checkpoint()?;
        let old_bytes = checked_mul(values.capacity(), size_of::<T>())?;
        let capacity = checked_add(values.len(), 1)?
            .max(values.capacity().saturating_mul(2))
            .max(if size_of::<T>() == 1 { 8 } else { 4 });
        let new_bytes = checked_mul(capacity, size_of::<T>())?;
        // A realloc may overlap the old and new payloads. Admit both first.
        self.lease.grow(new_bytes)?;
        if let Err(error) = values.try_reserve_exact(capacity - values.len()) {
            self.lease.shrink(new_bytes);
            return Err(HawDBError::Storage(format!(
                "cannot allocate a decoded field: {error}"
            )));
        }
        if values.capacity() > capacity {
            return Err(HawDBError::Execution(
                "decoded field capacity exceeded admission".into(),
            ));
        }
        self.lease.shrink(old_bytes);
        Ok(())
    }

    pub(super) fn admit_field(&self, index: usize) -> Result<()> {
        self.checkpoint()?;
        if index >= self.max_metadata_fields {
            return Err(HawDBError::Storage(
                "search spool metadata field count exceeds admission".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn reserve_entry(&mut self) -> Result<()> {
        self.lease.grow(MAP_ENTRY_BYTES)
    }

    pub(super) fn release(&mut self, bytes: usize) {
        self.lease.shrink(bytes);
    }

    pub(super) fn finish(&mut self, document: &SearchDocument) -> Result<()> {
        self.checkpoint()?;
        let actual = document_bytes(document)?;
        if actual > self.lease.bytes() {
            return Err(HawDBError::Execution(
                "decoded document capacity exceeded admission".into(),
            ));
        }
        self.lease.shrink(self.lease.bytes() - actual);
        Ok(())
    }

    pub(super) fn into_lease(self) -> QueryMemoryLease {
        self.lease
    }
}
