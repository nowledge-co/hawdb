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

//! Shared Source buffers retain their allocation inventories. Publication
//! detaches only its small segment directory, with admission before copying.

use super::*;
use crate::background::{CheckpointAllocationOwner, CheckpointDecodeContext};
use std::cell::RefCell;
use std::sync::Arc;

pub(super) fn context(work: &CheckpointWorkContext) -> CheckpointDecodeContext {
    CheckpointDecodeContext {
        work: work.clone(),
        memory: RefCell::default(),
    }
}

#[derive(Debug)]
struct Rows {
    values: Vec<SourceScanRow>,
    _memory: CheckpointAllocationOwner,
}

#[derive(Debug, Clone, Default)]
pub(super) struct SharedRows(Option<Arc<Rows>>);

impl std::ops::Deref for SharedRows {
    type Target = [SourceScanRow];
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().map_or(&[], |rows| rows.values.as_slice())
    }
}

impl PartialEq<&[SourceScanRow]> for SharedRows {
    fn eq(&self, other: &&[SourceScanRow]) -> bool {
        &**self == *other
    }
}

impl SharedRows {
    pub(super) fn new(values: Vec<SourceScanRow>, work: CheckpointDecodeContext) -> Result<Self> {
        work.reserve(std::mem::size_of::<Rows>() + 2 * std::mem::size_of::<usize>())?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let output = Self(Some(Arc::new(Rows {
            values,
            _memory: work.memory.into_inner(),
        })));
        unit.finish();
        Ok(output)
    }
}

#[derive(Debug)]
struct Segments {
    values: Vec<SourceScanSegment>,
    memory: CheckpointAllocationOwner,
}

#[derive(Debug, Clone)]
pub(super) struct SharedSegments(Option<Arc<Segments>>);

impl std::ops::Deref for SharedSegments {
    type Target = [SourceScanSegment];
    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .map_or(&[], |segments| segments.values.as_slice())
    }
}

impl SharedSegments {
    pub(super) fn new(
        values: Vec<SourceScanSegment>,
        work: CheckpointDecodeContext,
    ) -> Result<Self> {
        if values.is_empty() {
            return Ok(Self(None));
        }
        work.reserve(std::mem::size_of::<Segments>() + 2 * std::mem::size_of::<usize>())?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let output = Self(Some(Arc::new(Segments {
            values,
            memory: work.memory.into_inner(),
        })));
        unit.finish();
        Ok(output)
    }

    pub(super) fn make_mut(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> Result<&mut [SourceScanSegment]> {
        let Some(storage) = self.0.as_mut() else {
            return Ok(&mut []);
        };
        if Arc::get_mut(storage).is_none() {
            let allocation = context(work);
            let mut values = Vec::new();
            for segment in self.iter() {
                allocation.push(&mut values, segment.clone())?;
            }
            let replacement = Self::new(values, allocation)?;
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            *self = replacement;
        }
        Ok(
            &mut Arc::get_mut(self.0.as_mut().expect("nonempty directory"))
                .expect("publication owns its directory")
                .values,
        )
    }

    pub(super) fn into_unadmitted(self) -> Vec<SourceScanSegment> {
        let Some(storage) = self.0 else {
            return Vec::new();
        };
        let owned = Arc::try_unwrap(storage).expect("descriptor decoder owns its directory");
        assert!(
            owned.memory.is_empty(),
            "admitted directory cannot detach its inventory"
        );
        owned.values
    }
}
