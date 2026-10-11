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
use crate::background::{CheckpointBytes, CheckpointWorkContext};
use crate::scan::CheckpointRangeReadError;

pub(crate) struct CheckpointSpillValue {
    block: CheckpointBytes,
    value: std::ops::Range<usize>,
}

impl std::ops::Deref for CheckpointSpillValue {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.block[self.value.clone()]
    }
}

impl PropertySpillReader {
    pub(crate) fn checkpoint_value(
        &self,
        id: u64,
        work: &CheckpointWorkContext,
    ) -> Result<Option<CheckpointSpillValue>, PropertySpillError> {
        self.ensure_healthy()?;
        let result = self.checkpoint_value_inner(id, work);
        self.poison_on_physical_failure(&result);
        result
    }

    fn checkpoint_value_inner(
        &self,
        id: u64,
        work: &CheckpointWorkContext,
    ) -> Result<Option<CheckpointSpillValue>, PropertySpillError> {
        work.checkpoint()?;
        if id >= self.manifest.value_count {
            return Ok(None);
        }
        // Admit descriptor traversal independently from payload hydration.
        let mut selected = None;
        let mut error = None;
        let read = self.descriptor_reader.checkpoint_scan_from(
            &id.to_be_bytes(),
            GraphDescriptorTreeReadLimits {
                max_descriptors: NonZeroU64::new(1).expect("one spill block"),
                ..Default::default()
            },
            work,
            |key, value| {
                match PropertySpillBlockDescriptor::decode_descriptor_tree_entry(key, value) {
                    Ok(block) => selected = Some(block),
                    Err(e) => error = Some(e),
                }
                Ok(GraphDescriptorTreeScanControl::Stop)
            },
        );
        if let Some(error) = error {
            return Err(error);
        }
        read.map_err(|error| match error {
            crate::graph_descriptor_tree::GraphDescriptorTreeError::Work(error) => {
                PropertySpillError::Work(error)
            }
            error => PropertySpillError::DescriptorTree(error),
        })?;
        work.checkpoint()?;
        let descriptor = selected.ok_or_else(|| {
            PropertySpillError::Corrupt(format!(
                "property spill descriptor tree has no block for admitted id {id}"
            ))
        })?;
        if id < descriptor.min_spill_id || id > descriptor.max_spill_id {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill descriptor block {} does not contain admitted id {id}",
                descriptor.block_id
            )));
        }
        if descriptor.length.get() > self.max_block_bytes.get() {
            return Err(PropertySpillError::BlockTooLarge {
                block_bytes: descriptor.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        let range = SegmentReadRange::new(
            self.manifest.artifact_id,
            descriptor.block_id,
            descriptor.offset,
            descriptor.length,
        )
        .with_content_digest(descriptor.content_digest);
        let bytes = self
            .range_reader
            .checkpoint_range_with_work_context(&range, work)
            .map_err(|e| match e {
                CheckpointRangeReadError::Read(e) => PropertySpillError::Read(e),
                CheckpointRangeReadError::Work(e) => PropertySpillError::Work(e),
            })?;
        let unit = work.start_unit()?;
        let mut cursor = Cursor::new(&bytes);
        if cursor.read_exact(8)? != BLOCK_HEADER {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill block {} has an invalid header",
                descriptor.block_id
            )));
        }
        let generation = cursor.read_u64()?;
        let block_id = cursor.read_u64()?;
        let count = cursor.read_u32()?;
        if generation != self.manifest.generation.0
            || block_id != descriptor.block_id
            || count != descriptor.value_count
        {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill block {} metadata does not match its manifest",
                descriptor.block_id
            )));
        }
        unit.finish();
        work.checkpoint()?;
        let mut first = None;
        let mut previous = None;
        let mut found = None;
        for _ in 0..count {
            let unit = work.start_unit()?;
            let spill_id = cursor.read_u64()?;
            let length = usize::try_from(cursor.read_u64()?).map_err(|_| {
                PropertySpillError::Corrupt("property spill value length exceeds usize".into())
            })?;
            if previous.is_some_and(|previous| spill_id <= previous) {
                return Err(PropertySpillError::Corrupt(format!(
                    "property spill block {} ids are not strictly ordered",
                    descriptor.block_id
                )));
            }
            let start = cursor.offset;
            cursor.read_exact(length)?;
            if spill_id == id {
                found = Some(start..cursor.offset);
            }
            first.get_or_insert(spill_id);
            previous = Some(spill_id);
            unit.finish();
            work.checkpoint()?;
        }
        if !cursor.is_empty()
            || first != Some(descriptor.min_spill_id)
            || previous != Some(descriptor.max_spill_id)
        {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill block {} payload bounds are inconsistent",
                descriptor.block_id
            )));
        }
        work.checkpoint()?;
        Ok(found.map(|value| CheckpointSpillValue {
            block: bytes,
            value,
        }))
    }
}
