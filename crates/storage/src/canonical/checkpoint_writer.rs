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

//! Retain canonical property-key admission through its final manifest consumer.
//! The dictionary indexes fixed-width digests and compares collision candidates
//! in bounded byte units. Writer payloads and other arrays are separate resources.

use super::*;
use crate::background::{
    CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointOperationError,
};
use std::cell::RefCell;

pub(super) struct Keys {
    pub(super) values: Vec<String>,
    pub(super) memory: CheckpointAllocationOwner,
}

pub(crate) struct CheckpointCanonicalManifest {
    // The complete key table must die before its admitted inventory.
    pub(super) manifest: CanonicalSegmentManifest,
    pub(super) memory: CheckpointAllocationOwner,
}

impl CheckpointCanonicalManifest {
    pub(crate) fn encode_with_work_context(
        &self,
        work: &CheckpointWorkContext,
    ) -> Result<crate::background::CheckpointText, CanonicalSegmentError> {
        self.manifest.encode_with_work_context(work)
    }

    pub(super) fn into_unadmitted(self) -> CanonicalSegmentManifest {
        assert!(
            self.memory.is_empty(),
            "admitted canonical keys cannot detach their inventory"
        );
        self.manifest
    }
}

pub(super) struct Dictionary {
    keys: Vec<String>,
    ids: BTreeMap<[u8; 32], Vec<u32>>,
    key_memory: CheckpointAllocationOwner,
    index_memory: CheckpointAllocationOwner,
    work: CheckpointWorkContext,
}

impl Dictionary {
    pub(super) fn work_context(&self) -> &CheckpointWorkContext {
        &self.work
    }

    pub(super) fn new(work: CheckpointWorkContext) -> Self {
        Self {
            keys: Vec::new(),
            ids: BTreeMap::new(),
            key_memory: CheckpointAllocationOwner::default(),
            index_memory: CheckpointAllocationOwner::default(),
            work,
        }
    }

    pub(super) fn intern(&mut self, key: &str) -> Result<u32, CanonicalSegmentError> {
        let work = self.work.clone();
        work.classify(|work| {
            let mut hash = IntegrityHasher::new();
            for bytes in key.as_bytes().chunks(64 * 1024) {
                let unit = work.start_unit()?;
                hash.update(bytes);
                unit.finish();
                work.checkpoint()?;
            }
            let digest = *hash.finish().sha256.as_bytes();
            let keys = CheckpointDecodeContext {
                work: work.clone(),
                memory: RefCell::new(std::mem::take(&mut self.key_memory)),
            };
            let index = CheckpointDecodeContext {
                work: work.clone(),
                memory: RefCell::new(std::mem::take(&mut self.index_memory)),
            };
            let result = self.intern_digest(key, digest, &keys, &index);
            self.key_memory = keys.memory.into_inner();
            self.index_memory = index.memory.into_inner();
            result
        })
        .map_err(|error| match error {
            CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
            CheckpointOperationError::Operation(error) => error,
        })
    }

    fn intern_digest(
        &mut self,
        key: &str,
        digest: [u8; 32],
        keys: &CheckpointDecodeContext,
        index: &CheckpointDecodeContext,
    ) -> Result<u32, CanonicalSegmentError> {
        let unit = keys.start_unit()?;
        let existing = self.ids.get(&digest);
        unit.finish();
        if let Some(existing) = existing {
            for id in existing {
                let unit = keys.start_unit()?;
                let previous = &self.keys[*id as usize];
                let same_length = previous.len() == key.len();
                unit.finish();
                if same_length && equal(previous.as_bytes(), key.as_bytes(), keys)? {
                    return Ok(*id);
                }
            }
        }
        let id = u32_len(self.keys.len(), "canonical property key table")?;
        let unit = keys.start_unit()?;
        let token = keys.reserve(key.len()).map_err(source)?;
        let mut owned = String::new();
        owned.try_reserve_exact(key.len()).map_err(|error| {
            CanonicalSegmentError::Work(keys.record_failure(CheckpointWorkError::Allocation {
                bytes: key.len() as u64,
                reason: error.to_string(),
            }))
        })?;
        if owned.capacity() != key.len() {
            return Err(CanonicalSegmentError::Work(keys.record_failure(
                CheckpointWorkError::Allocation {
                    bytes: key.len() as u64,
                    reason: "canonical property key capacity exceeds its admission".into(),
                },
            )));
        }
        token.address(owned.as_ptr() as usize);
        unit.finish();
        let mut remaining = key;
        while !remaining.is_empty() {
            let mut end = remaining.len().min(64 * 1024);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            let unit = keys.start_unit()?;
            owned.push_str(&remaining[..end]);
            unit.finish();
            keys.checkpoint()?;
            remaining = &remaining[end..];
        }
        keys.push(&mut self.keys, owned).map_err(source)?;
        let unit = index.start_unit()?;
        if let std::collections::btree_map::Entry::Vacant(entry) = self.ids.entry(digest) {
            // Pinned Rust B-tree nodes have at most eleven key/value pairs
            // and twelve child pointers. Reserve four conservative node bounds
            // per distinct digest before insertion, covering split/root overlap.
            // Allocator rounding/latency and other writer containers are separate.
            let node = 64
                + 11 * (std::mem::size_of::<[u8; 32]>() + std::mem::size_of::<Vec<u32>>())
                + 12 * std::mem::size_of::<usize>();
            index.reserve(4 * node).map_err(source)?;
            entry.insert(Vec::new());
        }
        unit.finish();
        index
            .push(self.ids.get_mut(&digest).expect("inserted digest"), id)
            .map_err(source)?;
        keys.checkpoint()?;
        Ok(id)
    }

    pub(super) fn into_keys(self) -> Keys {
        let Self {
            keys,
            ids,
            key_memory,
            index_memory,
            work,
        } = self;
        drop(ids);
        drop(index_memory);
        drop(work);
        Keys {
            values: keys,
            memory: key_memory,
        }
    }
}

fn equal(a: &[u8], b: &[u8], work: &CheckpointWorkContext) -> Result<bool, CanonicalSegmentError> {
    for (a, b) in a.chunks(64 * 1024).zip(b.chunks(64 * 1024)) {
        let unit = work.start_unit()?;
        let equal = a == b;
        unit.finish();
        work.checkpoint()?;
        if !equal {
            return Ok(false);
        }
    }
    work.checkpoint()?;
    Ok(true)
}

fn source(error: hawdb_core::HawDBError) -> CanonicalSegmentError {
    // The classifier restores recorded work failures without parsing text.
    CanonicalSegmentError::Corrupt(error.to_string())
}

mod value;
pub(super) use value::encode as encode_value;

pub(super) mod record;

pub(super) enum Record {
    Ordinary(Vec<u8>),
    Checkpoint(crate::background::CheckpointBytes),
}

impl std::ops::Deref for Record {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Ordinary(bytes) => bytes,
            Self::Checkpoint(bytes) => bytes,
        }
    }
}

#[cfg(test)]
mod tests;
