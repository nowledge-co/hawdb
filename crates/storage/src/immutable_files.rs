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

//! Shared immutable handles. Logical references survive idle descriptor eviction.

use crate::file_descriptors::{BudgetState, DescriptorCache, DescriptorKind, FileOpenContext};
use crate::file_io::{File, OpenOptions};
use crate::immutable_object::ObjectReference;
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(crate) struct ImmutableFileBinding {
    pub(crate) reference: ObjectReference,
    pub(crate) object_path: PathBuf,
}

#[derive(Debug)]
pub(crate) struct ImmutableFileHandles {
    state: Arc<BudgetState>,
    handles: Mutex<BTreeMap<ObjectReference, Arc<File>>>,
    bindings: Mutex<BTreeMap<PathBuf, ImmutableFileBinding>>,
    opening: Mutex<()>,
}

impl ImmutableFileHandles {
    pub(crate) fn new(state: Arc<BudgetState>) -> Self {
        Self {
            state,
            handles: Mutex::new(BTreeMap::new()),
            bindings: Mutex::new(BTreeMap::new()),
            opening: Mutex::new(()),
        }
    }

    pub(crate) fn bind(&self, path: &Path, binding: ImmutableFileBinding) -> io::Result<()> {
        self.bindings
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(crate::file_descriptors::absolute_path(path)?, binding);
        Ok(())
    }

    pub(crate) fn binding(&self, path: &Path) -> io::Result<Option<ImmutableFileBinding>> {
        let path = crate::file_descriptors::absolute_path_ref(path)?;
        Ok(self
            .bindings
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(path.as_ref())
            .cloned())
    }

    pub(crate) fn unbind(&self, path: &Path) -> io::Result<()> {
        let path = crate::file_descriptors::absolute_path_ref(path)?;
        self.bindings
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(path.as_ref());
        Ok(())
    }

    /// A live read owns an Arc. Only the cache's final, idle Arc is evictable.
    pub(crate) fn get(
        &self,
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
    ) -> io::Result<Arc<File>> {
        if let Some(file) = self
            .handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&binding.reference)
            .cloned()
        {
            self.state.record_cache_hit();
            return Ok(file);
        }
        // Coalesce cold opens without holding the eviction map across admission.
        // This serializes file validation, never waits for descriptor capacity.
        let _opening = self
            .opening
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(file) = self
            .handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&binding.reference)
            .cloned()
        {
            self.state.record_cache_hit();
            return Ok(file);
        }
        self.state.record_cache_miss();
        let mut file = OpenOptions::new()
            .read(true)
            .descriptor_kind(DescriptorKind::ImmutableCache)
            .open_with_context(&binding.object_path, context)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != binding.reference.byte_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable handle identity length mismatch",
            ));
        }
        let mut hasher = crate::immutable_object::identity_hasher(
            binding.reference.kind,
            binding.reference.format_version,
            binding.reference.byte_length,
        );
        let mut buffer = [0_u8; 64 * 1024];
        let mut read_bytes = 0_u64;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            read_bytes = read_bytes
                .checked_add(read as u64)
                .ok_or_else(|| io::Error::other("immutable file length overflow"))?;
            if read_bytes > binding.reference.byte_length {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "immutable handle grew during validation",
                ));
            }
            hasher.update(&buffer[..read]);
        }
        if read_bytes != binding.reference.byte_length
            || hasher.finish().sha256 != binding.reference.sha256
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable handle identity digest mismatch",
            ));
        }
        let file = Arc::new(file);
        self.handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(binding.reference, file.clone());
        Ok(file)
    }
}

impl DescriptorCache for ImmutableFileHandles {
    fn evict_idle(&self, requested: usize) -> usize {
        let removed = {
            let mut handles = self
                .handles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let keys = handles
                .iter()
                .filter(|(_, handle)| Arc::strong_count(handle) == 1)
                .take(requested)
                .map(|(key, _)| *key)
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| handles.remove(&key))
                .collect::<Vec<_>>()
        };
        let count = removed.len();
        drop(removed);
        self.state.record_cache_evictions(count);
        count
    }
}
