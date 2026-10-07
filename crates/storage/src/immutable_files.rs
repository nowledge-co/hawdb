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
use crate::file_io::{self as fs, File, OpenOptions};
use crate::immutable_object::ObjectReference;
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_ALIAS_CANDIDATE: AtomicU64 = AtomicU64::new(0);

struct AliasCandidate(PathBuf);

impl Drop for AliasCandidate {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn alias_candidate_path(path: &Path) -> io::Result<PathBuf> {
    let mut name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "alias has no filename"))?
        .to_os_string();
    name.push(format!(
        ".alias-{}-{}",
        std::process::id(),
        NEXT_ALIAS_CANDIDATE.fetch_add(1, Ordering::Relaxed),
    ));
    Ok(path.with_file_name(name))
}

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

    /// The caller's reachability barrier excludes new users of this object.
    /// Refuse to unlink an in-flight read; idle cache entries must not retain
    /// unlinked storage indefinitely in a long-lived project.
    pub(crate) fn retire_unreachable(&self, reference: ObjectReference) -> io::Result<()> {
        let _opening = self
            .opening
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut handles = self
            .handles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if handles
            .get(&reference)
            .is_some_and(|file| Arc::strong_count(file) != 1)
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "unreachable immutable object still has an active read",
            ));
        }
        let removed = handles.remove(&reference);
        drop(handles);
        if removed.is_some() {
            drop(removed);
            self.state.record_cache_evictions(1);
        }
        self.bindings
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, binding| binding.reference != reference);
        Ok(())
    }

    /// Mounts verified checkpoint content without copying the dataset. The
    /// alias and the immutable object share an inode; mutable opens detach it.
    pub(crate) fn mount(
        &self,
        path: &Path,
        binding: ImmutableFileBinding,
        context: &FileOpenContext,
    ) -> io::Result<()> {
        // Mount validation uses a transient handle and a fixed-size buffer;
        // mounting an unopened artifact must not warm the descriptor cache.
        // Keep the verified handle alive until the alias has been installed.
        let _verified = Self::open_verified_object(&binding, context, DescriptorKind::Transient)?;
        let candidate = loop {
            let candidate = alias_candidate_path(path)?;
            match fs::hard_link(&binding.object_path, &candidate) {
                Ok(()) => break AliasCandidate(candidate),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        // Aliases are recoverable from the durable head. Their installation
        // does not publish a schema/data commit or require a durability barrier.
        fs::rename(&candidate.0, path)?;
        self.bind(path, binding)
    }

    /// Gives a mutator an independent inode before it can change shared bytes.
    /// Only an actual write copies content; a truncating write copies no data.
    pub(crate) fn detach_for_write(
        &self,
        path: &Path,
        context: &FileOpenContext,
        truncate: bool,
    ) -> io::Result<()> {
        let (candidate, mut destination) = loop {
            let candidate = alias_candidate_path(path)?;
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open_with_context(&candidate, context)
            {
                Ok(file) => break (AliasCandidate(candidate), file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        if !truncate {
            // Read through the immutable binding's positioned cursor, never
            // through the shared cache handle's sequential cursor.
            let mut source = OpenOptions::new()
                .read(true)
                .open_with_context(path, context)?;
            io::copy(&mut source, &mut destination)?;
            destination.set_permissions(source.metadata()?.permissions())?;
        }
        destination.flush()?;
        destination.sync_all()?;
        // Close the candidate before replacement, including on Windows.
        drop(destination);
        crate::durability::durable_replace_file(&candidate.0, path)
    }

    /// A live read owns an Arc. Only the cache's final, idle Arc is evictable.
    pub(crate) fn get(
        &self,
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
    ) -> io::Result<Arc<File>> {
        self.get_admitted(binding, context, |_| Ok(()))
            .map(|(file, _)| file)
    }

    /// Cold validation is admitted before payload I/O while the opening lock
    /// coalesces readers. A warm lease performs no additional validation read.
    pub(crate) fn get_admitted(
        &self,
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
        admit_validation: impl FnMut(u64) -> io::Result<()>,
    ) -> io::Result<(Arc<File>, u64)> {
        if let Some(file) = self
            .handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&binding.reference)
            .cloned()
        {
            self.state.record_cache_hit();
            return Ok((file, 0));
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
            return Ok((file, 0));
        }
        self.state.record_cache_miss();
        let file = Arc::new(Self::open_verified_object_admitted(
            binding,
            context,
            DescriptorKind::ImmutableCache,
            admit_validation,
        )?);
        self.handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(binding.reference, file.clone());
        Ok((file, binding.reference.byte_length))
    }

    fn open_verified_object(
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
        kind: DescriptorKind,
    ) -> io::Result<File> {
        Self::open_verified_object_admitted(binding, context, kind, |_| Ok(()))
    }

    fn open_verified_object_admitted(
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
        kind: DescriptorKind,
        mut admit_validation: impl FnMut(u64) -> io::Result<()>,
    ) -> io::Result<File> {
        let mut file = OpenOptions::new()
            .read(true)
            .descriptor_kind(kind)
            .open_with_context(&binding.object_path, context)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != binding.reference.byte_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable handle identity length mismatch",
            ));
        }
        // Descriptor rejection occurs before this charge. No payload may be
        // read until the caller has reserved the complete identity validation.
        admit_validation(binding.reference.byte_length)?;
        let mut hasher = crate::immutable_object::identity_hasher(
            binding.reference.kind,
            binding.reference.format_version,
            binding.reference.byte_length,
        );
        let mut buffer = [0_u8; 64 * 1024];
        let mut remaining = binding.reference.byte_length;
        while remaining > 0 {
            let chunk = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..chunk])?;
            remaining -= chunk as u64;
            hasher.update(&buffer[..chunk]);
        }
        // Detect length drift without fetching an unadmitted EOF sentinel.
        if file.metadata()?.len() != binding.reference.byte_length
            || hasher.finish().sha256 != binding.reference.sha256
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable handle identity digest mismatch",
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_descriptors::ProjectFileDescriptors;
    use crate::immutable_object::{ImmutableObjectStore, ObjectKind};
    use std::io::{Seek, SeekFrom};

    struct Fixture {
        root: PathBuf,
        project: ProjectFileDescriptors,
        objects: ImmutableObjectStore,
        reference: ObjectReference,
        bytes: Vec<u8>,
    }

    impl Fixture {
        fn new() -> Self {
            let root = loop {
                let root = std::env::temp_dir().join(format!(
                    "hawdb-immutable-alias-{}-{}",
                    std::process::id(),
                    NEXT_ALIAS_CANDIDATE.fetch_add(1, Ordering::Relaxed),
                ));
                match fs::create_dir(&root) {
                    Ok(()) => break root,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("create immutable alias fixture: {error}"),
                }
            };
            let project = ProjectFileDescriptors::acquire_existing(&root, 8).unwrap();
            let mut objects = ImmutableObjectStore::open(root.join("immutable")).unwrap();
            let bytes = (0..128 * 1024).map(|i| (i % 251) as u8).collect::<Vec<_>>();
            let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes);
            objects.publish(reference, &bytes).unwrap();
            Self {
                root,
                project,
                objects,
                reference,
                bytes,
            }
        }

        fn mount(&self, name: &str) -> PathBuf {
            let path = self.root.join(name);
            self.project
                .immutable_handles
                .mount(
                    &path,
                    ImmutableFileBinding {
                        reference: self.reference,
                        object_path: self.objects.object_path(self.reference),
                    },
                    &crate::file_descriptors::context_for_path(&path).unwrap(),
                )
                .unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let alias = std::fs::metadata(&path).unwrap();
                let object = std::fs::metadata(self.objects.object_path(self.reference)).unwrap();
                assert_eq!((alias.dev(), alias.ino()), (object.dev(), object.ino()));
            }
            path
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn shared_alias_detaches_before_append_or_truncate_and_preserves_readers() {
        for (truncate, native_flags) in [(false, false), (true, false), (false, true), (true, true)]
        {
            let fixture = Fixture::new();
            let path = fixture.mount("selected.hawdb");
            let sibling = fixture.mount("sibling.hawdb");
            assert_eq!(fixture.project.metrics().open, 0);
            assert_eq!(fixture.project.metrics().cached_handles, 0);
            let mut snapshot = File::open(&path).unwrap();
            assert_eq!(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::AlreadyExists,
            );
            assert_eq!(fs::read(&path).unwrap(), fixture.bytes);
            #[cfg(unix)]
            let _native_read_only = if native_flags {
                use std::os::unix::fs::OpenOptionsExt;
                // Opaque native flags detach before removing the binding, so
                // a later ordinary write cannot mutate the shared object.
                Some(
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(0)
                        .open(&path)
                        .unwrap(),
                )
            } else {
                None
            };
            #[cfg(not(unix))]
            let _ = native_flags;
            if truncate {
                fs::write(&path, b"replacement").unwrap();
                assert_eq!(fs::read(&path).unwrap(), b"replacement");
            } else {
                let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
                writer.write_all(b"suffix").unwrap();
                drop(writer);
                let mut expected = fixture.bytes.clone();
                expected.extend_from_slice(b"suffix");
                assert_eq!(fs::read(&path).unwrap(), expected);
            }
            assert_eq!(
                fixture.objects.read(fixture.reference).unwrap(),
                fixture.bytes
            );
            assert_eq!(fs::read(&sibling).unwrap(), fixture.bytes);
            let mut original = Vec::new();
            snapshot.read_to_end(&mut original).unwrap();
            assert_eq!(original, fixture.bytes);
            assert!(fixture.project.metrics().high_water <= 8);
        }
    }

    #[test]
    fn replacement_and_removal_update_future_opens_without_invalidating_snapshot_identity() {
        let fixture = Fixture::new();
        let path = fixture.mount("selected.hawdb");
        let mut snapshot = File::open(&path).unwrap();
        let candidate = fixture.root.join("replacement.tmp");
        let mut replacement = File::create(&candidate).unwrap();
        replacement.write_all(b"published replacement").unwrap();
        replacement.sync_all().unwrap();
        drop(replacement);
        crate::durability::durable_replace_file(&candidate, &path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"published replacement");
        fs::remove_file(&path).unwrap();
        assert_eq!(
            File::open(&path).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        let mut original = Vec::new();
        snapshot.read_to_end(&mut original).unwrap();
        assert_eq!(original, fixture.bytes);
        snapshot.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(
            fixture.objects.read(fixture.reference).unwrap(),
            fixture.bytes
        );
    }
}
