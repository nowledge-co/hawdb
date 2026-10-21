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
use crate::file_io::captured_immutable::{CacheKey, CapturedIdentity, CapturedSource};
use crate::file_io::{self as fs, File, OpenOptions};
use crate::immutable_object::ObjectReference;
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

static NEXT_ALIAS_CANDIDATE: AtomicU64 = AtomicU64::new(0);

mod checkpoint;
pub(crate) use checkpoint::CheckpointImmutableFileError;

#[cfg(test)]
#[path = "immutable_files/checkpoint_lock_tests.rs"]
mod checkpoint_lock_tests;

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
struct CachedHandle {
    file: Arc<File>,
    last_used: u64,
}

#[derive(Debug)]
pub(crate) struct ImmutableFileHandles {
    state: Arc<BudgetState>,
    handles: Mutex<BTreeMap<CacheKey, CachedHandle>>,
    access_tick: AtomicU64,
    bindings: Mutex<BTreeMap<PathBuf, ImmutableFileBinding>>,
    opening: Mutex<()>,
    // Private checkpoint validation keeps objects protected after releasing
    // the short cache-opening lock. Retirement can defer without waiting.
    checkpoint_validation: RwLock<()>,
}

impl ImmutableFileHandles {
    pub(crate) fn new(state: Arc<BudgetState>) -> Self {
        Self {
            state,
            handles: Mutex::new(BTreeMap::new()),
            access_tick: AtomicU64::new(0),
            bindings: Mutex::new(BTreeMap::new()),
            opening: Mutex::new(()),
            checkpoint_validation: RwLock::new(()),
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
        let _validation = match self.checkpoint_validation.try_write() {
            Ok(validation) => validation,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "immutable checkpoint validation is active",
                ));
            }
        };
        let handles = self
            .handles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if handles.iter().any(|(key, handle)| {
            key.reference() == Some(reference) && Arc::strong_count(&handle.file) != 1
        }) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "unreachable immutable object still has an active read",
            ));
        }
        drop(handles);
        self.remove_idle_matching(|key| key.reference() == Some(reference), usize::MAX);
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
        // Reopening a branch usually finds the checkpoint alias already mounted.
        // Only reuse a regular, no-follow alias to this exact verified inode.
        // Equal bytes, link counts and the binding map cannot establish that.
        // The live verified handle prevents file-ID reuse during this check.
        // Cooperating branch admission still excludes concurrent alias mutation.
        #[cfg(unix)]
        if let Ok(alias) = fs::symlink_metadata(path)
            && alias.is_file()
            && crate::native_file_identity::of_metadata(&alias)
                == crate::native_file_identity::of_metadata(&_verified.metadata()?)
        {
            // Restore copy-on-write tracking even when admission cleared it.
            return self.bind(path, binding);
        }
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
        self.get_controlled(binding, context, &mut ValidationAdmission(admit_validation))
    }

    pub(crate) fn get_controlled(
        &self,
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
        control: &mut impl crate::file_io::ReadAdmissionControl,
    ) -> io::Result<(Arc<File>, u64)> {
        self.get_controlled_from(Some(binding), None, context, control)
    }

    pub(crate) fn get_captured_controlled(
        &self,
        binding: Option<&ImmutableFileBinding>,
        source: &CapturedSource,
        context: &FileOpenContext,
        control: &mut impl crate::file_io::ReadAdmissionControl,
    ) -> io::Result<(Arc<File>, u64)> {
        self.get_controlled_from(binding, Some(source), context, control)
    }

    fn get_controlled_from(
        &self,
        binding: Option<&ImmutableFileBinding>,
        source: Option<&CapturedSource>,
        context: &FileOpenContext,
        control: &mut impl crate::file_io::ReadAdmissionControl,
    ) -> io::Result<(Arc<File>, u64)> {
        // Keep retirement from racing the gap between cache lookup and the
        // returned lease. A held native Arc then provides the in-flight proof.
        let _validation = match self.checkpoint_validation.try_read() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "immutable retirement is active",
                ))
            }
        };
        let key = match source {
            Some(source) => CacheKey::Captured(source.key.clone()),
            None => CacheKey::Managed(
                binding
                    .ok_or_else(|| io::Error::other("missing managed binding"))?
                    .reference,
            ),
        };
        control.checkpoint()?;
        if let Some(file) = self.cached_key(&key) {
            self.state.record_cache_hit();
            return Ok((file, 0));
        }
        // Coalesce cold opens without holding the eviction map across admission.
        // This serializes file validation, never waits for descriptor capacity.
        let _opening = self
            .opening
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        control.checkpoint()?;
        if let Some(file) = self.cached_key(&key) {
            self.state.record_cache_hit();
            return Ok((file, 0));
        }
        self.state.record_cache_miss();
        let file = Arc::new(Self::open_verified_object_controlled_from(
            binding,
            source,
            context,
            DescriptorKind::ImmutableCache,
            control,
        )?);
        self.handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                key,
                CachedHandle {
                    file: file.clone(),
                    last_used: self.next_tick(),
                },
            );
        let validated = binding.map_or(0, |binding| binding.reference.byte_length);
        Ok((file, validated))
    }

    fn next_tick(&self) -> u64 {
        self.access_tick
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |tick| {
                Some(tick.saturating_add(1))
            })
            .expect("the access tick update always succeeds")
    }

    fn cached(&self, reference: ObjectReference) -> Option<Arc<File>> {
        let _validation = match self.checkpoint_validation.try_read() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        self.cached_key(&CacheKey::Managed(reference))
    }

    fn cached_key(&self, key: &CacheKey) -> Option<Arc<File>> {
        let mut handles = self
            .handles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let handle = handles.get_mut(key)?;
        handle.last_used = self.next_tick();
        Some(handle.file.clone())
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
        admit_validation: impl FnMut(u64) -> io::Result<()>,
    ) -> io::Result<File> {
        Self::open_verified_object_controlled(
            binding,
            context,
            kind,
            &mut ValidationAdmission(admit_validation),
        )
    }

    fn open_verified_object_controlled(
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
        kind: DescriptorKind,
        control: &mut impl crate::file_io::ReadAdmissionControl,
    ) -> io::Result<File> {
        Self::open_verified_object_controlled_from(Some(binding), None, context, kind, control)
    }

    fn open_verified_object_controlled_from(
        binding: Option<&ImmutableFileBinding>,
        source: Option<&CapturedSource>,
        context: &FileOpenContext,
        kind: DescriptorKind,
        control: &mut impl crate::file_io::ReadAdmissionControl,
    ) -> io::Result<File> {
        control.checkpoint()?;
        let mut file = match source {
            Some(source) => source
                .directory
                .open_child_kind(context, &source.key.name, kind)?,
            None => OpenOptions::new()
                .read(true)
                .descriptor_kind(kind)
                .open_with_context(
                    &binding
                        .ok_or_else(|| io::Error::other("missing managed binding"))?
                        .object_path,
                    context,
                )?,
        };
        let metadata = file.metadata()?;
        if let Some(source) = source
            && let CapturedIdentity::Native { identity, length } = source.key.identity
        {
            control.checkpoint()?;
            if !metadata.is_file()
                || metadata.len() != length
                || file.with_native(crate::native_file_identity::of_file)? != identity
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "captured native identity mismatch",
                ));
            }
            return Ok(file);
        }
        let binding = binding.ok_or_else(|| io::Error::other("missing managed binding"))?;
        if !metadata.is_file() || metadata.len() != binding.reference.byte_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable handle identity length mismatch",
            ));
        }
        // Descriptor rejection occurs before this charge. No payload may be
        // read until the caller has reserved the complete identity validation.
        control.admit_validation(binding.reference.byte_length)?;
        let mut hasher = crate::immutable_object::identity_hasher(
            binding.reference.kind,
            binding.reference.format_version,
            binding.reference.byte_length,
        );
        let mut buffer = [0_u8; 64 * 1024];
        let mut remaining = binding.reference.byte_length;
        while remaining > 0 {
            control.checkpoint()?;
            let chunk = remaining.min(buffer.len() as u64) as usize;
            let read = match file.read(&mut buffer[..chunk]) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "immutable identity validation ended early",
                ));
            }
            remaining -= read as u64;
            hasher.update(&buffer[..read]);
        }
        control.checkpoint()?;
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

impl ImmutableFileHandles {
    // No temporary key vectors: the cloned key keeps its metadata admitted
    // through native close and empty BTree node deallocation outside the lock.
    fn remove_idle_matching(
        &self,
        eligible: impl Fn(&CacheKey) -> bool,
        requested: usize,
    ) -> usize {
        let mut count = 0;
        while count < requested {
            let removed = {
                let mut handles = self.handles.lock().unwrap_or_else(|e| e.into_inner());
                let key = handles
                    .iter()
                    .filter(|(key, handle)| eligible(key) && Arc::strong_count(&handle.file) == 1)
                    .min_by_key(|(_, handle)| handle.last_used)
                    .map(|(key, _)| key.clone());
                key.map(|key| {
                    let handle = handles.remove(&key).expect("selected idle handle exists");
                    let empty = handles.is_empty().then(|| std::mem::take(&mut *handles));
                    (key, handle, empty)
                })
            };
            let Some((key, handle, empty)) = removed else {
                break;
            };
            drop(handle);
            drop(empty);
            drop(key);
            count += 1;
        }
        self.state.record_cache_evictions(count);
        count
    }

    pub(crate) fn retire_source(&self, capture: u64) {
        self.remove_idle_matching(|key| key.capture() == Some(capture), usize::MAX);
    }
}

impl DescriptorCache for ImmutableFileHandles {
    fn evict_idle(&self, requested: usize) -> usize {
        self.remove_idle_matching(|_| true, requested)
    }
}

impl Drop for ImmutableFileHandles {
    fn drop(&mut self) {
        let handles = self.handles.get_mut().unwrap_or_else(|e| e.into_inner());
        while let Some((key, handle)) = handles.pop_first() {
            let empty = handles.is_empty().then(|| std::mem::take(handles));
            drop(handle);
            drop(empty);
            drop(key);
        }
    }
}

struct ValidationAdmission<F>(F);
impl<F: FnMut(u64) -> io::Result<()>> crate::file_io::ReadAdmissionControl
    for ValidationAdmission<F>
{
    fn admit_validation(&mut self, bytes: u64) -> io::Result<()> {
        (self.0)(bytes)
    }
    fn checkpoint(&mut self) -> io::Result<()> {
        Ok(())
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
            Self::new_with(|_| ()).0
        }

        fn new_with<T>(attach: impl FnOnce(&ProjectFileDescriptors) -> T) -> (Self, T) {
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
            let attached = attach(&project);
            let mut objects = ImmutableObjectStore::open(root.join("immutable")).unwrap();
            let bytes = (0..128 * 1024).map(|i| (i % 251) as u8).collect::<Vec<_>>();
            let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes);
            objects.publish(reference, &bytes).unwrap();
            (
                Self {
                    root,
                    project,
                    objects,
                    reference,
                    bytes,
                },
                attached,
            )
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

    #[cfg(unix)]
    #[test]
    fn verified_alias_reuse_avoids_namespace_mutation_and_restores_copy_on_write() {
        use crate::power_loss::{
            image::ImageLimits, IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel,
        };
        for event in [IoEvent::HardLink, IoEvent::Rename] {
            let (fixture, model) = Fixture::new_with(|project| {
                PowerLossModel::attach(project, ImageLimits::default()).unwrap()
            });
            // The fault model itself retains its native root descriptor.
            let retained = fixture.project.metrics().open;
            let point = ObservationPoint {
                event,
                relative_path: PathBuf::from("checkpoint"),
                boundary: ObservationBoundary::Before,
                skip_matches: 0,
                include_descendants: true,
                keep_last: false,
            };
            // Exercise the observer on a real first installation, including
            // the randomly named hard-link candidate in this directory.
            fs::create_dir(fixture.root.join("checkpoint")).unwrap();
            model.observe(point.clone()).unwrap();
            let path = fixture.mount("checkpoint/selected.hawdb");
            assert!(model.take_observation().unwrap().is_some());
            fixture.project.immutable_handles.unbind(&path).unwrap();
            assert!(fixture
                .project
                .immutable_handles
                .binding(&path)
                .unwrap()
                .is_none());
            model.observe(point).unwrap();
            fixture.mount("checkpoint/selected.hawdb");
            assert!(model.take_observation().unwrap().is_none(), "{event:?}");
            assert!(fixture
                .project
                .immutable_handles
                .binding(&path)
                .unwrap()
                .is_some());
            assert_eq!(fixture.project.metrics().open, retained);
            assert_eq!(fixture.project.metrics().cached_handles, 0);
            // Audit the actual native history, then mutate through the rebound
            // alias. The immutable object and a preexisting reader must survive.
            model.capture().unwrap();
            let mut reader = File::open(&path).unwrap();
            fs::write(&path, b"private update").unwrap();
            assert_eq!(
                fixture.objects.read(fixture.reference).unwrap(),
                fixture.bytes
            );
            let mut old = Vec::new();
            reader.read_to_end(&mut old).unwrap();
            assert_eq!(old, fixture.bytes);
            drop(reader);
            // Recovery remounts the checkpoint rather than reusing a detached
            // writable inode. Its old reader still retains the private update.
            let mut private = File::open(&path).unwrap();
            fixture.mount("checkpoint/selected.hawdb");
            assert_eq!(fs::read(&path).unwrap(), fixture.bytes);
            let mut old_private = Vec::new();
            private.read_to_end(&mut old_private).unwrap();
            assert_eq!(old_private, b"private update");
            drop(private);
            model.capture().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn equal_content_alias_with_multiple_links_is_remounted_to_verified_object() {
        let fixture = Fixture::new();
        let path = fixture.mount("selected.hawdb");
        // Bypass binding-aware IO to simulate an existing wrong alias on reopen.
        std::fs::remove_file(&path).unwrap();
        std::fs::copy(fixture.objects.object_path(fixture.reference), &path).unwrap();
        let other = fixture.root.join("other-copy");
        std::fs::hard_link(&path, &other).unwrap();
        let mut private = File::open(&path).unwrap();
        fixture.mount("selected.hawdb");
        assert_eq!(fs::read(&path).unwrap(), fixture.bytes);
        fs::write(&path, b"new private bytes").unwrap();
        assert_eq!(
            fixture.objects.read(fixture.reference).unwrap(),
            fixture.bytes
        );
        let mut previous = Vec::new();
        private.read_to_end(&mut previous).unwrap();
        assert_eq!(previous, fixture.bytes);
        assert_eq!(std::fs::read(other).unwrap(), fixture.bytes);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_to_verified_object_is_replaced_with_regular_alias() {
        let fixture = Fixture::new();
        let path = fixture.mount("selected.hawdb");
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(fixture.objects.object_path(fixture.reference), &path).unwrap();
        fixture.mount("selected.hawdb");
        assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
        fs::write(path, b"private update").unwrap();
        assert_eq!(
            fixture.objects.read(fixture.reference).unwrap(),
            fixture.bytes
        );
    }

    #[test]
    fn existing_alias_never_bypasses_complete_object_validation() {
        let fixture = Fixture::new();
        let path = fixture.mount("selected.hawdb");
        let binding = fixture
            .project
            .immutable_handles
            .binding(&path)
            .unwrap()
            .unwrap();
        // A same-length in-place corruption preserves both alias identity and
        // metadata. Managed-object validation must still reject its digest.
        let mut bytes = fixture.bytes.clone();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 1;
        std::fs::write(&binding.object_path, &bytes).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let context = crate::file_descriptors::context_for_path(&path).unwrap();
        let error = fixture
            .project
            .immutable_handles
            .mount(&path, binding, &context)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("digest mismatch"));
        assert_eq!(fixture.project.metrics().open, 0);
        assert_eq!(fixture.project.metrics().cached_handles, 0);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn repeated_checkpoint_mounts_keep_cache_cold_and_rebind_every_alias() {
        let fixture = Fixture::new();
        let aliases = (0..32)
            .map(|i| format!("checkpoint-{i}"))
            .collect::<Vec<_>>();
        for alias in &aliases {
            fixture.mount(alias);
        }
        let peak = fixture.project.metrics().high_water;
        let start = std::time::Instant::now();
        for _ in 0..8 {
            // A new admission cannot rely on the previous binding map.
            for alias in &aliases {
                fixture
                    .project
                    .immutable_handles
                    .unbind(&fixture.root.join(alias))
                    .unwrap();
            }
            for alias in &aliases {
                let path = fixture.mount(alias);
                let binding = fixture
                    .project
                    .immutable_handles
                    .binding(&path)
                    .unwrap()
                    .unwrap();
                assert_eq!(binding.reference, fixture.reference);
            }
            let metrics = fixture.project.metrics();
            assert_eq!(metrics.open, 0);
            assert_eq!(metrics.cached_handles, 0);
            assert_eq!(metrics.reserved, 0);
            assert_eq!(metrics.high_water, peak);
        }
        eprintln!(
            "256 remounts / 128 KiB per verified object: {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn checkpoint_units_immutable_handle_contended_opening_defers_and_retries() {
        use crate::background::{CheckpointWorkContext, CheckpointWorkError};
        let fixture = Fixture::new();
        let path = fixture.mount("checkpoint.page");
        let handles = &fixture.project.immutable_handles;
        let binding = handles.binding(&path).unwrap().unwrap();
        let context = crate::file_descriptors::context_for_path(&path).unwrap();
        let work = CheckpointWorkContext::default();
        let opening = handles.opening.lock().unwrap();
        assert!(matches!(
            handles.checkpoint_get(&binding, &context, &work),
            Err(CheckpointImmutableFileError::Work(
                CheckpointWorkError::Contended("immutable file validation")
            ))
        ));
        assert_eq!(fixture.project.metrics().open, 0);
        drop(opening);
        let file = handles.checkpoint_get(&binding, &context, &work).unwrap();
        let mut borrowed = file.as_ref();
        borrowed.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        borrowed.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, fixture.bytes);
        assert_eq!(fixture.project.metrics().cached_handles, 1);
        assert_eq!(handles.evict_idle(1), 0);
        drop(file);
        assert_eq!(handles.evict_idle(1), 1);
        assert_eq!(fixture.project.metrics().open, 0);
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
