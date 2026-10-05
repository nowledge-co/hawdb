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

//! Deterministic storage images for physical power-loss qualification.
//!
//! A completed file synchronization preserves that inode's bytes and length;
//! a completed directory synchronization preserves its direct entries. Those
//! barriers are immutable inputs to every crash image. Uncovered writes and
//! namespace changes may be lost, torn, or persisted in an explicitly selected
//! order. Inodes survive unlink/rename in the model, so hard links and cached
//! handles do not silently turn into independent files.
//!
//! This engine alone does not qualify HawDB. Its operations must be driven by
//! the actual storage IO path, and its materialized images must be reopened by
//! the real branch runtime. It assumes trustworthy completed synchronization
//! barriers and atomic same-directory rename, not an atomic multi-file commit.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

pub type InodeId = u64;
pub type OperationId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UncoveredWrite {
    pub operation: OperationId,
    pub inode: InodeId,
    pub offset: u64,
    pub length: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageLimits {
    pub max_inodes: usize,
    pub max_pending_operations: usize,
    pub max_bytes: usize,
}

impl Default for ImageLimits {
    fn default() -> Self {
        Self {
            max_inodes: 100_000,
            max_pending_operations: 100_000,
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
enum Inode {
    File {
        current: Arc<Vec<u8>>,
        durable: Arc<Vec<u8>>,
    },
    Directory {
        current: BTreeMap<OsString, InodeId>,
        durable: BTreeMap<OsString, InodeId>,
    },
}

#[derive(Debug, Clone)]
struct EntryChange {
    directory: InodeId,
    name: OsString,
    value: Option<InodeId>,
}

#[derive(Debug, Clone)]
enum PendingOperation {
    Write {
        inode: InodeId,
        offset: usize,
        bytes: Arc<Vec<u8>>,
    },
    Truncate {
        inode: InodeId,
        length: usize,
    },
    Namespace {
        changes: Vec<EntryChange>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistOperation {
    Whole(OperationId),
    /// A nonempty subrange of one uncovered write. Length may reach that
    /// fragment's end while absent intervening bytes read as zeroes.
    TornWrite {
        operation: OperationId,
        bytes: Range<usize>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrashPlan {
    /// Order is physical persistence order, intentionally independent of
    /// submission order. Empty means lose every unsynchronized operation.
    pub persistence: Vec<PersistOperation>,
}

#[derive(Debug, Clone)]
pub struct ImageEngine {
    limits: ImageLimits,
    inodes: BTreeMap<InodeId, Inode>,
    pending: BTreeMap<OperationId, PendingOperation>,
    next_inode: InodeId,
    next_operation: OperationId,
    completed_file_barriers: u64,
    completed_directory_barriers: u64,
}

impl ImageEngine {
    pub fn maximum_write_length(&self) -> usize {
        self.limits.max_bytes
    }

    pub fn uncovered_writes(&self) -> impl Iterator<Item = UncoveredWrite> + '_ {
        self.pending
            .iter()
            .filter_map(|(operation, pending)| match pending {
                PendingOperation::Write {
                    inode,
                    offset,
                    bytes,
                } => Some(UncoveredWrite {
                    operation: *operation,
                    inode: *inode,
                    offset: *offset as u64,
                    length: bytes.len(),
                }),
                _ => None,
            })
    }
    pub fn new(limits: ImageLimits) -> io::Result<Self> {
        if limits.max_inodes == 0 || limits.max_pending_operations == 0 || limits.max_bytes == 0 {
            return Err(invalid("power-loss image limits must be nonzero"));
        }
        let root = Inode::Directory {
            current: BTreeMap::new(),
            durable: BTreeMap::new(),
        };
        Ok(Self {
            limits,
            inodes: BTreeMap::from([(1, root)]),
            pending: BTreeMap::new(),
            next_inode: 2,
            next_operation: 1,
            completed_file_barriers: 0,
            completed_directory_barriers: 0,
        })
    }

    pub fn inode(&self, path: &Path) -> io::Result<InodeId> {
        let components = relative_components(path)?;
        let mut inode = 1;
        for component in components {
            inode = self
                .current_directory(inode)?
                .get(&component)
                .copied()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "power-loss image path is absent")
                })?;
        }
        Ok(inode)
    }

    pub fn is_directory(&self, inode: InodeId) -> bool {
        matches!(self.inodes.get(&inode), Some(Inode::Directory { .. }))
    }

    pub fn create_file(&mut self, path: &Path) -> io::Result<InodeId> {
        let (parent, name) = self.parent(path)?;
        self.require_absent(parent, &name)?;
        self.admit_new_inode()?;
        self.admit_operation(0)?;
        let inode = self.allocate_inode(Inode::File {
            current: Arc::new(Vec::new()),
            durable: Arc::new(Vec::new()),
        })?;
        self.namespace(vec![EntryChange {
            directory: parent,
            name,
            value: Some(inode),
        }])?;
        Ok(inode)
    }

    pub fn create_directory(&mut self, path: &Path) -> io::Result<InodeId> {
        let (parent, name) = self.parent(path)?;
        self.require_absent(parent, &name)?;
        self.admit_new_inode()?;
        self.admit_operation(0)?;
        let inode = self.allocate_inode(Inode::Directory {
            current: BTreeMap::new(),
            durable: BTreeMap::new(),
        })?;
        self.namespace(vec![EntryChange {
            directory: parent,
            name,
            value: Some(inode),
        }])?;
        Ok(inode)
    }

    pub fn hard_link(&mut self, source: &Path, destination: &Path) -> io::Result<()> {
        let inode = self.inode(source)?;
        if self.is_directory(inode) {
            return Err(invalid("hard links to directories are not modeled"));
        }
        let (parent, name) = self.parent(destination)?;
        self.require_absent(parent, &name)?;
        self.admit_operation(0)?;
        self.namespace(vec![EntryChange {
            directory: parent,
            name,
            value: Some(inode),
        }])?;
        Ok(())
    }

    pub fn rename(&mut self, source: &Path, destination: &Path) -> io::Result<()> {
        let inode = self.inode(source)?;
        let (source_parent, source_name) = self.parent(source)?;
        let (destination_parent, destination_name) = self.parent(destination)?;
        if source_parent == destination_parent && source_name == destination_name {
            return Ok(());
        }
        if self.is_directory(inode) {
            let destination_components = relative_components(destination)?;
            let source_components = relative_components(source)?;
            if destination_components.starts_with(&source_components) {
                return Err(invalid("directory rename creates a cycle"));
            }
        }
        if let Some(existing) = self
            .current_directory(destination_parent)?
            .get(&destination_name)
        {
            // POSIX rename is a no-op when both names already link the same
            // inode; removing the source would invent a namespace mutation.
            if *existing == inode {
                return Ok(());
            }
            if self.is_directory(*existing) != self.is_directory(inode)
                || self.is_directory(*existing) && !self.current_directory(*existing)?.is_empty()
            {
                return Err(invalid(
                    "rename replacement has an incompatible type or nonempty directory",
                ));
            }
        }
        let same_directory = source_parent == destination_parent;
        self.admit_operations(if same_directory { 1 } else { 2 }, 0)?;
        let source_change = EntryChange {
            directory: source_parent,
            name: source_name,
            value: None,
        };
        let destination_change = EntryChange {
            directory: destination_parent,
            name: destination_name,
            value: Some(inode),
        };
        if same_directory {
            self.namespace(vec![source_change, destination_change])?;
        } else {
            // Live rename is atomic, but its two unsynchronized directories
            // need not reach storage together. The model mutex still publishes
            // one complete live transition; crash plans select either entry.
            self.namespace(vec![source_change])?;
            self.namespace(vec![destination_change])?;
        }
        Ok(())
    }

    pub fn remove(&mut self, path: &Path) -> io::Result<()> {
        let inode = self.inode(path)?;
        if self.is_directory(inode) && !self.current_directory(inode)?.is_empty() {
            return Err(invalid(
                "removing a nonempty directory requires explicit child removals",
            ));
        }
        let (parent, name) = self.parent(path)?;
        self.admit_operation(0)?;
        self.namespace(vec![EntryChange {
            directory: parent,
            name,
            value: None,
        }])?;
        Ok(())
    }

    pub fn write(&mut self, inode: InodeId, offset: u64, bytes: &[u8]) -> io::Result<OperationId> {
        if bytes.is_empty() {
            return Err(invalid("empty writes have no persistence operation"));
        }
        let offset =
            usize::try_from(offset).map_err(|_| invalid("write offset does not fit memory"))?;
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| invalid("image write length overflow"))?;
        let current_length = self.current_bytes(inode)?.len();
        // Account conservatively for the COW current image and retained event.
        self.admit_operation(
            end.saturating_sub(current_length)
                .saturating_add(current_length)
                .saturating_add(bytes.len()),
        )?;
        match self.inodes.get_mut(&inode) {
            Some(Inode::File { current, .. }) => {
                let current = Arc::make_mut(current);
                current.resize(current.len().max(end), 0);
                current[offset..end].copy_from_slice(bytes);
            }
            _ => return Err(invalid("write target is not a file")),
        }
        self.record(PendingOperation::Write {
            inode,
            offset,
            bytes: Arc::new(bytes.to_vec()),
        })
    }

    pub fn truncate(&mut self, inode: InodeId, length: u64) -> io::Result<OperationId> {
        let length =
            usize::try_from(length).map_err(|_| invalid("truncate length does not fit memory"))?;
        let old_length = self.current_bytes(inode)?.len();
        self.admit_operation(length.max(old_length))?;
        match self.inodes.get_mut(&inode) {
            Some(Inode::File { current, .. }) => Arc::make_mut(current).resize(length, 0),
            _ => return Err(invalid("truncate target is not a file")),
        }
        self.record(PendingOperation::Truncate { inode, length })
    }

    pub fn sync_file(&mut self, inode: InodeId) -> io::Result<()> {
        match self.inodes.get_mut(&inode) {
            Some(Inode::File { current, durable }) => *durable = current.clone(),
            _ => return Err(invalid("file barrier target is not a file")),
        }
        self.pending.retain(|_, operation| match operation {
            PendingOperation::Write { inode: target, .. }
            | PendingOperation::Truncate { inode: target, .. } => *target != inode,
            PendingOperation::Namespace { .. } => true,
        });
        self.completed_file_barriers = self
            .completed_file_barriers
            .checked_add(1)
            .ok_or_else(|| invalid("file barrier count overflow"))?;
        Ok(())
    }

    pub fn sync_directory(&mut self, inode: InodeId) -> io::Result<()> {
        match self.inodes.get_mut(&inode) {
            Some(Inode::Directory { current, durable }) => *durable = current.clone(),
            _ => return Err(invalid("directory barrier target is not a directory")),
        }
        self.pending.retain(|_, operation| {
            if let PendingOperation::Namespace { changes } = operation {
                changes.retain(|change| change.directory != inode);
                !changes.is_empty()
            } else {
                true
            }
        });
        self.completed_directory_barriers = self
            .completed_directory_barriers
            .checked_add(1)
            .ok_or_else(|| invalid("directory barrier count overflow"))?;
        Ok(())
    }

    pub fn current_bytes(&self, inode: InodeId) -> io::Result<&[u8]> {
        match self.inodes.get(&inode) {
            Some(Inode::File { current, .. }) => Ok(current),
            _ => Err(invalid("image inode is not a file")),
        }
    }

    pub fn pending_operations(&self) -> Vec<OperationId> {
        self.pending.keys().copied().collect()
    }

    pub fn persist_all_plan(&self) -> CrashPlan {
        CrashPlan {
            persistence: self
                .pending
                .keys()
                .copied()
                .map(PersistOperation::Whole)
                .collect(),
        }
    }

    pub fn crash(&self, plan: &CrashPlan) -> io::Result<CrashImage> {
        let mut files = BTreeMap::new();
        let mut directories = BTreeMap::new();
        for (inode, value) in &self.inodes {
            match value {
                Inode::File { durable, .. } => {
                    files.insert(*inode, durable.clone());
                }
                Inode::Directory { durable, .. } => {
                    directories.insert(*inode, durable.clone());
                }
            }
        }
        let mut seen = BTreeSet::new();
        for persistence in &plan.persistence {
            let id = match persistence {
                PersistOperation::Whole(id) | PersistOperation::TornWrite { operation: id, .. } => {
                    *id
                }
            };
            if !seen.insert(id) {
                return Err(invalid("crash plan repeats a persistence operation"));
            }
            let operation = self.pending.get(&id).ok_or_else(|| {
                invalid("crash plan references a synchronized or unknown operation")
            })?;
            match (operation, persistence) {
                (
                    PendingOperation::Write {
                        inode,
                        offset,
                        bytes,
                    },
                    persistence,
                ) => {
                    let range = match persistence {
                        PersistOperation::Whole(_) => 0..bytes.len(),
                        PersistOperation::TornWrite { bytes: range, .. } => {
                            if range.is_empty() || range.end > bytes.len() {
                                return Err(invalid(
                                    "torn write range is empty or outside its operation",
                                ));
                            }
                            range.clone()
                        }
                    };
                    let image = Arc::make_mut(
                        files
                            .get_mut(inode)
                            .ok_or_else(|| invalid("pending write has no file inode"))?,
                    );
                    let start = offset
                        .checked_add(range.start)
                        .ok_or_else(|| invalid("torn write offset overflow"))?;
                    let end = offset
                        .checked_add(range.end)
                        .ok_or_else(|| invalid("torn write end overflow"))?;
                    image.resize(image.len().max(end), 0);
                    image[start..end].copy_from_slice(&bytes[range]);
                }
                (PendingOperation::Truncate { inode, length }, PersistOperation::Whole(_)) => {
                    Arc::make_mut(
                        files
                            .get_mut(inode)
                            .ok_or_else(|| invalid("pending truncate has no file inode"))?,
                    )
                    .resize(*length, 0);
                }
                (PendingOperation::Namespace { changes }, PersistOperation::Whole(_)) => {
                    for change in changes {
                        let entries = directories
                            .get_mut(&change.directory)
                            .ok_or_else(|| invalid("namespace change has no directory inode"))?;
                        apply_entry(entries, change);
                    }
                }
                _ => return Err(invalid("only file-write bytes can be torn")),
            }
        }
        let mut image = CrashImage {
            directories: BTreeSet::new(),
            files: BTreeMap::new(),
            file_barriers: self.completed_file_barriers,
            directory_barriers: self.completed_directory_barriers,
        };
        image.walk(1, Path::new(""), &directories, &files, &mut BTreeSet::new())?;
        Ok(image)
    }

    fn parent(&self, path: &Path) -> io::Result<(InodeId, OsString)> {
        let components = relative_components(path)?;
        let name = components
            .last()
            .ok_or_else(|| invalid("root namespace cannot be replaced"))?
            .clone();
        let mut parent = 1;
        for component in &components[..components.len() - 1] {
            parent = *self
                .current_directory(parent)?
                .get(component)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "image parent directory is absent")
                })?;
        }
        self.current_directory(parent)?;
        Ok((parent, name))
    }

    fn current_directory(&self, inode: InodeId) -> io::Result<&BTreeMap<OsString, InodeId>> {
        match self.inodes.get(&inode) {
            Some(Inode::Directory { current, .. }) => Ok(current),
            _ => Err(invalid("image parent is not a directory")),
        }
    }

    fn require_absent(&self, parent: InodeId, name: &OsString) -> io::Result<()> {
        if self.current_directory(parent)?.contains_key(name) {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "image path already exists",
            ))
        } else {
            Ok(())
        }
    }

    fn admit_new_inode(&self) -> io::Result<()> {
        if self.inodes.len() >= self.limits.max_inodes || self.next_inode == u64::MAX {
            Err(invalid("power-loss inode admission limit exceeded"))
        } else {
            Ok(())
        }
    }

    fn admit_operation(&self, additional_bytes: usize) -> io::Result<()> {
        self.admit_operations(1, additional_bytes)
    }

    fn admit_operations(&self, count: usize, additional_bytes: usize) -> io::Result<()> {
        if self.pending.len().saturating_add(count) > self.limits.max_pending_operations
            || self.next_operation.checked_add(count as u64).is_none()
        {
            return Err(invalid("power-loss operation admission limit exceeded"));
        }
        let bytes = self
            .inodes
            .values()
            .map(|inode| match inode {
                Inode::File { current, durable } if Arc::ptr_eq(current, durable) => current.len(),
                Inode::File { current, durable } => current.len().saturating_add(durable.len()),
                Inode::Directory { .. } => 0,
            })
            .chain(self.pending.values().map(|operation| match operation {
                PendingOperation::Write { bytes, .. } => bytes.len(),
                _ => 0,
            }))
            .fold(additional_bytes, usize::saturating_add);
        if bytes > self.limits.max_bytes {
            Err(invalid("power-loss image byte admission limit exceeded"))
        } else {
            Ok(())
        }
    }

    fn allocate_inode(&mut self, inode: Inode) -> io::Result<InodeId> {
        self.admit_new_inode()?;
        let id = self.next_inode;
        self.next_inode += 1;
        self.inodes.insert(id, inode);
        Ok(id)
    }

    fn namespace(&mut self, changes: Vec<EntryChange>) -> io::Result<OperationId> {
        for change in &changes {
            match self.inodes.get_mut(&change.directory) {
                Some(Inode::Directory { current, .. }) => apply_entry(current, change),
                _ => return Err(invalid("namespace change parent is not a directory")),
            }
        }
        self.record(PendingOperation::Namespace { changes })
    }

    fn record(&mut self, operation: PendingOperation) -> io::Result<OperationId> {
        let id = self.next_operation;
        self.next_operation = id
            .checked_add(1)
            .ok_or_else(|| invalid("power-loss operation sequence overflow"))?;
        self.pending.insert(id, operation);
        Ok(id)
    }
}

fn apply_entry(entries: &mut BTreeMap<OsString, InodeId>, change: &EntryChange) {
    match change.value {
        Some(inode) => {
            entries.insert(change.name.clone(), inode);
        }
        None => {
            entries.remove(&change.name);
        }
    }
}

fn relative_components(path: &Path) -> io::Result<Vec<OsString>> {
    if path.as_os_str().as_encoded_bytes().len() > 4096 {
        return Err(invalid("power-loss path exceeds its admission limit"));
    }
    path.components()
        .map(|component| match component {
            Component::Normal(name) if name.as_encoded_bytes().len() <= 255 => {
                Ok(name.to_os_string())
            }
            _ => Err(invalid("power-loss paths must be normalized and relative")),
        })
        .collect()
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[derive(Debug, Clone)]
pub struct CrashImage {
    directories: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, (InodeId, Arc<Vec<u8>>)>,
    pub file_barriers: u64,
    pub directory_barriers: u64,
}

impl CrashImage {
    pub fn bytes(&self, path: &Path) -> Option<&[u8]> {
        self.files.get(path).map(|(_, bytes)| bytes.as_slice())
    }

    pub fn file_inode(&self, path: &Path) -> Option<InodeId> {
        self.files.get(path).map(|(inode, _)| *inode)
    }

    pub fn paths(&self) -> impl Iterator<Item = &Path> {
        self.files.keys().map(PathBuf::as_path)
    }

    pub fn directory_paths(&self) -> impl Iterator<Item = &Path> {
        self.directories.iter().map(PathBuf::as_path)
    }

    /// Emit the selected physical image, preserving hard-link identity. The
    /// caller must reopen this fresh directory with the real database runtime.
    pub fn materialize(&self, destination: &Path) -> io::Result<()> {
        if destination.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "crash image destination must be new",
            ));
        }
        std::fs::create_dir(destination)?;
        for directory in &self.directories {
            if !directory.as_os_str().is_empty() {
                std::fs::create_dir_all(destination.join(directory))?;
            }
        }
        let mut first_paths = BTreeMap::new();
        for (relative, (inode, bytes)) in &self.files {
            let path = destination.join(relative);
            if let Some(first) = first_paths.get(inode) {
                std::fs::hard_link(first, &path)?;
            } else {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                file.write_all(bytes)?;
                file.sync_all()?;
                first_paths.insert(*inode, path);
            }
        }
        Ok(())
    }

    fn walk(
        &mut self,
        inode: InodeId,
        path: &Path,
        directories: &BTreeMap<InodeId, BTreeMap<OsString, InodeId>>,
        files: &BTreeMap<InodeId, Arc<Vec<u8>>>,
        ancestors: &mut BTreeSet<InodeId>,
    ) -> io::Result<()> {
        if let Some(bytes) = files.get(&inode) {
            self.files
                .insert(path.to_path_buf(), (inode, bytes.clone()));
            return Ok(());
        }
        let entries = directories
            .get(&inode)
            .ok_or_else(|| invalid("crash namespace names an unknown inode"))?;
        if !ancestors.insert(inode) {
            return Err(invalid("crash namespace contains a directory cycle"));
        }
        self.directories.insert(path.to_path_buf());
        for (name, child) in entries {
            self.walk(*child, &path.join(name), directories, files, ancestors)?;
        }
        ancestors.remove(&inode);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn durable_file(engine: &mut ImageEngine, path: &str, bytes: &[u8]) -> InodeId {
        let inode = engine.create_file(Path::new(path)).unwrap();
        engine.write(inode, 0, bytes).unwrap();
        engine.sync_file(inode).unwrap();
        engine.sync_directory(1).unwrap();
        inode
    }

    #[test]
    fn renaming_existing_hard_links_is_a_noop_and_empty_writes_do_not_extend_files() {
        let mut engine = ImageEngine::new(ImageLimits::default()).unwrap();
        let inode = durable_file(&mut engine, "object", b"durable");
        engine
            .hard_link(Path::new("object"), Path::new("alias"))
            .unwrap();
        engine.sync_directory(1).unwrap();
        let before = engine.persist_all_plan();
        engine
            .rename(Path::new("alias"), Path::new("object"))
            .unwrap();
        assert_eq!(engine.persist_all_plan(), before);
        assert_eq!(engine.inode(Path::new("alias")).unwrap(), inode);
        assert!(engine.write(inode, u64::MAX, &[]).is_err());
        assert_eq!(engine.current_bytes(inode).unwrap(), b"durable");
        assert_eq!(engine.persist_all_plan(), before);
        assert!(engine.create_file(Path::new(&"x".repeat(256))).is_err());
    }

    #[test]
    fn completed_barriers_preserve_the_prefix_while_relaxed_writes_can_be_lost() {
        let mut engine = ImageEngine::new(ImageLimits::default()).unwrap();
        let wal = durable_file(&mut engine, "wal", b"synced transaction");
        let pending = engine.write(wal, 18, b"relaxed transaction").unwrap();
        let crash = engine.crash(&CrashPlan::default()).unwrap();
        assert_eq!(
            crash.bytes(Path::new("wal")).unwrap(),
            b"synced transaction"
        );
        engine.sync_file(wal).unwrap();
        assert!(engine
            .crash(&CrashPlan {
                persistence: vec![PersistOperation::Whole(pending)]
            })
            .is_err());
        assert_eq!(
            engine
                .crash(&CrashPlan::default())
                .unwrap()
                .bytes(Path::new("wal"))
                .unwrap(),
            b"synced transactionrelaxed transaction"
        );
    }

    #[test]
    fn cross_directory_rename_can_persist_either_entry_without_an_atomic_directory_group() {
        let mut engine = ImageEngine::new(ImageLimits::default()).unwrap();
        let source_directory = engine.create_directory(Path::new("source")).unwrap();
        engine.create_directory(Path::new("destination")).unwrap();
        engine.sync_directory(1).unwrap();
        let file = engine.create_file(Path::new("source/evidence")).unwrap();
        engine.write(file, 0, b"retained evidence").unwrap();
        engine.sync_file(file).unwrap();
        engine.sync_directory(source_directory).unwrap();
        let mut rejected = engine.clone();
        rejected.limits.max_pending_operations = 1;
        assert!(rejected
            .rename(
                Path::new("source/evidence"),
                Path::new("destination/evidence")
            )
            .is_err());
        assert_eq!(rejected.inode(Path::new("source/evidence")).unwrap(), file);
        assert!(rejected.inode(Path::new("destination/evidence")).is_err());
        assert!(rejected.pending_operations().is_empty());
        engine
            .rename(
                Path::new("source/evidence"),
                Path::new("destination/evidence"),
            )
            .unwrap();
        let operations = engine.pending_operations();
        assert_eq!(operations.len(), 2);
        let destination_only = engine
            .crash(&CrashPlan {
                persistence: vec![PersistOperation::Whole(operations[1])],
            })
            .unwrap();
        assert_eq!(
            destination_only
                .bytes(Path::new("source/evidence"))
                .unwrap(),
            b"retained evidence"
        );
        assert_eq!(
            destination_only.file_inode(Path::new("source/evidence")),
            destination_only.file_inode(Path::new("destination/evidence"))
        );
        let source_only = engine
            .crash(&CrashPlan {
                persistence: vec![PersistOperation::Whole(operations[0])],
            })
            .unwrap();
        assert!(source_only.bytes(Path::new("source/evidence")).is_none());
        assert!(source_only
            .bytes(Path::new("destination/evidence"))
            .is_none());
        assert!(engine.inode(Path::new("source/evidence")).is_err());
        assert_eq!(
            engine.inode(Path::new("destination/evidence")).unwrap(),
            file
        );
    }

    #[test]
    fn torn_and_reordered_writes_are_distinct_physical_images() {
        let mut engine = ImageEngine::new(ImageLimits::default()).unwrap();
        let wal = durable_file(&mut engine, "wal", b"abcdefgh");
        let first = engine.write(wal, 2, b"1234").unwrap();
        let second = engine.write(wal, 3, b"WXYZ").unwrap();
        let reverse = engine
            .crash(&CrashPlan {
                persistence: vec![
                    PersistOperation::Whole(second),
                    PersistOperation::Whole(first),
                ],
            })
            .unwrap();
        assert_eq!(reverse.bytes(Path::new("wal")).unwrap(), b"ab1234Zh");
        let torn = engine
            .crash(&CrashPlan {
                persistence: vec![PersistOperation::TornWrite {
                    operation: second,
                    bytes: 1..3,
                }],
            })
            .unwrap();
        assert_eq!(torn.bytes(Path::new("wal")).unwrap(), b"abcdXYgh");
        assert_eq!(engine.current_bytes(wal).unwrap(), b"ab1WXYZh");
    }

    #[test]
    fn same_directory_head_replacement_selects_old_or_new_without_mutating_old_inode() {
        let mut engine = ImageEngine::new(ImageLimits::default()).unwrap();
        let old = durable_file(&mut engine, "head", b"old root");
        let new = engine.create_file(Path::new("candidate")).unwrap();
        engine.write(new, 0, b"new root").unwrap();
        engine.sync_file(new).unwrap();
        engine
            .rename(Path::new("candidate"), Path::new("head"))
            .unwrap();
        assert_eq!(
            engine
                .crash(&CrashPlan::default())
                .unwrap()
                .bytes(Path::new("head"))
                .unwrap(),
            b"old root"
        );
        let current = engine.crash(&engine.persist_all_plan()).unwrap();
        assert_eq!(current.bytes(Path::new("head")).unwrap(), b"new root");
        assert_eq!(engine.current_bytes(old).unwrap(), b"old root");
        engine.sync_directory(1).unwrap();
        assert_eq!(
            engine
                .crash(&CrashPlan::default())
                .unwrap()
                .bytes(Path::new("head"))
                .unwrap(),
            b"new root"
        );
    }

    #[test]
    fn syncing_a_new_directory_does_not_persist_its_parent_link() {
        let mut engine = ImageEngine::new(ImageLimits::default()).unwrap();
        let directory = engine.create_directory(Path::new("objects")).unwrap();
        let file = engine.create_file(Path::new("objects/root")).unwrap();
        engine.write(file, 0, b"root closure").unwrap();
        engine.sync_file(file).unwrap();
        engine.sync_directory(directory).unwrap();
        assert!(engine
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new("objects/root"))
            .is_none());
        engine.sync_directory(1).unwrap();
        assert_eq!(
            engine
                .crash(&CrashPlan::default())
                .unwrap()
                .bytes(Path::new("objects/root"))
                .unwrap(),
            b"root closure"
        );
    }

    #[test]
    fn unsynced_reclamation_retains_an_inode_reachable_from_the_durable_namespace() {
        let mut engine = ImageEngine::new(ImageLimits::default()).unwrap();
        let object = durable_file(&mut engine, "object", b"shared checkpoint");
        engine
            .hard_link(Path::new("object"), Path::new("alias"))
            .unwrap();
        engine.sync_directory(1).unwrap();
        engine.remove(Path::new("object")).unwrap();
        let crash = engine.crash(&CrashPlan::default()).unwrap();
        assert_eq!(crash.file_inode(Path::new("object")), Some(object));
        assert_eq!(crash.file_inode(Path::new("alias")), Some(object));
        engine.sync_directory(1).unwrap();
        let crash = engine.crash(&CrashPlan::default()).unwrap();
        assert!(crash.bytes(Path::new("object")).is_none());
        assert_eq!(
            crash.bytes(Path::new("alias")).unwrap(),
            b"shared checkpoint"
        );
    }

    #[test]
    fn invalid_fault_selection_and_resource_rejection_preserve_the_live_image() {
        let mut engine = ImageEngine::new(ImageLimits {
            max_inodes: 2,
            max_pending_operations: 4,
            max_bytes: 32,
        })
        .unwrap();
        let file = durable_file(&mut engine, "wal", b"base");
        assert!(engine.create_file(Path::new("other")).is_err());
        assert!(engine.write(file, 0, &[0; 64]).is_err());
        assert_eq!(engine.current_bytes(file).unwrap(), b"base");
        let pending = engine.write(file, 4, b"tail").unwrap();
        assert!(engine
            .crash(&CrashPlan {
                persistence: vec![
                    PersistOperation::Whole(pending),
                    PersistOperation::Whole(pending)
                ]
            })
            .is_err());
        assert!(engine
            .crash(&CrashPlan {
                persistence: vec![PersistOperation::TornWrite {
                    operation: pending,
                    bytes: 0..5
                }]
            })
            .is_err());
        assert!(engine.inode(Path::new("../wal")).is_err());
    }
}
