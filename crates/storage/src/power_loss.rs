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

//! Test-support-only capture of real project IO and physical crash images.
//!
//! Attach before opening a database in an empty, already durable project
//! directory. Descendant project installations inherit the recorder, allowing
//! their first directory installation to be observed. Capture at an acknowledgement boundary while the host excludes
//! concurrent database operations. Capture verifies the visible native tree
//! against the model, so unexpected visible changes cannot silently qualify.
//! A snapshot survives later handle closure and its additional flushes.
//!
//! Qualification currently assumes POSIX file/directory synchronization and
//! atomic same-directory rename. It does not claim Windows execution coverage.

pub mod image;

use crate::file_descriptors::{absolute_path_ref, FileOpenContext, ProjectFileDescriptors};
use image::{CrashImage, CrashPlan, ImageEngine, ImageLimits, InodeId, UncoveredWrite};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug)]
pub struct PowerLossModel {
    core: Arc<ModelCore>,
    // Keep the project registry entry alive even across database closure.
    project: ProjectFileDescriptors,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoEvent {
    Rename,
    Remove,
    HardLink,
    CreateFile,
    CreateDirectory,
    Write,
    Truncate,
    FileSync,
    DirectorySync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationBoundary {
    Before,
    After,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationPoint {
    pub event: IoEvent,
    pub relative_path: PathBuf,
    pub boundary: ObservationBoundary,
    pub skip_matches: usize,
    pub include_descendants: bool,
    /// Replace the single retained image at each matching event. Useful when
    /// bootstrap performs several checkpoint renames before its final selector.
    pub keep_last: bool,
}

#[derive(Debug)]
struct Observation {
    point: ObservationPoint,
    snapshot: Option<PowerLossSnapshot>,
}

#[derive(Debug)]
pub(crate) struct ModelCore {
    root: PathBuf,
    engine: Mutex<ImageEngine>,
    observation: Mutex<Option<Observation>>,
}

#[derive(Debug, Clone)]
pub struct PowerLossSnapshot {
    engine: ImageEngine,
    observed_path: Option<PathBuf>,
}

impl PowerLossModel {
    pub fn attach(project: &ProjectFileDescriptors, limits: ImageLimits) -> io::Result<Self> {
        if !cfg!(unix) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "power-loss capture currently requires POSIX synchronization",
            ));
        }
        let engine = ImageEngine::new(limits)?;
        let mut slot = project.state.power_loss.lock().map_err(|_| poisoned())?;
        let metrics = project.metrics();
        if slot.upgrade().is_some() || metrics.open != 0 || metrics.reserved != 0 {
            return Err(invalid(
                "attach power-loss capture before acquiring project handles",
            ));
        }
        if std::fs::read_dir(project.root())?.next().is_some() {
            return Err(invalid(
                "power-loss capture requires an empty project directory",
            ));
        }
        let core = Arc::new(ModelCore {
            root: project.root().to_path_buf(),
            engine: Mutex::new(engine),
            observation: Mutex::new(None),
        });
        *slot = Arc::downgrade(&core);
        Ok(Self {
            core,
            project: project.clone(),
        })
    }

    pub fn capture(&self) -> io::Result<PowerLossSnapshot> {
        let engine = self.core.lock()?;
        verify_native_tree(
            &self.core.root,
            &engine.crash(&engine.persist_all_plan())?,
            &self.project.io_context(),
        )?;
        Ok(PowerLossSnapshot {
            engine: engine.clone(),
            observed_path: None,
        })
    }

    pub fn project(&self) -> &ProjectFileDescriptors {
        &self.project
    }

    /// Retain at most one image at an actual namespace operation boundary.
    /// No callback runs under storage locks, and later barriers cannot advance
    /// the captured image. A subsequent final capture verifies IO coverage.
    pub fn observe(&self, point: ObservationPoint) -> io::Result<()> {
        let _engine = self.core.lock()?;
        if point.relative_path.as_os_str().is_empty()
            || !point
                .relative_path
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_)))
        {
            return Err(invalid(
                "observation path must be normalized and project-relative",
            ));
        }
        let mut observation = self.core.observation.lock().map_err(|_| poisoned())?;
        if observation.is_some() {
            return Err(invalid(
                "take the previous observation before arming another",
            ));
        }
        *observation = Some(Observation {
            point,
            snapshot: None,
        });
        Ok(())
    }

    pub fn take_observation(&self) -> io::Result<Option<PowerLossSnapshot>> {
        let _engine = self.core.lock()?;
        let mut observation = self.core.observation.lock().map_err(|_| poisoned())?;
        match observation
            .as_mut()
            .and_then(|observation| observation.snapshot.take())
        {
            Some(snapshot) => {
                *observation = None;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }
}

impl PowerLossSnapshot {
    pub fn observed_path(&self) -> Option<&Path> {
        self.observed_path.as_deref()
    }
    pub fn crash(&self, plan: &CrashPlan) -> io::Result<CrashImage> {
        self.engine.crash(plan)
    }

    pub fn persist_all_plan(&self) -> CrashPlan {
        self.engine.persist_all_plan()
    }

    pub fn uncovered_writes(&self, relative_path: &Path) -> io::Result<Vec<UncoveredWrite>> {
        let inode = self.engine.inode(relative_path)?;
        Ok(self
            .engine
            .uncovered_writes()
            .filter(|write| write.inode == inode)
            .collect())
    }
}

impl ModelCore {
    pub(crate) fn lock(&self) -> io::Result<MutexGuard<'_, ImageEngine>> {
        self.engine.lock().map_err(|_| poisoned())
    }

    pub(crate) fn relative(&self, path: &Path) -> io::Result<PathBuf> {
        self.relative_path(path)?
            .ok_or_else(|| invalid("captured IO escaped the project directory"))
    }

    fn relative_path(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        let absolute = absolute_path_ref(path)?;
        if let Ok(relative) = absolute.strip_prefix(&self.root) {
            return Ok(Some(relative.to_path_buf()));
        }
        // The descriptor registry also accepts a lexical root alias, e.g.
        // macOS /var -> /private/var. Resolve the nearest existing ancestor
        // before appending new filenames; those do not canonicalize yet.
        let mut ancestor = absolute.as_ref();
        let mut suffix = Vec::new();
        let mut resolved = loop {
            match std::fs::canonicalize(ancestor) {
                Ok(resolved) => break resolved,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    suffix.push(ancestor.file_name().ok_or(error)?);
                    ancestor = ancestor
                        .parent()
                        .ok_or_else(|| invalid("captured path has no existing ancestor"))?;
                }
                Err(error) => return Err(error),
            }
        };
        for component in suffix.iter().rev() {
            resolved.push(component);
        }
        Ok(resolved
            .strip_prefix(&self.root)
            .ok()
            .map(Path::to_path_buf))
    }

    /// Admit a complete model change before native mutation, then publish the
    /// model change only after native success. The mutex covers both steps.
    pub(crate) fn mutate_observed<T>(
        &self,
        event: Option<(IoEvent, &Path)>,
        simulate: impl FnOnce(&mut ImageEngine) -> io::Result<()>,
        native: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let mut engine = self.lock()?;
        let mut candidate = engine.clone();
        simulate(&mut candidate)?;
        self.observe_at(&engine, event, ObservationBoundary::Before)?;
        let value = native()?;
        *engine = candidate;
        self.observe_at(&engine, event, ObservationBoundary::After)?;
        Ok(value)
    }

    fn observe_at(
        &self,
        engine: &ImageEngine,
        event: Option<(IoEvent, &Path)>,
        boundary: ObservationBoundary,
    ) -> io::Result<()> {
        let Some((event, path)) = event else {
            return Ok(());
        };
        let mut observation = self.observation.lock().map_err(|_| poisoned())?;
        if let Some(observation) = observation.as_mut()
            && (observation.snapshot.is_none() || observation.point.keep_last)
            && observation.point.event == event
            && (observation.point.relative_path == path
                || observation.point.include_descendants
                    && path.starts_with(&observation.point.relative_path))
            && observation.point.boundary == boundary
        {
            if observation.point.skip_matches > 0 {
                observation.point.skip_matches -= 1;
            } else {
                observation.snapshot = Some(PowerLossSnapshot {
                    engine: engine.clone(),
                    observed_path: Some(path.to_path_buf()),
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn for_path(
    context: &FileOpenContext,
    path: &Path,
) -> io::Result<Option<Arc<ModelCore>>> {
    let core = context
        .state
        .power_loss
        .lock()
        .map_err(|_| poisoned())?
        .upgrade();
    match core {
        Some(core) if core.relative_path(path)?.is_some() => Ok(Some(core)),
        _ => Ok(None),
    }
}

pub(crate) fn namespace<T>(
    context: &FileOpenContext,
    path: &Path,
    event: Option<IoEvent>,
    simulate: impl FnOnce(&mut ImageEngine, &Path) -> io::Result<()>,
    native: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let Some(core) = for_path(context, path)? else {
        return native();
    };
    let relative = core.relative(path)?;
    core.mutate_observed(
        event.map(|event| (event, relative.as_path())),
        |engine| simulate(engine, &relative),
        native,
    )
}

pub(crate) fn two_paths<T>(
    source_context: &FileOpenContext,
    destination_context: &FileOpenContext,
    source: &Path,
    destination: &Path,
    event: Option<IoEvent>,
    simulate: impl FnOnce(&mut ImageEngine, &Path, &Path) -> io::Result<()>,
    native: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    match (
        for_path(source_context, source)?,
        for_path(destination_context, destination)?,
    ) {
        (None, None) => native(),
        (Some(core), Some(other)) if Arc::ptr_eq(&core, &other) => {
            let source = core.relative(source)?;
            let destination = core.relative(destination)?;
            core.mutate_observed(
                event.map(|event| (event, destination.as_path())),
                |engine| simulate(engine, &source, &destination),
                native,
            )
        }
        _ => Err(invalid(
            "cross-project namespace changes are outside the captured fault model",
        )),
    }
}

pub(crate) fn create_directories(engine: &mut ImageEngine, path: &Path) -> io::Result<()> {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        prefix.push(component);
        match engine.inode(&prefix) {
            Ok(inode) if engine.is_directory(inode) => {}
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "captured create_dir_all found a file",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                engine.create_directory(&prefix)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn verify_native_tree(
    root: &Path,
    image: &CrashImage,
    context: &FileOpenContext,
) -> io::Result<()> {
    let mut native_files = BTreeSet::new();
    let mut native_directories = BTreeSet::new();
    let mut directories = vec![root.to_path_buf()];
    let mut inode_paths: BTreeMap<InodeId, PathBuf> = BTreeMap::new();
    #[cfg(unix)]
    let mut native_inodes = BTreeMap::new();
    while let Some(directory) = directories.pop() {
        native_directories.insert(
            directory
                .strip_prefix(root)
                .map_err(|_| invalid("native directory escaped root"))?
                .to_path_buf(),
        );
        let _directory_permit =
            context.acquire(crate::file_descriptors::DescriptorKind::Transient)?;
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(invalid("symlinks are outside the captured fault model"));
            }
            if kind.is_dir() {
                directories.push(path);
                continue;
            }
            if !kind.is_file() {
                return Err(invalid("captured project contains a nonregular file"));
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|_| invalid("native path escaped root"))?;
            let expected = image
                .bytes(relative)
                .ok_or_else(|| invalid("native file has no captured IO history"))?;
            let _file_permit =
                context.acquire(crate::file_descriptors::DescriptorKind::Transient)?;
            if std::fs::read(&path)?.as_slice() != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("uncaptured native bytes at {}", relative.display()),
                ));
            }
            let inode = image
                .file_inode(relative)
                .ok_or_else(|| invalid("captured inode is absent"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let metadata = entry.metadata()?;
                if let Some(prior) = native_inodes.insert((metadata.dev(), metadata.ino()), inode)
                    && prior != inode
                {
                    return Err(invalid(
                        "native hard link has separate captured inode identities",
                    ));
                }
            }
            #[cfg(unix)]
            if let Some(first) = inode_paths.get(&inode) {
                use std::os::unix::fs::MetadataExt;
                let first = std::fs::metadata(first)?;
                let current = entry.metadata()?;
                if (first.dev(), first.ino()) != (current.dev(), current.ino()) {
                    return Err(invalid(
                        "captured hard-link identity differs from native identity",
                    ));
                }
            }
            inode_paths.insert(inode, path.clone());
            native_files.insert(relative.to_path_buf());
        }
    }
    if native_files != image.paths().map(Path::to_path_buf).collect()
        || native_directories != image.directory_paths().map(Path::to_path_buf).collect()
    {
        return Err(invalid("captured namespace differs from native namespace"));
    }
    Ok(())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn poisoned() -> io::Error {
    invalid("power-loss capture was interrupted while recording IO")
}

mod native;
pub(crate) use native::NativeTrace;

#[cfg(all(test, unix))]
mod tests;
