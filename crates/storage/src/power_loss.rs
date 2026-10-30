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
//! Qualification assumes completed file/directory barriers preserve bytes/names
//! and same-directory replacement is atomic. On Windows the directory-flush
//! assumption is conditional: native execution does not establish filesystem or
//! device namespace durability. See `docs/BRANCH_POWER_LOSS_QUALIFICATION.md`.

pub mod image;

use crate::file_descriptors::{absolute_path_ref, FileOpenContext, ProjectFileDescriptors};
#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
use image::InodeId;
use image::{CrashImage, CrashPlan, ImageEngine, ImageLimits, UncoveredWrite};
#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
use std::collections::{BTreeMap, BTreeSet};
use std::io;
#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
use std::io::Read;
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
    /// Normalized project-relative path; empty denotes the captured root.
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
    file_name_prefix: Option<String>,
    snapshot: Option<PowerLossSnapshot>,
}

#[derive(Debug)]
pub(crate) struct ModelCore {
    root: PathBuf,
    root_anchor: crate::file_io::PowerLossRoot,
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
        Self::attach_with_image(project, limits, None)
    }

    /// Continue recording recovery on an exact materialization of a previous
    /// physical crash. Its surviving bytes and names form the durable baseline;
    /// only subsequent real IO may change them. No runtime may already be open.
    pub fn resume_from_image(
        project: &ProjectFileDescriptors,
        image: &CrashImage,
        limits: ImageLimits,
    ) -> io::Result<Self> {
        Self::attach_with_image(project, limits, Some(image))
    }

    fn attach_with_image(
        project: &ProjectFileDescriptors,
        limits: ImageLimits,
        baseline: Option<&CrashImage>,
    ) -> io::Result<Self> {
        if !cfg!(any(unix, windows)) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "power-loss capture requires Unix or Windows native IO",
            ));
        }
        let engine = match baseline {
            Some(image) => ImageEngine::from_crash_image(image, limits)?,
            None => ImageEngine::new(limits)?,
        };
        let mut slot = project.state.power_loss.lock().map_err(|_| poisoned())?;
        let metrics = project.metrics();
        if slot.upgrade().is_some() || metrics.open != 0 || metrics.reserved != 0 {
            return Err(invalid(
                "attach power-loss capture before acquiring project handles",
            ));
        }
        let root_anchor =
            crate::file_io::PowerLossRoot::capture(project.root(), &project.io_context())?;
        if let Some(image) = baseline {
            #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
            verification::verify(&root_anchor, image)?;
            #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
            verify_native_tree(project.root(), image, &project.io_context())?;
        } else {
            root_anchor.require_empty(project.root())?;
        }
        let core = Arc::new(ModelCore {
            root: project.root().to_path_buf(),
            root_anchor,
            engine: Mutex::new(engine),
            observation: Mutex::new(None),
        });
        *slot = Arc::downgrade(&core);
        Ok(Self {
            core,
            project: project.clone(),
        })
    }

    // Local fault collaborator for the real post-native observer error path.
    // No global registration, I/O callback or production API is introduced.
    #[cfg(test)]
    pub(crate) fn poison_observation_for_test(&self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _observation = self
                .core
                .observation
                .lock()
                .expect("unpoisoned test observer");
            panic!("injected post-native observer poison");
        }));
        assert!(result.is_err());
    }

    pub fn capture(&self) -> io::Result<PowerLossSnapshot> {
        let engine = self.core.lock()?;
        self.core.root_anchor.require_path(&self.core.root)?;
        let image = engine.crash(&engine.persist_all_plan())?;
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        verification::verify(&self.core.root_anchor, &image)?;
        #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
        verify_native_tree(&self.core.root, &image, &self.project.io_context())?;
        self.core.root_anchor.require_path(&self.core.root)?;
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
        self.observe_filtered(point, None)
    }

    /// Select temporary publication writes without matching later validation
    /// scratch. The path, event and boundary constraints still apply first.
    pub fn observe_file_name_prefix(
        &self,
        point: ObservationPoint,
        prefix: &str,
    ) -> io::Result<()> {
        if prefix.is_empty()
            || Path::new(prefix).file_name() != Some(std::ffi::OsStr::new(prefix))
            || !Path::new(prefix)
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_)))
        {
            return Err(invalid(
                "observation prefix must be one normal filename component",
            ));
        }
        self.observe_filtered(point, Some(prefix.to_owned()))
    }

    fn observe_filtered(
        &self,
        point: ObservationPoint,
        file_name_prefix: Option<String>,
    ) -> io::Result<()> {
        let _engine = self.core.lock()?;
        if !point
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
            file_name_prefix,
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
            && observation.file_name_prefix.as_ref().is_none_or(|prefix| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(prefix))
            })
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
        Some(core) if core.relative_path(path)?.is_some() => {
            core.root_anchor
                .require_path_with_context(&core.root, context)?;
            Ok(Some(core))
        }
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

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
fn verify_native_tree(
    root: &Path,
    image: &CrashImage,
    context: &FileOpenContext,
) -> io::Result<()> {
    let mut native_files = BTreeSet::new();
    let mut native_directories = BTreeSet::new();
    let mut directories = vec![root.to_path_buf()];
    let mut native_by_inode: BTreeMap<InodeId, identity::NativeIdentity> = BTreeMap::new();
    let mut inode_by_native = BTreeMap::new();
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
            let metadata = entry.metadata()?;
            identity::require_direct_entry(&metadata)?;
            let kind = metadata.file_type();
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
            let mut file = std::fs::File::open(&path)?;
            let identity = identity::of_file(&file)?;
            if file.metadata()?.len() != expected.len() as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("uncaptured native length at {}", relative.display()),
                ));
            }
            let mut bytes = Vec::new();
            // Windows byte-range locks also cover bytes beyond EOF. Empty
            // lease files need only the handle's identity and length checked;
            // probing EOF through a second handle would violate their lock.
            if !expected.is_empty() {
                file.by_ref()
                    .take((expected.len() as u64).saturating_add(1))
                    .read_to_end(&mut bytes)?;
            }
            if bytes.as_slice() != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("uncaptured native bytes at {}", relative.display()),
                ));
            }
            let inode = image
                .file_inode(relative)
                .ok_or_else(|| invalid("captured inode is absent"))?;
            if let Some(prior) = inode_by_native.insert(identity, inode)
                && prior != inode
            {
                return Err(invalid(
                    "native hard link has separate captured inode identities",
                ));
            }
            if let Some(prior) = native_by_inode.insert(inode, identity)
                && prior != identity
            {
                return Err(invalid(
                    "captured hard-link identity differs from native identity",
                ));
            }
            native_files.insert(relative.to_path_buf());
        }
    }
    let captured_files = image.paths().map(Path::to_path_buf).collect();
    let captured_directories = image.directory_paths().map(Path::to_path_buf).collect();
    if native_files != captured_files || native_directories != captured_directories {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "captured namespace differs from native namespace: missing file {:?}, missing directory {:?}, unexpected directory {:?}",
                captured_files.difference(&native_files).next(),
                captured_directories.difference(&native_directories).next(),
                native_directories.difference(&captured_directories).next(),
            ),
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn poisoned() -> io::Error {
    invalid("power-loss capture was interrupted while recording IO")
}

pub(crate) mod identity;
mod native;
mod native_directory;
pub(crate) use native_directory::NativeDirectoryTrace;
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod verification;
pub(crate) use native::NativeTrace;

#[cfg(all(test, any(unix, windows)))]
mod tests;
