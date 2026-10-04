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

//! Project-wide descriptor admission, independent of durable branch existence.

use hawdb_core::error::{FileDescriptorError, HawDBError};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

pub const DEFAULT_MAX_OPEN_FILES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescriptorKind {
    OwnershipLock,
    MutableWal,
    ImmutableCache,
    Transient,
}

impl DescriptorKind {
    const fn index(self) -> usize {
        match self {
            Self::OwnershipLock => 0,
            Self::MutableWal => 1,
            Self::ImmutableCache => 2,
            Self::Transient => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileDescriptorMetrics {
    pub limit: usize,
    pub admitted_runtimes: usize,
    pub open: usize,
    pub reserved: usize,
    pub ownership_locks: usize,
    pub mutable_wals: usize,
    pub cached_handles: usize,
    pub transient_handles: usize,
    pub high_water: usize,
    pub budget_rejections: u64,
    pub os_limit_rejections: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
}

#[derive(Debug, Default)]
struct Counts {
    admitted_runtimes: usize,
    open: [usize; 4],
    reserved: usize,
    high_water: usize,
    budget_rejections: u64,
    os_limit_rejections: u64,
    cache_hits: u64,
    cache_misses: u64,
    cache_evictions: u64,
}

impl Counts {
    fn used(&self) -> usize {
        self.open.iter().sum::<usize>() + self.reserved
    }
    fn record_peak(&mut self) {
        self.high_water = self.high_water.max(self.used());
    }
}

pub(crate) trait DescriptorCache: std::fmt::Debug + Send + Sync {
    /// Remove idle entries and drop their handles outside the cache lock.
    fn evict_idle(&self, requested: usize) -> usize;
}

#[derive(Debug)]
pub(crate) struct BudgetState {
    root: PathBuf,
    limit: usize,
    counts: Mutex<Counts>,
    cache: Mutex<Option<Weak<dyn DescriptorCache>>>,
    immutable_handles: Mutex<Weak<crate::immutable_files::ImmutableFileHandles>>,
    active_reservations: AtomicUsize,
    root_namespace_durable: AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) power_loss: Mutex<Weak<crate::power_loss::ModelCore>>,
}

impl BudgetState {
    fn new(root: PathBuf, limit: usize) -> Self {
        Self {
            root,
            limit,
            counts: Mutex::new(Counts::default()),
            cache: Mutex::new(None),
            immutable_handles: Mutex::new(Weak::new()),
            active_reservations: AtomicUsize::new(0),
            root_namespace_durable: AtomicBool::new(false),
            #[cfg(any(test, feature = "test-support"))]
            power_loss: Mutex::new(Weak::new()),
        }
    }

    fn reserve(&self, requested: usize) -> io::Result<()> {
        let attempt = || {
            let mut counts = self
                .counts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let available = self.limit - counts.used();
            if requested <= available {
                counts.reserved += requested;
                counts.record_peak();
                Ok(())
            } else {
                Err((available, requested - available))
            }
        };
        let needed = match attempt() {
            Ok(()) => return Ok(()),
            Err((_, needed)) => needed,
        };
        let cache = self
            .cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(cache) = cache {
            cache.evict_idle(needed);
        }
        match attempt() {
            Ok(()) => Ok(()),
            Err((available, _)) => Err(io::Error::other(FileDescriptorError::BudgetExceeded {
                requested,
                available,
                limit: self.limit,
            })),
        }
    }

    pub(crate) fn metrics(&self) -> FileDescriptorMetrics {
        let counts = self
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        FileDescriptorMetrics {
            limit: self.limit,
            admitted_runtimes: counts.admitted_runtimes,
            open: counts.open.iter().sum(),
            reserved: counts.reserved,
            ownership_locks: counts.open[0],
            mutable_wals: counts.open[1],
            cached_handles: counts.open[2],
            transient_handles: counts.open[3],
            high_water: counts.high_water,
            budget_rejections: counts.budget_rejections,
            os_limit_rejections: counts.os_limit_rejections,
            cache_hits: counts.cache_hits,
            cache_misses: counts.cache_misses,
            cache_evictions: counts.cache_evictions,
        }
    }

    pub(crate) fn install_cache(&self, cache: &Arc<dyn DescriptorCache>) {
        *self.cache.lock().unwrap_or_else(|error| error.into_inner()) = Some(Arc::downgrade(cache));
    }
    pub(crate) fn record_cache_hit(&self) {
        self.counts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cache_hits += 1;
    }
    fn record_budget_rejection(&self) {
        self.counts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .budget_rejections += 1;
    }
    pub(crate) fn record_cache_miss(&self) {
        self.counts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cache_misses += 1;
    }
    pub(crate) fn record_cache_evictions(&self, count: usize) {
        self.counts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cache_evictions += count as u64;
    }
    pub(crate) fn existing_immutable_handles(
        &self,
    ) -> Option<Arc<crate::immutable_files::ImmutableFileHandles>> {
        self.immutable_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade()
    }

    pub(crate) fn immutable_handles(
        self: &Arc<Self>,
    ) -> Arc<crate::immutable_files::ImmutableFileHandles> {
        let mut cached = self
            .immutable_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(handles) = cached.upgrade() {
            return handles;
        }
        let handles = Arc::new(crate::immutable_files::ImmutableFileHandles::new(
            self.clone(),
        ));
        let eviction: Arc<dyn DescriptorCache> = handles.clone();
        self.install_cache(&eviction);
        *cached = Arc::downgrade(&handles);
        handles
    }
}

static PROJECTS: LazyLock<Mutex<BTreeMap<PathBuf, Weak<BudgetState>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static STANDALONE: LazyLock<Arc<BudgetState>> =
    LazyLock::new(|| Arc::new(BudgetState::new(PathBuf::new(), DEFAULT_MAX_OPEN_FILES)));

/// Contexts and retained descriptor permits share one canonical project state.
/// The registry is weak, so closure of the final owner releases the domain.
#[derive(Debug, Clone)]
pub struct ProjectFileDescriptors {
    pub(crate) state: Arc<BudgetState>,
    pub(crate) immutable_handles: Arc<crate::immutable_files::ImmutableFileHandles>,
}

impl ProjectFileDescriptors {
    pub fn acquire(root: &Path, limit: usize) -> Result<Self, HawDBError> {
        Self::acquire_root(root, limit, true)
    }

    pub fn acquire_existing(root: &Path, limit: usize) -> Result<Self, HawDBError> {
        Self::acquire_root(root, limit, false)
    }

    /// Persistent components borrow an existing project domain, including its
    /// configured limit. Standalone component roots get the finite default.
    #[doc(hidden)]
    pub fn acquire_component(root: &Path, create: bool) -> Result<Self, HawDBError> {
        match Self::containing(root)? {
            Some(project) => {
                if create {
                    ensure_project_namespace(&project.state, project.root())?;
                }
                Ok(project)
            }
            None => Self::acquire_root(root, DEFAULT_MAX_OPEN_FILES, create),
        }
    }

    /// Internal runtimes and maintenance borrow the containing project domain.
    /// They must never turn a branch/runtime subdirectory into a second budget.
    pub(crate) fn acquire_containing(root: &Path, limit: usize) -> Result<Self, HawDBError> {
        let context = context_for_path(root)?;
        if !context.state.root.as_os_str().is_empty() {
            configured_state(context.state, limit)
        } else {
            Self::acquire_existing(root, limit)
        }
    }

    pub(crate) fn registered(path: &Path) -> Result<Self, HawDBError> {
        Self::containing(path)?.ok_or_else(|| {
            HawDBError::Storage("persistent runtime has no project descriptor domain".into())
        })
    }

    pub(crate) fn containing(path: &Path) -> std::io::Result<Option<Self>> {
        let context = context_for_path(path)?;
        if context.state.root.as_os_str().is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            immutable_handles: context.state.immutable_handles(),
            state: context.state,
        }))
    }

    fn acquire_root(root: &Path, limit: usize, create: bool) -> Result<Self, HawDBError> {
        if limit == 0 {
            return Err(HawDBError::FileDescriptors(
                FileDescriptorError::InvalidBudget { limit },
            ));
        }
        let lexical = absolute_path(root)?;
        let mut projects = PROJECTS.lock().unwrap_or_else(|error| error.into_inner());
        projects.retain(|_, state| state.strong_count() != 0);
        if let Some(state) = projects.get(&lexical).and_then(Weak::upgrade) {
            let project = configured_state(state, limit)?;
            if create {
                ensure_project_namespace(&project.state, project.root())?;
            }
            return Ok(project);
        }
        let tentative = Arc::new(BudgetState::new(lexical.clone(), limit));
        let tentative_probe =
            FileOpenContext::from_state(tentative.clone()).acquire(DescriptorKind::Transient)?;
        // A directory installed by this operation is a new project object,
        // including when its parent path uses a filesystem alias. Existing
        // roots require alias resolution before choosing their resource domain.
        let created = create && create_project_directory(&lexical)?;
        // Windows canonicalization opens a directory handle. Reserve across
        // possible alias domains until its identity is known. Unix realpath
        // returns no engine-owned handle, so unrelated projects stay independent.
        let mut probe_permits = Vec::new();
        if cfg!(windows) && !created {
            let mut probed = Vec::new();
            for state in projects.values().filter_map(Weak::upgrade) {
                if probed.iter().any(|prior| Arc::ptr_eq(prior, &state)) {
                    continue;
                }
                probe_permits.push(
                    FileOpenContext::from_state(state.clone())
                        .acquire(DescriptorKind::Transient)?,
                );
                probed.push(state);
            }
        }
        let canonical = std::fs::canonicalize(&lexical)?;
        drop(tentative_probe);
        drop(probe_permits);
        if let Some(state) = projects.get(&canonical).and_then(Weak::upgrade) {
            let project = configured_state(state, limit)?;
            if create {
                ensure_project_namespace(&project.state, project.root())?;
            }
            projects.insert(lexical, Arc::downgrade(&project.state));
            return Ok(project);
        }
        if create {
            ensure_project_namespace(&tentative, &canonical)?;
        }
        let mut state = tentative;
        Arc::get_mut(&mut state)
            .expect("bootstrap permit has been released")
            .root = canonical.clone();
        projects.insert(canonical, Arc::downgrade(&state));
        projects.insert(lexical, Arc::downgrade(&state));
        Ok(Self {
            immutable_handles: state.immutable_handles(),
            state,
        })
    }

    pub fn root(&self) -> &Path {
        &self.state.root
    }
    pub fn metrics(&self) -> FileDescriptorMetrics {
        self.state.metrics()
    }

    /// Capture this project's admission domain for query-owned external IO.
    /// The context retains accounting, not an open descriptor or branch lease.
    #[doc(hidden)]
    pub fn io_context(&self) -> FileOpenContext {
        FileOpenContext::from_state(self.state.clone())
    }

    pub(crate) fn retain_admitted_runtime(&self) -> Arc<AdmittedRuntimeOwner> {
        self.state
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .admitted_runtimes += 1;
        Arc::new(AdmittedRuntimeOwner {
            state: self.state.clone(),
        })
    }

    /// Hold capacity across an admission/publication operation. Temporary
    /// handles return capacity to this reservation until it is finished.
    pub fn reserve(&self, requested: usize) -> io::Result<DescriptorReservation> {
        self.state.reserve(requested).inspect_err(|_| {
            self.state.record_budget_rejection();
        })?;
        Ok(self.begin_reservation(requested))
    }

    /// Reserve the target's lock/WAL and bounded IO-wave capacity through
    /// candidate validation. Nested storage calls borrow the outer operation's
    /// quota; independent contexts can use the remaining project capacity.
    pub(crate) fn reserve_admission(&self, minimum: usize) -> io::Result<DescriptorReservation> {
        if let Some(inventory) = FileOpenContext::from_state(self.state.clone()).inventory {
            let inner = inventory
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if inner.active {
                if inner.remaining < minimum {
                    self.state.record_budget_rejection();
                    return Err(io::Error::other(FileDescriptorError::BudgetExceeded {
                        requested: minimum,
                        available: inner.remaining,
                        limit: self.state.limit,
                    }));
                }
                drop(inner);
                return Ok(DescriptorReservation {
                    inventory,
                    owns_inventory: false,
                    _thread: PhantomData,
                });
            }
        }
        self.reserve(minimum)
    }

    fn begin_reservation(&self, requested: usize) -> DescriptorReservation {
        let inventory = Arc::new(Inventory {
            state: self.state.clone(),
            inner: Mutex::new(InventoryState {
                active: true,
                remaining: requested,
            }),
        });
        RESERVATIONS.with(|scopes| scopes.borrow_mut().push(Arc::downgrade(&inventory)));
        self.state
            .active_reservations
            .fetch_add(1, Ordering::Release);
        DescriptorReservation {
            inventory,
            owns_inventory: true,
            _thread: PhantomData,
        }
    }
}

fn ensure_project_namespace(state: &Arc<BudgetState>, root: &Path) -> io::Result<()> {
    if !state.root_namespace_durable.load(Ordering::Acquire) {
        let context = FileOpenContext::from_state(state.clone());
        crate::durability::sync_directory_tree_with_context(root, &context, None)?;
        state.root_namespace_durable.store(true, Ordering::Release);
    }
    Ok(())
}

fn create_project_directory(path: &Path) -> io::Result<bool> {
    let create = || match std::fs::create_dir(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error),
    };
    match create() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path.parent().ok_or(error)?;
            std::fs::create_dir_all(parent)?;
            create()
        }
        result => result,
    }
}

/// Shared by snapshots of one admitted runtime; closure of the final runtime
/// owner releases this count without tying durable branch existence to it.
#[derive(Debug)]
pub(crate) struct AdmittedRuntimeOwner {
    state: Arc<BudgetState>,
}

impl AdmittedRuntimeOwner {
    pub(crate) fn metrics(&self) -> FileDescriptorMetrics {
        self.state.metrics()
    }

    pub(crate) fn io_context(&self) -> FileOpenContext {
        FileOpenContext::from_state(self.state.clone())
    }
}

impl Drop for AdmittedRuntimeOwner {
    fn drop(&mut self) {
        self.state
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .admitted_runtimes -= 1;
    }
}

fn configured_state(
    state: Arc<BudgetState>,
    limit: usize,
) -> Result<ProjectFileDescriptors, HawDBError> {
    if state.limit != limit {
        Err(HawDBError::FileDescriptors(
            FileDescriptorError::ConfigurationConflict {
                configured: state.limit,
                requested: limit,
            },
        ))
    } else {
        Ok(ProjectFileDescriptors {
            immutable_handles: state.immutable_handles(),
            state,
        })
    }
}

pub(crate) fn absolute_path(path: &Path) -> io::Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            // Resolve parent components and symlinks through canonicalization;
            // lexically removing them can select a different filesystem root.
            component => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized)
}

pub(crate) fn context_for_path(path: &Path) -> io::Result<FileOpenContext> {
    // Path comparisons already normalize separators and CurDir components.
    // Borrow absolute paths so admission does not allocate an unaccounted path
    // copy on every query IO. Relative paths still require a cwd-owned prefix.
    let owned;
    let absolute = if path.is_absolute() {
        path
    } else {
        owned = absolute_path(path)?;
        &owned
    };
    let projects = PROJECTS.lock().unwrap_or_else(|error| error.into_inner());
    let state = projects
        .iter()
        .filter(|(root, _)| absolute.starts_with(root))
        .filter_map(|(root, state)| {
            state
                .upgrade()
                .map(|state| (root.components().count(), state))
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, state)| state)
        .unwrap_or_else(|| STANDALONE.clone());
    Ok(FileOpenContext::from_state(state))
}

pub(crate) fn absolute_path_ref(path: &Path) -> io::Result<std::borrow::Cow<'_, Path>> {
    if path.is_absolute() {
        Ok(std::borrow::Cow::Borrowed(path))
    } else {
        Ok(std::borrow::Cow::Owned(absolute_path(path)?))
    }
}

thread_local! {
    static RESERVATIONS: RefCell<Vec<Weak<Inventory>>> = const { RefCell::new(Vec::new()) };
}

#[derive(Debug)]
struct InventoryState {
    active: bool,
    remaining: usize,
}

#[derive(Debug)]
struct Inventory {
    state: Arc<BudgetState>,
    inner: Mutex<InventoryState>,
}

#[derive(Debug)]
pub struct DescriptorReservation {
    inventory: Arc<Inventory>,
    owns_inventory: bool,
    _thread: PhantomData<Rc<()>>,
}

impl Drop for DescriptorReservation {
    fn drop(&mut self) {
        if !self.owns_inventory {
            return;
        }
        let mut inventory = self
            .inventory
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        inventory.active = false;
        self.inventory
            .state
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .reserved -= inventory.remaining;
        inventory.remaining = 0;
        self.inventory
            .state
            .active_reservations
            .fetch_sub(1, Ordering::Release);
        RESERVATIONS.with(|scopes| {
            scopes.borrow_mut().retain(|scope| {
                scope
                    .upgrade()
                    .is_some_and(|scope| !Arc::ptr_eq(&scope, &self.inventory))
            })
        });
    }
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct FileOpenContext {
    pub(crate) state: Arc<BudgetState>,
    inventory: Option<Arc<Inventory>>,
}

impl FileOpenContext {
    pub(crate) fn project_root(&self) -> &Path {
        &self.state.root
    }

    #[cfg(unix)]
    pub(crate) fn metadata(&self, path: &Path) -> io::Result<std::fs::Metadata> {
        self.temporary(|| std::fs::metadata(path))
    }

    pub fn for_path(path: &Path) -> io::Result<Self> {
        context_for_path(path)
    }

    pub fn open(
        &self,
        options: &crate::file_io::OpenOptions,
        path: &Path,
    ) -> io::Result<crate::file_io::File> {
        options.open_with_context(path, self)
    }

    fn temporary<T>(&self, operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        let _permit = self.acquire(DescriptorKind::Transient)?;
        operation().map_err(|error| self.map_open_error(error))
    }

    pub fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.temporary(|| std::fs::create_dir_all(path))
    }

    pub fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.temporary(|| std::fs::canonicalize(path))
    }

    pub fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.temporary(|| std::fs::remove_file(path))
    }

    pub fn read_dir(&self, path: &Path) -> io::Result<crate::file_io::ReadDir> {
        crate::file_io::read_dir_with_context(path, self)
    }

    fn from_state(state: Arc<BudgetState>) -> Self {
        // Most query IO has no operation quota. Avoid initializing a droppable
        // thread-local Vec (and its native destructor registry allocation) for
        // every read worker when there is no reservation to inherit.
        let inventory = if state.active_reservations.load(Ordering::Acquire) == 0 {
            None
        } else {
            RESERVATIONS.with(|scopes| {
                scopes
                    .borrow()
                    .iter()
                    .rev()
                    .filter_map(Weak::upgrade)
                    .find(|inventory| Arc::ptr_eq(&inventory.state, &state))
            })
        };
        Self { state, inventory }
    }

    fn take_inventory(&self, kind: DescriptorKind) -> Option<DescriptorPermit> {
        let inventory = self.inventory.as_ref()?;
        let mut inner = inventory
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !inner.active || inner.remaining == 0 {
            return None;
        }
        inner.remaining -= 1;
        let mut counts = self
            .state
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        counts.reserved -= 1;
        counts.open[kind.index()] += 1;
        Some(DescriptorPermit {
            context: self.clone(),
            kind,
            from_inventory: true,
        })
    }

    pub(crate) fn acquire(&self, kind: DescriptorKind) -> io::Result<DescriptorPermit> {
        if let Some(permit) = self.take_inventory(kind) {
            return Ok(permit);
        }
        if let Err(error) = self.state.reserve(1) {
            // Eviction can return a slot to this operation's reserved quota.
            if let Some(permit) = self.take_inventory(kind) {
                return Ok(permit);
            }
            self.state.record_budget_rejection();
            return Err(error);
        }
        let mut counts = self
            .state
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        counts.reserved -= 1;
        counts.open[kind.index()] += 1;
        Ok(DescriptorPermit {
            context: self.clone(),
            kind,
            from_inventory: false,
        })
    }

    pub(crate) fn map_open_error(&self, error: io::Error) -> io::Error {
        if let Some(error) = hawdb_core::error::file_descriptor_error(&error) {
            if matches!(error, FileDescriptorError::OsLimit { .. }) {
                self.state
                    .counts
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .os_limit_rejections += 1;
            }
            io::Error::other(error)
        } else {
            error
        }
    }
}

#[derive(Debug)]
pub(crate) struct DescriptorPermit {
    pub(crate) context: FileOpenContext,
    kind: DescriptorKind,
    from_inventory: bool,
}

impl DescriptorPermit {
    pub(crate) fn kind(&self) -> DescriptorKind {
        self.kind
    }
}

impl Drop for DescriptorPermit {
    fn drop(&mut self) {
        let mut inventory = self
            .context
            .inventory
            .as_ref()
            .filter(|_| self.from_inventory)
            .map(|inventory| {
                inventory
                    .inner
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
            });
        let mut counts = self
            .context
            .state
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        counts.open[self.kind.index()] -= 1;
        if let Some(inventory) = &mut inventory
            && inventory.active
        {
            inventory.remaining += 1;
            counts.reserved += 1;
        }
    }
}

#[cfg(test)]
#[path = "file_descriptors/tests.rs"]
mod tests;
