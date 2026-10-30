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

mod os_limit;

/// Shared project descriptor capacity target; capacity is charged only when used.
/// Native Unix acquisition observes OS capacity without changing process limits.
pub const DEFAULT_MAX_OPEN_FILES: usize = 1024;

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
    /// Effective capacity target, retained for existing consumers.
    pub limit: usize,
    pub configured_limit: usize,
    pub effective_limit: usize,
    /// Whether foreground descriptor admission enforces the capacity target.
    pub strict_capacity_limit: bool,
    /// A readiness/reporting warning; reduced capacity does not block readiness.
    pub os_limit_clamped: bool,
    /// Current process soft limit on native Unix; unavailable on other platforms.
    pub os_soft_limit: Option<u64>,
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
    configured_limit: usize,
    limit: usize,
    strict_capacity_limit: bool,
    counts: Mutex<Counts>,
    cache: Mutex<Option<Weak<dyn DescriptorCache>>>,
    immutable_handles: Mutex<Weak<crate::immutable_files::ImmutableFileHandles>>,
    active_reservations: AtomicUsize,
    root_namespace_durable: AtomicBool,
    namespace_anchor: Option<PathBuf>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) power_loss: Mutex<Weak<crate::power_loss::ModelCore>>,
}

impl BudgetState {
    fn new(
        root: PathBuf,
        configured_limit: usize,
        limit: usize,
        strict_capacity_limit: bool,
    ) -> Self {
        Self {
            root,
            configured_limit,
            limit,
            strict_capacity_limit,
            counts: Mutex::new(Counts::default()),
            cache: Mutex::new(None),
            immutable_handles: Mutex::new(Weak::new()),
            active_reservations: AtomicUsize::new(0),
            root_namespace_durable: AtomicBool::new(false),
            namespace_anchor: None,
            #[cfg(any(test, feature = "test-support"))]
            power_loss: Mutex::new(Weak::new()),
        }
    }

    fn reserve(&self, requested: usize, strict_capacity_limit: bool) -> io::Result<()> {
        let attempt = |allow_excess: bool| {
            let mut counts = self
                .counts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let used = counts.used();
            let available = self.limit.saturating_sub(used);
            let budget_failure = || {
                (
                    FileDescriptorError::BudgetExceeded {
                        requested,
                        available,
                        limit: self.limit,
                    },
                    requested
                        .saturating_sub(available)
                        .saturating_add(used.saturating_sub(self.limit)),
                )
            };
            if used.checked_add(requested).is_none() {
                return Err((budget_failure().0, usize::MAX));
            }
            if !allow_excess && requested > available {
                return Err(budget_failure());
            }
            os_limit::reserve(requested)?;
            counts.reserved += requested;
            counts.record_peak();
            Ok(())
        };
        let needed = match attempt(false) {
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
        match attempt(!strict_capacity_limit) {
            Ok(()) => Ok(()),
            Err((error, _)) => Err(io::Error::other(error)),
        }
    }

    pub(crate) fn metrics(&self) -> FileDescriptorMetrics {
        let counts = self
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        FileDescriptorMetrics {
            limit: self.limit,
            configured_limit: self.configured_limit,
            effective_limit: self.limit,
            strict_capacity_limit: self.strict_capacity_limit,
            os_limit_clamped: self.limit < self.configured_limit,
            os_soft_limit: os_limit::current_soft_limit(),
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
    fn record_admission_rejection(&self, error: &io::Error) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if matches!(
            hawdb_core::error::file_descriptor_error(error),
            Some(FileDescriptorError::OsLimit { .. })
        ) {
            counts.os_limit_rejections += 1;
        } else {
            counts.budget_rejections += 1;
        }
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
// Project identity is not known until canonicalization finishes. PROJECTS
// serializes these short probes, so one process-owned slot covers the native
// directory handle without borrowing capacity from unrelated projects.
static NAMESPACE_PROBES: LazyLock<Arc<BudgetState>> =
    LazyLock::new(|| Arc::new(BudgetState::new(PathBuf::new(), 1, 1, false)));
static STANDALONE: LazyLock<Arc<BudgetState>> = LazyLock::new(|| {
    Arc::new(BudgetState::new(
        PathBuf::new(),
        DEFAULT_MAX_OPEN_FILES,
        DEFAULT_MAX_OPEN_FILES,
        false,
    ))
});

/// Contexts and retained descriptor permits share one canonical project state.
/// The registry is weak, so closure of the final owner releases the domain.
#[derive(Debug, Clone)]
pub struct ProjectFileDescriptors {
    pub(crate) state: Arc<BudgetState>,
    pub(crate) immutable_handles: Arc<crate::immutable_files::ImmutableFileHandles>,
}

impl ProjectFileDescriptors {
    pub fn acquire(root: &Path, limit: usize) -> Result<Self, HawDBError> {
        Self::acquire_root(root, limit, true, None, None)
    }

    pub fn acquire_existing(root: &Path, limit: usize) -> Result<Self, HawDBError> {
        Self::acquire_root(root, limit, false, None, None)
    }

    /// Configure foreground admission. Internal control I/O stays non-strict.
    pub fn acquire_with_capacity_limit(
        root: &Path,
        limit: usize,
        strict_capacity_limit: bool,
    ) -> Result<Self, HawDBError> {
        Self::acquire_root(root, limit, true, Some(strict_capacity_limit), None)
    }

    pub fn acquire_existing_with_capacity_limit(
        root: &Path,
        limit: usize,
        strict_capacity_limit: bool,
    ) -> Result<Self, HawDBError> {
        Self::acquire_root(root, limit, false, Some(strict_capacity_limit), None)
    }

    /// Admit a facade-configured project beneath a host-guaranteed durable
    /// ancestor. Internal component opens inherit that original decision.
    #[doc(hidden)]
    pub fn acquire_with_namespace_anchor(
        root: &Path,
        limit: usize,
        strict_capacity_limit: bool,
        create: bool,
        anchor: Option<&Path>,
    ) -> Result<Self, HawDBError> {
        let anchor = anchor
            .map(|anchor| validate_namespace_anchor(root, anchor))
            .transpose()?;
        Self::acquire_root(
            root,
            limit,
            create,
            Some(strict_capacity_limit),
            Some(&anchor),
        )
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
            None => Self::acquire_root(root, DEFAULT_MAX_OPEN_FILES, create, None, None),
        }
    }

    /// Admit a related component under the host's explicit namespace boundary.
    /// Existing containing domains retain their capacity and must agree on the
    /// canonical anchor; standalone components retain the finite default limit.
    #[doc(hidden)]
    pub fn acquire_component_with_namespace_anchor(
        root: &Path,
        create: bool,
        anchor: &Path,
    ) -> Result<Self, HawDBError> {
        let anchor = Some(validate_namespace_anchor(root, anchor)?);
        match Self::containing(root)? {
            Some(project) => {
                require_namespace_anchor(&project.state, Some(&anchor))?;
                if create {
                    ensure_project_namespace(&project.state, project.root())?;
                }
                Ok(project)
            }
            None => Self::acquire_root(root, DEFAULT_MAX_OPEN_FILES, create, None, Some(&anchor)),
        }
    }

    /// Retain the descriptor domain of an already captured native directory.
    /// Registered readers must not resolve a saved path again after acquiring
    /// the original directory, since that label may now name another root.
    #[doc(hidden)]
    pub fn from_open_context(context: &FileOpenContext) -> Self {
        let state = Arc::clone(&context.state);
        Self {
            immutable_handles: state.immutable_handles(),
            state,
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

    fn acquire_root(
        root: &Path,
        limit: usize,
        create: bool,
        strict_capacity_limit: Option<bool>,
        namespace_anchor: Option<&Option<PathBuf>>,
    ) -> Result<Self, HawDBError> {
        if limit == 0 {
            return Err(HawDBError::FileDescriptors(
                FileDescriptorError::InvalidBudget { limit },
            ));
        }
        let lexical = absolute_path(root)?;
        let mut projects = PROJECTS.lock().unwrap_or_else(|error| error.into_inner());
        projects.retain(|_, state| state.strong_count() != 0);
        if let Some(state) = projects.get(&lexical).and_then(Weak::upgrade) {
            require_namespace_anchor(&state, namespace_anchor)?;
            let project = configured_capacity_state(state, limit, strict_capacity_limit)?;
            if create {
                ensure_project_namespace(&project.state, project.root())?;
            }
            return Ok(project);
        }
        let effective_limit = os_limit::effective_limit(limit)?;
        let mut tentative = BudgetState::new(
            lexical.clone(),
            limit,
            effective_limit,
            strict_capacity_limit.unwrap_or(false),
        );
        tentative.namespace_anchor = namespace_anchor.cloned().flatten();
        let tentative = Arc::new(tentative);
        #[cfg(any(test, feature = "test-support"))]
        {
            // A recorder rooted at an existing durable ancestor also observes
            // first-time project installation. Keep resource domains separate;
            // only their test recorder is shared. Do not reenter PROJECTS here.
            if let Some(parent) = projects
                .iter()
                .filter(|(path, _)| lexical.starts_with(path))
                .filter_map(|(path, state)| state.upgrade().map(|state| (path, state)))
                .max_by_key(|(path, _)| path.components().count())
                .map(|(_, state)| state)
            {
                *tentative
                    .power_loss
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = parent
                    .power_loss
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
            }
        }
        let namespace_probe = FileOpenContext::from_state(NAMESPACE_PROBES.clone())
            .acquire_with_capacity_limit(DescriptorKind::Transient, false)?;
        // A directory installed by this operation is a new project object,
        // including when its parent path uses a filesystem alias. Existing
        // roots require alias resolution before choosing their resource domain.
        #[cfg(any(test, feature = "test-support"))]
        let _created = create
            && crate::power_loss::namespace(
                &FileOpenContext::from_state(tentative.clone()),
                &lexical,
                Some(crate::power_loss::IoEvent::CreateDirectory),
                crate::power_loss::create_directories,
                || create_project_directory(&lexical),
            )?;
        #[cfg(not(any(test, feature = "test-support")))]
        let _created = create && create_project_directory(&lexical)?;
        // Windows canonicalization may open a directory handle. Its process-
        // owned probe closes before the resolved project domain is selected.
        let canonical = std::fs::canonicalize(&lexical)?;
        drop(namespace_probe);
        if let Some(anchor) = &tentative.namespace_anchor
            && !canonical.starts_with(anchor)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "project moved outside its namespace durability anchor",
            )
            .into());
        }
        if let Some(state) = projects.get(&canonical).and_then(Weak::upgrade) {
            require_namespace_anchor(&state, namespace_anchor)?;
            let project = configured_capacity_state(state, limit, strict_capacity_limit)?;
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

    /// Borrow the same admitted domain, preserving the existing exact-config
    /// conflict check without resolving a saved namespace again.
    pub(crate) fn with_configured_limit(&self, limit: usize) -> Result<Self, HawDBError> {
        configured_state(Arc::clone(&self.state), limit)
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
        self.reserve_with_capacity_limit(requested, self.io_context().strict_capacity_limit())
    }

    fn reserve_with_capacity_limit(
        &self,
        requested: usize,
        strict_capacity_limit: bool,
    ) -> io::Result<DescriptorReservation> {
        self.state
            .reserve(requested, strict_capacity_limit)
            .inspect_err(|error| {
                self.state.record_admission_rejection(error);
            })?;
        Ok(self.begin_reservation(requested, strict_capacity_limit))
    }

    /// Reserve the target's lock/WAL and bounded IO-wave capacity through
    /// candidate validation. Nested storage calls borrow the outer operation's
    /// quota; independent contexts can use the remaining project capacity.
    #[doc(hidden)]
    pub fn reserve_admission(&self, minimum: usize) -> io::Result<DescriptorReservation> {
        if let Some(inventory) = FileOpenContext::from_state(self.state.clone()).inventory {
            let inner = inventory
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if inner.active && !inventory.strict_capacity_limit && inner.remaining >= minimum {
                drop(inner);
                return Ok(DescriptorReservation {
                    inventory,
                    owns_inventory: false,
                    _thread: PhantomData,
                });
            }
        }
        self.reserve_with_capacity_limit(minimum, false)
    }

    fn begin_reservation(
        &self,
        requested: usize,
        strict_capacity_limit: bool,
    ) -> DescriptorReservation {
        let inventory = Arc::new(Inventory {
            state: self.state.clone(),
            strict_capacity_limit,
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
        let _operation = context.internal_operation()?;
        crate::durability::sync_directory_tree_with_context(
            root,
            &context,
            state.namespace_anchor.as_deref(),
        )?;
        state.root_namespace_durable.store(true, Ordering::Release);
    }
    Ok(())
}

fn require_namespace_anchor(
    state: &BudgetState,
    requested: Option<&Option<PathBuf>>,
) -> Result<(), HawDBError> {
    // Internal acquisition inherits; facade acquisition explicitly requests
    // either a canonical anchor or the unchanged full-ancestry default.
    if requested.is_some_and(|requested| *requested != state.namespace_anchor) {
        return Err(HawDBError::Storage(
            "namespace durability anchor conflicts with the open project".into(),
        ));
    }
    Ok(())
}

fn validate_namespace_anchor(root: &Path, anchor: &Path) -> io::Result<PathBuf> {
    let context = FileOpenContext::from_state(NAMESPACE_PROBES.clone());
    let anchor = context.canonicalize(anchor)?;
    if !context.metadata(&anchor)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "namespace durability anchor must be an existing directory",
        ));
    }
    // Resolve existing symlinks before any mkdir. Missing suffixes must contain
    // ordinary names; resolving a missing `..` component could escape the anchor.
    let mut existing = absolute_path(root)?;
    let mut suffix = Vec::new();
    let resolved = loop {
        match context.canonicalize(&existing) {
            Ok(mut resolved) => {
                for name in suffix.iter().rev() {
                    resolved.push(name);
                }
                break resolved;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = existing.file_name().ok_or(error)?.to_os_string();
                suffix.push(name);
                existing.pop();
            }
            Err(error) => return Err(error),
        }
    };
    if !resolved.starts_with(&anchor) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "namespace durability anchor must contain the project",
        ));
    }
    Ok(anchor)
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
    if state.configured_limit != limit {
        Err(HawDBError::FileDescriptors(
            FileDescriptorError::ConfigurationConflict {
                configured: state.configured_limit,
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

fn configured_capacity_state(
    state: Arc<BudgetState>,
    limit: usize,
    strict_capacity_limit: Option<bool>,
) -> Result<ProjectFileDescriptors, HawDBError> {
    if let Some(requested) = strict_capacity_limit
        && requested != state.strict_capacity_limit
    {
        return Err(HawDBError::FileDescriptors(
            FileDescriptorError::CapacityPolicyConflict {
                configured: state.strict_capacity_limit,
                requested,
            },
        ));
    }
    configured_state(state, limit)
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

thread_local! {
    static COMPONENT_CONTEXTS: RefCell<Vec<(PathBuf, FileOpenContext)>> = const { RefCell::new(Vec::new()) };
}

impl FileOpenContext {
    /// Charge synchronous private producer I/O to its original project domain.
    /// This is admission only; callers retain and validate native ownership.
    #[doc(hidden)]
    pub fn with_component_context<T>(
        &self,
        label: &Path,
        action: impl FnOnce() -> T,
    ) -> io::Result<T> {
        let label = absolute_path(label)?;
        struct Scope;
        impl Drop for Scope {
            fn drop(&mut self) {
                COMPONENT_CONTEXTS.with(|contexts| {
                    contexts.borrow_mut().pop();
                });
            }
        }
        COMPONENT_CONTEXTS.with(|contexts| contexts.borrow_mut().push((label, self.clone())));
        let scope = Scope;
        let output = action();
        drop(scope);
        Ok(output)
    }
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
    if let Some(context) = COMPONENT_CONTEXTS.with(|contexts| {
        contexts
            .borrow()
            .iter()
            .rev()
            .find(|(root, _)| absolute.starts_with(root))
            .map(|(_, context)| context.for_operation())
    }) {
        return Ok(context);
    }
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
    strict_capacity_limit: bool,
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
        os_limit::release(inventory.remaining);
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
    /// Borrow this domain's current operation inventory, without changing the
    /// project registry or keeping the original operation's reservation alive.
    pub(crate) fn for_operation(&self) -> Self {
        let mut context = Self::from_state(self.state.clone());
        if self.inventory.as_ref().is_some_and(|inventory| {
            !inventory.strict_capacity_limit
                && inventory
                    .inner
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .active
        }) {
            context.inventory = self.inventory.clone();
        }
        context
    }

    /// Retain project identity without retaining an operation's FD inventory.
    /// Long-lived authorities must take fresh operation contexts for later I/O.
    pub(crate) fn without_inventory(&self) -> Self {
        Self {
            state: self.state.clone(),
            inventory: None,
        }
    }

    pub(crate) fn project_root(&self) -> &Path {
        &self.state.root
    }

    pub(crate) fn namespace_anchor(&self) -> Option<&Path> {
        self.state.namespace_anchor.as_deref()
    }

    pub(crate) fn metadata(&self, path: &Path) -> io::Result<std::fs::Metadata> {
        self.temporary(|| std::fs::metadata(path))
    }

    pub fn for_path(path: &Path) -> io::Result<Self> {
        context_for_path(path)
    }

    /// Keep internal recovery, publication and cleanup independent of the
    /// foreground capacity target while retaining the original project ledger.
    /// The operation guard is synchronous; pass its context to joined workers.
    #[doc(hidden)]
    pub fn internal_operation(&self) -> io::Result<Option<DescriptorReservation>> {
        if !self.state.strict_capacity_limit {
            return Ok(None);
        }
        ProjectFileDescriptors::from_open_context(self)
            .reserve_admission(0)
            .map(Some)
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
        let operation = self.for_operation();
        // A new internal scope also applies to previously captured contexts.
        // Foreground reservations remain explicitly borrowed by their caller.
        let context = if !operation.strict_capacity_limit() {
            &operation
        } else {
            self
        };
        let strict_capacity_limit = !matches!(
            kind,
            DescriptorKind::OwnershipLock | DescriptorKind::MutableWal
        ) && context.strict_capacity_limit();
        context.acquire_with_capacity_limit(kind, strict_capacity_limit)
    }

    fn acquire_with_capacity_limit(
        &self,
        kind: DescriptorKind,
        strict_capacity_limit: bool,
    ) -> io::Result<DescriptorPermit> {
        if let Some(permit) = self.take_inventory(kind) {
            return Ok(permit);
        }
        if let Err(error) = self.state.reserve(1, strict_capacity_limit) {
            // Eviction can return a slot to this operation's reserved quota.
            if let Some(permit) = self.take_inventory(kind) {
                return Ok(permit);
            }
            self.state.record_admission_rejection(&error);
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

    fn strict_capacity_limit(&self) -> bool {
        self.state.strict_capacity_limit
            && !self.inventory.as_ref().is_some_and(|inventory| {
                !inventory.strict_capacity_limit
                    && inventory
                        .inner
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .active
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
        } else {
            os_limit::release(1);
        }
    }
}

#[cfg(test)]
#[path = "file_descriptors/tests.rs"]
mod tests;
