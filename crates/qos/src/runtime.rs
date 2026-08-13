use crate::{IoConcurrencyBudget, RuntimeMemoryPressure, RuntimeResourceSnapshot};
use skein_core::RuntimeCancellationReason;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, RwLock};

const PER_MILLION: u64 = 1_000_000;
const DESKTOP_MEMORY_FRACTION_PER_MILLION: u32 = 750_000;
const MOBILE_MEMORY_FRACTION_PER_MILLION: u32 = 500_000;
const DESKTOP_FALLBACK_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const MOBILE_FALLBACK_MEMORY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const DESKTOP_RESULT_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
const MOBILE_RESULT_BUDGET_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeWorkPriority {
    Foreground,
    Background,
}

impl RuntimeWorkPriority {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Background => "background",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeWorkKind {
    Query,
    Mutation,
    Maintenance,
    BlockingCpu,
    Io,
    Control,
}

impl RuntimeWorkKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Mutation => "mutation",
            Self::Maintenance => "maintenance",
            Self::BlockingCpu => "blocking_cpu",
            Self::Io => "io",
            Self::Control => "control",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeWorkRequest {
    pub priority: RuntimeWorkPriority,
    pub kind: RuntimeWorkKind,
    pub cpu_slots: usize,
    pub memory_bytes: u64,
    pub io_slots: usize,
    pub result_bytes: u64,
    pub blocking: bool,
}

impl RuntimeWorkRequest {
    pub const fn new(priority: RuntimeWorkPriority, kind: RuntimeWorkKind) -> Self {
        Self {
            priority,
            kind,
            cpu_slots: 0,
            memory_bytes: 0,
            io_slots: 0,
            result_bytes: 0,
            blocking: false,
        }
    }

    pub const fn query(
        priority: RuntimeWorkPriority,
        memory_bytes: u64,
        result_bytes: u64,
    ) -> Self {
        Self {
            priority,
            kind: RuntimeWorkKind::Query,
            cpu_slots: 1,
            memory_bytes,
            io_slots: 0,
            result_bytes,
            blocking: true,
        }
    }

    pub const fn foreground_query(memory_bytes: u64, result_bytes: u64) -> Self {
        Self::query(RuntimeWorkPriority::Foreground, memory_bytes, result_bytes)
    }

    pub const fn mutation(priority: RuntimeWorkPriority, memory_bytes: u64) -> Self {
        Self {
            priority,
            kind: RuntimeWorkKind::Mutation,
            cpu_slots: 1,
            memory_bytes,
            io_slots: 0,
            result_bytes: 0,
            blocking: true,
        }
    }

    pub const fn foreground_mutation(memory_bytes: u64) -> Self {
        Self::mutation(RuntimeWorkPriority::Foreground, memory_bytes)
    }

    pub const fn background_maintenance(memory_bytes: u64) -> Self {
        Self {
            priority: RuntimeWorkPriority::Background,
            kind: RuntimeWorkKind::Maintenance,
            cpu_slots: 1,
            memory_bytes,
            io_slots: 0,
            result_bytes: 0,
            blocking: false,
        }
    }

    pub const fn blocking_cpu(priority: RuntimeWorkPriority, memory_bytes: u64) -> Self {
        Self {
            priority,
            kind: RuntimeWorkKind::BlockingCpu,
            cpu_slots: 1,
            memory_bytes,
            io_slots: 0,
            result_bytes: 0,
            blocking: true,
        }
    }

    pub const fn io(priority: RuntimeWorkPriority, io_slots: usize, memory_bytes: u64) -> Self {
        Self {
            priority,
            kind: RuntimeWorkKind::Io,
            cpu_slots: 0,
            memory_bytes,
            io_slots,
            result_bytes: 0,
            blocking: false,
        }
    }

    pub const fn with_cpu_slots(mut self, cpu_slots: usize) -> Self {
        self.cpu_slots = cpu_slots;
        self
    }

    pub const fn with_kind(mut self, kind: RuntimeWorkKind) -> Self {
        self.kind = kind;
        self
    }

    pub const fn with_memory_bytes(mut self, memory_bytes: u64) -> Self {
        self.memory_bytes = memory_bytes;
        self
    }

    pub const fn with_io_slots(mut self, io_slots: usize) -> Self {
        self.io_slots = io_slots;
        self
    }

    pub const fn with_result_bytes(mut self, result_bytes: u64) -> Self {
        self.result_bytes = result_bytes;
        self
    }

    pub const fn with_blocking(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }

    fn reserved_memory_bytes(self) -> u64 {
        self.memory_bytes.saturating_add(self.result_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeGovernorConfig {
    pub cpu_slot_limit: Option<NonZeroUsize>,
    pub foreground_task_limit: Option<NonZeroUsize>,
    pub background_task_limit: Option<NonZeroUsize>,
    pub blocking_task_limit: Option<NonZeroUsize>,
    pub memory_budget_bytes: Option<u64>,
    pub memory_fraction_per_million: u32,
    pub fallback_memory_budget_bytes: u64,
    pub result_budget_bytes: u64,
}

impl RuntimeGovernorConfig {
    pub const fn desktop_bound() -> Self {
        Self {
            cpu_slot_limit: None,
            foreground_task_limit: None,
            background_task_limit: None,
            blocking_task_limit: None,
            memory_budget_bytes: None,
            memory_fraction_per_million: DESKTOP_MEMORY_FRACTION_PER_MILLION,
            fallback_memory_budget_bytes: DESKTOP_FALLBACK_MEMORY_BUDGET_BYTES,
            result_budget_bytes: DESKTOP_RESULT_BUDGET_BYTES,
        }
    }

    pub const fn mobile_embedded() -> Self {
        Self {
            cpu_slot_limit: NonZeroUsize::new(2),
            foreground_task_limit: NonZeroUsize::new(2),
            background_task_limit: NonZeroUsize::new(1),
            blocking_task_limit: NonZeroUsize::new(1),
            memory_budget_bytes: None,
            memory_fraction_per_million: MOBILE_MEMORY_FRACTION_PER_MILLION,
            fallback_memory_budget_bytes: MOBILE_FALLBACK_MEMORY_BUDGET_BYTES,
            result_budget_bytes: MOBILE_RESULT_BUDGET_BYTES,
        }
    }
}

impl Default for RuntimeGovernorConfig {
    fn default() -> Self {
        Self::desktop_bound()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeGovernorLimits {
    pub configured_cpu_slots: NonZeroUsize,
    pub effective_cpu_slots: NonZeroUsize,
    pub foreground_task_limit: NonZeroUsize,
    pub background_task_limit: NonZeroUsize,
    pub blocking_task_limit: NonZeroUsize,
    pub foreground_io_depth: NonZeroUsize,
    pub background_io_depth: NonZeroUsize,
    pub memory_capacity_bytes: u64,
    pub memory_budget_bytes: u64,
    pub result_budget_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeAdmissionCode {
    ForegroundTaskSaturated,
    BackgroundTaskSaturated,
    BlockingTaskSaturated,
    CpuSaturated,
    MemorySaturated,
    IoSaturated,
    ResultBudgetExceeded,
    MemoryPressure,
}

impl RuntimeAdmissionCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ForegroundTaskSaturated => "foreground_task_saturated",
            Self::BackgroundTaskSaturated => "background_task_saturated",
            Self::BlockingTaskSaturated => "blocking_task_saturated",
            Self::CpuSaturated => "cpu_saturated",
            Self::MemorySaturated => "memory_saturated",
            Self::IoSaturated => "io_saturated",
            Self::ResultBudgetExceeded => "result_budget_exceeded",
            Self::MemoryPressure => "memory_pressure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeAdmissionError {
    pub code: RuntimeAdmissionCode,
    pub requested: u64,
    pub available: u64,
    pub retryable: bool,
}

impl RuntimeAdmissionError {
    pub const fn is_retryable(self) -> bool {
        self.retryable
    }
}

impl Display for RuntimeAdmissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime admission {}: requested {}, available {}",
            self.code.as_str(),
            self.requested,
            self.available
        )
    }
}

impl Error for RuntimeAdmissionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTelemetryEventKind {
    Admitted,
    AdmissionWait,
    AdmissionRejected,
    Completed,
    Cancelled,
    DeadlineExceeded,
    ResourcesAdjusted,
}

impl RuntimeTelemetryEventKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::AdmissionWait => "admission_wait",
            Self::AdmissionRejected => "admission_rejected",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::ResourcesAdjusted => "resources_adjusted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeTelemetryEvent {
    pub kind: RuntimeTelemetryEventKind,
    pub priority: Option<RuntimeWorkPriority>,
    pub work_kind: Option<RuntimeWorkKind>,
    pub admission_code: Option<RuntimeAdmissionCode>,
}

pub trait RuntimeTelemetrySink: Debug + Send + Sync {
    fn record_runtime(&self, event: RuntimeTelemetryEvent);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeGovernorSnapshot {
    pub resources: RuntimeResourceSnapshot,
    pub limits: RuntimeGovernorLimits,
    pub active_foreground_tasks: usize,
    pub active_background_tasks: usize,
    pub active_blocking_tasks: usize,
    pub active_cpu_slots: usize,
    pub active_foreground_io_slots: usize,
    pub active_background_io_slots: usize,
    pub admitted_memory_bytes: u64,
    pub admissions: u64,
    pub admission_waits: u64,
    pub admission_rejections: u64,
    pub completions: u64,
    pub cancellations: u64,
    pub deadline_exceeded: u64,
    pub pressure_adjustments: u64,
    pub overcommitted: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeGovernor {
    inner: Arc<RuntimeGovernorInner>,
}

#[derive(Debug)]
struct RuntimeGovernorInner {
    config: RuntimeGovernorConfig,
    storage_io: IoConcurrencyBudget,
    state: Mutex<RuntimeGovernorState>,
    telemetry: RwLock<Option<Arc<dyn RuntimeTelemetrySink>>>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeGovernorState {
    resources: RuntimeResourceSnapshot,
    limits: RuntimeGovernorLimits,
    active_foreground_tasks: usize,
    active_background_tasks: usize,
    active_blocking_tasks: usize,
    active_cpu_slots: usize,
    active_foreground_io_slots: usize,
    active_background_io_slots: usize,
    admitted_memory_bytes: u64,
    admissions: u64,
    admission_waits: u64,
    admission_rejections: u64,
    completions: u64,
    cancellations: u64,
    deadline_exceeded: u64,
    pressure_adjustments: u64,
}

#[derive(Debug)]
pub struct RuntimePermit {
    governor: Arc<RuntimeGovernorInner>,
    request: RuntimeWorkRequest,
    released: bool,
}

impl RuntimeGovernor {
    pub fn new(
        config: RuntimeGovernorConfig,
        resources: RuntimeResourceSnapshot,
        storage_io: IoConcurrencyBudget,
    ) -> Self {
        let limits = derive_limits(config, resources, storage_io, 0);
        Self {
            inner: Arc::new(RuntimeGovernorInner {
                config,
                storage_io,
                state: Mutex::new(RuntimeGovernorState {
                    resources,
                    limits,
                    active_foreground_tasks: 0,
                    active_background_tasks: 0,
                    active_blocking_tasks: 0,
                    active_cpu_slots: 0,
                    active_foreground_io_slots: 0,
                    active_background_io_slots: 0,
                    admitted_memory_bytes: 0,
                    admissions: 0,
                    admission_waits: 0,
                    admission_rejections: 0,
                    completions: 0,
                    cancellations: 0,
                    deadline_exceeded: 0,
                    pressure_adjustments: 0,
                }),
                telemetry: RwLock::new(None),
            }),
        }
    }

    pub fn detect(config: RuntimeGovernorConfig, storage_io: IoConcurrencyBudget) -> Self {
        Self::new(config, RuntimeResourceSnapshot::detect(), storage_io)
    }

    pub fn set_telemetry_sink(&self, telemetry: Option<Arc<dyn RuntimeTelemetrySink>>) {
        *write_lock(&self.inner.telemetry) = telemetry;
    }

    pub fn try_admit(
        &self,
        request: RuntimeWorkRequest,
    ) -> Result<RuntimePermit, RuntimeAdmissionError> {
        let result = {
            let mut state = mutex_lock(&self.inner.state);
            match admission_error(&state, request) {
                Some(error) => {
                    if !error.is_retryable() {
                        state.admission_rejections = state.admission_rejections.saturating_add(1);
                    }
                    Err(error)
                }
                None => {
                    reserve(&mut state, request);
                    state.admissions = state.admissions.saturating_add(1);
                    Ok(RuntimePermit {
                        governor: Arc::clone(&self.inner),
                        request,
                        released: false,
                    })
                }
            }
        };
        match &result {
            Ok(_) => self.inner.record(RuntimeTelemetryEvent {
                kind: RuntimeTelemetryEventKind::Admitted,
                priority: Some(request.priority),
                work_kind: Some(request.kind),
                admission_code: None,
            }),
            Err(error) if !error.is_retryable() => self.inner.record(RuntimeTelemetryEvent {
                kind: RuntimeTelemetryEventKind::AdmissionRejected,
                priority: Some(request.priority),
                work_kind: Some(request.kind),
                admission_code: Some(error.code),
            }),
            Err(_) => {}
        }
        result
    }

    pub fn record_admission_wait(&self, request: RuntimeWorkRequest, code: RuntimeAdmissionCode) {
        {
            let mut state = mutex_lock(&self.inner.state);
            state.admission_waits = state.admission_waits.saturating_add(1);
        }
        self.inner.record(RuntimeTelemetryEvent {
            kind: RuntimeTelemetryEventKind::AdmissionWait,
            priority: Some(request.priority),
            work_kind: Some(request.kind),
            admission_code: Some(code),
        });
    }

    pub fn record_cancellation(&self, reason: RuntimeCancellationReason) {
        {
            let mut state = mutex_lock(&self.inner.state);
            match reason {
                RuntimeCancellationReason::Cancelled => {
                    state.cancellations = state.cancellations.saturating_add(1);
                }
                RuntimeCancellationReason::DeadlineExceeded => {
                    state.deadline_exceeded = state.deadline_exceeded.saturating_add(1);
                }
            }
        }
        self.inner.record(RuntimeTelemetryEvent {
            kind: match reason {
                RuntimeCancellationReason::Cancelled => RuntimeTelemetryEventKind::Cancelled,
                RuntimeCancellationReason::DeadlineExceeded => {
                    RuntimeTelemetryEventKind::DeadlineExceeded
                }
            },
            priority: None,
            work_kind: None,
            admission_code: None,
        });
    }

    pub fn refresh_from_host(&self) -> bool {
        self.update_resources(RuntimeResourceSnapshot::detect())
    }

    pub fn update_resources(&self, resources: RuntimeResourceSnapshot) -> bool {
        let changed = {
            let mut state = mutex_lock(&self.inner.state);
            let limits = derive_limits(
                self.inner.config,
                resources,
                self.inner.storage_io,
                state.admitted_memory_bytes,
            );
            let changed = state.resources != resources || state.limits != limits;
            state.resources = resources;
            state.limits = limits;
            if changed {
                state.pressure_adjustments = state.pressure_adjustments.saturating_add(1);
            }
            changed
        };
        if changed {
            self.inner.record(RuntimeTelemetryEvent {
                kind: RuntimeTelemetryEventKind::ResourcesAdjusted,
                priority: None,
                work_kind: None,
                admission_code: None,
            });
        }
        changed
    }

    pub fn snapshot(&self) -> RuntimeGovernorSnapshot {
        let state = *mutex_lock(&self.inner.state);
        RuntimeGovernorSnapshot {
            resources: state.resources,
            limits: state.limits,
            active_foreground_tasks: state.active_foreground_tasks,
            active_background_tasks: state.active_background_tasks,
            active_blocking_tasks: state.active_blocking_tasks,
            active_cpu_slots: state.active_cpu_slots,
            active_foreground_io_slots: state.active_foreground_io_slots,
            active_background_io_slots: state.active_background_io_slots,
            admitted_memory_bytes: state.admitted_memory_bytes,
            admissions: state.admissions,
            admission_waits: state.admission_waits,
            admission_rejections: state.admission_rejections,
            completions: state.completions,
            cancellations: state.cancellations,
            deadline_exceeded: state.deadline_exceeded,
            pressure_adjustments: state.pressure_adjustments,
            overcommitted: is_overcommitted(&state),
        }
    }
}

impl RuntimePermit {
    pub fn request(&self) -> RuntimeWorkRequest {
        self.request
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        {
            let mut state = mutex_lock(&self.governor.state);
            release(&mut state, self.request);
            state.completions = state.completions.saturating_add(1);
        }
        self.governor.record(RuntimeTelemetryEvent {
            kind: RuntimeTelemetryEventKind::Completed,
            priority: Some(self.request.priority),
            work_kind: Some(self.request.kind),
            admission_code: None,
        });
        self.released = true;
    }
}

impl Drop for RuntimePermit {
    fn drop(&mut self) {
        self.release_inner();
    }
}

impl RuntimeGovernorInner {
    fn record(&self, event: RuntimeTelemetryEvent) {
        let telemetry = read_lock(&self.telemetry).clone();
        if let Some(telemetry) = telemetry {
            telemetry.record_runtime(event);
        }
    }
}

fn derive_limits(
    config: RuntimeGovernorConfig,
    resources: RuntimeResourceSnapshot,
    storage_io: IoConcurrencyBudget,
    admitted_memory_bytes: u64,
) -> RuntimeGovernorLimits {
    let configured_cpu_slots = config
        .cpu_slot_limit
        .unwrap_or(resources.cpu.host_parallelism);
    let effective_cpu_slots = configured_cpu_slots.min(resources.cpu.effective_parallelism);
    let foreground_task_limit = config
        .foreground_task_limit
        .unwrap_or(effective_cpu_slots)
        .min(effective_cpu_slots);
    let background_task_limit = config
        .background_task_limit
        .unwrap_or(resources.cpu.background_parallelism)
        .min(effective_cpu_slots);
    let blocking_task_limit = config
        .blocking_task_limit
        .unwrap_or(effective_cpu_slots)
        .min(effective_cpu_slots);
    RuntimeGovernorLimits {
        configured_cpu_slots,
        effective_cpu_slots,
        foreground_task_limit,
        background_task_limit,
        blocking_task_limit,
        foreground_io_depth: storage_io.foreground_depth,
        background_io_depth: storage_io.background_depth,
        memory_capacity_bytes: derived_memory_capacity(config, resources),
        memory_budget_bytes: derived_memory_budget(config, resources, admitted_memory_bytes),
        result_budget_bytes: config.result_budget_bytes,
    }
}

/// The headroom-independent maximum for the current resource snapshot:
/// explicit configuration and the limit-derived term
/// (`effective_limit_bytes` = min of cgroup `memory.max`, the kernel hard
/// limit, and `memory.high`, the throttle threshold Skein honors as its
/// policy ceiling), with the fallback when neither is sensed. A request
/// above this can never be satisfied by waiting, so admission reports it
/// non-retryable. A resource refresh may change this capacity when the
/// sensed host or cgroup policy ceiling changes; explicit configuration
/// remains an upper bound.
fn derived_memory_capacity(
    config: RuntimeGovernorConfig,
    resources: RuntimeResourceSnapshot,
) -> u64 {
    let fraction = u64::from(config.memory_fraction_per_million.min(PER_MILLION as u32));
    let total_budget = resources
        .memory
        .effective_limit_bytes
        .map(|bytes| scale_memory(bytes, fraction));
    [
        config.memory_budget_bytes,
        total_budget,
        total_budget
            .is_none()
            .then_some(config.fallback_memory_budget_bytes),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or_default()
}

/// The current dynamic budget: capacity further bounded by sensed
/// availability. A request above this but within capacity is a transient
/// shortage, so admission reports it retryable and waiters ride the
/// resource refresh.
fn derived_memory_budget(
    config: RuntimeGovernorConfig,
    resources: RuntimeResourceSnapshot,
    admitted_memory_bytes: u64,
) -> u64 {
    let fraction = u64::from(config.memory_fraction_per_million.min(PER_MILLION as u32));
    let capacity = derived_memory_capacity(config, resources);
    resources
        .memory
        .effective_available_bytes
        .map(|bytes| {
            scale_memory(bytes, fraction)
                .saturating_add(admitted_memory_bytes)
                .min(capacity)
        })
        .unwrap_or(capacity)
}

fn scale_memory(bytes: u64, fraction_per_million: u64) -> u64 {
    (u128::from(bytes) * u128::from(fraction_per_million) / u128::from(PER_MILLION))
        .min(u128::from(u64::MAX)) as u64
}

fn admission_error(
    state: &RuntimeGovernorState,
    request: RuntimeWorkRequest,
) -> Option<RuntimeAdmissionError> {
    if request.result_bytes > state.limits.result_budget_bytes {
        return Some(admission_error_value(
            RuntimeAdmissionCode::ResultBudgetExceeded,
            request.result_bytes,
            state.limits.result_budget_bytes,
            false,
        ));
    }
    // Statically unsatisfiable requests terminate non-retryably before any
    // dynamic saturation check: a request over the stable capacity must
    // never read as a transient shortage just because a task slot, CPU, or
    // I/O depth happened to be busy at submission time.
    let cpu_limit = state.limits.effective_cpu_slots.get();
    if request.cpu_slots > cpu_limit {
        return Some(admission_error_value(
            RuntimeAdmissionCode::CpuSaturated,
            request.cpu_slots as u64,
            cpu_limit as u64,
            false,
        ));
    }
    let reserved_memory = request.reserved_memory_bytes();
    if reserved_memory > state.limits.memory_capacity_bytes {
        return Some(admission_error_value(
            RuntimeAdmissionCode::MemorySaturated,
            reserved_memory,
            state.limits.memory_capacity_bytes,
            false,
        ));
    }
    let static_io_limit = match request.priority {
        RuntimeWorkPriority::Foreground => state.limits.foreground_io_depth.get(),
        RuntimeWorkPriority::Background => state.limits.background_io_depth.get(),
    };
    if request.io_slots > static_io_limit {
        return Some(admission_error_value(
            RuntimeAdmissionCode::IoSaturated,
            request.io_slots as u64,
            static_io_limit as u64,
            false,
        ));
    }
    if request.priority == RuntimeWorkPriority::Background
        && state.resources.memory.pressure == RuntimeMemoryPressure::Critical
    {
        return Some(admission_error_value(
            RuntimeAdmissionCode::MemoryPressure,
            1,
            0,
            true,
        ));
    }
    let (active_tasks, task_limit, task_code) = match request.priority {
        RuntimeWorkPriority::Foreground => (
            state.active_foreground_tasks,
            state.limits.foreground_task_limit.get(),
            RuntimeAdmissionCode::ForegroundTaskSaturated,
        ),
        RuntimeWorkPriority::Background => (
            state.active_background_tasks,
            state.limits.background_task_limit.get(),
            RuntimeAdmissionCode::BackgroundTaskSaturated,
        ),
    };
    if active_tasks >= task_limit {
        return Some(admission_error_value(
            task_code,
            1,
            task_limit.saturating_sub(active_tasks) as u64,
            true,
        ));
    }
    if request.blocking && state.active_blocking_tasks >= state.limits.blocking_task_limit.get() {
        return Some(admission_error_value(
            RuntimeAdmissionCode::BlockingTaskSaturated,
            1,
            state
                .limits
                .blocking_task_limit
                .get()
                .saturating_sub(state.active_blocking_tasks) as u64,
            true,
        ));
    }
    let available_cpu = cpu_limit.saturating_sub(state.active_cpu_slots);
    if request.cpu_slots > available_cpu {
        return Some(admission_error_value(
            RuntimeAdmissionCode::CpuSaturated,
            request.cpu_slots as u64,
            available_cpu as u64,
            true,
        ));
    }
    let available_memory = state
        .limits
        .memory_budget_bytes
        .saturating_sub(state.admitted_memory_bytes);
    if reserved_memory > available_memory {
        return Some(admission_error_value(
            RuntimeAdmissionCode::MemorySaturated,
            reserved_memory,
            available_memory,
            true,
        ));
    }
    let (active_io, io_limit) = match request.priority {
        RuntimeWorkPriority::Foreground => (
            state.active_foreground_io_slots,
            state.limits.foreground_io_depth.get(),
        ),
        RuntimeWorkPriority::Background => (
            state.active_background_io_slots,
            state.limits.background_io_depth.get(),
        ),
    };
    let available_io = io_limit.saturating_sub(active_io);
    (request.io_slots > available_io).then(|| {
        admission_error_value(
            RuntimeAdmissionCode::IoSaturated,
            request.io_slots as u64,
            available_io as u64,
            true,
        )
    })
}

const fn admission_error_value(
    code: RuntimeAdmissionCode,
    requested: u64,
    available: u64,
    retryable: bool,
) -> RuntimeAdmissionError {
    RuntimeAdmissionError {
        code,
        requested,
        available,
        retryable,
    }
}

fn reserve(state: &mut RuntimeGovernorState, request: RuntimeWorkRequest) {
    match request.priority {
        RuntimeWorkPriority::Foreground => {
            state.active_foreground_tasks = state.active_foreground_tasks.saturating_add(1);
            state.active_foreground_io_slots = state
                .active_foreground_io_slots
                .saturating_add(request.io_slots);
        }
        RuntimeWorkPriority::Background => {
            state.active_background_tasks = state.active_background_tasks.saturating_add(1);
            state.active_background_io_slots = state
                .active_background_io_slots
                .saturating_add(request.io_slots);
        }
    }
    if request.blocking {
        state.active_blocking_tasks = state.active_blocking_tasks.saturating_add(1);
    }
    state.active_cpu_slots = state.active_cpu_slots.saturating_add(request.cpu_slots);
    state.admitted_memory_bytes = state
        .admitted_memory_bytes
        .saturating_add(request.reserved_memory_bytes());
}

fn release(state: &mut RuntimeGovernorState, request: RuntimeWorkRequest) {
    match request.priority {
        RuntimeWorkPriority::Foreground => {
            state.active_foreground_tasks = state.active_foreground_tasks.saturating_sub(1);
            state.active_foreground_io_slots = state
                .active_foreground_io_slots
                .saturating_sub(request.io_slots);
        }
        RuntimeWorkPriority::Background => {
            state.active_background_tasks = state.active_background_tasks.saturating_sub(1);
            state.active_background_io_slots = state
                .active_background_io_slots
                .saturating_sub(request.io_slots);
        }
    }
    if request.blocking {
        state.active_blocking_tasks = state.active_blocking_tasks.saturating_sub(1);
    }
    state.active_cpu_slots = state.active_cpu_slots.saturating_sub(request.cpu_slots);
    state.admitted_memory_bytes = state
        .admitted_memory_bytes
        .saturating_sub(request.reserved_memory_bytes());
}

fn is_overcommitted(state: &RuntimeGovernorState) -> bool {
    state.active_foreground_tasks > state.limits.foreground_task_limit.get()
        || state.active_background_tasks > state.limits.background_task_limit.get()
        || state.active_blocking_tasks > state.limits.blocking_task_limit.get()
        || state.active_cpu_slots > state.limits.effective_cpu_slots.get()
        || state.active_foreground_io_slots > state.limits.foreground_io_depth.get()
        || state.active_background_io_slots > state.limits.background_io_depth.get()
        || state.admitted_memory_bytes > state.limits.memory_budget_bytes
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RuntimeMemorySnapshot, RuntimeResourceBudget};
    use std::sync::Mutex;

    fn resources(cpu: usize, available_memory: u64) -> RuntimeResourceSnapshot {
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(cpu).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(8 * 1024 * 1024 * 1024),
                Some(available_memory),
                None,
                None,
                None,
            ),
        )
    }

    fn governor(cpu: usize, available_memory: u64) -> RuntimeGovernor {
        RuntimeGovernor::new(
            RuntimeGovernorConfig::desktop_bound(),
            resources(cpu, available_memory),
            IoConcurrencyBudget::new(4, 1),
        )
    }

    #[test]
    fn admission_is_bounded_across_cpu_memory_and_io() {
        let governor = governor(2, 4 * 1024 * 1024 * 1024);
        let first = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                128 * 1024 * 1024,
            ))
            .unwrap();
        let second = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                128 * 1024 * 1024,
            ))
            .unwrap();
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::ForegroundTaskSaturated);
        assert!(error.is_retryable());
        drop(first);
        drop(second);

        let io = governor
            .try_admit(RuntimeWorkRequest::io(
                RuntimeWorkPriority::Background,
                1,
                0,
            ))
            .unwrap();
        let error = governor
            .try_admit(RuntimeWorkRequest::io(
                RuntimeWorkPriority::Background,
                1,
                0,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::BackgroundTaskSaturated);
        drop(io);
    }

    #[test]
    fn oversized_result_is_rejected_without_waiting() {
        let governor = governor(4, 1024 * 1024 * 1024);
        let error = governor
            .try_admit(RuntimeWorkRequest::foreground_query(
                0,
                DESKTOP_RESULT_BUDGET_BYTES + 1,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::ResultBudgetExceeded);
        assert!(!error.is_retryable());
    }

    #[test]
    fn adaptive_update_shrinks_new_admission_without_revoking_permits() {
        let governor = governor(4, 1024 * 1024 * 1024);
        let permit = governor
            .try_admit(
                RuntimeWorkRequest::blocking_cpu(
                    RuntimeWorkPriority::Foreground,
                    128 * 1024 * 1024,
                )
                .with_cpu_slots(2),
            )
            .unwrap();
        assert!(governor.update_resources(resources(1, 64 * 1024 * 1024)));
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.limits.effective_cpu_slots.get(), 1);
        assert!(snapshot.overcommitted);
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1,
            ))
            .unwrap_err();
        assert!(error.is_retryable());
        drop(permit);
        assert!(!governor.snapshot().overcommitted);
    }

    /// A cgroup policy change may lower capacity below an active reservation.
    /// The governor cannot revoke memory already handed to the operation, so
    /// it reports overcommit and blocks new work until release closes the gap.
    #[test]
    fn capacity_shrink_preserves_active_permits_and_blocks_new_admission() {
        let snapshot = |limit: u64| {
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(
                    Some(8 * 1024 * 1024 * 1024),
                    Some(6 * 1024 * 1024 * 1024),
                    Some(limit),
                    None,
                    Some(0),
                ),
            )
        };
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::desktop_bound(),
            snapshot(512 * 1024 * 1024),
            IoConcurrencyBudget::new(4, 1),
        );
        let permit = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                256 * 1024 * 1024,
            ))
            .unwrap();

        assert!(governor.update_resources(snapshot(128 * 1024 * 1024)));
        let shrunk = governor.snapshot();
        assert_eq!(shrunk.limits.memory_capacity_bytes, 96 * 1024 * 1024);
        assert_eq!(shrunk.admitted_memory_bytes, 256 * 1024 * 1024);
        assert!(shrunk.overcommitted);

        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(error.is_retryable());

        drop(permit);
        let recovered = governor.snapshot();
        assert_eq!(recovered.admitted_memory_bytes, 0);
        assert!(!recovered.overcommitted);
    }

    #[test]
    fn zero_capacity_rejects_nonzero_memory_without_waiting() {
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::desktop_bound(),
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(
                    Some(8 * 1024 * 1024 * 1024),
                    Some(6 * 1024 * 1024 * 1024),
                    Some(0),
                    None,
                    Some(u64::MAX),
                ),
            ),
            IoConcurrencyBudget::new(4, 1),
        );
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.limits.memory_capacity_bytes, 0);
        assert_eq!(snapshot.limits.memory_budget_bytes, 0);

        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(!error.is_retryable());
    }

    #[derive(Debug, Default)]
    struct RecordingTelemetry {
        events: Mutex<Vec<RuntimeTelemetryEvent>>,
    }

    impl RuntimeTelemetrySink for RecordingTelemetry {
        fn record_runtime(&self, event: RuntimeTelemetryEvent) {
            mutex_lock(&self.events).push(event);
        }
    }

    #[test]
    fn typed_telemetry_contains_only_stable_dimensions() {
        let governor = governor(1, 1024 * 1024 * 1024);
        let telemetry = Arc::new(RecordingTelemetry::default());
        governor.set_telemetry_sink(Some(telemetry.clone()));
        let permit = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                0,
            ))
            .unwrap();
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                0,
            ))
            .unwrap_err();
        governor.record_admission_wait(permit.request(), error.code);
        governor.record_cancellation(RuntimeCancellationReason::Cancelled);
        drop(permit);

        let events = mutex_lock(&telemetry.events);
        assert_eq!(events[0].kind, RuntimeTelemetryEventKind::Admitted);
        assert!(events
            .iter()
            .any(|event| event.kind == RuntimeTelemetryEventKind::AdmissionWait));
        assert!(events
            .iter()
            .any(|event| event.kind == RuntimeTelemetryEventKind::Cancelled));
        assert!(events
            .iter()
            .any(|event| event.kind == RuntimeTelemetryEventKind::Completed));
    }

    /// A saturated cgroup must reject new admissions even when the hard
    /// limit is large: with `memory.current == memory.max` the governor
    /// cannot distinguish anonymous saturation from reclaimable file
    /// pages, so admitting anything risks a kernel OOM kill — the failure
    /// the admission gate exists to prevent. Sensed zero headroom is
    /// authoritative; environments whose own artifacts consume the
    /// instance's memory must provision more, not weaken admission.
    ///
    /// Saturation is a transient state: the request fits the container's
    /// stable capacity, so the rejection reports retryable and waiting
    /// admissions ride the resource refresh.
    #[test]
    fn saturated_cgroup_rejects_admissions_despite_a_large_hard_limit() {
        let limit = 512 * 1024 * 1024;
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::desktop_bound(),
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(
                    Some(8 * 1024 * 1024 * 1024),
                    Some(6 * 1024 * 1024 * 1024),
                    Some(limit),
                    None,
                    Some(limit),
                ),
            ),
            IoConcurrencyBudget::new(4, 1),
        );
        assert_eq!(governor.snapshot().limits.memory_budget_bytes, 0);
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                48 * 1024 * 1024,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(error.is_retryable());
        assert_eq!(
            governor.snapshot().limits.memory_capacity_bytes,
            384 * 1024 * 1024
        );
    }

    /// A request above the stable capacity can never be satisfied by
    /// waiting, so it stays non-retryable regardless of current headroom.
    #[test]
    fn over_capacity_requests_stay_non_retryable() {
        let limit = 512 * 1024 * 1024;
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::desktop_bound(),
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(
                    Some(8 * 1024 * 1024 * 1024),
                    Some(6 * 1024 * 1024 * 1024),
                    Some(limit),
                    None,
                    Some(0),
                ),
            ),
            IoConcurrencyBudget::new(4, 1),
        );
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                600 * 1024 * 1024,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(!error.is_retryable());
    }
    /// A statically unsatisfiable request must terminate non-retryably even
    /// when a dynamic gate (here: the foreground task slots) is saturated at
    /// submission time — otherwise a waiter polls forever for an admission
    /// that can never come.
    #[test]
    fn over_capacity_while_task_saturated_stays_non_retryable() {
        let governor = governor(2, 4 * 1024 * 1024 * 1024);
        let first = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1024,
            ))
            .unwrap();
        let second = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1024,
            ))
            .unwrap();
        let capacity = governor.snapshot().limits.memory_capacity_bytes;
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                capacity + 1,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(!error.is_retryable());
        drop(first);
        drop(second);
    }

    /// Critical memory pressure gates background work retryably, but an
    /// over-capacity background request is still a terminal rejection.
    #[test]
    fn over_capacity_under_critical_pressure_stays_non_retryable() {
        let limit = 512 * 1024 * 1024;
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::desktop_bound(),
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(
                    Some(8 * 1024 * 1024 * 1024),
                    Some(6 * 1024 * 1024 * 1024),
                    Some(limit),
                    Some(limit / 2),
                    Some(limit),
                ),
            ),
            IoConcurrencyBudget::new(4, 1),
        );
        assert_eq!(
            governor.snapshot().resources.memory.pressure,
            RuntimeMemoryPressure::Critical
        );
        let capacity = governor.snapshot().limits.memory_capacity_bytes;
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Background,
                capacity + 1,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(!error.is_retryable());
    }
}
