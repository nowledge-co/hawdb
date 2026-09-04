use std::error::Error;
use std::fmt::Debug;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RuntimeCancellationToken {
    state: Arc<RuntimeCancellationState>,
}

#[derive(Debug)]
struct RuntimeCancellationState {
    cancelled: AtomicBool,
    parent: Option<RuntimeCancellationToken>,
}

impl Default for RuntimeCancellationToken {
    fn default() -> Self {
        Self {
            state: Arc::new(RuntimeCancellationState {
                cancelled: AtomicBool::new(false),
                parent: None,
            }),
        }
    }
}

impl RuntimeCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) -> bool {
        !self.state.cancelled.swap(true, Ordering::AcqRel)
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
            || self
                .state
                .parent
                .as_ref()
                .is_some_and(RuntimeCancellationToken::is_cancelled)
    }

    pub fn child(&self) -> Self {
        Self {
            state: Arc::new(RuntimeCancellationState {
                cancelled: AtomicBool::new(false),
                parent: Some(self.clone()),
            }),
        }
    }
}

/// A drop guard for one admitted storage I/O wave.
pub trait RuntimeIoWavePermit: Debug + Send {}

impl<T: Debug + Send> RuntimeIoWavePermit for T {}

/// Execution-owned hook for acquiring the I/O capacity declared at admission.
pub trait RuntimeIoWaveController: Debug + Send + Sync {
    fn acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeIoWaveError {
    Stopped(RuntimeCancellationReason),
    ReservationExceeded {
        requested_slots: usize,
        reserved_slots: usize,
    },
}

impl Display for RuntimeIoWaveError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped(reason) => write!(formatter, "runtime I/O wave stopped: {reason}"),
            Self::ReservationExceeded {
                requested_slots,
                reserved_slots,
            } => write!(
                formatter,
                "runtime I/O wave requested {requested_slots} slots, exceeding the {reserved_slots}-slot reservation"
            ),
        }
    }
}

impl Error for RuntimeIoWaveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Stopped(reason) => Some(reason),
            Self::ReservationExceeded { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeTaskContext {
    cancellation: RuntimeCancellationToken,
    deadline: Option<Instant>,
    admitted_parallelism: NonZeroUsize,
    executor_thread_limit: Option<NonZeroUsize>,
    memory_reservation: Option<RuntimeMemoryReservation>,
    io_wave_controller: Option<Arc<dyn RuntimeIoWaveController>>,
}

/// Memory already reserved for one task by the runtime governor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMemoryReservation {
    memory_bytes: u64,
    result_bytes: u64,
}

impl RuntimeMemoryReservation {
    pub const fn new(memory_bytes: u64, result_bytes: u64) -> Self {
        Self {
            memory_bytes,
            result_bytes,
        }
    }

    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }

    pub const fn result_bytes(self) -> u64 {
        self.result_bytes
    }

    pub const fn intersect(self, other: Self) -> Self {
        Self {
            memory_bytes: if self.memory_bytes < other.memory_bytes {
                self.memory_bytes
            } else {
                other.memory_bytes
            },
            result_bytes: if self.result_bytes < other.result_bytes {
                self.result_bytes
            } else {
                other.result_bytes
            },
        }
    }
}

impl RuntimeTaskContext {
    pub fn new(cancellation: RuntimeCancellationToken, deadline: Option<Instant>) -> Self {
        Self {
            cancellation,
            deadline,
            admitted_parallelism: NonZeroUsize::MIN,
            executor_thread_limit: None,
            memory_reservation: None,
            io_wave_controller: None,
        }
    }

    pub fn without_deadline(cancellation: RuntimeCancellationToken) -> Self {
        Self::new(cancellation, None)
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self::new(
            RuntimeCancellationToken::new(),
            Instant::now().checked_add(timeout),
        )
    }

    pub fn cancellation(&self) -> &RuntimeCancellationToken {
        &self.cancellation
    }

    pub fn child(&self) -> Self {
        Self {
            cancellation: self.cancellation.child(),
            deadline: self.deadline,
            admitted_parallelism: self.admitted_parallelism,
            executor_thread_limit: self.executor_thread_limit,
            memory_reservation: self.memory_reservation,
            io_wave_controller: self.io_wave_controller.clone(),
        }
    }

    /// Carries the CPU parallelism already reserved by the runtime governor.
    ///
    /// This is an execution ceiling, not a request to create worker threads.
    /// Executors must still apply their operator memory and input-size limits.
    pub fn with_admitted_parallelism(mut self, admitted_parallelism: NonZeroUsize) -> Self {
        self.admitted_parallelism = admitted_parallelism;
        self
    }

    pub fn admitted_parallelism(&self) -> NonZeroUsize {
        self.admitted_parallelism
    }

    /// Carries the physical executor-thread ceiling derived by the runtime governor.
    ///
    /// This is distinct from [`Self::admitted_parallelism`], which is the CPU
    /// reservation for one task. An absent limit denotes an ungoverned library call.
    pub fn with_executor_thread_limit(mut self, limit: NonZeroUsize) -> Self {
        self.executor_thread_limit = Some(
            self.executor_thread_limit
                .map_or(limit, |current| current.min(limit)),
        );
        self
    }

    pub fn executor_thread_limit(&self) -> Option<NonZeroUsize> {
        self.executor_thread_limit
    }

    /// Carries the memory already reserved by the runtime governor.
    ///
    /// Governed executors must use this reservation instead of a static
    /// per-query configuration limit. An absent reservation denotes an
    /// ungoverned library call and preserves the configured fallback.
    pub fn with_memory_reservation(mut self, reservation: RuntimeMemoryReservation) -> Self {
        self.memory_reservation = Some(reservation);
        self
    }

    pub fn memory_reservation(&self) -> Option<RuntimeMemoryReservation> {
        self.memory_reservation
    }

    /// Binds the controller owned by a successful runtime admission.
    pub fn with_io_wave_controller(mut self, controller: Arc<dyn RuntimeIoWaveController>) -> Self {
        self.io_wave_controller = Some(controller);
        self
    }

    /// Acquires capacity for one storage I/O wave when this is an admitted context.
    ///
    /// Raw contexts have no controller and return `Ok(None)`. The returned
    /// permit must remain live only while physical reads are in flight.
    pub fn acquire_io_wave(
        &self,
        slots: NonZeroUsize,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        self.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        self.io_wave_controller
            .as_ref()
            .map(|controller| controller.acquire(slots, self))
            .transpose()
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn checkpoint(&self) -> Result<(), RuntimeCancellationReason> {
        if self.cancellation.is_cancelled() {
            return Err(RuntimeCancellationReason::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(RuntimeCancellationReason::DeadlineExceeded);
        }
        Ok(())
    }
}

impl Default for RuntimeTaskContext {
    fn default() -> Self {
        Self::without_deadline(RuntimeCancellationToken::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCancellationReason {
    Cancelled,
    DeadlineExceeded,
}

impl RuntimeCancellationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

impl Display for RuntimeCancellationReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Error for RuntimeCancellationReason {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug)]
    struct RecordingIoController {
        active_slots: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct RecordingIoPermit {
        active_slots: Arc<AtomicUsize>,
        slots: usize,
    }

    impl Drop for RecordingIoPermit {
        fn drop(&mut self) {
            self.active_slots.fetch_sub(self.slots, Ordering::AcqRel);
        }
    }

    impl RuntimeIoWaveController for RecordingIoController {
        fn acquire(
            &self,
            slots: NonZeroUsize,
            context: &RuntimeTaskContext,
        ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
            context.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
            self.active_slots.fetch_add(slots.get(), Ordering::AcqRel);
            Ok(Box::new(RecordingIoPermit {
                active_slots: Arc::clone(&self.active_slots),
                slots: slots.get(),
            }))
        }
    }

    #[test]
    fn cancellation_is_shared_across_context_clones() {
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        assert!(context.checkpoint().is_ok());
        assert!(token.cancel());
        assert!(!token.cancel());
        assert_eq!(
            context.checkpoint(),
            Err(RuntimeCancellationReason::Cancelled)
        );
    }

    #[test]
    fn expired_deadline_fails_at_a_cooperative_checkpoint() {
        let context = RuntimeTaskContext::new(
            RuntimeCancellationToken::new(),
            Some(Instant::now() - Duration::from_millis(1)),
        );
        assert_eq!(
            context.checkpoint(),
            Err(RuntimeCancellationReason::DeadlineExceeded)
        );
    }

    #[test]
    fn child_cancellation_is_local_and_parent_cancellation_propagates() {
        let parent_token = RuntimeCancellationToken::new();
        let parent = RuntimeTaskContext::without_deadline(parent_token.clone());
        let child = parent.child();

        assert!(child.cancellation().cancel());
        assert!(child.checkpoint().is_err());
        assert!(parent.checkpoint().is_ok());

        let sibling = parent.child();
        assert!(parent_token.cancel());
        assert!(parent.checkpoint().is_err());
        assert!(sibling.checkpoint().is_err());
    }

    #[test]
    fn child_preserves_admitted_parallelism() {
        let context = RuntimeTaskContext::default()
            .with_admitted_parallelism(NonZeroUsize::new(4).expect("test parallelism is non-zero"))
            .with_executor_thread_limit(
                NonZeroUsize::new(2).expect("test thread limit is non-zero"),
            );

        assert_eq!(context.admitted_parallelism().get(), 4);
        assert_eq!(context.child().admitted_parallelism().get(), 4);
        assert_eq!(context.child().executor_thread_limit().unwrap().get(), 2);
    }

    #[test]
    fn child_preserves_memory_reservation() {
        let reservation = RuntimeMemoryReservation::new(128, 32);
        let context = RuntimeTaskContext::default().with_memory_reservation(reservation);

        assert_eq!(context.memory_reservation(), Some(reservation));
        assert_eq!(context.child().memory_reservation(), Some(reservation));
    }

    #[test]
    fn child_preserves_io_controller_and_permit_releases_slots() {
        let active_slots = Arc::new(AtomicUsize::new(0));
        let context = RuntimeTaskContext::default().with_io_wave_controller(Arc::new(
            RecordingIoController {
                active_slots: Arc::clone(&active_slots),
            },
        ));

        let permit = context
            .child()
            .acquire_io_wave(NonZeroUsize::new(2).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(active_slots.load(Ordering::Acquire), 2);
        drop(permit);
        assert_eq!(active_slots.load(Ordering::Acquire), 0);
    }
}
