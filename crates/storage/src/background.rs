//! Storage-owned admission boundary for optional background maintenance.

use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackgroundWorkRequest {
    pub cpu_slots: usize,
    pub memory_bytes: u64,
    pub io_slots: usize,
}

/// An explicitly implemented resource lease for admitted background work.
/// Implementations must retain their reservation until this value is dropped.
/// Providers may implement this trait without depending on a particular governor.
///
/// Arbitrary values are not permits:
///
/// ```compile_fail
/// use skein_storage::BackgroundWorkPermit;
/// let permit: Box<dyn BackgroundWorkPermit> = Box::new(());
/// ```
pub trait BackgroundWorkPermit: Debug + Send {}

pub trait BackgroundWorkAdmission: Debug + Send + Sync {
    fn try_admit(
        &self,
        request: BackgroundWorkRequest,
    ) -> std::result::Result<Box<dyn BackgroundWorkPermit>, String>;
}
