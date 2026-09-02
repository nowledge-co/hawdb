//! Storage-owned admission boundary for optional background maintenance.

use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackgroundWorkRequest {
    pub cpu_slots: usize,
    pub memory_bytes: u64,
    pub io_slots: usize,
}

pub trait BackgroundWorkPermit: Debug + Send {}

impl<T: Debug + Send> BackgroundWorkPermit for T {}

pub trait BackgroundWorkAdmission: Debug + Send + Sync {
    fn try_admit(
        &self,
        request: BackgroundWorkRequest,
    ) -> std::result::Result<Box<dyn BackgroundWorkPermit>, String>;
}
