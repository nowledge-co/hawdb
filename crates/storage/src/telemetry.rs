//! Storage-owned telemetry records for durable protocol outcomes.

use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalAppendTelemetry {
    pub success: bool,
    pub elapsed_micros: u64,
    pub operation_count: usize,
    pub byte_count: u64,
    pub fsync_micros: u64,
    pub generation: u64,
}

pub trait StorageTelemetrySink: Debug + Send + Sync {
    fn record_wal_append(&self, event: WalAppendTelemetry);
}
