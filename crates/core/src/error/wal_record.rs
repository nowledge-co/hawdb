// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A commit whose WAL record exceeds a configured per-record limit.

use std::fmt::{Display, Formatter};

/// The per-record WAL limit a commit exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordLimit {
    /// The encoded record is larger than `max_wal_record_bytes`.
    RecordBytes,
    /// The record holds more operations than `max_wal_batch_operations`.
    BatchOperations,
}

impl WalRecordLimit {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RecordBytes => "record_bytes",
            Self::BatchOperations => "batch_operations",
        }
    }

    /// The `DatabaseConfig` option that sets this limit.
    pub const fn config_option(self) -> &'static str {
        match self {
            Self::RecordBytes => "DatabaseConfig.max_wal_record_bytes",
            Self::BatchOperations => "DatabaseConfig.max_wal_batch_operations",
        }
    }
}

/// A commit refused because its single WAL record would exceed `limit`.
///
/// The refusal happens before any byte reaches the WAL and before the commit
/// is applied: the store, its commit epoch and earlier commits are unchanged,
/// and the handle stays usable. It is a caller error about the size of one
/// transaction, not a storage failure: split the transaction (for example,
/// write a very large property value in a separate transaction, or smaller
/// batches) or raise the option named by [`WalRecordLimit::config_option`].
/// Retrying the same transaction unchanged fails the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecordLimitError {
    pub limit: WalRecordLimit,
    /// The size of the refused record, in bytes or operations.
    pub requested: u64,
    /// The configured maximum, in the same unit.
    pub maximum: u64,
}

impl Display for WalRecordLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self.limit {
            WalRecordLimit::RecordBytes => write!(
                formatter,
                "WAL record byte limit exceeded before append: the transaction's WAL record is {} bytes, max_wal_record_bytes={}; nothing was written, split the transaction or raise {}",
                self.requested,
                self.maximum,
                self.limit.config_option()
            ),
            WalRecordLimit::BatchOperations => write!(
                formatter,
                "WAL batch operation limit exceeded before append: the transaction has {} operations, max_wal_batch_operations={}; nothing was written, split the transaction or raise {}",
                self.requested,
                self.maximum,
                self.limit.config_option()
            ),
        }
    }
}

impl std::error::Error for WalRecordLimitError {}
