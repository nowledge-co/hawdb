//! Compatibility shim for the storage-owned WAL protocol.

pub(crate) mod binary {
    pub(crate) use skein_storage::wal::binary::*;
}

pub(crate) mod frame {
    pub(crate) use skein_storage::wal::frame::*;
}

pub(crate) use skein_storage::wal::{
    quarantine_corrupt_wal, reject_corrupt_wal_record, validate_wal_op_values, WalCursorEvent,
    WalEntry, WalOp, WalOpenOutcome, WalRecordCursor,
};
