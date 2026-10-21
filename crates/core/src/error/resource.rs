// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

/// Units of an executor resource reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceBudgetKind {
    MemoryBytes,
    SpillBytes,
    SpillRuns,
    MetadataEntries,
}

impl ResourceBudgetKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MemoryBytes => "memory_bytes",
            Self::SpillBytes => "spill_bytes",
            Self::SpillRuns => "spill_runs",
            Self::MetadataEntries => "metadata_entries",
        }
    }
}

/// The boundary that refused a reservation, rather than the caller's stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceBudgetScope {
    Account,
    Query,
    Operator,
    SharedPool,
    FileSystem,
}

impl ResourceBudgetScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Query => "query",
            Self::Operator => "operator",
            Self::SharedPool => "shared_pool",
            Self::FileSystem => "file_system",
        }
    }
}

/// A rejected resource reservation. Rejection does not charge the reservation.
///
/// `requested` is the additional charge, not the resulting total. For an
/// ownership transfer, the query-level charge is the net increase. `used` and
/// `budget` describe the checked boundary in the resource's units;
/// `used + requested` need not fit in `u64`.
///
/// `budget_option` identifies the configuration option, runtime grant, or
/// constructor argument defining the effective boundary. Raising a configured
/// maximum does not widen a smaller runtime grant. A filesystem free-space
/// reserve is a minimum to preserve, not a maximum to raise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceBudgetError {
    pub resource: ResourceBudgetKind,
    pub scope: ResourceBudgetScope,
    pub owner: String,
    pub memory_class: Option<&'static str>,
    pub requested: u64,
    pub used: u64,
    pub budget: u64,
    pub budget_option: &'static str,
    /// The existing execution diagnostic, without its outer error prefix.
    pub message: String,
}

impl Display for ResourceBudgetError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ResourceBudgetError {}

pub(super) fn find_resource_budget_error(
    error: &(dyn std::error::Error + 'static),
) -> Option<ResourceBudgetError> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(resource) = error.downcast_ref::<ResourceBudgetError>() {
            return Some(resource.clone());
        }
        // io::Error can hide its immediate boxed cause from Error::source.
        if let Some(error) = error.downcast_ref::<std::io::Error>()
            && let Some(inner) = error.get_ref()
            && let Some(resource) = find_resource_budget_error(inner)
        {
            return Some(resource);
        }
        current = error.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HawDBError;

    #[test]
    fn resource_metadata_survives_nested_io_and_storage_contexts() {
        let cause = ResourceBudgetError {
            resource: ResourceBudgetKind::MemoryBytes,
            scope: ResourceBudgetScope::Account,
            owner: "source".into(),
            memory_class: Some("external_read"),
            requested: 8,
            used: 4,
            budget: 10,
            budget_option: "source grant",
            message: "source budget refused".into(),
        };
        let expected = HawDBError::from(cause.clone());
        for inner in [
            std::io::Error::other(cause),
            std::io::Error::other(expected.clone()),
        ] {
            let nested = std::io::Error::other(std::io::Error::other(inner));
            assert_eq!(
                HawDBError::from_storage_error_with_context(nested, "read node"),
                expected
            );
        }
        assert!(matches!(
            HawDBError::from_storage_error(std::io::Error::other("ordinary I/O failure")),
            HawDBError::Storage(_)
        ));
    }
}
