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

use hawdb::{EmbeddedQueryError, HawDBError};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    hawdb.exceptions,
    Error,
    PyException,
    "Base error raised by HawDB."
);
create_exception!(
    hawdb.exceptions,
    ParseError,
    Error,
    "Statement failed to parse."
);
create_exception!(
    hawdb.exceptions,
    SemanticError,
    Error,
    "Statement failed semantic checks."
);
create_exception!(
    hawdb.exceptions,
    StorageError,
    Error,
    "Storage layer failure."
);
create_exception!(
    hawdb.exceptions,
    IntegrityError,
    StorageError,
    "Storage integrity violation."
);
create_exception!(
    hawdb.exceptions,
    DescriptorError,
    StorageError,
    "File descriptor budget or storage descriptor failure."
);
create_exception!(
    hawdb.exceptions,
    ExecutionError,
    Error,
    "Statement failed during execution."
);
create_exception!(
    hawdb.exceptions,
    ResourceBudgetError,
    ExecutionError,
    "Execution resource reservation refused; attributes identify the effective budget."
);
create_exception!(
    hawdb.exceptions,
    ConflictError,
    Error,
    "Transaction conflict or exhausted commit sequence."
);
create_exception!(
    hawdb.exceptions,
    CapabilityError,
    Error,
    "A required runtime capability is unavailable."
);
create_exception!(
    hawdb.exceptions,
    BranchError,
    Error,
    "Branch command is unsupported or the branch is busy."
);
create_exception!(
    hawdb.exceptions,
    AdmissionError,
    Error,
    "Query admission was rejected by runtime resource governance."
);
create_exception!(
    hawdb.exceptions,
    TaskStoppedError,
    Error,
    "The runtime task was stopped before producing a result."
);
create_exception!(
    hawdb.exceptions,
    RetainedError,
    Error,
    "Experimental retained-interface failure; kind and retryable preserve the outcome."
);
create_exception!(
    hawdb.exceptions,
    BackpressureError,
    RetainedError,
    "Release held views and retry; the source has not advanced."
);

/// Maps a [`HawDBError`] onto the matching Python exception.
pub fn format_hawdb_error(error: &HawDBError) -> PyErr {
    let message = error.to_string();
    match error {
        HawDBError::Parse(_) => ParseError::new_err(message),
        HawDBError::Semantic(_) => SemanticError::new_err(message),
        HawDBError::Storage(_) => StorageError::new_err(message),
        HawDBError::WalRecoveryIo {
            operation,
            kind,
            os_code,
            ..
        } => Python::attach(|py| {
            let exception = StorageError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "wal_recovery_io")?;
                value.setattr("operation", *operation)?;
                value.setattr("io_kind", format!("{kind:?}"))?;
                value.setattr("os_code", *os_code)?;
                value.setattr("retryable", true)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::UnsupportedStorageFormat(_) => Python::attach(|py| {
            let exception = StorageError::new_err(message);
            let attributes = exception
                .value(py)
                .setattr("kind", "unsupported_storage_format");
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::FileDescriptors(_) => DescriptorError::new_err(message),
        HawDBError::StorageIntegrity(_) => IntegrityError::new_err(message),
        HawDBError::ResourceBudgetExceeded(budget) => Python::attach(|py| {
            let exception = ResourceBudgetError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("resource", budget.resource.as_str())?;
                value.setattr("scope", budget.scope.as_str())?;
                value.setattr("owner", &budget.owner)?;
                value.setattr("memory_class", budget.memory_class)?;
                value.setattr("requested", budget.requested)?;
                value.setattr("used", budget.used)?;
                value.setattr("budget", budget.budget)?;
                value.setattr("budget_option", budget.budget_option)
            })();
            // Preserve installation failures instead of returning an incomplete resource error.
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::ProjectionBusy => Python::attach(|py| {
            let exception = ExecutionError::new_err(message);
            let attributes = exception
                .value(py)
                .setattr("kind", "projection_busy")
                .and_then(|()| exception.value(py).setattr("retryable", true));
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::Execution(_) => Python::attach(|py| {
            let exception = ExecutionError::new_err(message);
            let attributes = exception
                .value(py)
                .setattr("kind", "execution")
                .and_then(|()| exception.value(py).setattr("retryable", false));
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::TransactionReadDependencyRefused { .. }
        | HawDBError::BranchEvidenceBudgetExceeded { .. }
        | HawDBError::ReadBudgetExceeded(_)
        | HawDBError::GraphExpansionCandidateLimitExceeded { .. }
        | HawDBError::GraphExpansionPayloadLimitExceeded { .. } => ExecutionError::new_err(message),
        HawDBError::TransactionConflict { .. }
        | HawDBError::TransactionRetryRequired(_)
        | HawDBError::AppendSequenceExhausted { .. } => ConflictError::new_err(message),
        HawDBError::CapabilityUnavailable { .. } => CapabilityError::new_err(message),
        HawDBError::BranchCommandUnsupported { .. }
        | HawDBError::BranchBusy { .. }
        | HawDBError::BranchReclamationBlocked { .. } => BranchError::new_err(message),
    }
}

/// Maps an [`EmbeddedQueryError`] onto the matching Python exception.
pub fn format_embedded_error(error: &EmbeddedQueryError) -> PyErr {
    match error {
        EmbeddedQueryError::Database(inner) => format_hawdb_error(inner),
        EmbeddedQueryError::Admission(_) => AdmissionError::new_err(error.to_string()),
        EmbeddedQueryError::Stopped(_) => TaskStoppedError::new_err(error.to_string()),
    }
}

/// Adds every exception type to the `hawdb.exceptions` module.
pub fn register_exceptions(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("Error", py.get_type::<Error>())?;
    module.add("ParseError", py.get_type::<ParseError>())?;
    module.add("SemanticError", py.get_type::<SemanticError>())?;
    module.add("StorageError", py.get_type::<StorageError>())?;
    module.add("IntegrityError", py.get_type::<IntegrityError>())?;
    module.add("DescriptorError", py.get_type::<DescriptorError>())?;
    module.add("ExecutionError", py.get_type::<ExecutionError>())?;
    module.add("ResourceBudgetError", py.get_type::<ResourceBudgetError>())?;
    module.add("ConflictError", py.get_type::<ConflictError>())?;
    module.add("CapabilityError", py.get_type::<CapabilityError>())?;
    module.add("BranchError", py.get_type::<BranchError>())?;
    module.add("AdmissionError", py.get_type::<AdmissionError>())?;
    module.add("TaskStoppedError", py.get_type::<TaskStoppedError>())?;
    module.add("RetainedError", py.get_type::<RetainedError>())?;
    module.add("BackpressureError", py.get_type::<BackpressureError>())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::{ResourceBudgetKind, ResourceBudgetScope};

    #[test]
    fn unsupported_storage_format_is_not_a_python_integrity_error() {
        Python::initialize();
        Python::attach(|py| {
            let error = format_hawdb_error(&HawDBError::UnsupportedStorageFormat(
                hawdb::StorageFormatError::UnsupportedVersion {
                    format: "append WAL",
                    found: 4,
                    supported: 3,
                },
            ));
            assert!(error.is_instance_of::<StorageError>(py));
            assert!(!error.is_instance_of::<IntegrityError>(py));
            assert_eq!(
                error
                    .value(py)
                    .getattr("kind")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "unsupported_storage_format"
            );
        });
    }

    #[test]
    fn embedded_resource_errors_keep_python_type_message_and_fields() {
        Python::initialize();
        Python::attach(|py| {
            for (resource, scope, class) in [
                (
                    ResourceBudgetKind::MemoryBytes,
                    ResourceBudgetScope::Query,
                    Some("blocking_state"),
                ),
                (
                    ResourceBudgetKind::SpillBytes,
                    ResourceBudgetScope::SharedPool,
                    None,
                ),
                (
                    ResourceBudgetKind::SpillRuns,
                    ResourceBudgetScope::Operator,
                    None,
                ),
            ] {
                let source = HawDBError::from(hawdb::ResourceBudgetError {
                    resource,
                    scope,
                    owner: "sort".into(),
                    memory_class: class,
                    requested: 9,
                    used: u64::MAX - 8,
                    budget: u64::MAX,
                    budget_option: "test grant",
                    message: "reservation refused".into(),
                });
                let exception = format_embedded_error(&EmbeddedQueryError::Database(source));
                assert!(exception.is_instance_of::<ResourceBudgetError>(py));
                assert!(exception.is_instance_of::<ExecutionError>(py));
                let value = exception.value(py);
                assert_eq!(
                    value.str().unwrap().to_str().unwrap(),
                    "execution error: reservation refused"
                );
                for (field, expected) in [
                    ("resource", resource.as_str()),
                    ("scope", scope.as_str()),
                    ("owner", "sort"),
                    ("budget_option", "test grant"),
                ] {
                    assert_eq!(
                        value.getattr(field).unwrap().extract::<String>().unwrap(),
                        expected
                    );
                }
                assert_eq!(
                    value
                        .getattr("memory_class")
                        .unwrap()
                        .extract::<Option<String>>()
                        .unwrap()
                        .as_deref(),
                    class
                );
                for (field, expected) in [
                    ("requested", 9),
                    ("used", u64::MAX - 8),
                    ("budget", u64::MAX),
                ] {
                    assert_eq!(
                        value.getattr(field).unwrap().extract::<u64>().unwrap(),
                        expected
                    );
                }
            }
        });
    }

    #[test]
    fn ordinary_execution_error_is_not_resource_exhaustion() {
        Python::initialize();
        Python::attach(|py| {
            let exception = format_hawdb_error(&HawDBError::Execution("consumer failed".into()));
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<ResourceBudgetError>(py));
            assert!(!exception.value(py).hasattr("budget").unwrap());
            assert!(!exception
                .value(py)
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }

    #[test]
    fn projection_busy_preserves_its_retryable_kind() {
        Python::initialize();
        Python::attach(|py| {
            let exception = format_hawdb_error(&HawDBError::ProjectionBusy);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert_eq!(
                exception
                    .value(py)
                    .getattr("kind")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "projection_busy"
            );
            assert!(exception
                .value(py)
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }
}
