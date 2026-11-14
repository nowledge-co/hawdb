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

use hawdb::{EmbeddedQueryError, HawDBError, RelationalErrorKind, RelationalErrorSurface};
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
        HawDBError::Constraint { surface, .. } => {
            relational_error(message, RelationalErrorKind::Constraint, *surface)
        }
        HawDBError::Schema { surface, .. } => {
            relational_error(message, RelationalErrorKind::Schema, *surface)
        }
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
        HawDBError::PublishOutcomeUnknown { destination, .. } => Python::attach(|py| {
            let exception = StorageError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "publish_outcome_unknown")?;
                value.setattr("destination", destination.display().to_string())
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::SearchProjectionMismatch { projection, .. } => Python::attach(|py| {
            let exception = StorageError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "search_projection_mismatch")?;
                value.setattr(
                    "projection",
                    projection.as_ref().map(|path| path.display().to_string()),
                )?;
                value.setattr("retryable", false)
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
        HawDBError::ReadBudgetExceeded(budget) => Python::attach(|py| {
            // A read ceiling is a typed budget refusal naming its open option;
            // the usage at the refusal is not tracked, so `requested` and
            // `used` are None.
            let exception = ResourceBudgetError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("resource", budget.resource.as_str())?;
                value.setattr("scope", "query")?;
                value.setattr("owner", "read result")?;
                value.setattr("memory_class", py.None())?;
                value.setattr("requested", py.None())?;
                value.setattr("used", py.None())?;
                value.setattr("budget", budget.limit)?;
                value.setattr("budget_option", budget.resource.budget_option())
            })();
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
        HawDBError::SearchProjectionRebuildRefused { reason, target, .. } => {
            Python::attach(|py| {
                // A void rebuild attempt, not a storage failure: the graph and
                // the current projection are untouched.
                let exception = ExecutionError::new_err(message);
                let value = exception.value(py);
                let attributes = (|| -> PyResult<()> {
                    value.setattr("kind", "search_projection_rebuild_refused")?;
                    value.setattr("reason", reason.as_str())?;
                    value.setattr("target", target.display().to_string())?;
                    value.setattr("retryable", false)
                })();
                attributes.err().unwrap_or(exception)
            })
        }
        HawDBError::SearchProjectionEpochAdvanceRefused {
            reason,
            source_graph_commit_epoch,
            target_graph_commit_epoch,
            ..
        } => Python::attach(|py| {
            // Nothing was published: the host falls back to an ordinary apply.
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "search_projection_epoch_advance_refused")?;
                value.setattr("reason", reason.as_str())?;
                value.setattr("source_graph_commit_epoch", *source_graph_commit_epoch)?;
                value.setattr("target_graph_commit_epoch", *target_graph_commit_epoch)?;
                value.setattr("retryable", false)
            })();
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

/// A relational constraint or schema refusal keeps the type its surface has
/// always raised, `StorageError` in autocommit and `ExecutionError` inside a
/// transaction, and gains `kind` (`"constraint"` or `"schema"`) and
/// `retryable = False`, so callers can tell it apart without parsing text.
fn relational_error(
    message: String,
    kind: RelationalErrorKind,
    surface: RelationalErrorSurface,
) -> PyErr {
    Python::attach(|py| {
        let exception = match surface {
            RelationalErrorSurface::Storage => StorageError::new_err(message),
            RelationalErrorSurface::Execution => ExecutionError::new_err(message),
        };
        let value = exception.value(py);
        let attributes = value
            .setattr("kind", kind.as_str())
            .and_then(|()| value.setattr("retryable", false));
        attributes.err().unwrap_or(exception)
    })
}

/// Maps an [`EmbeddedQueryError`] onto the matching Python exception. An
/// admission refusal carries the refused resource as `code` and `retryable`.
pub fn format_embedded_error(error: &EmbeddedQueryError) -> PyErr {
    match error {
        EmbeddedQueryError::Database(inner) => format_hawdb_error(inner),
        EmbeddedQueryError::Admission(admission) => Python::attach(|py| {
            let exception = AdmissionError::new_err(error.to_string());
            let value = exception.value(py);
            let attributes = value
                .setattr("code", admission.code.as_str())
                .and_then(|()| value.setattr("retryable", admission.is_retryable()));
            attributes.err().unwrap_or(exception)
        }),
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
            // A newer search projection manifest (#1763) is refused the same way.
            for (format, found, supported) in [
                ("append WAL", 4, 3),
                ("search projection manifest", 3, 2),
            ] {
                let error = format_hawdb_error(&HawDBError::UnsupportedStorageFormat(
                    hawdb::StorageFormatError::UnsupportedVersion {
                        format,
                        found,
                        supported,
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
            }
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
    fn relational_refusals_keep_their_surface_type_and_gain_a_kind() {
        Python::initialize();
        Python::attach(|py| {
            for (kind, name) in [
                (RelationalErrorKind::Constraint, "constraint"),
                (RelationalErrorKind::Schema, "schema"),
            ] {
                for surface in [
                    RelationalErrorSurface::Storage,
                    RelationalErrorSurface::Execution,
                ] {
                    let error = HawDBError::relational(kind, surface, format!("{kind}: detail"));
                    let exception = format_hawdb_error(&error);
                    let value = exception.value(py);
                    let (storage, prefix) = match surface {
                        RelationalErrorSurface::Storage => (true, "storage error"),
                        RelationalErrorSurface::Execution => (false, "execution error"),
                    };
                    assert_eq!(exception.is_instance_of::<StorageError>(py), storage);
                    assert_eq!(exception.is_instance_of::<ExecutionError>(py), !storage);
                    // Not the engine's IntegrityError, which reports damaged storage.
                    assert!(!exception.is_instance_of::<IntegrityError>(py));
                    assert_eq!(
                        value.str().unwrap().to_str().unwrap(),
                        format!("{prefix}: {kind}: detail")
                    );
                    assert_eq!(
                        value.getattr("kind").unwrap().extract::<String>().unwrap(),
                        name
                    );
                    assert!(!value
                        .getattr("retryable")
                        .unwrap()
                        .extract::<bool>()
                        .unwrap());
                }
            }
        });
    }

    #[test]
    fn admission_refusal_carries_its_code_and_retryability() {
        Python::initialize();
        Python::attach(|py| {
            for (code, retryable) in [
                (hawdb::RuntimeAdmissionCode::CpuSaturated, true),
                (hawdb::RuntimeAdmissionCode::MemorySaturated, false),
            ] {
                let exception = format_embedded_error(&EmbeddedQueryError::Admission(
                    hawdb::RuntimeAdmissionError {
                        code,
                        requested: 2,
                        available: 1,
                        retryable,
                    },
                ));
                assert!(exception.is_instance_of::<AdmissionError>(py));
                let value = exception.value(py);
                assert_eq!(
                    value.getattr("code").unwrap().extract::<String>().unwrap(),
                    code.as_str()
                );
                assert_eq!(
                    value
                        .getattr("retryable")
                        .unwrap()
                        .extract::<bool>()
                        .unwrap(),
                    retryable
                );
                assert!(value
                    .str()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .contains(code.as_str()));
            }
        });
    }

    #[test]
    fn read_ceilings_are_typed_budget_errors_naming_their_option() {
        Python::initialize();
        Python::attach(|py| {
            for (resource, name, option) in [
                (
                    hawdb::ReadBudgetResource::PayloadBytes,
                    "result_payload_bytes",
                    "max_read_result_payload_bytes",
                ),
                (
                    hawdb::ReadBudgetResource::Rows,
                    "result_rows",
                    "max_read_result_rows",
                ),
            ] {
                let exception = format_hawdb_error(&HawDBError::read_budget_exceeded(
                    resource,
                    7,
                    "relational SQL output exceeds the ceiling",
                ));
                assert!(exception.is_instance_of::<ResourceBudgetError>(py));
                assert!(exception.is_instance_of::<ExecutionError>(py));
                let value = exception.value(py);
                assert_eq!(
                    value.str().unwrap().to_str().unwrap(),
                    "execution error: relational SQL output exceeds the ceiling"
                );
                let text = |field: &str| value.getattr(field).unwrap().extract::<String>().unwrap();
                assert_eq!(text("resource"), name);
                assert_eq!(text("budget_option"), option);
                assert_eq!(text("scope"), "query");
                assert_eq!(
                    value.getattr("budget").unwrap().extract::<u64>().unwrap(),
                    7
                );
                for field in ["requested", "used", "memory_class"] {
                    assert!(value.getattr(field).unwrap().is_none(), "{field}");
                }
            }
        });
    }

    #[test]
    fn search_projection_mismatch_keeps_storage_type_and_is_not_retryable() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::SearchProjectionMismatch {
                projection: Some("/data/search".into()),
                message: "resident search snapshot does not match".into(),
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<StorageError>(py));
            assert!(!exception.is_instance_of::<IntegrityError>(py));
            let value = exception.value(py);
            assert_eq!(
                value.str().unwrap().to_str().unwrap(),
                "storage error: resident search snapshot does not match"
            );
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            assert_eq!(text("kind"), "search_projection_mismatch");
            assert_eq!(text("projection"), "/data/search");
            assert!(!value
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

    #[test]
    fn search_projection_rebuild_refusal_is_an_execution_error_with_its_reason() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::SearchProjectionRebuildRefused {
                reason: hawdb::SearchProjectionRebuildRefusal::TargetExists,
                target: "/data/search-rebuilt".into(),
                message: "target already exists".into(),
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            assert_eq!(
                value.str().unwrap().to_str().unwrap(),
                "search projection rebuild refused (target_exists): target already exists"
            );
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            assert_eq!(text("kind"), "search_projection_rebuild_refused");
            assert_eq!(text("reason"), "target_exists");
            assert_eq!(text("target"), "/data/search-rebuilt");
        });
    }

    #[test]
    fn search_projection_epoch_advance_refusal_is_an_execution_error_with_its_reason() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::SearchProjectionEpochAdvanceRefused {
                reason: hawdb::SearchProjectionEpochAdvanceRefusal::ProjectionChanged,
                source_graph_commit_epoch: Some(4),
                target_graph_commit_epoch: 9,
                message: "commit 6 changed a projected node".into(),
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            assert_eq!(
                value.str().unwrap().to_str().unwrap(),
                "search projection epoch advance refused (projection_changed): commit 6 changed a projected node"
            );
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            let number = |name: &str| value.getattr(name).unwrap().extract::<u64>().unwrap();
            assert_eq!(text("kind"), "search_projection_epoch_advance_refused");
            assert_eq!(text("reason"), "projection_changed");
            assert_eq!(number("source_graph_commit_epoch"), 4);
            assert_eq!(number("target_graph_commit_epoch"), 9);
            assert!(!value
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }
}
