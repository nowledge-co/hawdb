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
        HawDBError::SearchProjectionOwnershipMarkerCorrupt {
            projection, marker, ..
        } => Python::attach(|py| {
            // Not poisoning: reopen with the ownership reset to rewrite it.
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "search_projection_ownership_marker_corrupt")?;
                value.setattr("projection", projection.display().to_string())?;
                value.setattr("marker", marker.as_str())?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::SearchProjectionOwnershipMismatch {
            projection,
            recorded,
            requested,
        } => Python::attach(|py| {
            // A refused open: the projection and its recorded set are unchanged.
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "search_projection_ownership_mismatch")?;
                value.setattr("projection", projection.display().to_string())?;
                value.setattr("recorded", recorded.clone())?;
                value.setattr("requested", requested.clone())?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
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
        HawDBError::SearchProjectionPrefixBehindSource {
            complete_through_commit_epoch,
            source_commit_epoch,
        } => Python::attach(|py| {
            // Nothing was published. Retrying the same budgets refuses again:
            // cover the whole pending interval or re-prepare (#1924).
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "search_projection_prefix_behind_source")?;
                value.setattr(
                    "complete_through_commit_epoch",
                    *complete_through_commit_epoch,
                )?;
                value.setattr("source_commit_epoch", *source_commit_epoch)?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::SearchProjectionBatchBudgetExceeded {
            budget,
            commit_epoch,
            required_operations,
            limit,
        } => Python::attach(|py| {
            // A budget refusal, not storage damage: nothing was published
            // and the handle stays usable (#1924).
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "search_projection_batch_budget_exceeded")?;
                value.setattr("budget", budget.as_str())?;
                value.setattr("commit_epoch", *commit_epoch)?;
                value.setattr("required_operations", *required_operations)?;
                value.setattr("limit", *limit)?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::SearchProductionQualificationGraphEpochMismatch {
            expected_canonical_graph_commit_epoch,
            opened_graph_commit_epoch,
        } => Python::attach(|py| {
            // A stale qualification, not storage damage: the graph moved
            // after qualification. Qualify again and reopen (#1932).
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr(
                    "kind",
                    "search_production_qualification_graph_epoch_mismatch",
                )?;
                value.setattr(
                    "expected_canonical_graph_commit_epoch",
                    *expected_canonical_graph_commit_epoch,
                )?;
                value.setattr("opened_graph_commit_epoch", *opened_graph_commit_epoch)?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::SearchProductionQualificationRefused { blocker_codes } => {
            Python::attach(|py| {
                // The report does not qualify this projection for this
                // release; nothing is damaged. Qualify again (#1932).
                let exception = ExecutionError::new_err(message);
                let value = exception.value(py);
                let attributes = (|| -> PyResult<()> {
                    value.setattr("kind", "search_production_qualification_refused")?;
                    value.setattr("blocker_codes", blocker_codes.clone())?;
                    value.setattr("retryable", false)
                })();
                attributes.err().unwrap_or(exception)
            })
        }
        HawDBError::ReadOnly { operation } => Python::attach(|py| {
            // A caller error: the handle is read-only and nothing was written.
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "read_only")?;
                value.setattr("operation", *operation)?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::WalRecordLimitExceeded(limit) => Python::attach(|py| {
            // A caller error about one transaction's size: nothing was
            // written and the handle stays usable.
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "wal_record_limit_exceeded")?;
                value.setattr("limit", limit.limit.as_str())?;
                value.setattr("requested", limit.requested)?;
                value.setattr("maximum", limit.maximum)?;
                value.setattr("config_option", limit.limit.config_option())?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::RuntimeTaskStopped(reason) => Python::attach(|py| {
            // The caller's deadline or cancellation stopped the work before
            // it committed: nothing was written and the handle stays usable.
            // A stop is a stop whichever layer raised it.
            let exception = TaskStoppedError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "runtime_task_stopped")?;
                value.setattr("reason", reason.as_str())?;
                value.setattr("retryable", false)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::RuntimeAdmissionRefused(refusal) => Python::attach(|py| {
            // The runtime governor had no capacity for this read: nothing
            // was written, the handle stays usable, and a transient refusal
            // can be retried once capacity frees up.
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "runtime_admission_refused")?;
                value.setattr("code", refusal.code)?;
                value.setattr("requested", refusal.requested)?;
                value.setattr("available", refusal.available)?;
                value.setattr("retryable", refusal.retryable)
            })();
            attributes.err().unwrap_or(exception)
        }),
        HawDBError::RuntimeIoWaveReservationExceeded {
            requested_slots,
            reserved_slots,
        } => Python::attach(|py| {
            let exception = ExecutionError::new_err(message);
            let value = exception.value(py);
            let attributes = (|| -> PyResult<()> {
                value.setattr("kind", "runtime_io_wave_reservation_exceeded")?;
                value.setattr("requested_slots", *requested_slots)?;
                value.setattr("reserved_slots", *reserved_slots)?;
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
                (hawdb::SEARCH_PROJECTION_MANIFEST_FORMAT, 3, 2),
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

    #[test]
    fn search_projection_prefix_behind_source_is_a_typed_execution_error() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::SearchProjectionPrefixBehindSource {
                complete_through_commit_epoch: 3,
                source_commit_epoch: 7,
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            assert_eq!(
                value.str().unwrap().to_str().unwrap(),
                "execution error: search projection complete-through epoch 3 differs from source snapshot epoch 7; prepare the complete source interval before publishing its watermark"
            );
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            let number = |name: &str| value.getattr(name).unwrap().extract::<u64>().unwrap();
            assert_eq!(text("kind"), "search_projection_prefix_behind_source");
            assert_eq!(number("complete_through_commit_epoch"), 3);
            assert_eq!(number("source_commit_epoch"), 7);
            assert!(!value
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }

    #[test]
    fn search_projection_batch_budget_refusal_is_an_execution_error_not_storage() {
        Python::initialize();
        Python::attach(|py| {
            for (budget, commit_epoch, text) in [
                (
                    hawdb::SearchProjectionBatchBudget::ChangeBatch,
                    Some(5),
                    "storage error: search projection change at commit epoch 5 requires 9 operations, exceeding configured per-batch limit 4",
                ),
                (
                    hawdb::SearchProjectionBatchBudget::GraphDelta,
                    None,
                    "storage error: search projection graph delta operation count 9 exceeded configured limit 4",
                ),
                (
                    hawdb::SearchProjectionBatchBudget::ProjectionDelta,
                    None,
                    "storage error: incremental projection update operation count 9 exceeded configured limit 4",
                ),
            ] {
                let error = HawDBError::SearchProjectionBatchBudgetExceeded {
                    budget,
                    commit_epoch,
                    required_operations: 9,
                    limit: 4,
                };
                let exception = format_hawdb_error(&error);
                assert!(exception.is_instance_of::<ExecutionError>(py));
                assert!(!exception.is_instance_of::<StorageError>(py));
                let value = exception.value(py);
                assert_eq!(value.str().unwrap().to_str().unwrap(), text);
                let field = |name: &str| value.getattr(name).unwrap();
                assert_eq!(
                    field("kind").extract::<String>().unwrap(),
                    "search_projection_batch_budget_exceeded"
                );
                assert_eq!(
                    field("budget").extract::<String>().unwrap(),
                    budget.as_str()
                );
                assert_eq!(
                    field("commit_epoch").extract::<Option<u64>>().unwrap(),
                    commit_epoch
                );
                assert_eq!(field("required_operations").extract::<usize>().unwrap(), 9);
                assert_eq!(field("limit").extract::<usize>().unwrap(), 4);
                assert!(!field("retryable").extract::<bool>().unwrap());
            }
        });
    }

    #[test]
    fn stale_search_production_qualification_is_an_execution_error_not_storage() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::SearchProductionQualificationGraphEpochMismatch {
                expected_canonical_graph_commit_epoch: 3,
                opened_graph_commit_epoch: 4,
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            assert_eq!(
                value.str().unwrap().to_str().unwrap(),
                "storage error: qualified out-of-core search expected canonical graph commit epoch 3, opened graph is at 4"
            );
            let field = |name: &str| value.getattr(name).unwrap();
            assert_eq!(
                field("kind").extract::<String>().unwrap(),
                "search_production_qualification_graph_epoch_mismatch"
            );
            assert_eq!(
                field("expected_canonical_graph_commit_epoch")
                    .extract::<u64>()
                    .unwrap(),
                3
            );
            assert_eq!(
                field("opened_graph_commit_epoch").extract::<u64>().unwrap(),
                4
            );
            assert!(!field("retryable").extract::<bool>().unwrap());

            let error = HawDBError::SearchProductionQualificationRefused {
                blocker_codes: vec![
                    "dataset_too_small".to_owned(),
                    "projection_identity_mismatch".to_owned(),
                ],
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            assert_eq!(
                value.str().unwrap().to_str().unwrap(),
                "storage error: segmented lexical projection is not qualified for the current production release: dataset_too_small,projection_identity_mismatch"
            );
            let field = |name: &str| value.getattr(name).unwrap();
            assert_eq!(
                field("kind").extract::<String>().unwrap(),
                "search_production_qualification_refused"
            );
            assert_eq!(
                field("blocker_codes").extract::<Vec<String>>().unwrap(),
                ["dataset_too_small", "projection_identity_mismatch"]
            );
            assert!(!field("retryable").extract::<bool>().unwrap());
        });
    }

    #[test]
    fn search_projection_ownership_mismatch_is_an_execution_error_with_both_sets() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::SearchProjectionOwnershipMismatch {
                projection: "/data/search".into(),
                recorded: vec!["memory".into(), "entity".into()],
                requested: vec!["memory".into()],
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            let list = |name: &str| {
                value
                    .getattr(name)
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap()
            };
            assert_eq!(text("kind"), "search_projection_ownership_mismatch");
            assert_eq!(text("projection"), "/data/search");
            assert_eq!(list("recorded"), ["memory", "entity"]);
            assert_eq!(list("requested"), ["memory"]);
            assert!(!value
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }

    #[test]
    fn search_projection_ownership_marker_corruption_is_a_non_poisoning_execution_error() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::SearchProjectionOwnershipMarkerCorrupt {
                projection: "/data/search".into(),
                marker: ".externally_owned_rows_required".into(),
                message: "has no epoch".into(),
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<IntegrityError>(py));
            let value = exception.value(py);
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            assert_eq!(text("kind"), "search_projection_ownership_marker_corrupt");
            assert_eq!(text("projection"), "/data/search");
            assert_eq!(text("marker"), ".externally_owned_rows_required");
        });
    }

    #[test]
    fn wal_record_limit_is_a_non_retryable_execution_error_with_its_limit() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::WalRecordLimitExceeded(Box::new(hawdb::WalRecordLimitError {
                limit: hawdb::WalRecordLimit::RecordBytes,
                requested: 20,
                maximum: 16,
            }));
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            let number = |name: &str| value.getattr(name).unwrap().extract::<u64>().unwrap();
            assert_eq!(text("kind"), "wal_record_limit_exceeded");
            assert_eq!(text("limit"), "record_bytes");
            assert_eq!(text("config_option"), "DatabaseConfig.max_wal_record_bytes");
            assert_eq!((number("requested"), number("maximum")), (20, 16));
            assert!(!value
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }

    #[test]
    fn runtime_task_stop_is_a_task_stopped_error_with_its_reason() {
        Python::initialize();
        Python::attach(|py| {
            let error =
                HawDBError::RuntimeTaskStopped(hawdb::RuntimeCancellationReason::DeadlineExceeded);
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<TaskStoppedError>(py));
            assert!(!exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            assert_eq!(text("kind"), "runtime_task_stopped");
            assert_eq!(text("reason"), "deadline_exceeded");
        });
    }

    #[test]
    fn runtime_admission_refusal_is_a_retryable_execution_error_with_its_code() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::RuntimeAdmissionRefused(hawdb::RuntimeAdmissionRefusal {
                code: "memory_saturated",
                requested: 4096,
                available: 0,
                retryable: true,
                waited: None,
            });
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            let number = |name: &str| value.getattr(name).unwrap().extract::<u64>().unwrap();
            assert_eq!(text("kind"), "runtime_admission_refused");
            assert_eq!(text("code"), "memory_saturated");
            assert_eq!((number("requested"), number("available")), (4096, 0));
            assert!(value
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }

    #[test]
    fn io_wave_reservation_refusal_is_a_non_retryable_execution_error() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::RuntimeIoWaveReservationExceeded {
                requested_slots: 2,
                reserved_slots: 1,
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            let number = |name: &str| value.getattr(name).unwrap().extract::<usize>().unwrap();
            assert_eq!(text("kind"), "runtime_io_wave_reservation_exceeded");
            assert_eq!(
                (number("requested_slots"), number("reserved_slots")),
                (2, 1)
            );
            assert!(!value
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }

    #[test]
    fn read_only_is_a_non_retryable_execution_error_with_its_operation() {
        Python::initialize();
        Python::attach(|py| {
            let error = HawDBError::ReadOnly {
                operation: "mark_search_projection_full_reindex_needed",
            };
            let exception = format_hawdb_error(&error);
            assert!(exception.is_instance_of::<ExecutionError>(py));
            assert!(!exception.is_instance_of::<StorageError>(py));
            let value = exception.value(py);
            let text = |name: &str| value.getattr(name).unwrap().extract::<String>().unwrap();
            assert_eq!(text("kind"), "read_only");
            assert_eq!(
                text("operation"),
                "mark_search_projection_full_reindex_needed"
            );
            assert!(!value
                .getattr("retryable")
                .unwrap()
                .extract::<bool>()
                .unwrap());
        });
    }
}
