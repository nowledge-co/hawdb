// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb::{ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope};

fn delivered(error: FfiError) -> serde_json::Value {
    let mut buffer = HawdbBuffer {
        ptr: ptr::null_mut(),
        len: 0,
    };
    unsafe {
        set_error(&mut buffer, error);
        let bytes = std::slice::from_raw_parts(buffer.ptr.cast::<u8>(), buffer.len);
        let parsed = serde_json::from_slice(bytes).unwrap();
        hawdb_buffer_free(&mut buffer);
        assert!(buffer.ptr.is_null());
        parsed
    }
}

#[test]
fn resource_fields_survive_embedded_and_direct_error_delivery() {
    let source = HawDBError::from(ResourceBudgetError {
        resource: ResourceBudgetKind::SpillBytes,
        scope: ResourceBudgetScope::SharedPool,
        owner: "sort".into(),
        memory_class: None,
        requested: 9,
        used: u64::MAX - 8,
        budget: u64::MAX,
        budget_option: "ExecutionMemoryConfig.max_total_spill_bytes",
        message: "spill budget refused".into(),
    });
    let errors = [
        FfiError::open(&source),
        FfiError::query(&EmbeddedQueryError::Database(source)),
    ];
    for error in errors {
        assert_eq!(
            delivered(error),
            json!({
                "kind": "resource_budget_exceeded",
                "message": "execution error: spill budget refused",
                "resource_budget": {
                    "resource": "spill_bytes", "scope": "shared_pool", "owner": "sort",
                    "memory_class": null, "requested": 9, "used": u64::MAX - 8,
                    "budget": u64::MAX,
                    "budget_option": "ExecutionMemoryConfig.max_total_spill_bytes",
                },
            })
        );
    }
}

#[test]
fn ordinary_error_retains_the_existing_json_shape() {
    assert_eq!(
        delivered(FfiError::open(&HawDBError::Execution(
            "consumer failed".into()
        ))),
        json!({
            "kind": "execution", "message": "execution error: consumer failed",
        })
    );
}

#[test]
fn relational_refusals_report_their_kind_and_keep_their_message() {
    for (kind, name) in [
        (hawdb::RelationalErrorKind::Constraint, "constraint"),
        (hawdb::RelationalErrorKind::Schema, "schema"),
    ] {
        for (surface, prefix) in [
            (hawdb::RelationalErrorSurface::Storage, "storage error"),
            (hawdb::RelationalErrorSurface::Execution, "execution error"),
        ] {
            let error = HawDBError::relational(kind, surface, format!("{kind}: detail"));
            assert_eq!(
                delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
                json!({"kind": name, "message": format!("{prefix}: {kind}: detail")})
            );
        }
    }
}

#[test]
fn search_projection_mismatch_reports_its_kind_and_keeps_its_message() {
    let error = HawDBError::SearchProjectionMismatch {
        projection: Some("/data/search".into()),
        message: "resident search snapshot does not match".into(),
    };
    let expected = json!({
        "kind": "search_projection_mismatch",
        "message": "storage error: resident search snapshot does not match",
    });
    assert_eq!(delivered(FfiError::open(&error)), expected);
    assert_eq!(
        delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
        expected
    );
}

#[test]
fn search_projection_rebuild_refusal_reports_its_kind_and_keeps_its_message() {
    let error = HawDBError::SearchProjectionRebuildRefused {
        reason: hawdb::SearchProjectionRebuildRefusal::VerificationMismatch,
        target: "/data/search-rebuilt".into(),
        message: "differs from graph snapshot 7".into(),
    };
    let expected = json!({
        "kind": "search_projection_rebuild_refused",
        "message": "search projection rebuild refused (verification_mismatch): differs from graph snapshot 7",
    });
    assert_eq!(delivered(FfiError::open(&error)), expected);
    assert_eq!(
        delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
        expected
    );
}

#[test]
fn search_projection_epoch_advance_refusal_reports_its_kind_and_keeps_its_message() {
    let error = HawDBError::SearchProjectionEpochAdvanceRefused {
        reason: hawdb::SearchProjectionEpochAdvanceRefusal::ProjectionChanged,
        source_graph_commit_epoch: Some(4),
        target_graph_commit_epoch: 9,
        message: "commit 6 changed a projected node".into(),
    };
    let expected = json!({
        "kind": "search_projection_epoch_advance_refused",
        "message": "search projection epoch advance refused (projection_changed): commit 6 changed a projected node",
    });
    assert_eq!(delivered(FfiError::open(&error)), expected);
    assert_eq!(
        delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
        expected
    );
}

#[test]
fn search_projection_catch_up_refusals_report_their_kinds_and_keep_their_messages() {
    for (error, kind, message) in [
        (
            HawDBError::SearchProjectionPrefixBehindSource {
                complete_through_commit_epoch: 3,
                source_commit_epoch: 7,
            },
            "search_projection_prefix_behind_source",
            "execution error: search projection complete-through epoch 3 differs from source snapshot epoch 7; prepare the complete source interval before publishing its watermark",
        ),
        (
            HawDBError::SearchProjectionBatchBudgetExceeded {
                budget: hawdb::SearchProjectionBatchBudget::ChangeBatch,
                commit_epoch: Some(5),
                required_operations: 9,
                limit: 4,
            },
            "search_projection_batch_budget_exceeded",
            "storage error: search projection change at commit epoch 5 requires 9 operations, exceeding configured per-batch limit 4",
        ),
        (
            HawDBError::SearchProjectionBatchBudgetExceeded {
                budget: hawdb::SearchProjectionBatchBudget::GraphDelta,
                commit_epoch: None,
                required_operations: 9,
                limit: 4,
            },
            "search_projection_batch_budget_exceeded",
            "storage error: search projection graph delta operation count 9 exceeded configured limit 4",
        ),
        (
            HawDBError::SearchProjectionBatchBudgetExceeded {
                budget: hawdb::SearchProjectionBatchBudget::ProjectionDelta,
                commit_epoch: None,
                required_operations: 9,
                limit: 4,
            },
            "search_projection_batch_budget_exceeded",
            "storage error: incremental projection update operation count 9 exceeded configured limit 4",
        ),
    ] {
        let expected = json!({"kind": kind, "message": message});
        assert_eq!(delivered(FfiError::open(&error)), expected);
        assert_eq!(
            delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
            expected
        );
    }
}

#[test]
fn search_production_qualification_refusals_report_their_kinds_and_keep_their_messages() {
    for (error, kind, message) in [
        (
            HawDBError::SearchProductionQualificationGraphEpochMismatch {
                expected_canonical_graph_commit_epoch: 3,
                opened_graph_commit_epoch: 4,
            },
            "search_production_qualification_graph_epoch_mismatch",
            "storage error: qualified out-of-core search expected canonical graph commit epoch 3, opened graph is at 4",
        ),
        (
            HawDBError::SearchProductionQualificationRefused {
                blocker_codes: vec![
                    "dataset_too_small".to_owned(),
                    "projection_identity_mismatch".to_owned(),
                ],
            },
            "search_production_qualification_refused",
            "storage error: segmented lexical projection is not qualified for the current production release: dataset_too_small,projection_identity_mismatch",
        ),
    ] {
        let expected = json!({"kind": kind, "message": message});
        assert_eq!(delivered(FfiError::open(&error)), expected);
        assert_eq!(
            delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
            expected
        );
    }
}

#[test]
fn newer_search_projection_manifest_reports_unsupported_storage_format() {
    let error =
        HawDBError::UnsupportedStorageFormat(hawdb::StorageFormatError::UnsupportedVersion {
            format: hawdb::SEARCH_PROJECTION_MANIFEST_FORMAT,
            found: 3,
            supported: 2,
        });
    let expected = json!({
        "kind": "unsupported_storage_format",
        "message": "unsupported search projection manifest version 3; this reader supports version 2",
    });
    assert_eq!(delivered(FfiError::open(&error)), expected);
    assert_eq!(
        delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
        expected
    );
}

#[test]
fn newer_relational_checkpoint_layout_reports_unsupported_storage_format() {
    let error =
        HawDBError::UnsupportedStorageFormat(hawdb::StorageFormatError::UnsupportedFeature {
            format: hawdb::RELATIONAL_CHECKPOINT_LAYOUT_FORMAT,
            feature: "row-root relational checkpoint",
            tag: 5,
        });
    let expected = json!({
        "kind": "unsupported_storage_format",
        "message": "unsupported relational checkpoint layout feature row-root relational checkpoint tag 5",
    });
    assert_eq!(delivered(FfiError::open(&error)), expected);
    assert_eq!(
        delivered(FfiError::query(&EmbeddedQueryError::Database(error))),
        expected
    );
}

#[test]
fn wal_record_limit_is_delivered_as_its_own_kind() {
    let error = HawDBError::WalRecordLimitExceeded(Box::new(hawdb::WalRecordLimitError {
        limit: hawdb::WalRecordLimit::RecordBytes,
        requested: 20,
        maximum: 16,
    }));
    let delivered = delivered(FfiError::open(&error));
    assert_eq!(
        delivered["kind"], "wal_record_limit_exceeded",
        "{delivered}"
    );
    assert_eq!(delivered["message"], error.to_string());
}

#[test]
fn read_only_is_delivered_as_its_own_kind() {
    let error = HawDBError::ReadOnly {
        operation: "mark_search_projection_full_reindex_needed",
    };
    let delivered = delivered(FfiError::open(&error));
    assert_eq!(delivered["kind"], "read_only", "{delivered}");
    assert_eq!(delivered["message"], error.to_string());
}

#[test]
fn runtime_admission_refusal_is_delivered_as_its_own_kind() {
    let error = HawDBError::RuntimeAdmissionRefused(hawdb::RuntimeAdmissionRefusal {
        code: "memory_saturated",
        requested: 4096,
        available: 0,
        retryable: true,
        waited: None,
    });
    let delivered = delivered(FfiError::open(&error));
    assert_eq!(
        delivered["kind"], "runtime_admission_refused",
        "{delivered}"
    );
    assert_eq!(delivered["message"], error.to_string());
}

#[test]
fn io_wave_reservation_refusal_is_delivered_as_its_own_kind() {
    let error = HawDBError::RuntimeIoWaveReservationExceeded {
        requested_slots: 2,
        reserved_slots: 1,
    };
    let delivered = delivered(FfiError::open(&error));
    assert_eq!(
        delivered["kind"], "runtime_io_wave_reservation_exceeded",
        "{delivered}"
    );
    assert_eq!(delivered["message"], error.to_string());
}

#[test]
fn runtime_task_stop_is_delivered_as_its_own_kind() {
    let error = HawDBError::RuntimeTaskStopped(hawdb::RuntimeCancellationReason::Cancelled);
    let delivered = delivered(FfiError::open(&error));
    assert_eq!(delivered["kind"], "runtime_task_stopped", "{delivered}");
    assert_eq!(delivered["message"], error.to_string());
}
