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
