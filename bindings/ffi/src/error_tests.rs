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
