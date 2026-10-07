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

use super::*;

fn mixed_state(overflow: bool) -> RelationalState {
    let mut state = state();
    apply(
        &mut state,
        "CREATE TABLE aux (id BIGINT PRIMARY KEY, bucket BIGINT, body TEXT)",
        &[],
    );
    for id in 0..8 {
        apply(
            &mut state,
            "INSERT INTO aux (id, bucket, body) VALUES ($1, $2, $3)",
            &[
                Value::Int(id),
                Value::Int(id % 3),
                Value::String(if overflow {
                    "actual-overflow".repeat(2048)
                } else {
                    format!("aux-{id}")
                }),
            ],
        );
    }
    state
}

fn mixed_runtime<'a>(
    fixture: &'a Fixture,
    snapshot: RelationalRowPageSnapshotReader,
    task: &'a RuntimeTaskContext,
) -> RelationalRowRuntime<'a> {
    RelationalRowRuntime::new(
        &fixture.state,
        Some(snapshot),
        Some((&fixture.projection, &fixture.tables)),
        fields(
            "SELECT d.id, a.body FROM docs d JOIN aux a ON d.id = a.id",
            &fixture.state,
        ),
        Default::default(),
        Default::default(),
        task,
    )
}

#[test]
fn failed_snapshot_hydration_exhausts_projection_before_frame_read() {
    let fixture = Fixture::with_table_layouts(
        mixed_state(true),
        &[("docs", usize::MAX), ("aux", usize::MAX)],
    );
    let task = RuntimeTaskContext::default();
    let mut runtime = mixed_runtime(&fixture, fixture.snapshot(), &task);
    runtime.limits.demand.max_rows = NonZeroUsize::new(1).unwrap();
    runtime.hydration.get_mut().max_decompressed_bytes = 1;
    assert!(matches!(
        fixture.state.row_entry("aux", &key(0)).unwrap().1.values()[2],
        RelationalValue::Overflow(_)
    ));
    assert!(matches!(
        runtime.read_output_point("aux", &key(0)),
        Err(HawDBError::Execution(_))
    ));
    let reader = runtime.backend.as_ref().unwrap();
    assert!(!reader.is_poisoned());
    let admitted = reader.cumulative_read_report().unwrap();
    assert_eq!(admitted.admitted_rows, 1);
    assert_eq!(admitted.demand.rows_emitted, 0);
    assert_eq!(runtime.evidence().rows_visited, 0);
    assert_eq!(runtime.hydration().hydrated_rows, 0);

    // This is the production read_page preflight, before any frame payload I/O.
    assert!(
        matches!(runtime.projection_page_limits(), Err(HawDBError::Execution(ref message)) if message.contains("row")),
        "failed row admission must leave no projection allowance"
    );
    let mut calls = 0;
    assert!(matches!(
        runtime.visit_all("docs", |_| {
            calls += 1;
            Ok(true)
        }),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(calls, 0);
    assert_eq!(reader.cumulative_read_report().unwrap(), admitted);
    fixture.remove();
}

fn projection_preflight_preserves_tightened_cap(resource: &str) {
    let fixture = Fixture::with_table_layouts(
        mixed_state(false),
        &[("docs", usize::MAX), ("aux", usize::MAX)],
    );
    let task = RuntimeTaskContext::default();
    {
        let snapshot = fixture.snapshot();
        let mut tighter = RelationalRowPageSnapshotReadLimits::default();
        if resource == "row" {
            tighter.demand.max_rows = NonZeroUsize::new(1).unwrap();
        } else {
            tighter.demand.max_bytes = NonZeroUsize::new(17).unwrap();
        }
        snapshot.restrict_cumulative_read_limits(tighter).unwrap();
        let runtime = mixed_runtime(&fixture, snapshot, &task);
        let allowance = runtime.projection_page_limits().unwrap();
        if resource == "row" {
            assert_eq!(allowance.max_rows.get(), 1);
            assert_eq!(runtime.remaining_limits().unwrap().demand.max_rows.get(), 1);
        } else {
            assert_eq!(allowance.max_payload_bytes.get(), 17);
            assert_eq!(allowance.max_record_bytes.get(), 17);
            assert_eq!(
                runtime.remaining_limits().unwrap().demand.max_bytes.get(),
                17
            );
        }
    }
    fixture.remove();
}

#[test]
fn projection_preflight_preserves_previously_tightened_row_cap() {
    projection_preflight_preserves_tightened_cap("row");
}

#[test]
fn projection_preflight_preserves_previously_tightened_byte_cap() {
    projection_preflight_preserves_tightened_cap("byte");
}

#[test]
fn mixed_nested_sources_share_rows_and_preserve_complete_admitted_results() {
    let fixture = Fixture::with_table_layouts(
        mixed_state(false),
        &[("docs", usize::MAX), ("aux", usize::MAX)],
    );
    let task = RuntimeTaskContext::default();
    for parent in ["aux", "docs"] {
        for cap in [15, 16] {
            let mut runtime = mixed_runtime(&fixture, fixture.snapshot(), &task);
            runtime.limits.demand.max_rows = NonZeroUsize::new(cap).unwrap();
            let child = if parent == "aux" { "docs" } else { "aux" };
            let mut parent_keys = Vec::new();
            let mut child_keys = Vec::new();
            let result = runtime.visit_all_ref(parent, |row| {
                parent_keys.push(row.row.primary_key().clone());
                if parent_keys.len() == 1 {
                    assert!(runtime.visit_all_ref(child, |row| {
                        child_keys.push(row.row.primary_key().clone());
                        Ok(true)
                    })?);
                }
                Ok(true)
            });
            if cap == 16 {
                assert!(result.unwrap());
                assert_eq!(parent_keys, (0..8).map(key).collect::<Vec<_>>());
                assert_eq!(child_keys, (0..8).map(key).collect::<Vec<_>>());
                assert_eq!(runtime.evidence().owned_rows_visited, 8);
                assert_eq!(runtime.evidence().borrowed_rows_visited, 8);
            } else {
                assert!(
                    matches!(result, Err(HawDBError::Execution(ref message)) if message.contains("row")),
                    "{parent}: {result:?}"
                );
            }
            assert_eq!(runtime.evidence().rows_visited, cap);
            assert_eq!(
                runtime
                    .backend
                    .as_ref()
                    .unwrap()
                    .cumulative_read_report()
                    .unwrap()
                    .admitted_rows,
                cap
            );
        }
    }
    fixture.remove();
}

#[test]
fn mixed_nested_sources_share_slot_and_projection_payload_bytes() {
    let fixture = Fixture::with_table_layouts(
        mixed_state(false),
        &[("docs", usize::MAX), ("aux", usize::MAX)],
    );
    let task = RuntimeTaskContext::default();
    let slot = usize::try_from(fixture.root.manifest().page_bytes).unwrap();
    let projection = fixture
        .projection
        .read_page(None, Default::default())
        .unwrap();
    assert_eq!(projection.members.len(), 8);
    let payload = projection.report.payload_bytes;
    for parent in ["aux", "docs"] {
        for complete in [false, true] {
            let cap = slot + payload - usize::from(!complete);
            let mut runtime = mixed_runtime(&fixture, fixture.snapshot(), &task);
            runtime.limits.demand.max_bytes = NonZeroUsize::new(cap).unwrap();
            let mut calls = 0;
            let result = runtime.visit_all_ref(parent, |_| {
                calls += 1;
                if parent == "aux" {
                    let mut projected_keys = Vec::new();
                    assert!(runtime.visit_all_ref("docs", |row| {
                        projected_keys.push(row.row.primary_key().clone());
                        Ok(true)
                    })?);
                    assert_eq!(projected_keys, (0..8).map(key).collect::<Vec<_>>());
                } else {
                    assert!(runtime.read_point("aux", &key(0))?.is_some());
                }
                Ok(false)
            });
            if complete {
                assert!(!result.unwrap());
                assert_eq!(calls, 1);
                assert_eq!(runtime.evidence().logical_bytes, cap);
                assert_eq!(runtime.evidence().rows_visited, 9);
            } else {
                assert!(
                    matches!(result, Err(HawDBError::Execution(ref message)) if message.contains("byte")),
                    "{parent}: {result:?}"
                );
                assert!(runtime.evidence().logical_bytes <= cap);
            }
        }
    }
    fixture.remove();
}
