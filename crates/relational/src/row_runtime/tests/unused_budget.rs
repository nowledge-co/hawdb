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

fn live_changes(
    fixture: &Fixture,
    all: bool,
) -> hawdb_storage::relational::RelationalRowChangeCapture {
    let mut updated = fixture.state.clone();
    let ids = if all { 0..8 } else { 7..8 };
    let parameters = ids.map(|id| [Value::Int(id)]).collect::<Vec<_>>();
    let statements = parameters
        .iter()
        .map(|values| {
            (
                "UPDATE docs SET bucket = 100 WHERE id = $1",
                values.as_slice(),
            )
        })
        .collect::<Vec<_>>();
    fixtures::capture(&mut updated, &statements)
}

fn overlay_children_at_exhausted_page_envelope(borrowed: bool, bytes: bool) {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let slot = usize::try_from(fixture.root.manifest().page_bytes).unwrap();
    let mut limits = RelationalRowPageSnapshotReadLimits::default();
    limits.demand.max_rows = NonZeroUsize::new(16).unwrap();
    if bytes {
        limits.demand.max_bytes = NonZeroUsize::new(slot).unwrap();
    } else {
        limits.demand.max_pages = NonZeroUsize::new(1).unwrap();
    }
    let runtime = RelationalRowRuntime::new(
        &fixture.state,
        Some(fixture.live_snapshot(live_changes(&fixture, true))),
        None,
        fields("SELECT id FROM docs", &fixture.state),
        limits,
        Default::default(),
        &task,
    );
    let mut parent = Vec::new();
    let mut children = Vec::new();
    let mut callback = |row: RelationalReadRowRef<'_>| {
        let primary_key = row.row.primary_key().clone();
        let child = runtime
            .read_point("docs", &primary_key)?
            .expect("live child row");
        assert_eq!(child.row.primary_key, primary_key);
        children.push(child.row.primary_key.clone());
        parent.push(primary_key);
        Ok(true)
    };
    let result = if borrowed {
        runtime.visit_all_ref("docs", &mut callback)
    } else {
        runtime.visit_all("docs", |row| callback(row.as_ref()))
    };
    assert!(
        result.unwrap(),
        "unused exhausted page/byte budget rejected live-only child"
    );
    let expected = (0..8).map(key).collect::<Vec<_>>();
    assert_eq!(parent, expected);
    assert_eq!(children, expected);
    let evidence = runtime.evidence();
    assert_eq!(evidence.logical_pages, 1);
    assert_eq!(evidence.logical_bytes, slot);
    assert_eq!(evidence.rows_visited, 16);
    assert_eq!(evidence.overlay_entries, 16);
    assert_eq!(evidence.owned_rows_visited, 16);
    assert_eq!(evidence.borrowed_rows_visited, 0);
    let reader = runtime.backend.as_ref().unwrap();
    assert!(!reader.is_poisoned());
    let charged = reader.cumulative_read_report().unwrap();
    assert_eq!(charged.admitted_rows, 16);
    fixture.remove();
}

#[test]
fn exhausted_page_allows_owned_overlay_only_children() {
    overlay_children_at_exhausted_page_envelope(false, false);
}
#[test]
fn exhausted_page_allows_borrowed_overlay_only_children() {
    overlay_children_at_exhausted_page_envelope(true, false);
}
#[test]
fn exhausted_slot_bytes_allow_owned_overlay_only_children() {
    overlay_children_at_exhausted_page_envelope(false, true);
}
#[test]
fn exhausted_slot_bytes_allow_borrowed_overlay_only_children() {
    overlay_children_at_exhausted_page_envelope(true, true);
}

fn base_point_at_exhausted_overlay_envelope(bytes: bool) {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let changes = live_changes(&fixture, false);
    let mut limits = RelationalRowPageSnapshotReadLimits::default();
    if bytes {
        let probe = fixture.live_snapshot(changes.clone());
        let (_, report) = probe
            .point_projected(
                "docs",
                &key(7),
                &[0],
                Default::default(),
                &mut RelationalHydrationBudget::default(),
                &task,
            )
            .unwrap();
        limits.max_overlay_bytes = NonZeroUsize::new(report.overlay_resident_bytes).unwrap();
    } else {
        limits.max_overlay_entries = NonZeroUsize::new(1).unwrap();
    }
    let runtime = RelationalRowRuntime::new(
        &fixture.state,
        Some(fixture.live_snapshot(changes)),
        None,
        fields("SELECT id FROM docs", &fixture.state),
        limits,
        Default::default(),
        &task,
    );
    assert_eq!(
        runtime
            .read_point("docs", &key(7))
            .unwrap()
            .unwrap()
            .row
            .primary_key,
        key(7)
    );
    assert_eq!(
        runtime
            .read_point("docs", &key(0))
            .unwrap()
            .unwrap()
            .row
            .primary_key,
        key(0)
    );
    let reader = runtime.backend.as_ref().unwrap();
    let charged = reader.cumulative_read_report().unwrap();
    assert_eq!(charged.admitted_rows, 2);
    assert_eq!(charged.demand.pages_read, 1);
    assert_eq!(charged.overlay_entries, 1);
    assert!(matches!(
        runtime.read_point("docs", &key(7)),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(reader.cumulative_read_report().unwrap(), charged);
    assert!(!reader.is_poisoned());
    fixture.remove();
}
#[test]
fn exhausted_overlay_entries_allow_base_only_point() {
    base_point_at_exhausted_overlay_envelope(false);
}
#[test]
fn exhausted_overlay_bytes_allow_base_only_point() {
    base_point_at_exhausted_overlay_envelope(true);
}

#[test]
fn exhausted_rows_allow_absent_point_without_refund() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
    runtime.limits.demand.max_rows = NonZeroUsize::new(1).unwrap();
    assert_eq!(
        runtime
            .read_point("docs", &key(0))
            .unwrap()
            .unwrap()
            .row
            .primary_key,
        key(0)
    );
    let before = runtime
        .backend
        .as_ref()
        .unwrap()
        .cumulative_read_report()
        .unwrap();
    assert!(runtime.read_point("docs", &key(99)).unwrap().is_none());
    let after = runtime
        .backend
        .as_ref()
        .unwrap()
        .cumulative_read_report()
        .unwrap();
    assert_eq!(after.admitted_rows, 1);
    assert_eq!(after.demand.rows_decoded, 1);
    assert_eq!(after.demand.rows_emitted, 1);
    assert_eq!(after.demand.pages_read, before.demand.pages_read);
    assert_eq!(after.demand.bytes_read, before.demand.bytes_read);
    assert!(matches!(
        runtime.read_point("docs", &key(1)),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(
        runtime
            .backend
            .as_ref()
            .unwrap()
            .cumulative_read_report()
            .unwrap()
            .admitted_rows,
        1
    );
    fixture.remove();
}

#[test]
fn exhausted_rows_allow_absent_batch_within_input_window() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
    runtime.limits.demand.max_rows = NonZeroUsize::new(2).unwrap();
    for id in 0..2 {
        assert_eq!(
            runtime
                .read_point("docs", &key(id))
                .unwrap()
                .unwrap()
                .row
                .primary_key,
            key(id)
        );
    }
    let reader = runtime.backend.as_ref().unwrap();
    let before = reader.cumulative_read_report().unwrap();
    assert_eq!(before.admitted_rows, 2);
    assert!(runtime
        .read_points("docs", &[key(99), key(100)])
        .unwrap()
        .is_empty());
    let mut after_misses = before;
    after_misses.demand.descriptor_reads += 2;
    assert_eq!(reader.cumulative_read_report().unwrap(), after_misses);
    // The configured input window is still bounded, even for all misses.
    assert!(matches!(
        runtime.read_points("docs", &[key(99), key(100), key(101)]),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(reader.cumulative_read_report().unwrap(), after_misses);
    assert!(matches!(
        runtime.read_points("docs", &[key(2), key(3)]),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(reader.cumulative_read_report().unwrap().admitted_rows, 2);
    assert!(!reader.is_poisoned());
    fixture.remove();
}

#[test]
fn partially_spent_rows_keep_absent_batch_input_window() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
    runtime.limits.demand.max_rows = NonZeroUsize::new(2).unwrap();
    assert_eq!(
        runtime
            .read_point("docs", &key(0))
            .unwrap()
            .unwrap()
            .row
            .primary_key,
        key(0)
    );
    let reader = runtime.backend.as_ref().unwrap();
    let before = reader.cumulative_read_report().unwrap();
    assert_eq!(before.admitted_rows, 1);
    assert!(runtime
        .read_points("docs", &[key(99), key(100)])
        .unwrap()
        .is_empty());
    let mut after_misses = before;
    after_misses.demand.descriptor_reads += 2;
    assert_eq!(reader.cumulative_read_report().unwrap(), after_misses);
    let rows = runtime.read_points("docs", &[key(1), key(99)]).unwrap();
    assert_eq!(rows.keys().cloned().collect::<Vec<_>>(), vec![key(1)]);
    assert_eq!(rows[&key(1)].row.primary_key, key(1));
    assert_eq!(reader.cumulative_read_report().unwrap().admitted_rows, 2);
    assert!(matches!(
        runtime.read_points("docs", &[key(2), key(3)]),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(reader.cumulative_read_report().unwrap().admitted_rows, 2);
    assert!(!reader.is_poisoned());
    fixture.remove();
}
