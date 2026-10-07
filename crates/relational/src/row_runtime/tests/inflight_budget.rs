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

fn visit(
    runtime: &RelationalRowRuntime<'_>,
    borrowed: bool,
    mut callback: impl for<'row> FnMut(RelationalReadRowRef<'row>) -> Result<bool>,
) -> Result<bool> {
    if borrowed {
        runtime.visit_all_ref("docs", callback)
    } else {
        runtime.visit_all("docs", |row| callback(row.as_ref()))
    }
}

#[test]
fn parent_rows_are_charged_before_nested_point_reads() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for borrowed in [false, true] {
        let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
        runtime.limits.demand.max_rows = NonZeroUsize::new(8).unwrap();
        let mut completed = 0;
        let result = visit(&runtime, borrowed, |_| {
            assert!(runtime.read_point("docs", &key(completed))?.is_some());
            completed += 1;
            Ok(true)
        });
        assert!(
            matches!(result, Err(HawDBError::Execution(ref message)) if message.contains("row")),
            "{result:?}"
        );
        assert_eq!(completed, 4);
        assert_eq!(runtime.evidence().rows_visited, 8);
    }
    fixture.remove();
}

#[test]
fn parent_slot_bytes_are_charged_before_nested_point_reads() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let bytes = usize::try_from(fixture.root.manifest().page_bytes).unwrap();
    for borrowed in [false, true] {
        let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
        runtime.limits.demand.max_bytes = NonZeroUsize::new(8 * bytes).unwrap();
        let mut completed = 0;
        let result = visit(&runtime, borrowed, |_| {
            assert!(runtime.read_point("docs", &key(completed))?.is_some());
            completed += 1;
            Ok(true)
        });
        assert!(
            matches!(result, Err(HawDBError::Execution(ref message)) if message.contains("byte")),
            "{result:?}"
        );
        assert_eq!(completed, 7);
        assert_eq!(runtime.evidence().logical_bytes, 8 * bytes);
    }
    fixture.remove();
}

#[test]
fn parent_later_pages_include_completed_nested_work() {
    let fixture = Fixture::with_table_layouts(state(), &[("docs", 1)]);
    let task = RuntimeTaskContext::default();
    for borrowed in [false, true] {
        let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
        runtime.limits.demand.max_pages = NonZeroUsize::new(8).unwrap();
        let mut callbacks = 0;
        let result = visit(&runtime, borrowed, |_| {
            if callbacks == 0 {
                assert!(runtime.read_point("docs", &key(0))?.is_some());
            }
            callbacks += 1;
            Ok(true)
        });
        assert!(
            matches!(result, Err(HawDBError::Execution(ref message)) if message.contains("page")),
            "{result:?}"
        );
        assert_eq!(callbacks, 7);
        assert_eq!(runtime.evidence().logical_pages, 8);
        assert_eq!(runtime.evidence().rows_visited, 8);
    }
    fixture.remove();
}

#[test]
fn admitted_nested_reads_preserve_complete_rows_and_representation() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for borrowed in [false, true] {
        let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
        runtime.limits.demand.max_pages = NonZeroUsize::new(9).unwrap();
        let mut ids = Vec::new();
        assert!(visit(&runtime, borrowed, |row| {
            let RelationalValueRef::BigInt(id) = row.value(0)? else {
                panic!("expected id")
            };
            let nested = runtime.read_point("docs", &key(id))?.unwrap();
            assert_eq!(nested.value(0)?, &RelationalValue::BigInt(id));
            ids.push(id);
            Ok(true)
        })
        .unwrap());
        assert_eq!(ids, (0..8).collect::<Vec<_>>());
        let evidence = runtime.evidence();
        assert_eq!(evidence.logical_pages, 9);
        assert_eq!(evidence.rows_visited, 16);
        assert_eq!(evidence.borrowed_rows_visited, if borrowed { 8 } else { 0 });
        assert_eq!(evidence.owned_rows_visited, if borrowed { 8 } else { 16 });
        assert_eq!(evidence.file_pages + evidence.cache_hits, 9);
    }
    fixture.remove();
}

#[test]
fn callback_unwind_retains_parent_work_without_reset_or_poison() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for borrowed in [false, true] {
        let mut runtime = fixture.runtime(1, "SELECT id FROM docs", &task);
        runtime.limits.demand.max_pages = NonZeroUsize::new(3).unwrap();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = visit(&runtime, borrowed, |_| {
                assert!(runtime.read_point("docs", &key(0))?.is_some());
                panic!("caller callback unwind");
            });
        }));
        assert!(unwound.is_err());
        assert_eq!(runtime.evidence().logical_pages, 2);
        assert_eq!(runtime.evidence().rows_visited, 2);
        assert!(runtime.read_point("docs", &key(1)).unwrap().is_some());
        assert_eq!(runtime.evidence().logical_pages, 3);
        assert_eq!(runtime.evidence().rows_visited, 3);
        assert!(
            matches!(runtime.read_point("docs", &key(2)), Err(HawDBError::Execution(ref message)) if message.contains("page"))
        );
    }
    fixture.remove();
}

fn live_snapshot(fixture: &Fixture) -> RelationalRowPageSnapshotReader {
    let mut updated = fixture.state.clone();
    let parameters = (0..8).map(|id| [Value::Int(id)]).collect::<Vec<_>>();
    let statements = parameters
        .iter()
        .map(|values| {
            (
                "UPDATE docs SET bucket = 100 WHERE id = $1",
                values.as_slice(),
            )
        })
        .collect::<Vec<_>>();
    let changes = fixtures::capture(&mut updated, &statements);
    fixture.live_snapshot(changes)
}

#[test]
fn parent_overlay_entries_include_nested_points() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    for borrowed in [false, true] {
        let limits = RelationalRowPageSnapshotReadLimits {
            max_overlay_entries: NonZeroUsize::new(8).unwrap(),
            ..Default::default()
        };
        let runtime = RelationalRowRuntime::new(
            &fixture.state,
            Some(live_snapshot(&fixture)),
            None,
            fields("SELECT id FROM docs", &fixture.state),
            limits,
            Default::default(),
            &task,
        );
        let mut completed = 0;
        let result = visit(&runtime, borrowed, |_| {
            assert!(runtime.read_point("docs", &key(completed))?.is_some());
            completed += 1;
            Ok(true)
        });
        assert!(
            matches!(result, Err(HawDBError::Execution(ref message)) if message.contains("overlay")),
            "{result:?}"
        );
        assert_eq!(completed, 4);
        assert_eq!(runtime.evidence().overlay_entries, 8);
    }
    fixture.remove();
}

#[test]
fn parent_overlay_peak_is_charged_before_nested_points() {
    let fixture = Fixture::new();
    let task = RuntimeTaskContext::default();
    let reader = live_snapshot(&fixture);
    let mut hydration = RelationalHydrationBudget::default();
    let (_, point) = reader
        .point_projected(
            "docs",
            &key(0),
            &[0],
            Default::default(),
            &mut hydration,
            &task,
        )
        .unwrap();
    let range = reader
        .visit_projected_range(
            hawdb_storage::relational::RelationalRowPageProjectedRange {
                table: "docs",
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                requested_fields: &[0],
            },
            Default::default(),
            &mut hydration,
            &task,
            |_| false,
        )
        .unwrap();
    let budget = range.overlay_resident_bytes + 3 * point.overlay_resident_bytes;
    assert!(range.overlay_resident_bytes > 0 && point.overlay_resident_bytes > 0);
    for borrowed in [false, true] {
        let limits = RelationalRowPageSnapshotReadLimits {
            max_overlay_bytes: NonZeroUsize::new(budget).unwrap(),
            ..Default::default()
        };
        let runtime = RelationalRowRuntime::new(
            &fixture.state,
            Some(live_snapshot(&fixture)),
            None,
            fields("SELECT id FROM docs", &fixture.state),
            limits,
            Default::default(),
            &task,
        );
        let mut completed = 0;
        let result = visit(&runtime, borrowed, |_| {
            assert!(runtime.read_point("docs", &key(completed))?.is_some());
            completed += 1;
            Ok(true)
        });
        assert!(
            matches!(result, Err(HawDBError::Execution(ref message)) if message.contains("byte")),
            "{result:?}"
        );
        assert_eq!(completed, 3);
        assert_eq!(runtime.evidence().overlay_resident_bytes, budget);
    }
    drop(reader);
    fixture.remove();
}
