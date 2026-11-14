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
use crate::{
    estimate_relational_access_path_cost, estimate_relational_join_cost, PlanCostBreakdown,
    RelationalJoinCardinality, RelationalJoinRightInput, RelationalJoinSelectivity,
};
use std::num::NonZeroU64;

fn scan(rows: usize) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        name: "scan".into(),
        index_columns: Vec::new(),
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
        range: None,
        reverse_order: false,
        unique_point: false,
        covering: false,
        requires_row_fetch: false,
        estimated_rows: rows,
    }
}

fn index(rows: usize, covering: bool) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::Index,
        name: if covering { "covering" } else { "fetch" }.into(),
        index_columns: vec!["key".into(), "order".into()],
        access_columns: BTreeSet::from(["key".into()]),
        equality_prefix_len: 1,
        covering,
        requires_row_fetch: !covering,
        ..scan(rows)
    }
}

fn primary_key_range(rows: usize) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::PrimaryKeyRange,
        name: "primary_key_range".into(),
        requires_row_fetch: false,
        ..index(rows, false)
    }
}

// Keep the oracle in a wider type and independent of production cost helpers.
fn access_oracle(path: &RelationalAccessPathDescriptor) -> [u64; 5] {
    let n = path.estimated_rows.max(1) as u128;
    let (cpu, random, sequential) = match path.kind {
        RelationalAccessPathKind::FullScan => (n + 4, 0, n),
        RelationalAccessPathKind::PrimaryKey | RelationalAccessPathKind::SearchIndex => (n, n, 0),
        RelationalAccessPathKind::PrimaryKeyRange => (n, 1, n),
        RelationalAccessPathKind::Index => (
            n * (1 + u128::from(path.requires_row_fetch)),
            1 + n * u128::from(path.requires_row_fetch),
            n * u128::from(!path.unique_point),
        ),
    };
    [
        cpu + 2 * random + sequential + n,
        cpu,
        random,
        sequential,
        n,
    ]
    .map(|v| v.min(u128::from(u64::MAX)) as u64)
}

fn assert_cost(path: &RelationalAccessPathDescriptor) {
    path.validate().unwrap();
    let cost = estimate_relational_access_path_cost(path);
    assert_eq!(
        [
            cost.cost,
            cost.cpu,
            cost.random_io,
            cost.sequential_io,
            cost.output_rows
        ],
        access_oracle(path),
        "{path:?}"
    );
    assert_eq!(cost.estimated_rows, path.estimated_rows.max(1) as u64);
}

fn snapshot_context(table_rows: u64, table_pages: u64) -> RelationalAccessCostContext {
    RelationalAccessCostContext::for_snapshot_rows(
        NonZeroU64::new(table_rows).unwrap(),
        NonZeroU64::new(table_pages).unwrap(),
    )
}

#[test]
fn snapshot_metadata_reads_have_distinct_access_components() {
    let context = snapshot_context(8_192, 32);
    let scan_cost = estimate_relational_access_path_cost_with_context(&scan(8_192), context);
    // Each ordered descriptor consumes its fixed record and two bound keys.
    // Canonical row visits remain separately charged by the base descriptor.
    assert_eq!(scan_cost.cpu, 8_292);
    assert_eq!(scan_cost.random_io, 0);
    assert_eq!(scan_cost.sequential_io, 8_288);

    let fetch_cost = estimate_relational_access_path_cost_with_context(&index(738, false), context);
    // A 32-page root admits at most six checked descriptor probes per point.
    // All three metadata reads are repeated for each canonical row locator.
    assert_eq!(fetch_cost.cpu, 7_380);
    assert_eq!(fetch_cost.random_io, 14_023);
    assert_eq!(fetch_cost.sequential_io, 738);
    assert_eq!(fetch_cost.output_rows, 738);
    assert_eq!(fetch_cost.cost, 36_902);

    assert_eq!(
        estimate_relational_access_path_cost_with_context(&index(738, true), context),
        estimate_relational_access_path_cost(&index(738, true)),
        "covering access must not pay for unvisited canonical metadata"
    );
}

#[test]
fn snapshot_metadata_costing_selects_scan_for_medium_prefix() {
    let context = snapshot_context(8_192, 32);
    let full_scan = scan(8_192);
    let medium = index(738, false);
    // The context-free path intentionally preserves its existing contract.
    assert_eq!(
        select_relational_access_path([full_scan.clone(), medium.clone()])
            .unwrap()
            .unwrap(),
        medium
    );
    for candidates in [
        vec![full_scan.clone(), medium.clone()],
        vec![medium, full_scan.clone()],
    ] {
        let frontier =
            skyline_prune_relational_access_paths_with_context(candidates.clone(), context)
                .unwrap();
        assert!(frontier.contains(&full_scan));
        assert_eq!(
            select_relational_access_path_with_context(candidates, context)
                .unwrap()
                .unwrap(),
            full_scan
        );
    }
    for selective in [index(81, false), index(738, true)] {
        assert_eq!(
            select_relational_access_path_with_context(
                [full_scan.clone(), selective.clone()],
                context
            )
            .unwrap()
            .unwrap(),
            selective,
            "sparse and covering alternatives retain their logical advantage"
        );
    }
    let mut point = index(1, false);
    point.kind = RelationalAccessPathKind::PrimaryKey;
    point.name = "primary_key".into();
    point.index_columns.truncate(1);
    point.unique_point = true;
    point.requires_row_fetch = false;
    assert_eq!(
        select_relational_access_path_with_context([full_scan, point.clone()], context)
            .unwrap()
            .unwrap(),
        point
    );
}

// Independent wide-arithmetic oracle over the unpruned descriptor set. Do not
// call the production context, skyline or scalar constructors to derive it.
fn snapshot_access_oracle(
    path: &RelationalAccessPathDescriptor,
    table_rows: u64,
    table_pages: u64,
) -> [u64; 5] {
    let n = path.estimated_rows.max(1) as u128;
    let (mut cpu, mut random, mut sequential) = match path.kind {
        RelationalAccessPathKind::FullScan => (
            n.max(u128::from(table_rows)) + 4,
            0,
            n.max(u128::from(table_rows)),
        ),
        RelationalAccessPathKind::PrimaryKey | RelationalAccessPathKind::SearchIndex => (n, n, 0),
        RelationalAccessPathKind::PrimaryKeyRange => (n, 1, n),
        RelationalAccessPathKind::Index => (
            n * (1 + u128::from(path.requires_row_fetch)),
            1 + n * u128::from(path.requires_row_fetch),
            n * u128::from(!path.unique_point),
        ),
    };
    let mut search_steps = 0;
    let mut pages = table_pages;
    while pages != 0 {
        search_steps += 1;
        pages /= 2;
    }
    match path.kind {
        RelationalAccessPathKind::FullScan => {
            let visits = u128::from(table_pages);
            // Actual demand scan calls read_table_page_descriptor per page;
            // that operation opens the descriptor and key artifacts each time.
            cpu += visits * 3;
            sequential += visits * 3;
        }
        RelationalAccessPathKind::PrimaryKey | RelationalAccessPathKind::SearchIndex => {
            cpu += n * (2 + search_steps);
            random += n * search_steps * 3;
        }
        RelationalAccessPathKind::PrimaryKeyRange => {
            // One positioned search reaches the first page; later pages
            // holding the interval are read in order.
            let following = (n * u128::from(table_pages))
                .div_ceil(u128::from(table_rows))
                .clamp(1, u128::from(table_pages))
                - 1;
            cpu += 2 + search_steps + following * 3;
            random += search_steps * 3;
            sequential += following * 3;
        }
        RelationalAccessPathKind::Index if path.requires_row_fetch => {
            cpu += n * (2 + search_steps);
            random += n * search_steps * 3;
        }
        RelationalAccessPathKind::Index => {}
    }
    [
        cpu + 2 * random + sequential + n,
        cpu,
        random,
        sequential,
        n,
    ]
    .map(|component| component.min(u128::from(u64::MAX)) as u64)
}

fn assert_snapshot_cost(path: &RelationalAccessPathDescriptor, rows: u64, pages: u64) {
    let actual =
        estimate_relational_access_path_cost_with_context(path, snapshot_context(rows, pages));
    assert_eq!(
        [
            actual.cost,
            actual.cpu,
            actual.random_io,
            actual.sequential_io,
            actual.output_rows
        ],
        snapshot_access_oracle(path, rows, pages),
        "{path:?}, table_rows={rows}, table_pages={pages}"
    );
    assert_eq!(actual.estimated_rows, path.estimated_rows.max(1) as u64);
}

#[test]
fn snapshot_cost_context_changes_fetch_selection_before_skyline_pruning() {
    let context = snapshot_context(1_000, 4);
    let scan_path = scan(1_000);
    let fetching = index(500, false);
    // The historical model selects the fetching index on its lower cost.
    assert_eq!(
        select_relational_access_path([scan_path.clone(), fetching.clone()])
            .unwrap()
            .unwrap(),
        fetching
    );
    for candidates in [
        vec![scan_path.clone(), fetching.clone()],
        vec![fetching.clone(), scan_path.clone()],
    ] {
        let frontier =
            skyline_prune_relational_access_paths_with_context(candidates.clone(), context)
                .unwrap();
        assert!(frontier.contains(&scan_path));
        assert_eq!(
            select_relational_access_path_with_context(candidates, context)
                .unwrap()
                .unwrap(),
            scan_path
        );
    }
    let covering = index(500, true);
    assert_eq!(
        select_relational_access_path_with_context([scan_path.clone(), covering.clone()], context)
            .unwrap()
            .unwrap(),
        covering
    );
    let selective = index(9, false);
    assert_eq!(
        select_relational_access_path_with_context([scan_path, selective.clone()], context)
            .unwrap()
            .unwrap(),
        selective
    );
    // Public descriptors admit conservative/untrusted point cardinalities.
    // Unlike a fetching secondary index, this point's properties let the
    // historical skyline remove the scan. The context reverses that cost
    // ordering, so applying it only after pruning cannot recover the winner.
    let scan_path = scan(1_000);
    let mut point = index(500, false);
    point.kind = RelationalAccessPathKind::PrimaryKey;
    point.name = "primary_key".into();
    point.index_columns.truncate(1);
    point.unique_point = true;
    point.requires_row_fetch = false;
    point.validate().unwrap();
    let candidates = [point.clone(), scan_path.clone()];
    assert!(!skyline_prune_relational_access_paths(candidates.clone())
        .unwrap()
        .contains(&scan_path));
    for ordered in [candidates.to_vec(), vec![scan_path.clone(), point]] {
        assert!(
            skyline_prune_relational_access_paths_with_context(ordered.clone(), context)
                .unwrap()
                .contains(&scan_path)
        );
        assert_eq!(
            select_relational_access_path_with_context(ordered, context)
                .unwrap()
                .unwrap(),
            scan_path
        );
    }
}

#[test]
fn snapshot_cost_context_preserves_cardinality_covering_and_saturation() {
    for rows in [0, 1, 9, 500, usize::MAX] {
        let mut point = index(rows, false);
        point.kind = RelationalAccessPathKind::PrimaryKey;
        point.index_columns.truncate(1);
        point.unique_point = true;
        point.requires_row_fetch = false;
        for path in [
            scan(rows),
            index(rows, false),
            index(rows, true),
            point,
            primary_key_range(rows),
        ] {
            path.validate().unwrap();
            assert_eq!(
                estimate_relational_access_path_cost(&path),
                estimate_relational_access_path_cost_with_context(
                    &path,
                    RelationalAccessCostContext::default()
                )
            );
            for (table_rows, pages) in [(1, 1), (1_000, 4), (8_192, 32), (u64::MAX, u64::MAX)] {
                assert_snapshot_cost(&path, table_rows, pages);
            }
            if path.kind == RelationalAccessPathKind::Index && !path.requires_row_fetch {
                assert_eq!(
                    estimate_relational_access_path_cost(&path),
                    estimate_relational_access_path_cost_with_context(
                        &path,
                        snapshot_context(8_192, 32)
                    )
                );
            }
        }
    }
}

#[test]
fn snapshot_cost_context_composes_probe_and_materialized_work_once() {
    let left =
        estimate_relational_access_path_cost_with_context(&scan(7), snapshot_context(100, 4));
    let right = estimate_relational_access_path_cost_with_context(
        &index(3, false),
        snapshot_context(1_000, 16),
    );
    for input in [
        RelationalJoinRightInput::Probe,
        RelationalJoinRightInput::Materialized,
        RelationalJoinRightInput::Hash,
        RelationalJoinRightInput::Merge,
    ] {
        let result = estimate_relational_join_cost(
            left,
            right,
            RelationalJoinCardinality::Inner,
            input,
            RelationalJoinSelectivity::Unknown,
        );
        let multiplier = if input == RelationalJoinRightInput::Probe {
            left.estimated_rows
        } else {
            1
        };
        assert_eq!(
            result.random_io,
            left.random_io + right.random_io * multiplier
        );
        assert_eq!(
            result.sequential_io,
            left.sequential_io + right.sequential_io * multiplier
        );
        assert_eq!(
            result.output_rows,
            left.output_rows + right.output_rows * multiplier
        );
        let joined = if input == RelationalJoinRightInput::Probe {
            21
        } else {
            3
        };
        let join_cpu = match input {
            RelationalJoinRightInput::Probe => 0,
            RelationalJoinRightInput::Materialized => 21,
            RelationalJoinRightInput::Hash => 7 + 2 * 3 + joined,
            RelationalJoinRightInput::Merge => 7 + 3 + joined,
        };
        assert_eq!(result.estimated_rows, joined);
        assert_eq!(result.cpu, left.cpu + right.cpu * multiplier + join_cpu);
        assert_eq!(
            result.cost,
            result.cpu + 2 * result.random_io + result.sequential_io + result.output_rows
        );
    }
}

#[test]
fn access_components_distinguish_scan_navigation_and_row_fetches() {
    for rows in [0, 1, 2, 10, 1_000, usize::MAX] {
        for path in [
            scan(rows),
            index(rows, true),
            index(rows, false),
            primary_key_range(rows),
        ] {
            assert_cost(&path);
        }
        let mut point = index(rows, false);
        point.unique_point = true;
        point.equality_prefix_len = 2;
        point.access_columns.insert("order".into());
        assert_cost(&point);
        point.kind = RelationalAccessPathKind::PrimaryKey;
        point.requires_row_fetch = false;
        assert_cost(&point);
    }
    let mut ordered = index(123, false);
    ordered.order_prefix_len = 1;
    ordered.exclusive_range = true;
    ordered.reverse_order = true;
    assert_cost(&ordered);
    assert_eq!(
        estimate_relational_access_path_cost(&ordered),
        estimate_relational_access_path_cost(&index(123, false))
    );
}

#[test]
fn non_covering_index_cannot_prune_a_cheaper_scan() {
    for rows in [2, 10, 1_000, 1_000_000] {
        for candidates in [
            vec![scan(rows), index(rows, false)],
            vec![index(rows, false), scan(rows)],
        ] {
            let frontier = skyline_prune_relational_access_paths(candidates.clone()).unwrap();
            assert!(frontier
                .iter()
                .any(|p| p.kind == RelationalAccessPathKind::FullScan));
            assert_eq!(
                select_relational_access_path(candidates)
                    .unwrap()
                    .unwrap()
                    .name,
                "scan"
            );
        }
    }
    assert_eq!(
        select_relational_access_path([scan(1_000), index(1, false)])
            .unwrap()
            .unwrap()
            .name,
        "fetch"
    );
    assert_eq!(
        select_relational_access_path([index(100, false), index(100, true)])
            .unwrap()
            .unwrap()
            .name,
        "covering"
    );
}

#[test]
fn skyline_compares_cost_even_for_untrusted_primary_key_cardinality() {
    // Descriptor validation deliberately accepts untrusted row estimates above
    // one even for a point key. Extra predicate/uniqueness properties must not
    // remove a cheaper scan before costing such an estimate.
    let mut point = index(10, false);
    point.kind = RelationalAccessPathKind::PrimaryKey;
    point.unique_point = true;
    point.index_columns.truncate(1);
    point.requires_row_fetch = false;
    point.validate().unwrap();
    let candidates = [point, scan(10)];
    assert_eq!(
        skyline_prune_relational_access_paths(candidates.clone())
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        select_relational_access_path(candidates)
            .unwrap()
            .unwrap()
            .name,
        "scan"
    );
}

#[test]
fn weights_are_applied_once_across_scalar_and_join_composition() {
    let left = estimate_relational_access_path_cost(&scan(7));
    let right = estimate_relational_access_path_cost(&index(3, false));
    let scalar = PlanCostBreakdown::from_scalar(right.as_plan_cost());
    assert_eq!(scalar.cost, right.cost);
    for mode in [
        RelationalJoinRightInput::Probe,
        RelationalJoinRightInput::Hash,
        RelationalJoinRightInput::Merge,
        RelationalJoinRightInput::Materialized,
    ] {
        let compose = |r| {
            estimate_relational_join_cost(
                left,
                r,
                RelationalJoinCardinality::PreserveLeft,
                mode,
                RelationalJoinSelectivity::equi_join(Some(7), Some(3)),
            )
        };
        let cost = compose(right);
        assert_eq!(cost.as_plan_cost(), compose(scalar).as_plan_cost());
        let multiplier = if mode == RelationalJoinRightInput::Probe {
            7
        } else {
            1
        };
        assert_eq!(cost.random_io, right.random_io * multiplier);
        assert_eq!(
            cost.sequential_io,
            left.sequential_io + right.sequential_io * multiplier
        );
        assert_eq!(
            cost.output_rows,
            left.output_rows + right.output_rows * multiplier
        );
    }
    let max = PlanCostBreakdown::new(0, u64::MAX, u64::MAX, u64::MAX, u64::MAX);
    assert_eq!(max.cost, u64::MAX);
    assert_eq!(max.estimated_rows, 1);
    assert_eq!(max.with_cpu(9, 1, 1).cost, u64::MAX);
    assert_eq!(max.with_random_io(9, 1, 1).cost, u64::MAX);
}

fn campaign(cases: usize) {
    let mut state = 216_u64;
    for case in 0..cases {
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as usize
        };
        let rows = if case % 31 == 0 {
            usize::MAX
        } else {
            next() % 10_000
        };
        let mut paths = vec![scan(rows)];
        let mut point = index(next() % rows.saturating_add(1).max(1), false);
        point.kind = RelationalAccessPathKind::PrimaryKey;
        point.unique_point = true;
        point.index_columns.truncate(1);
        point.requires_row_fetch = false;
        point.name = "primary_key".into();
        assert_cost(&point);
        paths.push(point);
        let interval = primary_key_range(next() % rows.saturating_add(1).max(1));
        assert_cost(&interval);
        paths.push(interval);
        for ordinal in 0..7 {
            let mut p = index(next() % rows.saturating_add(1).max(1), next() % 2 == 0);
            p.name = format!("index_{ordinal}");
            if ordinal % 2 == 0 {
                p.order_prefix_len = 1;
            }
            if ordinal % 3 == 0 {
                p.index_columns[0] = "other".into();
                p.access_columns = BTreeSet::from(["other".into()]);
            }
            assert_cost(&p);
            paths.push(p);
        }
        let expected = paths.iter().map(|p| access_oracle(p)[0]).min().unwrap();
        let selected = select_relational_access_path(paths.clone())
            .unwrap()
            .unwrap();
        assert_eq!(access_oracle(&selected)[0], expected, "case={case}");
        for _ in 0..paths.len() {
            paths.rotate_left(1);
            assert_eq!(
                select_relational_access_path(paths.clone())
                    .unwrap()
                    .unwrap(),
                selected
            );
            paths.reverse();
            assert_eq!(
                select_relational_access_path(paths.clone())
                    .unwrap()
                    .unwrap(),
                selected
            );
        }
        for (table_rows, table_pages) in [(1, 1), (8_192, 32), (u64::MAX, u64::MAX)] {
            let context = snapshot_context(table_rows, table_pages);
            for path in &paths {
                assert_snapshot_cost(path, table_rows, table_pages);
            }
            let expected = paths
                .iter()
                .map(|path| snapshot_access_oracle(path, table_rows, table_pages)[0])
                .min()
                .unwrap();
            let selected = select_relational_access_path_with_context(paths.clone(), context)
                .unwrap()
                .unwrap();
            assert_eq!(
                snapshot_access_oracle(&selected, table_rows, table_pages)[0],
                expected,
                "snapshot case={case}"
            );
            for _ in 0..paths.len() {
                paths.rotate_left(1);
                assert_eq!(
                    select_relational_access_path_with_context(paths.clone(), context)
                        .unwrap()
                        .unwrap(),
                    selected
                );
                paths.reverse();
                assert_eq!(
                    select_relational_access_path_with_context(paths.clone(), context)
                        .unwrap()
                        .unwrap(),
                    selected
                );
            }
        }
    }
}

#[test]
fn skyline_and_selection_preserve_the_unpruned_minimum_cost() {
    campaign(64);
}

#[test]
#[ignore = "local-only deterministic access-cost campaign"]
fn access_cost_differential_campaign() {
    campaign(2_048);
}

#[test]
fn capped_scan_output_does_not_reduce_pinned_input_work() {
    let context = snapshot_context(8_192, 32);
    let complete = estimate_relational_access_path_cost_with_context(&scan(8_192), context);
    for candidates in [1, 2, 17, 8_191, 8_192] {
        let cost = estimate_relational_access_path_cost_with_context(&scan(candidates), context);
        assert_eq!(cost.cpu, complete.cpu);
        assert_eq!(cost.sequential_io, complete.sequential_io);
        assert_eq!(cost.random_io, 0);
        assert_eq!(cost.output_rows, candidates as u64);
        assert_eq!(cost.estimated_rows, candidates as u64);
    }
    let small = snapshot_context(128, 1);
    assert!(estimate_relational_access_path_cost_with_context(&scan(1), small).cpu < complete.cpu);
    let point = index(1, false);
    assert_eq!(
        estimate_relational_access_path_cost_with_context(&point, context),
        estimate_relational_access_path_cost_with_context(&point, snapshot_context(8_193, 32)),
        "point lookup visits are independent of a full-scan input bound"
    );
    assert_cost(&scan(1));
}

#[test]
fn capped_scan_cannot_beat_a_selective_covering_access_by_hiding_input() {
    let context = snapshot_context(8_192, 32);
    let scan = scan(2);
    let covering = index(64, true);
    assert_eq!(
        select_relational_access_path_with_context([scan, covering.clone()], context)
            .unwrap()
            .unwrap(),
        covering
    );
}

#[test]
fn clustered_interval_needs_no_row_fetch_and_rejects_point_metadata() {
    for rows in [1, 64, 3_000] {
        let interval = primary_key_range(rows);
        for context in [
            RelationalAccessCostContext::default(),
            snapshot_context(9_000, 64),
        ] {
            let interval_cost =
                estimate_relational_access_path_cost_with_context(&interval, context);
            for other in [index(rows, false), scan(9_000)] {
                assert!(
                    interval_cost.cost
                        < estimate_relational_access_path_cost_with_context(&other, context).cost,
                    "{rows} {other:?}"
                );
            }
        }
    }
    let mut invalid = primary_key_range(1);
    invalid.unique_point = true;
    assert!(invalid.validate().is_err());
    let mut invalid = primary_key_range(1);
    invalid.requires_row_fetch = true;
    assert!(invalid.validate().is_err());
    let mut invalid = primary_key_range(1);
    invalid.equality_prefix_len = 0;
    invalid.access_columns.clear();
    assert!(invalid.validate().is_err());
    let mut invalid = primary_key_range(1);
    invalid.reverse_order = true;
    invalid.order_prefix_len = 1;
    assert!(invalid.validate().is_err());
}
