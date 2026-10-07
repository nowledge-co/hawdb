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
    estimate_relational_join_cost, PlanCostBreakdown, RelationalJoinCardinality,
    RelationalJoinRightInput, RelationalJoinSelectivity,
};

fn scan(rows: usize) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        name: "scan".into(),
        index_columns: Vec::new(),
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
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

// Keep the oracle in a wider type and independent of production cost helpers.
fn access_oracle(path: &RelationalAccessPathDescriptor) -> [u64; 5] {
    let n = path.estimated_rows.max(1) as u128;
    let (cpu, random, sequential) = match path.kind {
        RelationalAccessPathKind::FullScan => (n + 4, 0, n),
        RelationalAccessPathKind::PrimaryKey => (n, n, 0),
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

#[test]
fn access_components_distinguish_scan_navigation_and_row_fetches() {
    for rows in [0, 1, 2, 10, 1_000, usize::MAX] {
        for path in [scan(rows), index(rows, true), index(rows, false)] {
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
