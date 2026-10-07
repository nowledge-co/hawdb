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

use crate::estimate_relational_access_path_cost;
use std::collections::BTreeSet;

#[cfg(test)]
#[path = "relational/cost_tests.rs"]
mod cost_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationalAccessPathKind {
    FullScan,
    PrimaryKey,
    Index,
}

/// Conservative row-work estimate for an index nested-loop join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalNestedLoopJoinCost {
    pub outer_rows: usize,
    pub inner_rows_per_outer: usize,
    pub estimated_work: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalAccessPathDescriptor {
    pub kind: RelationalAccessPathKind,
    pub name: String,
    pub index_columns: Vec<String>,
    pub access_columns: BTreeSet<String>,
    pub equality_prefix_len: usize,
    pub order_prefix_len: usize,
    pub exclusive_range: bool,
    pub reverse_order: bool,
    pub unique_point: bool,
    pub covering: bool,
    pub requires_row_fetch: bool,
    pub estimated_rows: usize,
}

impl RelationalAccessPathDescriptor {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.name.is_empty() {
            return Err("relational access path name must not be empty");
        }
        if self.kind != RelationalAccessPathKind::FullScan && self.index_columns.is_empty() {
            return Err("relational index access path must contain index columns");
        }
        if self.equality_prefix_len > self.index_columns.len() {
            return Err("relational equality prefix exceeds index columns");
        }
        if self
            .equality_prefix_len
            .saturating_add(self.order_prefix_len)
            > self.index_columns.len()
        {
            return Err("relational order prefix exceeds remaining index columns");
        }
        if (self.exclusive_range || self.reverse_order)
            && (self.kind != RelationalAccessPathKind::Index || self.order_prefix_len == 0)
        {
            return Err("relational range metadata requires an ordered index access");
        }
        let expected_access_columns = self.index_columns[..self.equality_prefix_len]
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if self.access_columns != expected_access_columns {
            return Err("relational access columns must equal the constrained leading prefix");
        }
        if self.unique_point
            && (self.kind == RelationalAccessPathKind::FullScan
                || self.equality_prefix_len != self.index_columns.len())
        {
            return Err("relational unique point access must constrain the complete key");
        }
        if self.covering && self.requires_row_fetch {
            return Err("covering relational access must not require a row fetch");
        }
        Ok(())
    }

    fn dominates(&self, other: &Self) -> bool {
        let access_is_superset = self.access_columns.is_superset(&other.access_columns);
        let no_worse = access_is_superset
            && self.equality_prefix_len >= other.equality_prefix_len
            && self.order_prefix_len >= other.order_prefix_len
            && (self.exclusive_range || !other.exclusive_range)
            && (self.unique_point || !other.unique_point)
            && (self.covering || !other.covering)
            && (!self.requires_row_fetch || other.requires_row_fetch)
            && estimate_relational_access_path_cost(self).cost
                <= estimate_relational_access_path_cost(other).cost
            && self.estimated_rows <= other.estimated_rows;
        let strictly_better = self.access_columns != other.access_columns
            || self.equality_prefix_len > other.equality_prefix_len
            || self.order_prefix_len > other.order_prefix_len
            || (self.exclusive_range && !other.exclusive_range)
            || (self.unique_point && !other.unique_point)
            || (self.covering && !other.covering)
            || (!self.requires_row_fetch && other.requires_row_fetch)
            || self.estimated_rows < other.estimated_rows;
        no_worse && strictly_better
    }
}

/// Retains the Pareto frontier of relational access paths.
///
/// Paths using different, non-superset predicate columns remain incomparable.
/// This prevents a locally attractive index from pruning the only useful path
/// for a different conjunction shape before costing.
pub fn skyline_prune_relational_access_paths(
    candidates: impl IntoIterator<Item = RelationalAccessPathDescriptor>,
) -> Result<Vec<RelationalAccessPathDescriptor>, &'static str> {
    let mut frontier = Vec::<RelationalAccessPathDescriptor>::new();
    for mut candidate in candidates {
        candidate.estimated_rows = candidate.estimated_rows.max(1);
        candidate.validate()?;
        if frontier
            .iter()
            .any(|existing| existing.dominates(&candidate))
        {
            continue;
        }
        frontier.retain(|existing| !candidate.dominates(existing));
        frontier.push(candidate);
    }
    frontier.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(frontier)
}

pub fn select_relational_access_path(
    candidates: impl IntoIterator<Item = RelationalAccessPathDescriptor>,
) -> Result<Option<RelationalAccessPathDescriptor>, &'static str> {
    let frontier = skyline_prune_relational_access_paths(candidates)?;
    Ok(frontier.into_iter().min_by(|left, right| {
        estimate_relational_access_path_cost(left)
            .cost
            .cmp(&estimate_relational_access_path_cost(right).cost)
            .then_with(|| left.estimated_rows.cmp(&right.estimated_rows))
            .then_with(|| right.unique_point.cmp(&left.unique_point))
            .then_with(|| right.equality_prefix_len.cmp(&left.equality_prefix_len))
            .then_with(|| right.order_prefix_len.cmp(&left.order_prefix_len))
            .then_with(|| right.exclusive_range.cmp(&left.exclusive_range))
            .then_with(|| right.covering.cmp(&left.covering))
            .then_with(|| left.requires_row_fetch.cmp(&right.requires_row_fetch))
            .then_with(|| left.index_columns.len().cmp(&right.index_columns.len()))
            .then_with(|| left.name.cmp(&right.name))
    }))
}

/// Estimates row visits for an index nested-loop join.
///
/// The planning boundary is responsible for supplying a conservative
/// per-outer-row estimate in the inner descriptor. This keeps the join cost
/// independent of where that estimate came from (fresh statistics or a
/// table-cardinality fallback).
pub fn estimate_relational_nested_loop_join_cost(
    outer: &RelationalAccessPathDescriptor,
    inner: &RelationalAccessPathDescriptor,
) -> RelationalNestedLoopJoinCost {
    let outer_rows = outer.estimated_rows.max(1);
    let inner_rows_per_outer = inner.estimated_rows.max(1);
    RelationalNestedLoopJoinCost {
        outer_rows,
        inner_rows_per_outer,
        estimated_work: outer_rows.saturating_add(outer_rows.saturating_mul(inner_rows_per_outer)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(
        name: &str,
        columns: &[&str],
        equality_prefix_len: usize,
        unique_point: bool,
        estimated_rows: usize,
    ) -> RelationalAccessPathDescriptor {
        RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.to_string(),
            index_columns: columns.iter().map(|column| (*column).to_string()).collect(),
            access_columns: columns[..equality_prefix_len]
                .iter()
                .map(|column| (*column).to_string())
                .collect(),
            equality_prefix_len,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point,
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        }
    }

    #[test]
    fn composite_prefix_dominates_a_shorter_access_condition() {
        let frontier = skyline_prune_relational_access_paths([
            path("idx_a", &["a"], 1, false, 100),
            path("idx_a_b", &["a", "b"], 2, false, 10),
        ])
        .unwrap();

        assert_eq!(frontier.len(), 1);
        assert_eq!(frontier[0].name, "idx_a_b");
    }

    #[test]
    fn different_predicate_columns_remain_incomparable() {
        let frontier = skyline_prune_relational_access_paths([
            path("idx_a", &["a"], 1, false, 10),
            path("idx_b", &["b"], 1, false, 5),
        ])
        .unwrap();

        assert_eq!(
            frontier
                .iter()
                .map(|path| path.name.as_str())
                .collect::<Vec<_>>(),
            ["idx_a", "idx_b"]
        );
    }

    #[test]
    fn complete_unique_composite_key_dominates_unique_prefix_when_both_are_bound() {
        let frontier = skyline_prune_relational_access_paths([
            path("idx_f", &["f"], 1, true, 1),
            path("idx_f_g", &["f", "g"], 2, true, 1),
        ])
        .unwrap();

        assert_eq!(frontier.len(), 1);
        assert_eq!(frontier[0].name, "idx_f_g");
    }

    #[test]
    fn final_selection_is_deterministic_across_incomparable_paths() {
        let selected = select_relational_access_path([
            path("idx_a", &["a"], 1, false, 10),
            path("idx_b", &["b"], 1, false, 5),
        ])
        .unwrap()
        .unwrap();

        assert_eq!(selected.name, "idx_b");
    }

    #[test]
    fn ordered_suffix_must_fit_after_the_equality_prefix() {
        let mut candidate = path("idx_a_b", &["a", "b"], 1, false, 10);
        candidate.order_prefix_len = 2;

        assert_eq!(
            candidate.validate(),
            Err("relational order prefix exceeds remaining index columns")
        );
    }

    #[test]
    fn zero_row_estimates_are_normalized_before_skyline_comparison() {
        let frontier =
            skyline_prune_relational_access_paths([path("idx_empty", &["id"], 1, true, 0)])
                .unwrap();

        assert_eq!(frontier[0].estimated_rows, 1);
    }

    #[test]
    fn nested_loop_cost_prefers_a_selective_outer_with_an_indexed_probe() {
        let full_outer = RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: 100,
        };
        let unique_probe = path("documents_pk", &["id"], 1, true, 1);
        let unique_outer = path("documents_owner", &["owner_kind", "owner_id"], 2, true, 1);
        let indexed_probe = path(
            "chunks_document",
            &["document_id", "ordinal"],
            1,
            false,
            100,
        );

        let syntax_order = estimate_relational_nested_loop_join_cost(&full_outer, &unique_probe);
        let reordered = estimate_relational_nested_loop_join_cost(&unique_outer, &indexed_probe);

        assert_eq!(syntax_order.estimated_work, 200);
        assert_eq!(reordered.estimated_work, 101);
    }

    #[test]
    fn nested_loop_cost_saturates_for_untrusted_cardinality_inputs() {
        let outer = path("outer", &["id"], 1, false, usize::MAX);
        let inner = path("inner", &["id"], 1, false, usize::MAX);

        assert_eq!(
            estimate_relational_nested_loop_join_cost(&outer, &inner).estimated_work,
            usize::MAX
        );
    }
}
