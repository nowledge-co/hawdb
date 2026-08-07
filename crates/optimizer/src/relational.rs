use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationalAccessPathKind {
    FullScan,
    PrimaryKey,
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalAccessPathDescriptor {
    pub kind: RelationalAccessPathKind,
    pub name: String,
    pub index_columns: Vec<String>,
    pub access_columns: BTreeSet<String>,
    pub equality_prefix_len: usize,
    pub order_prefix_len: usize,
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
            && (self.unique_point || !other.unique_point)
            && (self.covering || !other.covering)
            && (!self.requires_row_fetch || other.requires_row_fetch)
            && self.estimated_rows <= other.estimated_rows;
        let strictly_better = self.access_columns != other.access_columns
            || self.equality_prefix_len > other.equality_prefix_len
            || self.order_prefix_len > other.order_prefix_len
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
    for candidate in candidates {
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
        left.estimated_rows
            .cmp(&right.estimated_rows)
            .then_with(|| right.unique_point.cmp(&left.unique_point))
            .then_with(|| right.equality_prefix_len.cmp(&left.equality_prefix_len))
            .then_with(|| right.order_prefix_len.cmp(&left.order_prefix_len))
            .then_with(|| right.covering.cmp(&left.covering))
            .then_with(|| left.requires_row_fetch.cmp(&right.requires_row_fetch))
            .then_with(|| left.index_columns.len().cmp(&right.index_columns.len()))
            .then_with(|| left.name.cmp(&right.name))
    }))
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
}
