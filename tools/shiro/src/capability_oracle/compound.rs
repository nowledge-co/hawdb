// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::VecDeque;

#[derive(Clone, Copy)]
enum SetOperation {
    Union,
    Intersect,
    Except,
}

impl SetOperation {
    fn keyword(self) -> &'static str {
        match self {
            Self::Union => "UNION",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        }
    }

    fn multiplicity(self, left: usize, right: usize, all: bool) -> usize {
        if all {
            match self {
                Self::Union => left + right,
                Self::Intersect => left.min(right),
                Self::Except => left.saturating_sub(right),
            }
        } else {
            usize::from(match self {
                Self::Union => left != 0 || right != 0,
                Self::Intersect => left != 0 && right != 0,
                Self::Except => left != 0 && right == 0,
            })
        }
    }
}

fn set_operations(case: &Case, db: &mut Database, run: &mut Run) -> Result<()> {
    run.sql(
        db,
        "compound_schema",
        "CREATE TABLE operands (side BIGINT NOT NULL, value BIGINT)",
        &[],
    )?;
    let mut counts = BTreeMap::<Option<i64>, [usize; 2]>::new();
    let left = (case.variant % 5) as i64;
    let right = ((case.variant / 5) % 5) as i64;
    for number in &case.values {
        let value = (number.rem_euclid(5) != 0).then_some(number.rem_euclid(3));
        for (side, cutoff) in [left, right].into_iter().enumerate() {
            let selection = if side == 0 { *number } else { -*number };
            if selection.rem_euclid(5) >= cutoff {
                continue;
            }
            counts.entry(value).or_default()[side] += 1;
            run.sql(
                db,
                "compound_insert",
                "INSERT INTO operands VALUES ($1, $2)",
                &[
                    Value::Int(side as i64),
                    value.map_or(Value::Null, Value::Int),
                ],
            )?;
        }
    }
    for op in [
        SetOperation::Union,
        SetOperation::Intersect,
        SetOperation::Except,
    ] {
        for all in [false, true] {
            let keyword = op.keyword();
            let quantifier = if all { " ALL" } else { "" };
            let phase = format!(
                "compound_{}{}",
                keyword.to_ascii_lowercase(),
                if all { "_all" } else { "" }
            );
            let sql = format!("SELECT value FROM operands WHERE side = $1 {keyword}{quantifier} SELECT value FROM operands WHERE side = $2");
            let actual = run.sql(db, &phase, &sql, &[Value::Int(0), Value::Int(1)])?;
            let expected = counts
                .iter()
                .flat_map(|(key, &[left, right])| {
                    std::iter::repeat_n(
                        row(&[("value", key.map_or(Value::Null, Value::Int))]),
                        op.multiplicity(left, right, all),
                    )
                })
                .collect::<Vec<_>>();
            run.equal(&phase, &actual, &expected, false)?;
        }
    }
    Ok(())
}

fn recursive(case: &Case, db: &mut Database, run: &mut Run) -> Result<()> {
    run.sql(
        db,
        "recursive_schema",
        "CREATE TABLE edges (id BIGINT PRIMARY KEY, src BIGINT NOT NULL, dst BIGINT NOT NULL)",
        &[],
    )?;
    let mut edges = Vec::new();
    for (id, number) in case.values.iter().copied().enumerate() {
        let (src, dst) = (number.rem_euclid(4), number.div_euclid(4).rem_euclid(4));
        run.sql(
            db,
            "recursive_insert",
            "INSERT INTO edges VALUES ($1, $2, $3)",
            &[Value::Int(id as i64), Value::Int(src), Value::Int(dst)],
        )?;
        edges.push((src, dst));
    }
    let root_id = case.variant as usize % (edges.len() + 1);
    let root = edges.get(root_id).map(|(src, _)| *src);
    let children = |node| {
        edges
            .iter()
            .filter(move |(src, _)| *src == node)
            .map(|&(_, dst)| dst)
    };
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from_iter(root);
    while let Some(node) = queue.pop_front() {
        if seen.insert(node) {
            queue.extend(children(node));
        }
    }
    let actual = run.sql(
        db,
        "recursive_union",
        "WITH RECURSIVE reach(node) AS (SELECT src FROM edges WHERE id = $1 \
         UNION SELECT edges.dst FROM edges JOIN reach ON edges.src = reach.node) \
         SELECT node FROM reach ORDER BY node",
        &[Value::Int(root_id as i64)],
    )?;
    let ordered = seen
        .into_iter()
        .map(|node| row(&[("node", Value::Int(node))]))
        .collect::<Vec<_>>();
    run.equal("recursive_union", &actual, &ordered, true)?;

    let max_depth = (case.variant % 4) as i64;
    let mut frontier = root.into_iter().collect::<Vec<_>>();
    let mut walks = Vec::new();
    for depth in 0..=max_depth {
        walks.extend(
            frontier
                .iter()
                .map(|&node| row(&[("node", Value::Int(node)), ("depth", Value::Int(depth))])),
        );
        if depth < max_depth {
            frontier = frontier.into_iter().flat_map(children).collect();
        }
    }
    let actual = run.sql(
        db,
        "recursive_union_all",
        "WITH RECURSIVE walk(node, depth) AS (SELECT src, 0 FROM edges WHERE id = $1 \
         UNION ALL SELECT edges.dst, walk.depth + 1 FROM edges JOIN walk ON edges.src = walk.node \
         WHERE walk.depth < $2) SELECT node, depth FROM walk",
        &[Value::Int(root_id as i64), Value::Int(max_depth)],
    )?;
    run.equal("recursive_union_all", &actual, &walks, false)
}

pub(super) fn check(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    set_operations(case, &mut db, run)?;
    recursive(case, &mut db, run)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compound_query_models_cover_bags_nulls_cycles_and_empty_anchors() {
        assert_eq!(SetOperation::Except.multiplicity(3, 1, true), 2);
        assert_eq!(SetOperation::Except.multiplicity(3, 1, false), 0);
        for values in [vec![], vec![0, 0, 1, 1, 4, 4, 5, 5, 10, -1]] {
            for variant in [0, 3, 10, 19, 24] {
                let case = Case {
                    seed: 7,
                    family: 4,
                    variant,
                    values: values.clone(),
                };
                let mut run = Run::default();
                check(&case, &mut run).unwrap();
                for phase in [
                    "compound_union_all",
                    "compound_intersect_all",
                    "compound_except_all",
                    "recursive_union",
                    "recursive_union_all",
                ] {
                    assert!(run.verified.contains(phase));
                }
            }
        }
    }
}
