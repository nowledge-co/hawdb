// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn pipeline(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    let mut entities = Vec::new();
    let mut sources = BTreeMap::<String, i64>::new();
    for (index, value) in case.values.iter().enumerate() {
        let id = format!("item-{index}");
        let name = if value.is_negative() {
            "Alpha"
        } else {
            "Other"
        };
        let aliases = match value.rem_euclid(4) {
            0 => Value::Null,
            1 => Value::List(vec![]),
            2 => Value::List(vec![text("ALPHA"), text("ALPHA")]),
            _ => Value::List(vec![text("Other"), text("")]),
        };
        run.cypher(
            &mut db,
            "entity",
            "CREATE (:Entity {id: $id, name: $name, aliases: $aliases})",
            &[
                ("id", text(&id)),
                ("name", text(name)),
                ("aliases", aliases.clone()),
            ],
        )?;
        let mut search = match aliases {
            Value::List(items) => items
                .into_iter()
                .map(|item| match item {
                    Value::String(s) => s,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
            Value::Null => vec![String::new()],
            _ => unreachable!(),
        };
        search.extend([name.to_string(), id.clone()]);
        entities.push((id, search));
        let source = match value.rem_euclid(4) {
            0 => None,
            1 => Some("Alpha Code"),
            2 => Some("ALPHA CODE"),
            _ => Some("Other"),
        };
        run.cypher(
            &mut db,
            "thread",
            "CREATE (:Thread {source: $source})",
            &[("source", source.map_or(Value::Null, text))],
        )?;
        if let Some(source) = source {
            *sources
                .entry(source.replace(' ', "_").to_lowercase())
                .or_default() += 1;
        }
    }
    const SEARCH: &str = "MATCH (e:Entity) UNWIND list_append(list_append(COALESCE(e.aliases, ['']), e.name), e.id) AS term WITH e, term WHERE lower(term) CONTAINS $needle WITH DISTINCT e RETURN e.id AS id ORDER BY id";
    for needle in ["alpha", "item", "missing", "", "other"] {
        let mut expected = entities
            .iter()
            .filter(|(_, terms)| {
                terms
                    .iter()
                    .any(|term| term.to_lowercase().contains(needle))
            })
            .map(|(id, _)| row(&[("id", text(id))]))
            .collect::<Vec<_>>();
        expected.sort_by(|a, b| a["id"].cmp(&b["id"]));
        let actual = run.cypher(
            &mut db,
            "unwind_with_rebind",
            SEARCH,
            &[("needle", text(needle))],
        )?;
        run.equal("unwind_with_rebind", &actual, &expected, true)?;
    }
    let actual = run.cypher(&mut db, "normalized_group", "MATCH (t:Thread) WHERE t.source IS NOT NULL WITH LOWER(regexp_replace(t.source, ' ', '_', 'g')) AS source, COUNT(t) AS count RETURN source, count ORDER BY source", &[])?;
    let expected = sources
        .into_iter()
        .map(|(source, count)| row(&[("source", text(source)), ("count", Value::Int(count))]))
        .collect::<Vec<_>>();
    run.equal("normalized_group", &actual, &expected, true)?;
    let values = case
        .values
        .iter()
        .map(|v| Value::Int(*v))
        .collect::<Vec<_>>();
    let tail = Value::List(vec![Value::Int(case.variant as i64), Value::Null]);
    let actual = run.cypher(
        &mut db,
        "nested_list_append",
        "UNWIND list_append($items, $tail) AS item RETURN item",
        &[
            ("items", Value::List(values.clone())),
            ("tail", tail.clone()),
        ],
    )?;
    let expected = values
        .into_iter()
        .chain([tail])
        .map(|v| row(&[("item", v)]))
        .collect::<Vec<_>>();
    run.equal("nested_list_append", &actual, &expected, true)
}

pub(super) fn binding_set(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    let mut expected = Vec::new();
    for id in 0..3 {
        let value = case.values.get(id).copied().unwrap_or(id as i64);
        expected.push(value);
        run.cypher(
            &mut db,
            "create_binding",
            "CREATE (:Memory {id: $id, value: $value, space: $space})",
            &[
                ("id", Value::Int(id as i64)),
                ("value", Value::Int(value)),
                (
                    "space",
                    [Value::Null, text(" \t\n\u{a0}"), text("private")][id].clone(),
                ),
            ],
        )?;
    }
    const SET: &str = "MATCH (a:Memory {id: $left}), (b:Memory {id: $right}) SET a.value = $first, b.value = $second";
    // Include distinct bindings, aliases and a missing endpoint in every case.
    for (step, (left, right)) in [(0, 1), (1, 1), (0, 99)].into_iter().enumerate() {
        let first = case
            .values
            .get(step)
            .copied()
            .unwrap_or(case.variant as i64);
        let second = first + 1;
        run.cypher(
            &mut db,
            "multi_binding_set",
            SET,
            &[
                ("left", Value::Int(left)),
                ("right", Value::Int(right)),
                ("first", Value::Int(first)),
                ("second", Value::Int(second)),
            ],
        )?;
        if right < 3 {
            expected[left as usize] = first;
            expected[right as usize] = second;
        }
        let actual = run.cypher(
            &mut db,
            "binding_result",
            "MATCH (m:Memory) RETURN m.id AS id, m.value AS value ORDER BY id",
            &[],
        )?;
        let rows = expected
            .iter()
            .enumerate()
            .map(|(id, value)| row(&[("id", Value::Int(id as i64)), ("value", Value::Int(*value))]))
            .collect::<Vec<_>>();
        run.equal("binding_result", &actual, &rows, true)?;
    }
    let actual = run.cypher(&mut db, "trim_case", "MATCH (m:Memory) WHERE CASE WHEN m.space IS NULL OR trim(m.space) = '' THEN 'default' ELSE m.space END = $space RETURN m.id AS id ORDER BY id", &[("space", text("default"))])?;
    run.equal(
        "trim_case",
        &actual,
        &[row(&[("id", Value::Int(0))]), row(&[("id", Value::Int(1))])],
        true,
    )
}

fn date(day: i64) -> Value {
    Value::Date(hawdb::temporal::parse_date(&format!("2026-10-{day:02}")).expect("bounded day"))
}

pub(super) fn temporal(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    let mut ranges = Vec::new();
    for (id, value) in case.values.iter().chain([&0]).enumerate() {
        let day = (value % 3 != 0).then(|| value.rem_euclid(28) + 1);
        run.cypher(
            &mut db,
            "temporal_create",
            "CREATE (:Event {id: $id, first: $day, last: $day})",
            &[
                ("id", Value::Int(id as i64)),
                ("day", day.map_or(Value::Null, date)),
            ],
        )?;
        ranges.push((day, day));
    }
    const SET: &str = "MATCH (e:Event) SET e.first = CASE WHEN $incoming IS NULL THEN e.first WHEN $preserve = true AND e.first IS NOT NULL AND e.first < $incoming THEN e.first ELSE $incoming END, e.last = CASE WHEN $incoming IS NULL THEN e.last WHEN $preserve = true AND e.last IS NOT NULL AND e.last > $incoming THEN e.last ELSE $incoming END";
    for (incoming, preserve) in [
        (None, true),
        (Some((case.variant % 28 + 1) as i64), true),
        (Some(1), true),
        (Some(28), false),
        (None, false),
    ] {
        run.cypher(
            &mut db,
            "temporal_set",
            SET,
            &[
                ("incoming", incoming.map_or(Value::Null, date)),
                ("preserve", Value::Bool(preserve)),
            ],
        )?;
        if let Some(day) = incoming {
            for (first, last) in &mut ranges {
                *first = Some(if preserve {
                    first.map_or(day, |old| old.min(day))
                } else {
                    day
                });
                *last = Some(if preserve {
                    last.map_or(day, |old| old.max(day))
                } else {
                    day
                });
            }
        }
        let actual = run.cypher(
            &mut db,
            "temporal_result",
            "MATCH (e:Event) RETURN e.id AS id, e.first AS first, e.last AS last ORDER BY id",
            &[],
        )?;
        let expected = ranges
            .iter()
            .enumerate()
            .map(|(id, (first, last))| {
                row(&[
                    ("id", Value::Int(id as i64)),
                    ("first", first.map_or(Value::Null, date)),
                    ("last", last.map_or(Value::Null, date)),
                ])
            })
            .collect::<Vec<_>>();
        run.equal("temporal_result", &actual, &expected, true)?;
    }
    let hour = case.variant % 20;
    let east = Value::DateTime(
        hawdb::temporal::parse_datetime(&format!("2026-10-30T{:02}:00+02:00", hour + 2)).unwrap(),
    );
    let utc = Value::DateTime(
        hawdb::temporal::parse_datetime(&format!("2026-10-30T{hour:02}:00Z")).unwrap(),
    );
    let actual = run.cypher(
        &mut db,
        "temporal_offset",
        "UNWIND [1] AS one RETURN CASE WHEN $east = $utc THEN true ELSE false END AS same, date($east) AS day",
        &[("east", east), ("utc", utc)],
    )?;
    run.equal(
        "temporal_offset",
        &actual,
        &[row(&[("same", Value::Bool(false)), ("day", date(30))])],
        true,
    )
}
