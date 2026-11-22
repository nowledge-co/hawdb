// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

struct Input {
    body: Option<String>,
    payload: Option<Vec<u8>>,
}

impl Input {
    fn generate(value: i64) -> Self {
        let token = ["", "ascii", "é", "🦀"][value.rem_euclid(4) as usize];
        let repeats = value.rem_euclid(5) as usize;
        Self {
            body: (value.rem_euclid(3) != 0).then(|| token.repeat(repeats)),
            payload: (value.rem_euclid(7) != 0).then(|| vec![value as u8; repeats]),
        }
    }
}

#[derive(Clone, Copy)]
enum Octets {
    Text,
    Binary,
}

impl Octets {
    fn column(self) -> &'static str {
        match self {
            Self::Text => "body",
            Self::Binary => "payload",
        }
    }

    fn sql(self) -> String {
        format!("OCTET_LENGTH({})", self.column())
    }

    fn evaluate(self, input: &Input) -> Option<i64> {
        match self {
            Self::Text => input.body.as_ref().map(String::len),
            Self::Binary => input.payload.as_ref().map(Vec::len),
        }
        .map(|length| length as i64)
    }
}

fn sql(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    run.sql(
        &mut db,
        "functions_sql_schema",
        "CREATE TABLE lengths (id BIGINT PRIMARY KEY, body TEXT, payload BYTEA)",
        &[],
    )?;
    let inputs = case
        .values
        .iter()
        .copied()
        .map(Input::generate)
        .collect::<Vec<_>>();
    for (id, input) in inputs.iter().enumerate() {
        run.sql(
            &mut db,
            "functions_sql_insert",
            "INSERT INTO lengths VALUES ($1, $2, $3)",
            &[
                Value::Int(id as i64),
                input.body.clone().map_or(Value::Null, text),
                input.payload.clone().map_or(Value::Null, Value::Binary),
            ],
        )?;
    }
    let (raw, fallback) = if case.variant.is_multiple_of(2) {
        (Octets::Text, Octets::Binary)
    } else {
        (Octets::Binary, Octets::Text)
    };
    let fallback_value = -(case.variant as i64);
    let query = format!(
        "SELECT id, {} AS raw, COALESCE({}, $1) AS resolved FROM lengths ORDER BY id",
        raw.sql(),
        fallback.sql()
    );
    let actual = run.sql(
        &mut db,
        "functions_sql_scalar",
        &query,
        &[Value::Int(fallback_value)],
    )?;
    let expected = inputs
        .iter()
        .enumerate()
        .map(|(id, input)| {
            row(&[
                ("id", Value::Int(id as i64)),
                ("raw", raw.evaluate(input).map_or(Value::Null, Value::Int)),
                (
                    "resolved",
                    Value::Int(fallback.evaluate(input).unwrap_or(fallback_value)),
                ),
            ])
        })
        .collect::<Vec<_>>();
    run.equal("functions_sql_scalar", &actual, &expected, true)?;

    let minimum = (case.variant % 8) as i64;
    let expression = raw.sql();
    let query = format!(
        "SELECT COUNT({}) AS n, SUM({expression}) AS total, \
         MIN(id) AS smallest, MAX(id) AS largest \
         FROM lengths WHERE {} >= $1",
        raw.column(),
        fallback.sql()
    );
    let actual = run.sql(
        &mut db,
        "functions_sql_aggregate",
        &query,
        &[Value::Int(minimum)],
    )?;
    let selected = inputs
        .iter()
        .enumerate()
        .filter(|(_, input)| {
            fallback
                .evaluate(input)
                .is_some_and(|length| length >= minimum)
        })
        .collect::<Vec<_>>();
    let lengths = selected
        .iter()
        .filter_map(|(_, input)| raw.evaluate(input))
        .collect::<Vec<_>>();
    let total = if lengths.is_empty() {
        Value::Null
    } else {
        Value::Int(lengths.iter().sum())
    };
    run.equal(
        "functions_sql_aggregate",
        &actual,
        &[row(&[
            ("n", Value::Int(lengths.len() as i64)),
            ("total", total),
            (
                "smallest",
                selected
                    .first()
                    .map_or(Value::Null, |(id, _)| Value::Int(*id as i64)),
            ),
            (
                "largest",
                selected
                    .last()
                    .map_or(Value::Null, |(id, _)| Value::Int(*id as i64)),
            ),
        ])],
        true,
    )
}

fn cypher(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    for statement in [
        "CREATE NODE TABLE FunctionRow",
        "CREATE PROPERTY ON NODE TABLE FunctionRow(id) TYPE INT NOT NULL",
        "CREATE PROPERTY ON NODE TABLE FunctionRow(doc) TYPE STRING",
    ] {
        run.cypher(&mut db, "functions_cypher_schema", statement, &[])?;
    }
    let mut expected = Vec::new();
    for (id, value) in case.values.iter().copied().enumerate() {
        let token = ["", "é", "🦀", "quoted \"string\""][case.variant as usize % 4];
        let (document, string, scalar) = match value.rem_euclid(6) {
            0 => (json!({"nested": {"x": token}}), text(token), text(token)),
            1 => (
                json!({"nested": {"x": value}}),
                Value::Null,
                Value::Int(value),
            ),
            2 => (
                json!({"nested": {"x": true}}),
                Value::Null,
                Value::Bool(true),
            ),
            3 => (json!({"nested": {"x": [value]}}), Value::Null, Value::Null),
            4 => (json!({"nested": {"x": null}}), Value::Null, Value::Null),
            _ => (json!({"nested": {}}), Value::Null, Value::Null),
        };
        let id = Value::Int(id as i64);
        run.cypher(
            &mut db,
            "functions_cypher_create",
            "CREATE (:FunctionRow {id: $id, doc: $doc})",
            &[("id", id.clone()), ("doc", text(document.to_string()))],
        )?;
        expected.push(row(&[
            ("id", id),
            (
                "string",
                if string == Value::Null {
                    text("fallback")
                } else {
                    string
                },
            ),
            ("scalar", scalar),
            ("label", text("FunctionRow")),
            ("labels", Value::List(vec![text("FunctionRow")])),
        ]));
    }
    let actual = run.cypher(
        &mut db,
        "functions_cypher_scalar",
        "MATCH (n:FunctionRow) RETURN n.id AS id, \
         coalesce(json_string(n.doc, $path), 'fallback') AS string, \
         json_value(n.doc, $path NULL ON NONSCALAR) AS scalar, \
         label(n) AS label, labels(n) AS labels ORDER BY id",
        &[("path", text("$.nested.x"))],
    )?;
    run.equal("functions_cypher_scalar", &actual, &expected, true)
}

pub(super) fn check(case: &Case, run: &mut Run) -> Result<()> {
    sql(case, run)?;
    cypher(case, run)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_functions_compare_typed_results_and_replay() {
        for (variant, values) in [
            (0, vec![]),
            (2, vec![-5, -1, 0, 1, 2, 3, 4, 5, 6]),
            (7, vec![0, 4, 4, 7, 14]),
        ] {
            let case = Case {
                seed: 42,
                family: 1,
                variant,
                values,
            };
            let mut run = Run::default();
            check(&Case::read(&case.json()).unwrap(), &mut run).unwrap();
            for phase in [
                "functions_sql_scalar",
                "functions_sql_aggregate",
                "functions_cypher_scalar",
            ] {
                assert!(run.verified.contains(phase));
            }
        }
    }
}
