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
use hawdb_storage::RelationalOverflowRef;
use std::collections::BTreeMap;

pub(super) type TestRow = [RelationalValue; 7];
pub(super) type Output = Vec<(Vec<RelationalValue>, Vec<(String, Value)>)>;

pub(super) const PROJECTIONS: &str = "COUNT(*) AS rows, COUNT(n) AS present, COUNT(DISTINCT n) AS distinct_count, SUM(n) AS total, SUM(DISTINCT n) AS distinct_sum, MAX(n) AS maximum, MAX(body) AS body_max, SUM(amount) AS amount_sum, COALESCE(SUM(n), 0) AS fallback, COUNT(*) FILTER (WHERE flag = TRUE) AS flagged, SUM(OCTET_LENGTH(payload)) AS payload_bytes";
pub(super) const HAVING: [&str; 10] = [
    "COUNT(*) >= $1",
    "SUM(n) > $1",
    "COUNT(DISTINCT n) IN (0, 1, $1)",
    "SUM(n) IS NULL",
    "NOT (MAX(n) < $1)",
    "MAX(body) LIKE 'z%'",
    "COUNT(*) >= $1 AND SUM(n) IS NOT NULL",
    "COUNT(*) = 0 OR MAX(flag) = TRUE",
    "MAX(body) NOT IN ('z', NULL)",
    "MAX(body) IS NOT NULL AND MAX(body) ILIKE 'Z%'",
];

pub(super) fn select(sql: &str) -> SelectStatement {
    let hawdb_sql::SqlStatement::Select(select) = hawdb_sql::prepare_postgres_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .statement
    else {
        panic!("expected SELECT")
    };
    select
}

pub(super) fn state() -> RelationalState {
    let mut state = RelationalState::default();
    for sql in [
        "CREATE TABLE records (id BIGINT PRIMARY KEY, bucket BIGINT, n BIGINT, amount DOUBLE PRECISION, body TEXT, flag BOOLEAN, payload BYTEA)",
        "CREATE TABLE peers (id BIGINT PRIMARY KEY, n BIGINT)",
        "CREATE TABLE composite (id BIGINT, bucket BIGINT, body TEXT, PRIMARY KEY (id, bucket))",
        "CREATE TABLE ids (id UUID PRIMARY KEY)",
    ] {
        let transaction = crate::compile_relational_statement_sql(sql, &[], &state).unwrap();
        state = state.stage_transaction(transaction, Default::default(), Default::default()).unwrap();
    }
    state
}

pub(super) fn expr(source: &str) -> Expr {
    let select = select(&format!("SELECT {source} FROM records"));
    let SelectProjection::Expression { expression, .. } =
        select.projection.into_iter().next().unwrap()
    else {
        panic!("expected expression")
    };
    expression
}

pub(super) fn function(name: &str, arguments: Vec<SqlFunctionArgument>, distinct: bool) -> Expr {
    Expr::unspanned(ExprKind::Function {
        name: name.into(),
        arguments,
        distinct,
        filter: None,
    })
}

pub(super) fn overflow(bytes: u64) -> RelationalValue {
    RelationalValue::Overflow(RelationalOverflowRef {
        digest: "00".repeat(32).parse().unwrap(),
        scalar_type: RelationalScalarType::Bytea,
        compressed_bytes: 1,
        uncompressed_bytes: bytes,
    })
}

pub(super) fn value_bytes(value: &RelationalValue) -> usize {
    std::mem::size_of::<RelationalValue>() + value.estimated_payload_bytes()
}

pub(super) fn bind<'a>(
    row: &'a TestRow,
    column: &SqlColumnRef,
) -> Result<(&'a RelationalValue, RelationalScalarType)> {
    let (index, scalar_type) = match column.name.as_str() {
        "id" => (0, RelationalScalarType::BigInt),
        "bucket" => (1, RelationalScalarType::BigInt),
        "n" => (2, RelationalScalarType::BigInt),
        "amount" => (3, RelationalScalarType::DoublePrecision),
        "body" => (4, RelationalScalarType::Text),
        "flag" => (5, RelationalScalarType::Boolean),
        "payload" => (6, RelationalScalarType::Bytea),
        _ => {
            return Err(HawDBError::Semantic(format!(
                "unknown test column {}",
                column.name
            )))
        }
    };
    Ok((&row[index], scalar_type))
}

pub(super) fn null_row() -> TestRow {
    std::array::from_fn(|_| RelationalValue::Null)
}

fn groups(rows: &[TestRow], grouped: bool) -> BTreeMap<Vec<RelationalValue>, Vec<&TestRow>> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    if !grouped {
        groups.insert(Vec::new(), Vec::new());
    }
    for row in rows {
        let key = if grouped {
            vec![row[1].clone()]
        } else {
            Vec::new()
        };
        groups.entry(key).or_default().push(row);
    }
    groups
}

pub(super) fn execute(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    rows: &[TestRow],
) -> Result<Output> {
    validate_having(select, parameters, state)?;
    let template = projection_template(select, parameters, state)?;
    let budget = std::num::NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = hawdb_executor::QueryMemoryLedger::new(budget);
    let mut output = Vec::new();
    for (key, rows) in groups(rows, !select.group_by.is_empty()) {
        let mut projections = template.clone();
        let mut tracker = OperatorMemoryTracker::with_account(
            budget,
            ledger.account(
                hawdb_executor::QueryMemoryClass::BlockingState,
                "aggregate test",
                budget,
            ),
        );
        charge_aggregate_memory(
            aggregate_group_base_memory_bytes(&key, &projections),
            &mut tracker,
        )?;
        for row in rows {
            for projection in &mut projections {
                let delta = projection.update(&|column| bind(row, column), parameters)?;
                tracker.release(delta.released_bytes);
                charge_aggregate_memory(delta.added_bytes, &mut tracker)?;
                assert_eq!(ledger.snapshot().used_bytes, tracker.used_bytes);
            }
        }
        if let Some(projections) = filter_group(projections)? {
            let values = projections
                .into_iter()
                .map(AggregateProjectionState::finish)
                .collect::<Result<_>>()?;
            output.push((key, values));
        }
        drop(tracker);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    Ok(output)
}

pub(super) fn reference(
    rows: &[TestRow],
    grouped: bool,
    condition: Option<usize>,
    threshold: i64,
) -> Output {
    let mut output = Vec::new();
    for (key, rows) in groups(rows, grouped) {
        let numbers = rows
            .iter()
            .filter_map(|row| match row[2] {
                RelationalValue::BigInt(value) => Some(value),
                RelationalValue::Null => None,
                _ => panic!("numeric fixture"),
            })
            .collect::<Vec<_>>();
        let unique = numbers.iter().copied().collect::<BTreeSet<_>>();
        let sum = (!numbers.is_empty()).then(|| {
            i64::try_from(numbers.iter().map(|value| i128::from(*value)).sum::<i128>()).unwrap()
        });
        let maximum = numbers.iter().copied().max();
        let body_max = rows
            .iter()
            .filter_map(|row| match &row[4] {
                RelationalValue::Text(value) => Some(value.as_str()),
                RelationalValue::Null => None,
                _ => panic!("text fixture"),
            })
            .max();
        let flags = rows
            .iter()
            .filter(|row| row[5] == RelationalValue::Boolean(true))
            .count();
        let keep = match condition {
            None => true,
            Some(0) => rows.len() as i64 >= threshold,
            Some(1) => sum.is_some_and(|sum| sum > threshold),
            Some(2) => [0, 1, threshold].contains(&(unique.len() as i64)),
            Some(3) => sum.is_none(),
            Some(4) => maximum.is_some_and(|maximum| maximum >= threshold),
            Some(5) => body_max.is_some_and(|body| body.starts_with('z')),
            Some(6) => rows.len() as i64 >= threshold && sum.is_some(),
            Some(7) => rows.is_empty() || flags > 0,
            Some(8) => false,
            Some(9) => body_max.is_some_and(|body| body.to_lowercase().starts_with('z')),
            _ => panic!("unknown predicate shape"),
        };
        if !keep {
            continue;
        }
        let amounts = rows
            .iter()
            .filter_map(|row| match row[3] {
                RelationalValue::DoublePrecision(value) => Some(value),
                RelationalValue::Null => None,
                _ => panic!("float fixture"),
            })
            .collect::<Vec<_>>();
        let lengths = rows
            .iter()
            .filter_map(|row| match &row[6] {
                RelationalValue::Bytea(value) => Some(value.len() as u64),
                RelationalValue::Overflow(reference) => Some(reference.uncompressed_bytes),
                RelationalValue::Null => None,
                _ => panic!("length fixture"),
            })
            .collect::<Vec<_>>();
        let mut values = Vec::new();
        if grouped {
            values.push((
                "bucket".into(),
                crate::query_value::relational_to_value(&key[0]).unwrap(),
            ));
        }
        values.extend(
            [
                ("rows", Value::Int(rows.len() as i64)),
                ("present", Value::Int(numbers.len() as i64)),
                ("distinct_count", Value::Int(unique.len() as i64)),
                ("total", sum.map_or(Value::Null, Value::Int)),
                (
                    "distinct_sum",
                    if unique.is_empty() {
                        Value::Null
                    } else {
                        Value::Int(unique.iter().sum())
                    },
                ),
                ("maximum", maximum.map_or(Value::Null, Value::Int)),
                (
                    "body_max",
                    body_max.map_or(Value::Null, |body| Value::String(body.into())),
                ),
                (
                    "amount_sum",
                    if amounts.is_empty() {
                        Value::Null
                    } else {
                        Value::Float(amounts.iter().sum())
                    },
                ),
                ("fallback", Value::Int(sum.unwrap_or(0))),
                ("flagged", Value::Int(flags as i64)),
                (
                    "payload_bytes",
                    if lengths.is_empty() {
                        Value::Null
                    } else {
                        Value::Int(lengths.iter().sum::<u64>() as i64)
                    },
                ),
            ]
            .into_iter()
            .map(|(name, value)| (name.into(), value)),
        );
        output.push((key, values));
    }
    output
}

pub(super) struct Rng(pub(super) u64);
impl Rng {
    pub(super) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub(super) fn rows(&mut self, count: usize) -> Vec<TestRow> {
        (0..count)
            .map(|id| {
                let n = self.next();
                let body = self.next();
                let flag = self.next();
                let payload = self.next();
                [
                    RelationalValue::BigInt(id as i64),
                    RelationalValue::BigInt((n % 3) as i64),
                    if n.is_multiple_of(4) {
                        RelationalValue::Null
                    } else {
                        RelationalValue::BigInt((n % 7) as i64 - 3)
                    },
                    if n.is_multiple_of(3) {
                        RelationalValue::Null
                    } else {
                        RelationalValue::DoublePrecision(((n % 17) as i64 - 8) as f64 / 2.0)
                    },
                    match body % 4 {
                        0 => RelationalValue::Null,
                        1 => {
                            RelationalValue::Text(format!("z{}", "a".repeat((body % 13) as usize)))
                        }
                        _ => RelationalValue::Text("\u{e9}".repeat((body % 11) as usize)),
                    },
                    match flag % 3 {
                        0 => RelationalValue::Null,
                        value => RelationalValue::Boolean(value == 1),
                    },
                    match payload % 4 {
                        0 => RelationalValue::Null,
                        1 => overflow(payload % 128 + 1),
                        _ => RelationalValue::Bytea(vec![
                            (payload % 256) as u8;
                            (payload % 17) as usize
                        ]),
                    },
                ]
            })
            .collect()
    }
}
