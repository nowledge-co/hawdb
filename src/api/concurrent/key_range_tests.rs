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

fn immediate_transaction(database: &ConcurrentDatabase) -> ConcurrentDatabaseTransaction {
    database
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(Duration::ZERO))
        .unwrap()
}

#[test]
fn conjunctive_content_updates_keep_disjoint_keys_concurrent() {
    let database = Database::new().into_concurrent();
    database
        .query_sql(
            "CREATE TABLE thread_messages (content_message_id TEXT PRIMARY KEY, \
         thread_storage_id TEXT NOT NULL, order_index BIGINT NOT NULL, updated_at TEXT NOT NULL)",
        )
        .unwrap();
    database.query_sql(
        "INSERT INTO thread_messages (content_message_id, thread_storage_id, order_index, updated_at) \
         VALUES ('first', 'thread', 1, 'before'), ('second', 'thread', 2, 'before'), \
                ('retained', 'other', 3, 'before')",
    ).unwrap();

    let mut first = immediate_transaction(&database);
    let mut second = immediate_transaction(&database);
    first
        .query_sql_with_params(
            "UPDATE thread_messages SET order_index = $1, updated_at = $2 \
         WHERE thread_storage_id = $3 AND content_message_id = $4",
            &[
                Value::Int(11),
                Value::String("after".into()),
                Value::String("thread".into()),
                Value::String("first".into()),
            ],
        )
        .unwrap();
    second
        .query_sql_with_params(
            "UPDATE thread_messages SET order_index = $1, updated_at = $2 \
         WHERE content_message_id = $4 AND thread_storage_id = $3",
            &[
                Value::Int(22),
                Value::String("after".into()),
                Value::String("thread".into()),
                Value::String("second".into()),
            ],
        )
        .unwrap();

    let mut conflicting = immediate_transaction(&database);
    let error = conflicting
        .query_sql(
            "UPDATE thread_messages SET order_index = 99 \
         WHERE content_message_id = 'first' AND thread_storage_id = 'thread'",
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    assert!(conflicting
        .commit()
        .unwrap_err()
        .to_string()
        .contains("aborted"));
    first.commit().unwrap();
    second.commit().unwrap();

    let rows = database.query_sql(
        "SELECT content_message_id, order_index, updated_at FROM thread_messages ORDER BY content_message_id",
    ).unwrap().rows;
    assert_eq!(rows.len(), 3);
    for (row, (id, order, updated)) in rows.iter().zip([
        ("first", 11, "after"),
        ("retained", 3, "before"),
        ("second", 22, "after"),
    ]) {
        assert_eq!(row["content_message_id"], Value::String(id.into()));
        assert_eq!(row["order_index"], Value::Int(order));
        assert_eq!(row["updated_at"], Value::String(updated.into()));
    }
}

#[test]
fn conjunctive_range_locks_retain_phantom_protection() {
    let database = Database::new().into_concurrent();
    database
        .query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let mut owner = immediate_transaction(&database);
    owner
        .query_sql(
            "SELECT id FROM messages WHERE body = 'ready' AND id >= 10 AND id < 20 FOR UPDATE",
        )
        .unwrap();
    for id in [10, 15, 19] {
        let mut blocked = immediate_transaction(&database);
        let error = blocked
            .query_sql_with_params(
                "INSERT INTO messages (id, body) VALUES ($1, 'ready')",
                &[Value::Int(id)],
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("transaction lock wait timed out"));
        assert!(blocked
            .commit()
            .unwrap_err()
            .to_string()
            .contains("aborted"));
    }
    for id in [9, 20] {
        let mut disjoint = immediate_transaction(&database);
        disjoint
            .query_sql_with_params(
                "INSERT INTO messages (id, body) VALUES ($1, 'ready')",
                &[Value::Int(id)],
            )
            .unwrap();
        disjoint.commit().unwrap();
    }
    owner.rollback();
    let mut released = immediate_transaction(&database);
    released
        .query_sql("INSERT INTO messages (id, body) VALUES (15, 'ready')")
        .unwrap();
    released.commit().unwrap();
}

#[test]
fn unknown_or_branch_still_requires_a_table_lock() {
    let database = Database::new().into_concurrent();
    database
        .query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let mut owner = immediate_transaction(&database);
    owner
        .query_sql("SELECT id FROM messages WHERE id = 1 OR body = 'ready' FOR UPDATE")
        .unwrap();
    let mut blocked = immediate_transaction(&database);
    let error = blocked
        .query_sql("INSERT INTO messages (id, body) VALUES (2, 'ready')")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    owner.rollback();
}

type KeyRanges = Option<Vec<(Bound<RelationalKey>, Bound<RelationalKey>)>>;

fn derive_ranges(predicate: &str, parameters: &[Value]) -> KeyRanges {
    let sql = format!("SELECT m.id FROM messages AS m WHERE {predicate} FOR UPDATE");
    let cache = crate::relational_sql::RelationalPlanTemplateCache::new(Some(1));
    let prepared = cache.prepare(&sql).unwrap();
    let SqlStatement::Select(select) = prepared.statement() else {
        panic!("expected SELECT")
    };
    single_key_ranges(
        select.selection.as_ref().unwrap(),
        "id",
        &select.from,
        select.from_alias.as_deref(),
        parameters,
    )
}

fn key(value: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(value)])
}

fn point(value: i64) -> (Bound<RelationalKey>, Bound<RelationalKey>) {
    (Bound::Included(key(value)), Bound::Included(key(value)))
}

#[test]
fn conjunctive_key_ranges_preserve_intersections_and_safe_fallbacks() {
    for predicate in [
        "m.id = $1 AND m.body = 'ready'",
        "m.body = 'ready' AND m.id = $1",
    ] {
        assert_eq!(
            derive_ranges(predicate, &[Value::Int(3)]),
            Some(vec![point(3)])
        );
    }
    assert_eq!(
        derive_ranges(
            "m.id >= -2 AND (m.body = 'ready' OR m.id = 100) AND m.id < 4",
            &[]
        ),
        Some(vec![(Bound::Included(key(-2)), Bound::Excluded(key(4)))])
    );
    assert_eq!(
        derive_ranges("m.id = 3 AND m.id = 4 AND m.body = 'ready'", &[]),
        Some(vec![])
    );
    assert_eq!(
        derive_ranges("m.id IN (1, 3) AND m.body = 'ready'", &[]),
        Some(vec![point(1), point(3)])
    );
    assert_eq!(
        derive_ranges(
            "(m.id = 1 AND m.body = 'left') OR (m.id = 3 AND m.body = 'right')",
            &[]
        ),
        Some(vec![point(1), point(3)])
    );
    assert_eq!(derive_ranges("m.id = 3 OR m.body = 'ready'", &[]), None);
    assert_eq!(
        derive_ranges("m.body = 'left' AND m.body = 'right'", &[]),
        None
    );
}

#[derive(Debug)]
enum PredicateModel {
    Key(i64, u64),
    Body(bool),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

impl PredicateModel {
    fn sql(&self) -> String {
        match self {
            Self::Key(value, op) => format!(
                "m.id {} {value}",
                ["=", "<", "<=", ">", ">=", "<>"][*op as usize]
            ),
            Self::Body(value) => format!("m.body = '{}'", if *value { "yes" } else { "no" }),
            Self::And(left, right) => format!("({} AND {})", left.sql(), right.sql()),
            Self::Or(left, right) => format!("({} OR {})", left.sql(), right.sql()),
            Self::Not(inner) => format!("NOT ({})", inner.sql()),
        }
    }

    fn matches(&self, key: i64, body: bool) -> bool {
        match self {
            Self::Key(value, op) => match op {
                0 => key == *value,
                1 => key < *value,
                2 => key <= *value,
                3 => key > *value,
                4 => key >= *value,
                5 => key != *value,
                _ => unreachable!(),
            },
            Self::Body(value) => body == *value,
            Self::And(left, right) => left.matches(key, body) && right.matches(key, body),
            Self::Or(left, right) => left.matches(key, body) || right.matches(key, body),
            Self::Not(inner) => !inner.matches(key, body),
        }
    }
}

fn next_random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn generated_predicate(seed: &mut u64, depth: usize) -> PredicateModel {
    let shape = next_random(seed) % if depth == 0 { 2 } else { 5 };
    match shape {
        0 => PredicateModel::Key((next_random(seed) % 9) as i64 - 4, next_random(seed) % 6),
        1 => PredicateModel::Body(next_random(seed).is_multiple_of(2)),
        2 => PredicateModel::And(
            Box::new(generated_predicate(seed, depth - 1)),
            Box::new(generated_predicate(seed, depth - 1)),
        ),
        3 => PredicateModel::Or(
            Box::new(generated_predicate(seed, depth - 1)),
            Box::new(generated_predicate(seed, depth - 1)),
        ),
        _ => PredicateModel::Not(Box::new(generated_predicate(seed, depth - 1))),
    }
}

fn covers(ranges: &KeyRanges, value: i64) -> bool {
    let key = key(value);
    ranges.as_ref().is_none_or(|ranges| {
        ranges.iter().any(|(lower, upper)| {
            let lower_matches = match lower {
                Bound::Unbounded => true,
                Bound::Included(bound) => key >= *bound,
                Bound::Excluded(bound) => key > *bound,
            };
            let upper_matches = match upper {
                Bound::Unbounded => true,
                Bound::Included(bound) => key <= *bound,
                Bound::Excluded(bound) => key < *bound,
            };
            lower_matches && upper_matches
        })
    })
}

fn run_campaign(cases: usize) {
    let mut seed = 0x0232_c0de_5eed_u64;
    for case in 0..cases {
        let model = generated_predicate(&mut seed, 3);
        let sql = model.sql();
        let ranges = derive_ranges(&sql, &[]);
        let guard = (next_random(&mut seed) % 9) as i64 - 4;
        let guarded = if case.is_multiple_of(2) {
            format!("m.id = $1 AND ({sql})")
        } else {
            format!("({sql}) AND m.id = $1")
        };
        let guarded_ranges = derive_ranges(&guarded, &[Value::Int(guard)]);
        assert!(guarded_ranges.is_some(), "lost known key bound: {guarded}");
        for value in -8..=8 {
            for body in [false, true] {
                if model.matches(value, body) {
                    assert!(
                        covers(&ranges, value),
                        "under-locked {sql}: key={value}, body={body}"
                    );
                    if value == guard {
                        assert!(
                            covers(&guarded_ranges, value),
                            "under-locked {guarded}: key={value}, body={body}"
                        );
                    }
                }
            }
            if value != guard {
                assert!(
                    !covers(&guarded_ranges, value),
                    "lost selective bound: {guarded}, key={value}"
                );
            }
        }
    }
}

#[test]
fn key_range_differential_smoke() {
    run_campaign(64);
}

#[test]
#[ignore = "explicit local differential campaign"]
fn key_range_differential_campaign() {
    run_campaign(1024);
}
