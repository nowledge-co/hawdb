// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Primary-key prefix intervals (#1523). Each query is compared with the same
//! rows stored under a surrogate key, whose only access path is a full scan.

use super::*;
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

const KEYED: &str =
    "CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, c BIGINT NOT NULL, v BIGINT, \
     PRIMARY KEY (a, b, c))";
const SURROGATE: &str = "CREATE TABLE t (rid BIGINT PRIMARY KEY, a BIGINT NOT NULL, \
                         b TEXT NOT NULL, c BIGINT NOT NULL, v BIGINT)";

struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> i64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        i64::try_from((self.0 >> 33) % bound).unwrap()
    }
}

const TEXTS: [&str; 4] = ["", "x", "x\u{0}", "y"];

/// Distinct random keys with an occasional NULL payload.
fn random_rows(seed: u64, count: usize) -> Vec<(i64, &'static str, i64, Option<i64>)> {
    let mut random = Lcg(seed);
    let mut keys = BTreeSet::new();
    let mut rows = Vec::new();
    while rows.len() < count {
        let a = random.next(4);
        let b = TEXTS[usize::try_from(random.next(4)).unwrap()];
        let c = random.next(40) - 8;
        if keys.insert((a, b, c)) {
            let v = (random.next(5) != 0).then(|| random.next(10));
            rows.push((a, b, c, v));
        }
    }
    rows
}

/// One query shape: its SQL, the parameter sets to bind and whether the plan
/// must be a primary-key interval whose order removes the sort.
struct Case {
    sql: String,
    params: Vec<Vec<Value>>,
    ordered: bool,
    deterministic: bool,
    unordered: bool,
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    let bounds: [(&str, Vec<Vec<Value>>); 9] = [
        ("", vec![vec![]]),
        (
            "AND c > $3",
            vec![vec![Value::Int(-1)], vec![Value::Int(i64::MAX)]],
        ),
        (
            "AND c >= $3",
            vec![vec![Value::Int(5)], vec![Value::Int(i64::MIN)]],
        ),
        (
            "AND c < $3",
            vec![vec![Value::Int(7)], vec![Value::Int(-8)]],
        ),
        (
            "AND c <= $3",
            vec![vec![Value::Int(7)], vec![Value::Int(i64::MAX)]],
        ),
        (
            "AND c > $3 AND c <= $4",
            vec![
                vec![Value::Int(2), Value::Int(20)],
                vec![Value::Int(9), Value::Int(9)],
                vec![Value::Int(20), Value::Int(2)],
            ],
        ),
        (
            "AND c >= $3 AND c < $4 AND c > $5",
            vec![
                vec![Value::Int(3), Value::Int(9), Value::Int(3)],
                vec![Value::Int(4), Value::Int(4), Value::Int(0)],
            ],
        ),
        (
            "AND c >= $3 AND c <= $4",
            vec![
                vec![Value::Int(9), Value::Int(9)],
                vec![Value::Null, Value::Int(9)],
            ],
        ),
        ("AND c > $3", vec![vec![Value::Null]]),
    ];
    for (bound, bound_params) in &bounds {
        for residual in ["", " AND v > 3"] {
            for (order, ordered) in [
                ("", false),
                (" ORDER BY c", true),
                (" ORDER BY c DESC", false),
                (" ORDER BY a, b, c", false),
            ] {
                for page in ["", " LIMIT 1", " OFFSET 2 LIMIT 3"] {
                    // Without ORDER BY a page has no defined membership.
                    let deterministic = !order.is_empty() || page.is_empty();
                    let unordered = order.is_empty();
                    let sql = format!(
                        "SELECT a, b, c, v FROM t WHERE a = $1 AND b = $2 {bound}{residual}{order}{page}"
                    );
                    let params = [Value::Int(1), Value::Int(2), Value::Int(3)]
                        .iter()
                        .flat_map(|a| {
                            [Value::String("x".into()), Value::String("x\u{0}".into())]
                                .into_iter()
                                .flat_map(move |b| {
                                    bound_params.iter().map(move |rest| {
                                        let mut params = vec![a.clone(), b.clone()];
                                        params.extend(rest.iter().cloned());
                                        params
                                    })
                                })
                        })
                        .collect();
                    cases.push(Case {
                        sql,
                        params,
                        // A residual is not covered by the interval; ordered
                        // streaming still removes the sort.
                        ordered,
                        deterministic,
                        unordered,
                    });
                }
            }
        }
    }
    // One-column prefixes, bounds on the second key column, and bounds on the
    // first key column without any equality prefix.
    for (sql, params, ordered) in [
        (
            "SELECT a, b, c FROM t WHERE a = $1 ORDER BY b, c LIMIT 4",
            vec![vec![Value::Int(2)]],
            true,
        ),
        (
            "SELECT a, b, c FROM t WHERE a = $1 AND b > $2 ORDER BY b, c",
            vec![
                vec![Value::Int(2), Value::String("x".into())],
                vec![Value::Int(0), Value::String("".into())],
            ],
            true,
        ),
        (
            "SELECT a, b, c FROM t WHERE a = $1 AND b <= $2 ORDER BY b, c LIMIT 5",
            vec![vec![Value::Int(3), Value::String("x".into())]],
            true,
        ),
        (
            "SELECT a, b, c FROM t WHERE a = $1 AND b >= $2 AND b < $3 ORDER BY b DESC, c DESC",
            vec![vec![
                Value::Int(1),
                Value::String("x".into()),
                Value::String("y".into()),
            ]],
            false,
        ),
        (
            "SELECT a, b, c FROM t WHERE a > $1 ORDER BY a, b, c LIMIT 6",
            vec![vec![Value::Int(1)], vec![Value::Int(3)]],
            true,
        ),
        (
            "SELECT a, b, c FROM t WHERE a >= $1 AND a < $2 AND c = 4 ORDER BY a, b, c",
            vec![vec![Value::Int(1), Value::Int(3)]],
            false,
        ),
        (
            "SELECT COUNT(*) AS n FROM t WHERE a = $1 AND b = $2 AND c > $3",
            vec![vec![
                Value::Int(1),
                Value::String("x".into()),
                Value::Int(4),
            ]],
            false,
        ),
        (
            "SELECT a, b, c FROM t AS q WHERE q.a = $1 AND q.b = $2 AND q.c < $3 ORDER BY q.c",
            vec![vec![
                Value::Int(2),
                Value::String("y".into()),
                Value::Int(12),
            ]],
            true,
        ),
    ] {
        cases.push(Case {
            sql: sql.to_string(),
            params,
            ordered,
            deterministic: true,
            unordered: sql.contains("COUNT"),
        });
    }
    cases
}

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

fn values(output: &QueryOutput) -> Vec<Vec<Value>> {
    output
        .rows
        .iter()
        .map(|row| row.values().cloned().collect())
        .collect()
}

fn assert_interval_plan(plan: &QueryOutput, sql: &str, ordered: bool) {
    let ids = plan
        .rows
        .iter()
        .filter_map(|row| match row.get("id") {
            Some(Value::String(id)) => Some(id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        ids.iter().any(|id| id.contains("TableRangeScanExec")),
        "{sql}: {ids:?}"
    );
    assert!(
        plan.rows.iter().any(|row| matches!(row.get("access object"), Some(Value::String(access)) if access.ends_with("primary_key_range"))),
        "{sql}: {plan:?}"
    );
    if ordered {
        assert!(
            ids.iter()
                .all(|id| !id.contains("TopNExec") && !id.contains("SortExec")),
            "{sql}: {ids:?}"
        );
    }
}

/// Compares one query, in a statement or an open transaction on both sides.
fn compare(
    sql: &str,
    params: &[Value],
    (deterministic, unordered): (bool, bool),
    actual: Result<QueryOutput>,
    expected: Result<QueryOutput>,
    unpaged: impl FnOnce() -> Vec<Vec<Value>>,
) {
    let actual = values(&actual.unwrap_or_else(|error| panic!("{sql} {params:?}: {error}")));
    let expected = values(&expected.unwrap());
    if deterministic && unordered {
        assert_eq!(sorted(actual), sorted(expected), "{sql} {params:?}");
    } else if deterministic {
        assert_eq!(actual, expected, "{sql} {params:?}");
    } else {
        // An unordered page must be a correctly sized subset of all matches.
        assert_eq!(actual.len(), expected.len(), "{sql} {params:?}");
        let all = unpaged();
        assert!(
            actual.iter().all(|row| all.contains(row)),
            "{sql} {params:?}"
        );
    }
}

fn unpaged_sql(sql: &str) -> String {
    sql.split(" LIMIT")
        .next()
        .unwrap()
        .split(" OFFSET")
        .next()
        .unwrap()
        .to_string()
}

fn check_database(db: &mut Database, reference: &mut Database, context: &str) {
    for case in cases() {
        for params in &case.params {
            let plan = db
                .query_sql_with_params(&format!("EXPLAIN {}", case.sql), params)
                .unwrap();
            assert_interval_plan(&plan, &case.sql, case.ordered);
            let actual = db.query_sql_with_params(&case.sql, params);
            let expected = reference.query_sql_with_params(&case.sql, params);
            let unpaged = unpaged_sql(&case.sql);
            compare(
                &format!("{context}: {}", case.sql),
                params,
                (case.deterministic, case.unordered),
                actual,
                expected,
                || values(&reference.query_sql_with_params(&unpaged, params).unwrap()),
            );
        }
    }
}

type Mutation = (&'static str, Vec<Value>);

fn text(value: &str) -> Value {
    Value::String(value.to_string())
}

/// Uncommitted writes that cross interval boundaries: inserts beside the
/// bounds, a key moved between prefixes, payload updates and deletes.
fn private_mutations() -> Vec<Mutation> {
    vec![
        (
            "INSERT INTO t (a, b, c, v) VALUES (1, $1, -9, 9), (1, $1, 45, 8), (2, $2, 40, 7), (3, $1, -30, NULL)",
            vec![text("x"), text("x\u{0}")],
        ),
        ("UPDATE t SET v = 4 WHERE a = 1 AND b = $1", vec![text("x")]),
        (
            "UPDATE t SET a = 1, c = c + 100 WHERE a = 2 AND b = $1 AND c > 10",
            vec![text("x")],
        ),
        (
            "DELETE FROM t WHERE a = 1 AND b = $1 AND c <= 5",
            vec![text("x\u{0}")],
        ),
        ("DELETE FROM t WHERE a = 3 AND c > 20", vec![]),
    ]
}

/// Committed writes after a checkpoint remain in the live overlay.
fn committed_mutations() -> Vec<Mutation> {
    vec![
        (
            "INSERT INTO t (a, b, c, v) VALUES (2, $1, 50, 1), (2, $2, -20, 2), (1, $3, 77, 5)",
            vec![text("x"), text("y"), text("x\u{0}")],
        ),
        (
            "UPDATE t SET v = NULL WHERE a = 2 AND b = $1 AND c < 5",
            vec![text("y")],
        ),
        (
            "DELETE FROM t WHERE a = 3 AND b = $1 AND c >= 9",
            vec![text("x")],
        ),
    ]
}

fn seed(db: &mut Database, keyed: bool, rows: &[(i64, &'static str, i64, Option<i64>)]) {
    db.query_sql(if keyed { KEYED } else { SURROGATE }).unwrap();
    for (rid, (a, b, c, v)) in rows.iter().enumerate() {
        let mut params = vec![
            Value::Int(*a),
            text(b),
            Value::Int(*c),
            v.map_or(Value::Null, Value::Int),
        ];
        if keyed {
            db.query_sql_with_params(
                "INSERT INTO t (a, b, c, v) VALUES ($1, $2, $3, $4)",
                &params,
            )
            .unwrap();
        } else {
            params.push(Value::Int(rid as i64));
            db.query_sql_with_params(
                "INSERT INTO t (rid, a, b, c, v) VALUES ($5, $1, $2, $3, $4)",
                &params,
            )
            .unwrap();
        }
    }
}

/// The surrogate table needs explicit row ids for inserted rows.
fn for_rows(mutations: &[Mutation], keyed: bool, base: usize) -> Vec<(String, Vec<Value>)> {
    mutations
        .iter()
        .enumerate()
        .map(|(ordinal, (sql, params))| {
            if keyed || !sql.starts_with("INSERT") {
                return (sql.to_string(), params.clone());
            }
            let base = base + 100 * ordinal;
            let values = sql
                .split_once("VALUES ")
                .unwrap()
                .1
                .split("), (")
                .enumerate()
                .map(|(offset, tuple)| {
                    let tuple = tuple.trim_start_matches('(').trim_end_matches(')');
                    format!("({}, {tuple})", base + offset)
                })
                .collect::<Vec<_>>()
                .join(", ");
            (
                format!("INSERT INTO t (rid, a, b, c, v) VALUES {values}"),
                params.clone(),
            )
        })
        .collect()
}

fn apply(db: &mut Database, reference: &mut Database, mutations: &[Mutation]) {
    for ((keyed, params), (surrogate, _)) in for_rows(mutations, true, 0)
        .iter()
        .zip(&for_rows(mutations, false, 1_000_000))
    {
        db.query_sql_with_params(keyed, params).unwrap();
        reference.query_sql_with_params(surrogate, params).unwrap();
    }
}

/// Reads every case inside a transaction holding uncommitted writes on both
/// sides, then proves rollback leaves no private row visible.
fn check_all(db: &mut Database, reference: &mut Database, context: &str) {
    check_database(db, reference, context);
    let keyed = for_rows(&private_mutations(), true, 0);
    let surrogate = for_rows(&private_mutations(), false, 2_000_000);
    let mut transaction = db.begin_transaction().unwrap();
    let mut reference_transaction = reference.begin_transaction().unwrap();
    for ((keyed, params), (surrogate, _)) in keyed.iter().zip(&surrogate) {
        transaction.query_sql_with_params(keyed, params).unwrap();
        reference_transaction
            .query_sql_with_params(surrogate, params)
            .unwrap();
    }
    for case in cases() {
        for params in &case.params {
            let plan = transaction
                .query_sql_with_params(&format!("EXPLAIN {}", case.sql), params)
                .unwrap();
            assert_interval_plan(&plan, &case.sql, case.ordered);
            let actual = transaction.query_sql_with_params(&case.sql, params);
            let expected = reference_transaction.query_sql_with_params(&case.sql, params);
            let unpaged = unpaged_sql(&case.sql);
            compare(
                &format!("{context} transaction: {}", case.sql),
                params,
                (case.deterministic, case.unordered),
                actual,
                expected,
                || {
                    values(
                        &reference_transaction
                            .query_sql_with_params(&unpaged, params)
                            .unwrap(),
                    )
                },
            );
        }
    }
    transaction.rollback();
    reference_transaction.rollback();
    check_database(db, reference, &format!("{context} after rollback"));
}

#[test]
fn sql_primary_key_prefix_intervals_match_a_full_scan_in_memory_and_in_transactions() {
    for seed_value in [7, 1523] {
        let rows = random_rows(seed_value, 260);
        let mut db = Database::new();
        let mut reference = Database::new();
        seed(&mut db, true, &rows);
        seed(&mut reference, false, &rows);
        check_all(
            &mut db,
            &mut reference,
            &format!("memory seed {seed_value}"),
        );
        apply(&mut db, &mut reference, &committed_mutations());
        check_all(
            &mut db,
            &mut reference,
            &format!("memory committed {seed_value}"),
        );
    }
}

#[test]
fn sql_primary_key_prefix_intervals_match_across_residency_overlay_and_recovery() {
    let rows = random_rows(42, 300);
    for (residency, index) in [
        (
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::DemandPaged,
        ),
    ] {
        let path = unique_test_dir("sql-pk-range");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: index,
            automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
            ..Default::default()
        };
        let context = format!("{residency:?}");
        let mut reference = Database::new();
        seed(&mut reference, false, &rows);
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        seed(&mut db, true, &rows);
        db.checkpoint().unwrap();
        drop(db);
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        check_all(&mut db, &mut reference, &format!("{context} checkpoint"));
        // Committed writes after the checkpoint are served by the live overlay.
        apply(&mut db, &mut reference, &committed_mutations());
        check_all(&mut db, &mut reference, &format!("{context} overlay"));
        drop(db);
        let mut db = Database::open_with_config(&path, config).unwrap();
        check_database(&mut db, &mut reference, &format!("{context} recovered"));
        db.checkpoint().unwrap();
        check_database(&mut db, &mut reference, &format!("{context} recheckpoint"));
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

const GET_NEW_ORDER: &str = "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = $1 AND NO_W_ID = $2 \
                             AND NO_O_ID > -1 LIMIT 1";

fn new_order(db: &mut Database) {
    db.query_sql(
        "CREATE TABLE NEW_ORDER (NO_O_ID INTEGER NOT NULL, NO_D_ID INTEGER NOT NULL, \
         NO_W_ID INTEGER NOT NULL, PRIMARY KEY (NO_D_ID, NO_W_ID, NO_O_ID))",
    )
    .unwrap();
    for district in 1..=10 {
        let values = (2101..=3000)
            .map(|order| format!("({order}, {district}, 1)"))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_sql(&format!(
            "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES {values}"
        ))
        .unwrap();
    }
}

fn first_order(output: &QueryOutput) -> Option<i64> {
    output.rows.first().map(|row| match row.get("no_o_id") {
        Some(Value::Int(order)) => *order,
        other => panic!("{other:?}"),
    })
}

fn row_rows(profile: &QueryOutput) -> i64 {
    profile
        .rows
        .iter()
        .find_map(|row| match (row.get("id"), row.get("operator info")) {
            (Some(Value::String(id)), Some(Value::String(info)))
                if id.contains("TableRangeScanExec") =>
            {
                let value = info.split("row_rows=").nth(1)?.split(',').next()?;
                Some(value.parse().unwrap())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("{profile:?}"))
}

/// The py-tpcc DELIVERY.getNewOrder statement seeks one row inside its
/// district, including uncommitted transaction writes, in every residency.
#[test]
fn sql_tpcc_get_new_order_seeks_one_primary_key_row_inside_a_transaction() {
    for residency in [
        None,
        Some((
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        )),
        Some((
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::DemandPaged,
        )),
    ] {
        let path = unique_test_dir("sql-pk-range-tpcc");
        let mut db = match residency {
            None => Database::new(),
            Some((residency, index)) => Database::open_with_config(
                &path,
                DatabaseConfig {
                    storage_residency_mode: residency,
                    relational_index_mode: index,
                    ..Default::default()
                },
            )
            .unwrap(),
        };
        new_order(&mut db);
        if residency.is_some() {
            db.checkpoint().unwrap();
        }
        let params = [Value::Int(10), Value::Int(1)];
        let plan = db
            .query_sql_with_params(&format!("EXPLAIN {GET_NEW_ORDER}"), &params)
            .unwrap();
        assert_interval_plan(&plan, GET_NEW_ORDER, false);
        assert!(
            plan.rows.iter().all(|row| !matches!(row.get("id"), Some(Value::String(id)) if id.contains("TableFullScanExec") || id.contains("SelectionExec"))),
            "{plan:?}"
        );
        let mut transaction = db.begin_transaction().unwrap();
        assert_eq!(
            first_order(
                &transaction
                    .query_sql_with_params(GET_NEW_ORDER, &params)
                    .unwrap()
            ),
            Some(2101)
        );
        // DELIVERY deletes the order it delivered; the next read inside the
        // same transaction must see that delete and an uncommitted insert.
        transaction
            .query_sql(
                "DELETE FROM NEW_ORDER WHERE NO_D_ID = 10 AND NO_W_ID = 1 AND NO_O_ID = 2101",
            )
            .unwrap();
        assert_eq!(
            first_order(
                &transaction
                    .query_sql_with_params(GET_NEW_ORDER, &params)
                    .unwrap()
            ),
            Some(2102)
        );
        transaction
            .query_sql(
                "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (0, 10, 1), (-5, 10, 1)",
            )
            .unwrap();
        assert_eq!(
            first_order(
                &transaction
                    .query_sql_with_params(GET_NEW_ORDER, &params)
                    .unwrap()
            ),
            Some(0)
        );
        let profile = transaction
            .query_sql_with_params(&format!("EXPLAIN ANALYZE {GET_NEW_ORDER}"), &params)
            .unwrap();
        // The interval starts at the bound: no district row before it and no
        // row after the LIMIT is visited.
        assert!(row_rows(&profile) <= 2, "{profile:?}");
        transaction
            .query_sql("DELETE FROM NEW_ORDER WHERE NO_D_ID = 10 AND NO_W_ID = 1")
            .unwrap();
        assert_eq!(
            first_order(
                &transaction
                    .query_sql_with_params(GET_NEW_ORDER, &params)
                    .unwrap()
            ),
            None
        );
        transaction.rollback();
        assert_eq!(
            first_order(&db.query_sql_with_params(GET_NEW_ORDER, &params).unwrap()),
            Some(2101)
        );
        let profile = db
            .query_sql_with_params(&format!("EXPLAIN ANALYZE {GET_NEW_ORDER}"), &params)
            .unwrap();
        assert_eq!(row_rows(&profile), 1, "{residency:?} {profile:?}");
        if matches!(residency, Some((StorageResidencyMode::OutOfCore, _))) {
            assert!(
                profile.rows.iter().any(|row| matches!(row.get("operator info"), Some(Value::String(info)) if info.contains("row_runtime_path=snapshot_rows"))),
                "{profile:?}"
            );
        }
        drop(db);
        if residency.is_some() {
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

/// Cached templates carry no access path: each execution binds the current
/// parameters, including NULL and empty intervals, before access planning.
#[test]
fn sql_primary_key_interval_rebinds_cached_template_parameters() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(32),
        ..DatabaseConfig::default()
    });
    new_order(&mut db);
    let sql = "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = $1 AND NO_W_ID = $2 AND NO_O_ID > $3 \
               ORDER BY NO_O_ID LIMIT 2";
    let before = db.relational_plan_template_cache_stats().unwrap();
    for (params, expected) in [
        (
            vec![Value::Int(3), Value::Int(1), Value::Int(-1)],
            vec![2101, 2102],
        ),
        (
            vec![Value::Int(4), Value::Int(1), Value::Int(2999)],
            vec![3000],
        ),
        (
            vec![Value::Int(4), Value::Int(1), Value::Int(i64::MAX)],
            vec![],
        ),
        (vec![Value::Int(4), Value::Int(1), Value::Null], vec![]),
        (vec![Value::Int(11), Value::Int(1), Value::Int(0)], vec![]),
        (vec![Value::Int(5), Value::Int(2), Value::Int(0)], vec![]),
    ] {
        let plan = db
            .query_sql_with_params(&format!("EXPLAIN {sql}"), &params)
            .unwrap();
        assert_interval_plan(&plan, sql, true);
        let rows = db.query_sql_with_params(sql, &params).unwrap();
        assert_eq!(
            rows.rows
                .iter()
                .map(|row| row["no_o_id"].clone())
                .collect::<Vec<_>>(),
            expected.into_iter().map(Value::Int).collect::<Vec<_>>(),
            "{params:?}"
        );
    }
    let after = db.relational_plan_template_cache_stats().unwrap();
    assert!(after.hits >= before.hits + 5, "{before:?} {after:?}");
    // A complete key keeps the point read; an unconstrained key keeps the scan.
    let point = db
        .query_sql("EXPLAIN SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = 1 AND NO_W_ID = 1 AND NO_O_ID = 2500")
        .unwrap();
    assert!(point.rows.iter().any(
        |row| matches!(row.get("id"), Some(Value::String(id)) if id.contains("TablePointGetExec"))
    ));
    let scan = db
        .query_sql("EXPLAIN SELECT NO_O_ID FROM NEW_ORDER WHERE NO_W_ID = 1 AND NO_O_ID > 5")
        .unwrap();
    assert!(scan.rows.iter().any(
        |row| matches!(row.get("id"), Some(Value::String(id)) if id.contains("TableFullScanExec"))
    ));
}
