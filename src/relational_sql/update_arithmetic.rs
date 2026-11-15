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

//! `UPDATE ... SET column = column +/- value` on DOUBLE PRECISION, BIGINT and
//! DECIMAL columns. Every UPDATE expression runs through the shared numeric
//! kernel (#1429); these cases pin its assignment contract for the TPC-C style
//! balance updates of #1446.

use crate::{Database, Value};
use hawdb_storage::config::DurabilityPolicy;

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().unwrap())
}

fn create_accounts(database: &mut Database) {
    database
        .query_sql(
            "CREATE TABLE accounts (\
                id BIGINT PRIMARY KEY, \
                region BIGINT NOT NULL, \
                balance DOUBLE PRECISION NOT NULL, \
                ytd DOUBLE PRECISION NOT NULL, \
                payments BIGINT NOT NULL, \
                amount DECIMAL(12,2) NOT NULL, \
                note DOUBLE PRECISION, \
                label TEXT NOT NULL\
            )",
        )
        .expect("create accounts table");
    for (id, region, balance) in [(1, 10, 100.0), (2, 10, -10.0), (3, 20, 0.5)] {
        database
            .query_sql_with_params(
                "INSERT INTO accounts (id, region, balance, ytd, payments, amount, label) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    Value::Int(id),
                    Value::Int(region),
                    Value::Float(balance),
                    Value::Float(0.0),
                    Value::Int(0),
                    decimal("100.00"),
                    Value::String(format!("account-{id}")),
                ],
            )
            .expect("insert account");
    }
}

fn column(database: &mut Database, column: &str, id: i64) -> Value {
    database
        .query_sql_with_params(
            &format!("SELECT {column} FROM accounts WHERE id = $1"),
            &[Value::Int(id)],
        )
        .expect("read account column")
        .rows[0][column]
        .clone()
}

fn snapshot(database: &mut Database) -> hawdb_executor::QueryRows {
    database
        .query_sql("SELECT * FROM accounts ORDER BY id")
        .expect("read every account")
        .rows
}

#[test]
fn double_update_arithmetic_uses_the_shared_numeric_rules() {
    let mut database = Database::new();
    create_accounts(&mut database);

    // DOUBLE PRECISION op DOUBLE PRECISION, through a cast literal, a DOUBLE
    // parameter, or (#1516) a bare numeric literal coerced to DOUBLE.
    database
        .query_sql(
            "UPDATE accounts SET balance = balance + CAST(12.5 AS DOUBLE PRECISION) WHERE id = 1",
        )
        .expect("DOUBLE PRECISION column + cast literal");
    assert_eq!(column(&mut database, "balance", 1), Value::Float(112.5));
    database
        .query_sql(
            "UPDATE accounts SET balance = CAST(0.25 AS DOUBLE PRECISION) + balance WHERE id = 1",
        )
        .expect("cast literal + DOUBLE PRECISION column");
    // #1516: like PostgreSQL's implicit numeric-to-float8 cast, a numeric
    // literal beside a DOUBLE PRECISION operand becomes the nearest double.
    database
        .query_sql("UPDATE accounts SET balance = balance + 12.5 WHERE id = 1")
        .expect("DOUBLE PRECISION column + numeric literal");
    database
        .query_sql("UPDATE accounts SET balance = -12.5 + balance WHERE id = 1")
        .expect("negative numeric literal + DOUBLE PRECISION column");
    database
        .query_sql_with_params(
            "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
            &[Value::Float(2.75), Value::Int(1)],
        )
        .expect("DOUBLE PRECISION column - DOUBLE parameter");
    assert_eq!(column(&mut database, "balance", 1), Value::Float(110.0));

    // double precision +/- bigint -> double precision.
    database
        .query_sql_with_params(
            "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
            &[Value::Int(5), Value::Int(1)],
        )
        .expect("DOUBLE PRECISION column + BIGINT parameter");
    database
        .query_sql("UPDATE accounts SET balance = balance - 3 WHERE id = 1")
        .expect("DOUBLE PRECISION column - BIGINT literal");
    assert_eq!(column(&mut database, "balance", 1), Value::Float(112.0));
    database
        .query_sql_with_params(
            "UPDATE accounts SET ytd = region + $1 WHERE id = 1",
            &[Value::Float(0.5)],
        )
        .expect("BIGINT column + DOUBLE parameter into DOUBLE PRECISION");
    assert_eq!(column(&mut database, "ytd", 1), Value::Float(10.5));
    // bigint + bigint is BIGINT arithmetic, assignment-converted to double.
    database
        .query_sql("UPDATE accounts SET ytd = region + 1 WHERE id = 1")
        .expect("BIGINT expression into DOUBLE PRECISION");
    assert_eq!(column(&mut database, "ytd", 1), Value::Float(11.0));
    // An integral DOUBLE PRECISION result converts exactly to BIGINT.
    database
        .query_sql_with_params(
            "UPDATE accounts SET payments = payments + $1 WHERE id = 2",
            &[Value::Float(2.0)],
        )
        .expect("integral DOUBLE expression into BIGINT");
    assert_eq!(column(&mut database, "payments", 2), Value::Int(2));
    database
        .query_sql("UPDATE accounts SET payments = 0 WHERE id = 2")
        .expect("reset payments");

    // Several assignments in one UPDATE read the pre-update row.
    database
        .query_sql_with_params(
            "UPDATE accounts SET balance = balance - $1, ytd = ytd + $1, \
             payments = payments + $2 WHERE id = $3",
            &[Value::Float(10.0), Value::Int(1), Value::Int(2)],
        )
        .expect("multi-assignment arithmetic UPDATE");
    assert_eq!(column(&mut database, "balance", 2), Value::Float(-20.0));
    assert_eq!(column(&mut database, "ytd", 2), Value::Float(10.0));
    assert_eq!(column(&mut database, "payments", 2), Value::Int(1));

    // Dependent assignments also read the original row, not the values
    // assigned earlier in the same SET list: a swap exchanges the columns,
    // and arithmetic on the swapped pair uses the pre-update values.
    database
        .query_sql("UPDATE accounts SET balance = ytd, ytd = balance WHERE id = 3")
        .expect("swap UPDATE");
    assert_eq!(column(&mut database, "balance", 3), Value::Float(0.0));
    assert_eq!(column(&mut database, "ytd", 3), Value::Float(0.5));
    database
        .query_sql_with_params(
            "UPDATE accounts SET balance = ytd + $1, ytd = balance - $1 WHERE id = 3",
            &[Value::Float(2.0)],
        )
        .expect("dependent arithmetic UPDATE");
    // Sequential evaluation would give ytd = 2.5 - 2.0 = 0.5.
    assert_eq!(column(&mut database, "balance", 3), Value::Float(2.5));
    assert_eq!(column(&mut database, "ytd", 3), Value::Float(-2.0));
    database
        .query_sql_with_params(
            "UPDATE accounts SET balance = $1, ytd = $2 WHERE id = 3",
            &[Value::Float(0.5), Value::Float(0.0)],
        )
        .expect("restore account 3");

    // A non-primary-key predicate updates every matching row.
    database
        .query_sql_with_params(
            "UPDATE accounts SET balance = balance + $1 WHERE region = 10",
            &[Value::Float(1.5)],
        )
        .expect("arithmetic UPDATE by non-key predicate");
    assert_eq!(column(&mut database, "balance", 1), Value::Float(113.5));
    assert_eq!(column(&mut database, "balance", 2), Value::Float(-18.5));
    assert_eq!(column(&mut database, "balance", 3), Value::Float(0.5));

    // NULL +/- x is NULL.
    database
        .query_sql("UPDATE accounts SET note = note + 1 WHERE id = 1")
        .expect("NULL source column yields NULL");
    assert_eq!(column(&mut database, "note", 1), Value::Null);
    database
        .query_sql_with_params(
            "UPDATE accounts SET note = balance + $1 WHERE id = 1",
            &[Value::Null],
        )
        .expect("NULL parameter yields NULL");
    assert_eq!(column(&mut database, "note", 1), Value::Null);

    // Fail closed before publication: the whole UPDATE leaves no partial row.
    database
        .query_sql_with_params(
            "UPDATE accounts SET ytd = $1 WHERE id = 2",
            &[Value::Float(f64::MAX)],
        )
        .expect("seed the largest finite DOUBLE PRECISION");
    for (sql, parameters, expected) in [
        (
            "UPDATE accounts SET balance = balance - $1 WHERE id = 1",
            vec![Value::Null],
            "NOT NULL",
        ),
        (
            "UPDATE accounts SET ytd = ytd + $1 WHERE region = 10",
            vec![Value::Float(f64::MAX)],
            "non-finite arithmetic result",
        ),
        (
            "UPDATE accounts SET note = balance + $1 WHERE region = 10",
            vec![Value::Float(f64::INFINITY)],
            "finite numeric operands",
        ),
        (
            // A fractional DOUBLE PRECISION result does not round into BIGINT.
            "UPDATE accounts SET payments = payments + $1 WHERE region = 10",
            vec![Value::Float(1.5)],
            "exact in-range integer",
        ),
        (
            "UPDATE accounts SET payments = payments + $1 WHERE id = 1",
            vec![Value::Float(1e19)],
            "exact in-range integer",
        ),
        (
            "UPDATE accounts SET payments = payments + $1 WHERE region = 10",
            vec![Value::Int(i64::MAX)],
            "integer arithmetic overflow",
        ),
        (
            // #1516 coerces only literals: a DECIMAL parameter beside a DOUBLE
            // PRECISION operand still needs a cast (#1429).
            "UPDATE accounts SET balance = balance + $1 WHERE id = 1",
            vec![decimal("12.5")],
            "explicit CAST",
        ),
        (
            "UPDATE accounts SET label = label + 1 WHERE id = 1",
            Vec::new(),
            "arithmetic requires numeric operands",
        ),
        (
            "UPDATE accounts SET label = balance + 1 WHERE id = 1",
            Vec::new(),
            "numeric operands and target",
        ),
        (
            "UPDATE accounts SET balance = balance + $1 WHERE id = 1",
            vec![Value::String("1.5".into())],
            "arithmetic requires numeric operands",
        ),
        (
            "UPDATE accounts SET balance = balance + $1 WHERE id = 1",
            vec![Value::Bool(true)],
            "numeric or CAST text operands",
        ),
    ] {
        let before = snapshot(&mut database);
        let error = database
            .query_sql_with_params(sql, &parameters)
            .expect_err("invalid arithmetic assignment must fail");
        assert!(error.to_string().contains(expected), "{sql}: {error}");
        assert_eq!(snapshot(&mut database), before, "{sql}");
    }
    assert_eq!(column(&mut database, "ytd", 1), Value::Float(11.0));
}

#[test]
fn decimal_update_arithmetic_stays_exact_and_requires_casts_beside_double() {
    let mut database = Database::new();
    create_accounts(&mut database);

    // decimal +/- decimal is exact decimal, rescaled to the column's scale.
    database
        .query_sql("UPDATE accounts SET amount = amount + 0.1 WHERE id = 1")
        .expect("DECIMAL column + DECIMAL literal");
    database
        .query_sql_with_params(
            "UPDATE accounts SET amount = amount - $1 WHERE id = 1",
            &[decimal("0.3")],
        )
        .expect("DECIMAL column - DECIMAL parameter");
    assert_eq!(column(&mut database, "amount", 1), decimal("99.80"));
    // bigint +/- decimal -> decimal.
    database
        .query_sql_with_params(
            "UPDATE accounts SET amount = amount + $1, payments = payments + 2.00 WHERE id = 1",
            &[Value::Int(3)],
        )
        .expect("DECIMAL + BIGINT and integral DECIMAL into BIGINT");
    assert_eq!(column(&mut database, "amount", 1), decimal("102.80"));
    assert_eq!(column(&mut database, "payments", 1), Value::Int(2));
    database
        .query_sql("UPDATE accounts SET amount = region + 0.5 WHERE id = 1")
        .expect("BIGINT column + DECIMAL literal into DECIMAL");
    assert_eq!(column(&mut database, "amount", 1), decimal("10.50"));
    // Assignment rescales once, ties away from zero.
    database
        .query_sql("UPDATE accounts SET amount = amount + 0.005 WHERE id = 1")
        .expect("DECIMAL rescale on assignment");
    assert_eq!(column(&mut database, "amount", 1), decimal("10.51"));

    // Decimal and DOUBLE PRECISION mix only through an explicit CAST.
    database
        .query_sql_with_params(
            "UPDATE accounts SET amount = amount + CAST($1 AS DECIMAL(12,2)) WHERE id = 1",
            &[Value::Float(1.25)],
        )
        .expect("DOUBLE parameter cast to DECIMAL");
    assert_eq!(column(&mut database, "amount", 1), decimal("11.76"));
    database
        .query_sql(
            "UPDATE accounts SET balance = CAST(amount AS DOUBLE PRECISION) + balance WHERE id = 1",
        )
        .expect("DECIMAL column cast to DOUBLE PRECISION");
    assert_eq!(
        column(&mut database, "balance", 1),
        Value::Float(11.76 + 100.0)
    );
    // A DECIMAL result assigned to a DOUBLE PRECISION column converts.
    database
        .query_sql("UPDATE accounts SET ytd = amount + 1 WHERE id = 1")
        .expect("DECIMAL expression into DOUBLE PRECISION");
    assert_eq!(column(&mut database, "ytd", 1), Value::Float(12.76));

    for (sql, parameters, expected) in [
        (
            "UPDATE accounts SET amount = amount + $1 WHERE id = 1",
            vec![Value::Float(1.5)],
            "explicit CAST",
        ),
        (
            "UPDATE accounts SET balance = amount + balance WHERE id = 1",
            Vec::new(),
            "explicit CAST",
        ),
        (
            // DOUBLE PRECISION into DECIMAL is never implicit.
            "UPDATE accounts SET amount = balance + 1 WHERE id = 1",
            Vec::new(),
            "requires CAST",
        ),
        (
            // A fractional DECIMAL result does not round into BIGINT; #1516
            // names the fraction instead of the 38-digit precision limit.
            "UPDATE accounts SET payments = payments + 0.5 WHERE id = 1",
            Vec::new(),
            "fractional part; BIGINT requires an exact integer",
        ),
        (
            // DECIMAL(12,2) holds at most ten integer digits.
            "UPDATE accounts SET amount = amount + 99999999999 WHERE region = 10",
            Vec::new(),
            "decimal",
        ),
        (
            "UPDATE accounts SET amount = amount - $1 WHERE id = 1",
            vec![Value::Null],
            "NOT NULL",
        ),
    ] {
        let before = snapshot(&mut database);
        let error = database
            .query_sql_with_params(sql, &parameters)
            .expect_err("invalid DECIMAL arithmetic assignment must fail");
        assert!(error.to_string().contains(expected), "{sql}: {error}");
        assert_eq!(snapshot(&mut database), before, "{sql}");
    }
}

#[test]
fn double_update_arithmetic_is_transactional() {
    let mut database = Database::new();
    create_accounts(&mut database);

    let mut transaction = database.begin_transaction().unwrap();
    for _ in 0..2 {
        transaction
            .query_sql_with_params(
                "UPDATE accounts SET balance = balance + $1, ytd = ytd + $1 WHERE id = $2",
                &[Value::Float(1.25), Value::Int(1)],
            )
            .expect("stage transaction-local increment");
    }
    let staged = transaction
        .query_sql("SELECT balance, ytd FROM accounts WHERE id = 1")
        .unwrap();
    assert_eq!(staged.rows[0]["balance"], Value::Float(102.5));
    assert_eq!(staged.rows[0]["ytd"], Value::Float(2.5));
    transaction.rollback();
    assert_eq!(column(&mut database, "balance", 1), Value::Float(100.0));
    assert_eq!(column(&mut database, "ytd", 1), Value::Float(0.0));

    let mut transaction = database.begin_transaction().unwrap();
    for _ in 0..2 {
        transaction
            .query_sql_with_params(
                "UPDATE accounts SET balance = balance - $1 WHERE id = 1",
                &[Value::Float(0.5)],
            )
            .unwrap();
    }
    transaction.commit().expect("commit staged decrements");
    assert_eq!(column(&mut database, "balance", 1), Value::Float(99.0));
}

#[test]
fn double_update_arithmetic_survives_wal_replay_and_checkpoint_reopen() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-relational-double-arithmetic-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let read = |database: &mut Database| {
        (
            column(database, "balance", 1),
            column(database, "payments", 1),
            column(database, "amount", 1),
        )
    };
    let expected = |balance: f64, payments: i64, amount: &str| {
        (Value::Float(balance), Value::Int(payments), decimal(amount))
    };
    {
        let mut database = Database::open_with_durability(&path, DurabilityPolicy::default())
            .expect("open durable database");
        create_accounts(&mut database);
        database
            .query_sql_with_params(
                "UPDATE accounts SET balance = balance + $1, payments = payments + $2, \
                 amount = amount + 0.10 WHERE id = 1",
                &[Value::Float(0.1), Value::Float(2.0)],
            )
            .expect("durable arithmetic update");
        database
            .query_sql_with_params(
                "UPDATE accounts SET balance = balance - $1 WHERE id = 1",
                &[Value::Float(0.2)],
            )
            .expect("second durable arithmetic update");
        database
            .query_sql_with_params(
                "UPDATE accounts SET payments = payments + $1 WHERE id = 1",
                &[Value::Float(f64::MAX)],
            )
            .expect_err("rejected update must not reach the WAL");
    }
    let replayed = 100.0 + 0.1 - 0.2;
    {
        let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
            .expect("reopen from WAL");
        assert_eq!(read(&mut reopened), expected(replayed, 2, "100.10"));
        reopened.checkpoint().expect("checkpoint arithmetic result");
        reopened
            .query_sql_with_params(
                "UPDATE accounts SET balance = balance + $1, amount = amount - 1 WHERE id = 1",
                &[Value::Float(1.0)],
            )
            .expect("arithmetic update after checkpoint");
    }
    {
        let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
            .expect("reopen from checkpoint plus WAL tail");
        assert_eq!(read(&mut reopened), expected(replayed + 1.0, 2, "99.10"));
        reopened.checkpoint().expect("second checkpoint");
    }
    {
        let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
            .expect("reopen from checkpoint");
        assert_eq!(read(&mut reopened), expected(replayed + 1.0, 2, "99.10"));
    }
    std::fs::remove_dir_all(path).expect("remove durable arithmetic test directory");
}
