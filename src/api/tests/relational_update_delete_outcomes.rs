use super::*;
use crate::{ConcurrentTransactionOptions, SqlStatementResult, TransactionCommitResult};
use std::time::Duration;

fn mutation_fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE items (\
                id BIGINT PRIMARY KEY, \
                state TEXT NOT NULL\
            )",
        )
        .expect("create items table");
    database
        .query_sql(
            "INSERT INTO items (id, state) VALUES \
             (1, 'ready'), (2, 'ready'), (3, 'ready')",
        )
        .expect("insert items");
    database
}

fn assert_statement_outcome(result: SqlStatementResult, affected_rows: usize) {
    let mutation = result.mutation.expect("provisional mutation outcome");
    assert_eq!(mutation.affected_rows, affected_rows);
    assert_eq!(mutation.conflict_rows, 0);
    assert!(mutation.rows.is_empty());
    assert!(mutation.provisional);
    assert!(result.output.rows.is_empty());
}

fn assert_commit_outcome(result: TransactionCommitResult, affected_rows: usize) {
    assert_eq!(result.mutations.len(), 1);
    let mutation = &result.mutations[0];
    assert_eq!(mutation.affected_rows, affected_rows);
    assert_eq!(mutation.conflict_rows, 0);
    assert!(mutation.rows.is_empty());
    assert!(!mutation.provisional);
    assert!(result.output.rows.is_empty());
}

#[test]
fn relational_update_delete_report_matching_zero_and_multi_row_outcomes() {
    let mut database = mutation_fixture();

    let mut update = database.begin_transaction();
    assert_statement_outcome(
        update
            .query_sql_with_result("UPDATE items SET state = 'updated' WHERE id = 1")
            .expect("stage matching update"),
        1,
    );
    assert_commit_outcome(
        update.commit_with_result().expect("commit matching update"),
        1,
    );

    let mut zero_update = database.begin_transaction();
    assert_statement_outcome(
        zero_update
            .query_sql_with_result("UPDATE items SET state = 'missing' WHERE id = 99")
            .expect("stage zero-row update"),
        0,
    );
    assert_commit_outcome(
        zero_update
            .commit_with_result()
            .expect("commit zero-row update"),
        0,
    );

    let mut multi_update = database.begin_transaction();
    assert_statement_outcome(
        multi_update
            .query_sql_with_result("UPDATE items SET state = 'batch' WHERE state = 'ready'")
            .expect("stage multi-row update"),
        2,
    );
    assert_commit_outcome(
        multi_update
            .commit_with_result()
            .expect("commit multi-row update"),
        2,
    );

    let mut delete = database.begin_transaction();
    assert_statement_outcome(
        delete
            .query_sql_with_result("DELETE FROM items WHERE id = 1")
            .expect("stage matching delete"),
        1,
    );
    assert_commit_outcome(
        delete.commit_with_result().expect("commit matching delete"),
        1,
    );

    let mut zero_delete = database.begin_transaction();
    assert_statement_outcome(
        zero_delete
            .query_sql_with_result("DELETE FROM items WHERE id = 99")
            .expect("stage zero-row delete"),
        0,
    );
    assert_commit_outcome(
        zero_delete
            .commit_with_result()
            .expect("commit zero-row delete"),
        0,
    );

    let mut multi_delete = database.begin_transaction();
    assert_statement_outcome(
        multi_delete
            .query_sql_with_result("DELETE FROM items WHERE state = 'batch'")
            .expect("stage multi-row delete"),
        2,
    );
    assert_commit_outcome(
        multi_delete
            .commit_with_result()
            .expect("commit multi-row delete"),
        2,
    );
}

#[test]
fn relational_update_delete_outcomes_fail_closed_on_affected_row_budget() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_affected_rows = NonZeroUsize::new(1).unwrap();
    let mut database = Database::new_with_config(config);
    database
        .query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, state TEXT NOT NULL)")
        .expect("create items table");
    database
        .query_sql("INSERT INTO items (id, state) VALUES (1, 'ready')")
        .expect("insert first item");
    database
        .query_sql("INSERT INTO items (id, state) VALUES (2, 'ready')")
        .expect("insert second item");

    let mut transaction = database.begin_transaction();
    let update_error = transaction
        .query_sql_with_result("UPDATE items SET state = 'changed' WHERE state = 'ready'")
        .expect_err("multi-row update exceeds the affected-row budget");
    assert!(update_error.to_string().contains("max_affected_rows 1"));
    let unchanged = transaction
        .query_sql("SELECT id FROM items WHERE state = 'ready' ORDER BY id")
        .expect("failed update leaves the transaction workspace unchanged");
    assert_eq!(unchanged.rows.len(), 2);

    let delete_error = transaction
        .query_sql_with_result("DELETE FROM items WHERE state = 'ready'")
        .expect_err("multi-row delete exceeds the affected-row budget");
    assert!(delete_error.to_string().contains("max_affected_rows 1"));
    let retained = transaction
        .query_sql("SELECT id FROM items ORDER BY id")
        .expect("failed delete leaves the transaction workspace unchanged");
    assert_eq!(retained.rows.len(), 2);
    transaction.rollback();
}

#[test]
fn concurrent_update_delete_outcomes_cover_retry_and_both_transaction_modes() {
    let database = mutation_fixture().into_concurrent();

    let mut optimistic = database
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .expect("begin optimistic update");
    assert_statement_outcome(
        optimistic
            .query_sql_with_result("UPDATE items SET state = 'optimistic' WHERE id = 1")
            .expect("stage optimistic update"),
        1,
    );
    assert_commit_outcome(
        optimistic
            .commit_with_result()
            .expect("commit optimistic update"),
        1,
    );

    let mut pessimistic = database
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .expect("begin pessimistic delete");
    assert_statement_outcome(
        pessimistic
            .query_sql_with_result("DELETE FROM items WHERE id = 2")
            .expect("stage pessimistic delete"),
        1,
    );
    assert_commit_outcome(
        pessimistic
            .commit_with_result()
            .expect("commit pessimistic delete"),
        1,
    );

    let mut stale = database
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .expect("begin stale optimistic update");
    assert_statement_outcome(
        stale
            .query_sql_with_result("UPDATE items SET state = 'stale' WHERE id = 3")
            .expect("stage stale optimistic update"),
        1,
    );
    database
        .query_sql("INSERT INTO items (id, state) VALUES (4, 'winner')")
        .expect("advance the durable commit epoch");
    let conflict = stale
        .commit_with_result()
        .expect_err("stale optimistic outcome must not be confirmed");
    assert!(conflict
        .to_string()
        .contains("optimistic transaction conflict"));

    let mut retry = database
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .expect("begin optimistic retry");
    assert_statement_outcome(
        retry
            .query_sql_with_result("UPDATE items SET state = 'retry' WHERE id = 3")
            .expect("stage optimistic retry"),
        1,
    );
    assert_commit_outcome(
        retry.commit_with_result().expect("commit optimistic retry"),
        1,
    );
}

#[test]
fn relational_update_delete_outcomes_survive_wal_checkpoint_and_reopen() {
    let path = super::unique_test_dir("relational_update_delete_outcomes");
    {
        let mut database = Database::open(&path).expect("open durable database");
        database
            .query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, state TEXT NOT NULL)")
            .expect("create items table");
        database
            .query_sql("INSERT INTO items (id, state) VALUES (1, 'ready'), (2, 'ready')")
            .expect("insert items");
        database.checkpoint().expect("checkpoint fixture");

        let mut transaction = database.begin_transaction();
        assert_statement_outcome(
            transaction
                .query_sql_with_result("UPDATE items SET state = 'updated' WHERE id = 1")
                .expect("stage durable update"),
            1,
        );
        assert_statement_outcome(
            transaction
                .query_sql_with_result("DELETE FROM items WHERE id = 2")
                .expect("stage durable delete"),
            1,
        );
        let committed = transaction
            .commit_with_result()
            .expect("commit durable mutations");
        assert_eq!(committed.mutations.len(), 2);
        assert!(committed
            .mutations
            .iter()
            .all(|mutation| mutation.affected_rows == 1 && !mutation.provisional));
        database.checkpoint().expect("checkpoint durable mutations");
    }

    {
        let mut database = Database::open(&path).expect("reopen durable database");
        let rows = database
            .query_sql("SELECT id, state FROM items ORDER BY id")
            .expect("read recovered rows");
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(
            rows.rows[0].get("state"),
            Some(&Value::String("updated".to_string()))
        );

        let mut zero_update = database.begin_transaction();
        assert_statement_outcome(
            zero_update
                .query_sql_with_result("UPDATE items SET state = 'missing' WHERE id = 99")
                .expect("stage recovered zero-row update"),
            0,
        );
        assert_commit_outcome(
            zero_update
                .commit_with_result()
                .expect("commit recovered zero-row update"),
            0,
        );

        let mut zero_delete = database.begin_transaction();
        assert_statement_outcome(
            zero_delete
                .query_sql_with_result("DELETE FROM items WHERE id = 99")
                .expect("stage recovered zero-row delete"),
            0,
        );
        assert_commit_outcome(
            zero_delete
                .commit_with_result()
                .expect("commit recovered zero-row delete"),
            0,
        );
    }
    std::fs::remove_dir_all(path).expect("remove durable fixture");
}
