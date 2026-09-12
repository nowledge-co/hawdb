use super::*;

#[test]
fn relational_ddl_requires_exactly_one_primary_key_declaration() {
    let missing = skein_sql::prepare_postgres_sql(
        "CREATE TABLE documents (id TEXT NOT NULL, payload TEXT NOT NULL)",
    )
    .expect("valid PostgreSQL syntax");
    let error = compile_schema_statement(missing.statement)
        .expect_err("Skein relational tables require a primary key");
    assert_eq!(
        error,
        SkeinError::Semantic("relational table documents must declare a primary key".to_string())
    );

    let duplicate = skein_sql::prepare_postgres_sql(
        "CREATE TABLE documents (tenant_id TEXT PRIMARY KEY, id TEXT PRIMARY KEY)",
    )
    .expect("parser preserves duplicate declarations for semantic validation");
    let error = compile_schema_statement(duplicate.statement)
        .expect_err("multiple primary-key declarations must be rejected");
    assert_eq!(
        error,
        SkeinError::Semantic("table declares more than one primary key".to_string())
    );
}

#[test]
fn table_level_composite_primary_key_is_not_nullable() {
    let prepared = skein_sql::prepare_postgres_sql(
        "CREATE TABLE documents (tenant_id TEXT, id TEXT, payload TEXT, \
         PRIMARY KEY (tenant_id, id))",
    )
    .expect("valid composite primary key");
    let writes = compile_schema_statement(prepared.statement).expect("compiled schema");
    let [RelationalWrite::CreateTable(schema)] = writes.as_slice() else {
        panic!("expected one CREATE TABLE write");
    };

    assert_eq!(schema.primary_key, ["tenant_id", "id"]);
    assert!(!schema.columns[0].nullable);
    assert!(!schema.columns[1].nullable);
    assert!(schema.columns[2].nullable);
}

fn state() -> RelationalState {
    let empty = RelationalState::default();
    let transaction = compile_relational_statement_sql(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, counter BIGINT NOT NULL DEFAULT 0, owner_id UUID, body BYTEA)",
        &[],
        &empty,
    )
    .unwrap();
    empty
        .stage_transaction(transaction, Default::default(), Default::default())
        .unwrap()
}

#[test]
fn schema_and_index_compilation_need_no_database_runtime() {
    let state = state();
    let schema = state.table_schema("items").unwrap();
    assert_eq!(schema.primary_key, ["id"]);
    assert!(!schema.columns[0].nullable);
    assert_eq!(schema.columns[2].scalar_type, RelationalScalarType::Uuid);
    assert_eq!(schema.columns[3].scalar_type, RelationalScalarType::Bytea);
    assert_eq!(
        compile_relational_statement_sql(
            "CREATE INDEX counter_idx ON items (counter, id)",
            &[],
            &state
        )
        .unwrap()
        .writes,
        [RelationalWrite::CreateIndex {
            table: "items".into(),
            index: RelationalIndexSchema {
                name: "counter_idx".into(),
                columns: vec!["counter".into(), "id".into()],
                unique: false,
            },
        }],
    );
    let alter = compile_relational_statement_sql(
        "ALTER TABLE items ADD COLUMN label TEXT DEFAULT 'new'",
        &[],
        &state,
    )
    .unwrap();
    assert!(
        matches!(&alter.writes[..], [RelationalWrite::AddColumn { table, column }]
        if table == "items" && column.name == "label" && column.scalar_type == RelationalScalarType::Text)
    );
}

#[test]
fn returning_and_scalar_binding_stay_compilation_only() {
    let state = state();
    let id = skein_core::Uuid::parse_str("018f4e6a-7c1b-7cc8-8f4d-1234567890ab").unwrap();
    let compiled = compile_relational_statement_sql_with_result(
        "INSERT INTO items (body, owner_id, id) VALUES ($1, $2, $3) RETURNING id, owner_id",
        &[
            Value::Binary(vec![0, 128, 255]),
            Value::String(id.to_string()),
            Value::Int(42),
        ],
        &state,
    )
    .unwrap();
    assert_eq!(
        compiled.transaction.writes,
        [RelationalWrite::Insert {
            table: "items".into(),
            rows: vec![RelationalRow::new(vec![
                RelationalValue::BigInt(42),
                RelationalValue::BigInt(0),
                RelationalValue::Uuid(id),
                RelationalValue::Bytea(vec![0, 128, 255]),
            ])],
            mode: RelationalInsertMode::Error,
        }]
    );
    let returning = compiled.returning.unwrap();
    assert_eq!(returning.table, "items");
    assert_eq!(returning.columns, ["id", "owner_id"]);
    assert_eq!(state.materialized_row_count(), 0);
}

#[test]
fn invalid_schema_and_mutation_shapes_keep_the_existing_errors() {
    let state = state();
    for (sql, parameters, message) in [
        ("INSERT INTO items (id) VALUES ($1)", vec![], "requires 1 parameters"),
        ("INSERT INTO items (id, id) VALUES (1, 2)", vec![], "specified more than once"),
        ("INSERT INTO items (id, owner_id) VALUES (1, 'invalid')", vec![], "invalid UUID"),
        ("UPDATE items SET counter = 1", vec![], "unbounded relational UPDATE"),
        ("DELETE FROM items", vec![], "unbounded relational DELETE"),
        ("DELETE FROM items WHERE other.id = 1", vec![], "unknown qualifier"),
        ("UPDATE items SET missing = 1 WHERE id = 1", vec![], "has no column missing"),
        ("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY)", vec![], "must not hide drift"),
        ("CREATE TABLE t (id BIGINT)", vec![], "must declare a primary key"),
        ("INSERT INTO items (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET counter = 1 RETURNING id", vec![], "RETURNING with ON CONFLICT DO UPDATE"),
    ] {
        let error = compile_relational_statement_sql(sql, &parameters, &state).unwrap_err();
        assert!(error.to_string().contains(message), "{sql}: {error}");
    }
    assert_eq!(state.materialized_row_count(), 0);
}

fn campaign(cases: usize) {
    let state = state();
    let mut seed = 0x418_d01d_21a1_u64;
    for case in 0..cases {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let id = match case % 8 {
            0 => i64::MIN,
            1 => i64::MAX,
            _ => seed as i64,
        };
        let delta = (seed.rotate_left(17) as i64) / 2;
        let payload = seed.to_le_bytes().to_vec();
        let expected_row = RelationalRow::new(vec![
            RelationalValue::BigInt(id),
            RelationalValue::BigInt(0),
            RelationalValue::Null,
            RelationalValue::Bytea(payload.clone()),
        ]);
        let actual = compile_relational_statement_sql(
            "INSERT INTO items (body, id) VALUES ($1, $2)",
            &[Value::Binary(payload), Value::Int(id)],
            &state,
        )
        .unwrap();
        assert_eq!(
            actual.writes,
            [RelationalWrite::Insert {
                table: "items".into(),
                rows: vec![expected_row],
                mode: RelationalInsertMode::Error,
            }],
            "case {case}"
        );
        let predicate = RelationalPredicate::Compare {
            column: "id".into(),
            op: RelationalComparisonOp::Eq,
            value: RelationalValue::BigInt(id),
        };
        let updated = compile_relational_statement_sql(
            "UPDATE items SET counter = counter + $1 WHERE id = $2",
            &[Value::Int(delta), Value::Int(id)],
            &state,
        )
        .unwrap();
        assert_eq!(
            updated.writes,
            [RelationalWrite::UpdateWhere {
                table: "items".into(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "counter".into(),
                    value: RelationalUpdateValue::BigIntArithmetic {
                        left: RelationalBigIntOperand::Column("counter".into()),
                        operator: RelationalBigIntArithmeticOperator::Add,
                        right: RelationalBigIntOperand::Value(RelationalValue::BigInt(delta)),
                    },
                }],
                predicate: predicate.clone(),
            }],
            "case {case}"
        );
        let deleted = compile_relational_statement_sql(
            "DELETE FROM items WHERE id = $1",
            &[Value::Int(id)],
            &state,
        )
        .unwrap();
        assert_eq!(
            deleted.writes,
            [RelationalWrite::DeleteWhere {
                table: "items".into(),
                predicate,
            }],
            "case {case}"
        );
    }
    assert_eq!(state.materialized_row_count(), 0);
    println!("relational statement differential cases: {cases}");
}

#[test]
fn statement_compilation_differential_smoke() {
    campaign(16);
}

#[test]
#[ignore = "explicit local statement compilation fuzz campaign"]
fn statement_compilation_differential_campaign() {
    campaign(256);
}
