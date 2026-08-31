use super::*;
use crate::error::SkeinError;
use crate::{
    ProjectionGenerationBatchLimits, ProjectionGenerationBegin, ProjectionGenerationDigestBuilder,
    ProjectionGenerationIdentity, ProjectionGenerationMember, ProjectionGenerationReadLimits,
    ProjectionRelationalReadBinding, RelationalRow, RelationalValue,
};

const COMMUNITY_PROJECTION: &str = "community_detection";
const COMMUNITY_OWNER: &[u8] = b"workspace-1/space-1";

fn text(value: &str) -> RelationalValue {
    RelationalValue::Text(value.to_string())
}

fn community_binding() -> ProjectionRelationalReadBinding {
    ProjectionRelationalReadBinding::new(
        COMMUNITY_PROJECTION,
        COMMUNITY_OWNER,
        1,
        ["communities", "community_entities"],
    )
    .expect("valid community projection binding")
}

fn seal_relational_generation(
    database: &Database,
    generation: &str,
    source_watermark: u64,
    expected_head: Option<&str>,
    rows: impl IntoIterator<Item = (&'static str, RelationalRow)>,
) -> crate::SealedProjectionGeneration {
    let mut members = rows
        .into_iter()
        .map(|(table, row)| {
            database
                .encode_projection_relational_row(table, row)
                .expect("encode projection row")
        })
        .collect::<Vec<_>>();
    members
        .sort_by(|left, right| (&left.collection, &left.key).cmp(&(&right.collection, &right.key)));
    let store = database
        .projection_generation_store()
        .expect("open projection generation catalog");
    let mut writer = store
        .begin_candidate(
            ProjectionGenerationBegin {
                identity: ProjectionGenerationIdentity {
                    projection: COMMUNITY_PROJECTION.to_string(),
                    owner_key: COMMUNITY_OWNER.to_vec(),
                    generation: generation.to_string(),
                },
                source_watermark,
                projection_version: 1,
                expected_head: expected_head.map(str::to_string),
            },
            ProjectionGenerationBatchLimits::default(),
        )
        .expect("begin relational projection candidate");
    writer
        .append_batch(&members)
        .expect("append relational projection rows");
    let mut digest = ProjectionGenerationDigestBuilder::default();
    for member in &members {
        digest.update(member).expect("digest projection row");
    }
    writer
        .seal(digest.finish())
        .expect("seal relational projection candidate")
}

fn generation_one_rows() -> Vec<(&'static str, RelationalRow)> {
    vec![
        (
            "communities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-a"),
                text("stable-a"),
                text("Alpha"),
                RelationalValue::BigInt(1),
            ]),
        ),
        (
            "communities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-b"),
                text("stable-b"),
                text("Beta"),
                RelationalValue::BigInt(1),
            ]),
        ),
        (
            "community_entities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-a"),
                text("entity-1"),
            ]),
        ),
        (
            "community_entities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-b"),
                text("entity-2"),
            ]),
        ),
    ]
}

fn generation_two_rows() -> Vec<(&'static str, RelationalRow)> {
    vec![
        (
            "communities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-b"),
                text("stable-b"),
                text("Beta revised"),
                RelationalValue::BigInt(2),
            ]),
        ),
        (
            "communities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-c"),
                text("stable-c"),
                text("Gamma"),
                RelationalValue::BigInt(1),
            ]),
        ),
        (
            "community_entities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-b"),
                text("entity-2"),
            ]),
        ),
        (
            "community_entities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-b"),
                text("entity-3"),
            ]),
        ),
        (
            "community_entities",
            RelationalRow::new(vec![
                text("workspace-1"),
                text("space-1"),
                text("community-c"),
                text("entity-4"),
            ]),
        ),
    ]
}

#[test]
fn durable_database_exposes_projection_generations_across_reopen() {
    assert!(Database::new().projection_generation_store().is_err());

    let path = unique_test_dir("projection_generation_facade");
    let rows = vec![
        ProjectionGenerationMember {
            collection: "spans".to_string(),
            key: b"span-1".to_vec(),
            payload: b"first".to_vec(),
        },
        ProjectionGenerationMember {
            collection: "spans".to_string(),
            key: b"span-2".to_vec(),
            payload: b"second".to_vec(),
        },
    ];
    let begin = ProjectionGenerationBegin {
        identity: ProjectionGenerationIdentity {
            projection: "session_spans".to_string(),
            owner_key: b"session-1".to_vec(),
            generation: "generation-1".to_string(),
        },
        source_watermark: 42,
        projection_version: 1,
        expected_head: None,
    };

    {
        let database = Database::open(&path).expect("open durable database");
        let store = database
            .projection_generation_store()
            .expect("open projection generation catalog");
        let mut writer = store
            .begin_candidate(begin, ProjectionGenerationBatchLimits::default())
            .expect("begin candidate");
        writer.append_batch(&rows).expect("append candidate");
        let mut digest = ProjectionGenerationDigestBuilder::default();
        for row in &rows {
            digest.update(row).expect("digest candidate member");
        }
        let sealed = writer.seal(digest.finish()).expect("seal candidate");
        store.publish(&sealed).expect("publish candidate");
    }

    {
        let database = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .expect("reopen read-only durable database");
        let store = database
            .projection_generation_store()
            .expect("reopen projection generation catalog");
        let reader = store
            .open_active("session_spans", b"session-1")
            .expect("open active generation");
        let page = reader
            .read_page(None, ProjectionGenerationReadLimits::default())
            .expect("read active generation");
        assert_eq!(page.members, rows);
        assert!(page.next.is_none());
        assert!(store
            .begin_candidate(
                ProjectionGenerationBegin {
                    identity: ProjectionGenerationIdentity {
                        projection: "session_spans".to_string(),
                        owner_key: b"session-1".to_vec(),
                        generation: "generation-2".to_string(),
                    },
                    source_watermark: 43,
                    projection_version: 1,
                    expected_head: Some("generation-1".to_string()),
                },
                ProjectionGenerationBatchLimits::default(),
            )
            .is_err());
    }

    std::fs::remove_dir_all(path).expect("remove durable database fixture");
}

#[test]
fn projection_relational_reads_pin_one_published_generation_for_postgres_sql() {
    const QUERY: &str = "\
        SELECT c.id, c.name, ce.entity_id \
        FROM communities AS c \
        INNER JOIN community_entities AS ce \
          ON ce.workspace_id = c.workspace_id \
         AND ce.space_id = c.space_id \
         AND ce.community_id = c.id \
        WHERE c.workspace_id = $1 AND c.space_id = $2 \
        ORDER BY c.id, ce.entity_id";

    let path = unique_test_dir("projection_relational_read");
    let mut database = Database::open(&path).expect("open durable database");
    database
        .query_sql(
            "CREATE TABLE communities (\
                workspace_id TEXT NOT NULL, \
                space_id TEXT NOT NULL, \
                id TEXT NOT NULL, \
                stable_key TEXT NOT NULL, \
                name TEXT NOT NULL, \
                member_count BIGINT NOT NULL, \
                PRIMARY KEY (workspace_id, space_id, id))",
        )
        .expect("create communities projection table");
    database
        .query_sql(
            "CREATE TABLE community_entities (\
                workspace_id TEXT NOT NULL, \
                space_id TEXT NOT NULL, \
                community_id TEXT NOT NULL, \
                entity_id TEXT NOT NULL, \
                PRIMARY KEY (workspace_id, space_id, community_id, entity_id))",
        )
        .expect("create community entity projection table");
    database
        .query_sql("CREATE TABLE projection_audit (id BIGINT PRIMARY KEY, note TEXT NOT NULL)")
        .expect("create unbound canonical table");
    database
        .query_sql("INSERT INTO projection_audit (id, note) VALUES (1, 'canonical')")
        .expect("insert unbound canonical row");

    let generation_one =
        seal_relational_generation(&database, "generation-1", 41, None, generation_one_rows());
    database
        .projection_generation_store()
        .expect("open projection catalog")
        .publish(&generation_one)
        .expect("publish first generation");
    let pinned_generation_one = database
        .begin_projection_read_transaction(community_binding())
        .expect("pin first generation");

    let generation_two = seal_relational_generation(
        &database,
        "generation-2",
        42,
        Some("generation-1"),
        generation_two_rows(),
    );
    let before_publish = database
        .begin_projection_read_transaction(community_binding())
        .expect("candidate is not visible before publication");
    database
        .projection_generation_store()
        .expect("open projection catalog")
        .publish(&generation_two)
        .expect("publish replacement generation");
    let pinned_generation_two = database
        .begin_projection_read_transaction(community_binding())
        .expect("pin replacement generation");
    let version_mismatch = ProjectionRelationalReadBinding::new(
        COMMUNITY_PROJECTION,
        COMMUNITY_OWNER,
        2,
        ["communities", "community_entities"],
    )
    .expect("valid mismatched binding");
    assert!(matches!(
        database.begin_projection_read_transaction(version_mismatch),
        Err(SkeinError::StorageIntegrity(_))
    ));

    let parameters = [
        Value::String("workspace-1".to_string()),
        Value::String("space-1".to_string()),
    ];
    for old_reader in [&pinned_generation_one, &before_publish] {
        let old_rows = old_reader
            .query_sql_with_params(QUERY, &parameters)
            .expect("query first generation through PostgreSQL SQL");
        assert_eq!(old_rows.rows.len(), 2);
        assert_eq!(old_rows.rows[0]["id"], Value::String("community-a".into()));
        assert_eq!(old_rows.rows[0]["name"], Value::String("Alpha".into()));
        assert_eq!(old_rows.rows[1]["id"], Value::String("community-b".into()));
    }

    let budget_error = pinned_generation_two
        .query_sql_with_params_options(
            QUERY,
            &parameters,
            QueryStreamOptions {
                max_rows: None,
                max_payload_bytes: Some(1),
            },
        )
        .expect_err("projection payload exhaustion must fail the whole statement");
    assert!(matches!(budget_error, SkeinError::Execution(_)));

    let profiled = pinned_generation_two
        .query_sql_with_params_options_profiled(QUERY, &parameters, QueryStreamOptions::default())
        .expect("query replacement generation through PostgreSQL SQL");
    assert_eq!(profiled.output.rows.len(), 3);
    assert_eq!(
        profiled.output.rows[0]["id"],
        Value::String("community-b".into())
    );
    assert_eq!(
        profiled.output.rows[0]["name"],
        Value::String("Beta revised".into())
    );
    assert_eq!(
        profiled.output.rows[2]["id"],
        Value::String("community-c".into())
    );
    assert_eq!(
        profiled.profile.row_read.runtime_path,
        "projection_generation"
    );
    assert_eq!(
        profiled.profile.row_read.projection_generation.as_deref(),
        Some("generation-2")
    );
    assert_eq!(
        profiled.profile.row_read.projection_source_watermark,
        Some(42)
    );
    assert_eq!(profiled.profile.row_read.projection_version, Some(1));
    assert!(profiled
        .profile
        .row_read
        .projection_publication_commit_epoch
        .is_some());
    assert!(profiled.profile.row_read.logical_pages > 0);
    assert!(profiled.profile.index_reads.is_empty());

    let unbound = pinned_generation_two
        .query_sql_with_params_options_profiled(
            "SELECT note FROM projection_audit WHERE id = 1",
            &[],
            QueryStreamOptions::default(),
        )
        .expect("query unbound table from the pinned database snapshot");
    assert_eq!(
        unbound.output.rows[0]["note"],
        Value::String("canonical".into())
    );
    assert_ne!(
        unbound.profile.row_read.runtime_path,
        "projection_generation"
    );
    assert_eq!(unbound.profile.row_read.projection_generation, None);

    let explained = pinned_generation_two
        .query_sql_with_params(&format!("EXPLAIN ANALYZE {QUERY}"), &parameters)
        .expect("explain replacement generation query");
    let explain_info = explained
        .rows
        .iter()
        .filter_map(|row| match row.get("operator info") {
            Some(Value::String(info)) => Some(info.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(explain_info.contains("row_runtime_path=projection_generation"));
    assert!(explain_info.contains("row_projection_generation=generation-2"));
    assert!(explain_info.contains("row_projection_source_watermark=42"));
    assert!(explain_info.contains("row_projection_version=1"));

    let canonical = database.begin_read_transaction();
    assert!(canonical
        .query_sql("SELECT id FROM communities")
        .expect("query canonical relational table")
        .rows
        .is_empty());

    drop(canonical);
    drop(pinned_generation_two);
    drop(before_publish);
    drop(pinned_generation_one);
    drop(database);

    let constrained = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_read_result_rows: Some(2),
            ..DatabaseConfig::default()
        },
    )
    .expect("reopen projection database with a constrained row budget");
    let constrained_reader = constrained
        .begin_projection_read_transaction(community_binding())
        .expect("pin generation with a constrained row budget");
    let row_budget_error = constrained_reader
        .query_sql("SELECT COUNT(*) AS count FROM community_entities")
        .expect_err("projection scan row exhaustion must fail the whole statement");
    assert!(matches!(row_budget_error, SkeinError::Execution(_)));
    drop(constrained_reader);
    drop(constrained);

    let mut mixed_source = Database::open(&path).expect("reopen writable projection database");
    mixed_source
        .query_sql(
            "INSERT INTO communities (\
                workspace_id, space_id, id, stable_key, name, member_count\
             ) VALUES (\
                'workspace-1', 'space-1', 'canonical', 'canonical', 'canonical', 1\
             )",
        )
        .expect("insert canonical row into a bound table");
    let mixed_source_error = mixed_source
        .begin_projection_read_transaction(community_binding())
        .expect_err("bound projection tables must reject canonical rows");
    assert!(matches!(
        mixed_source_error,
        SkeinError::StorageIntegrity(message) if message.contains("canonical rows")
    ));
    drop(mixed_source);
    std::fs::remove_dir_all(path).expect("remove relational projection fixture");
}
