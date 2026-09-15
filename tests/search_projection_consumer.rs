//! External embedded-library callers use only the public Skein facade.
use skein::{
    Database, DatabaseReadTransaction, SearchDocument, SearchIndex, SearchProjectionChangeBatch,
    SearchProjectionConsumerError, SearchProjectionConsumerId, SearchProjectionConsumerOptions,
    SearchProjectionConsumerRebuildReason, SearchProjectionConsumerState, SearchProjectionDelta,
    SearchProjectionKind, SearchProjectionRelationalDelta, SearchProjectionRow,
    SearchRebuildOptions, SkeinError, Value,
};
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "skein-consumer-facade-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn options(commits: u64) -> SearchProjectionConsumerOptions {
    SearchProjectionConsumerOptions::new(NonZeroU64::new(commits).unwrap())
}
fn initialize(
    snapshot: &mut DatabaseReadTransaction,
    index: &mut SearchIndex,
) -> skein::Result<()> {
    snapshot.rebuild_search_projection(index, SearchRebuildOptions { max_rows: Some(16) })?;
    let output = snapshot.query_sql_with_params_bounded(
        "SELECT body FROM consumer_messages WHERE id = $1 LIMIT $2",
        &[Value::Int(1), Value::Int(1)],
        Some(1),
    )?;
    let Value::String(body) = &output.rows[0]["body"] else {
        return Err(SkeinError::Execution("expected message body".into()));
    };
    index.upsert(SearchDocument {
        id: "message:1".into(),
        title: "message".into(),
        content: body.clone(),
        embedding: None,
        metadata: BTreeMap::new(),
    })
}
fn hydrate(
    snapshot: &mut DatabaseReadTransaction,
    batch: &SearchProjectionChangeBatch,
) -> skein::Result<SearchProjectionRelationalDelta> {
    assert_eq!(batch.relational_primary_key_changes().len(), 1);
    assert_eq!(
        batch.relational_primary_key_changes()[0].table,
        "consumer_messages"
    );
    assert_eq!(
        batch.relational_primary_key_changes()[0].primary_keys.len(),
        1
    );
    let output = snapshot.query_sql_with_params_bounded(
        "SELECT body FROM consumer_messages WHERE id = $1 LIMIT $2",
        &[Value::Int(1), Value::Int(1)],
        Some(1),
    )?;
    let Value::String(body) = &output.rows[0]["body"] else {
        return Err(SkeinError::Execution("expected message body".into()));
    };
    Ok(SearchProjectionRelationalDelta {
        processed_primary_key_count: 1,
        delta: SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Message,
                external_id: "1".into(),
                title: "message".into(),
                body: body.clone(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            ..SearchProjectionDelta::default()
        },
    })
}

#[test]
fn public_consumer_initializes_mixed_content_catches_up_and_reopens() {
    let fixture = Fixture::new();
    let mut db = Database::open(fixture.0.join("database")).unwrap();
    db.query("CREATE (:Memory {id: 'm1', title: 'Graph document'})")
        .unwrap();
    db.query_sql("CREATE TABLE consumer_messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql("INSERT INTO consumer_messages (id, body) VALUES (1, 'first')")
        .unwrap();
    let epoch = db.commit_epoch();
    let id = SearchProjectionConsumerId::new("mixed").unwrap();
    let mut consumer = db
        .create_search_projection_consumer(
            id.clone(),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    assert_eq!(consumer.id(), &id);
    assert_eq!(db.commit_epoch(), epoch);
    assert_eq!(consumer.search_index().document_count(), 2);
    assert_eq!(
        consumer
            .search_index()
            .document("message:1")
            .unwrap()
            .content,
        "first"
    );
    assert!(consumer.search_index().document("memory:m1").is_some());
    db.query_sql("UPDATE consumer_messages SET body = 'second' WHERE id = 1")
        .unwrap();
    let report = db
        .catch_up_search_projection_consumer(&mut consumer, 1, 1, 1, hydrate)
        .unwrap();
    assert!(report.catch_up.complete);
    assert_eq!(report.catch_up.applied_batch_count, 1);
    assert_eq!(report.consumer.state, SearchProjectionConsumerState::Active);
    assert_eq!(
        consumer
            .search_index()
            .document("message:1")
            .unwrap()
            .content,
        "second"
    );
    let renewal = db.renew_search_projection_consumer(&consumer).unwrap();
    assert_eq!(renewal.expires_at_commit_epoch, db.commit_epoch() + 100);
    drop(consumer);
    drop(db);
    let mut db = Database::open(fixture.0.join("database")).unwrap();
    assert_eq!(
        db.search_projection_consumer_status(&id).unwrap().state,
        SearchProjectionConsumerState::Unverified
    );
    let consumer = db
        .open_search_projection_consumer(&id, fixture.0.join("projection"))
        .unwrap();
    assert!(db
        .search_projection_consumer_readiness(&consumer, Some(1))
        .unwrap()
        .is_ready());
    assert_eq!(
        consumer
            .search_index()
            .document("message:1")
            .unwrap()
            .content,
        "second"
    );
    db.unregister_search_projection_consumer(&id).unwrap();
    assert!(matches!(
        db.renew_search_projection_consumer(&consumer),
        Err(SearchProjectionConsumerError::InvalidHandle)
    ));
}

#[test]
fn public_types_validate_ids_and_require_a_durable_source() {
    for invalid in ["", "has space", "a/b", "bad\tfield", "é"] {
        assert!(SearchProjectionConsumerId::new(invalid).is_err());
    }
    assert!(SearchProjectionConsumerId::new("a".repeat(128)).is_ok());
    assert!(SearchProjectionConsumerId::new("a".repeat(129)).is_err());
    let id = SearchProjectionConsumerId::new("consumer_A-1.v2").unwrap();
    assert_eq!(id.as_str(), "consumer_A-1.v2");
    assert_eq!(options(100).max_idle_commits().get(), 100);
    let fixture = Fixture::new();
    let mut db = Database::new();
    let error = db
        .create_search_projection_consumer(
            id,
            fixture.0.join("projection"),
            options(100),
            |_, _| panic!("in-memory source must fail before callback"),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        SearchProjectionConsumerError::SourceNotDurable
    ));
    let _: &dyn std::error::Error = &error;
    assert!(!error.to_string().is_empty());
}

#[test]
fn public_zero_retention_and_expiry_report_typed_rebuild_reasons() {
    let fixture = Fixture::new();
    let config = skein::DatabaseConfig {
        max_search_projection_change_log_entries: Some(0),
        ..skein::DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(fixture.0.join("database"), config).unwrap();
    let id = SearchProjectionConsumerId::new("stalled").unwrap();
    let mut consumer = db
        .create_search_projection_consumer(
            id.clone(),
            fixture.0.join("projection"),
            options(100),
            |snapshot, index| {
                snapshot
                    .rebuild_search_projection(index, SearchRebuildOptions::default())
                    .map(|_| ())
            },
        )
        .unwrap();
    db.query("CREATE (:Memory {id: 'm1', content: 'new'})")
        .unwrap();
    let status = db.search_projection_consumer_status(&id).unwrap();
    assert_eq!(status.changefeed.retained_mutation_count, 0);
    assert!(matches!(
        status.state,
        SearchProjectionConsumerState::RebuildRequired(
            SearchProjectionConsumerRebuildReason::RetentionLimitExceeded { .. }
        )
    ));
    let result = db.catch_up_search_projection_consumer(&mut consumer, 1, 1, 1, |_, _| {
        panic!("retention loss must fail before hydration")
    });
    assert!(matches!(
        result,
        Err(SearchProjectionConsumerError::RebuildRequired(
            SearchProjectionConsumerRebuildReason::RetentionLimitExceeded { .. }
        ))
    ));
}
