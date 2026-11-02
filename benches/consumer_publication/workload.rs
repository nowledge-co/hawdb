// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb::*;
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

const FEED_ITEMS: usize = 100_000;
// This fixture's Done-only mapper consumes an intake and a mapping call per
// item (including End), followed by one close. It emits no delta mutations.
pub const STAGING_ADVANCE_CALL_LIMIT: usize = 2 * FEED_ITEMS + 1;

fn document(row: QueryRowRef<'_>) -> SearchDocument {
    let Value::String(id) = &row["id"] else {
        panic!("text ID")
    };
    let Value::String(content) = &row["content"] else {
        panic!("text content")
    };
    SearchDocument {
        id: id.clone(),
        title: String::new(),
        content: content.clone(),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}

const READ: &str = "MATCH (n:Memory) WHERE id(n)=$id RETURN n.id AS id,n.content AS content";

fn delta(
    context: &mut ProjectionMappingContext<'_, '_, '_>,
    input: ProjectionMappingInput<'_>,
    _: &[u8],
) -> Result<ProjectionMappingStatus> {
    if let ProjectionMappingInput::GraphNode { node_id, .. } = input {
        let rows = context.query_with_params(
            READ,
            &BTreeMap::from([("id".into(), Value::Int(node_id.try_into().unwrap()))]),
        )?;
        if let Some(row) = rows.output.rows.first() {
            context.upsert(document(row))?;
        }
    }
    Ok(ProjectionMappingStatus::Done)
}

fn full(
    context: &mut ProjectionBaselineMappingContext<'_, '_>,
    input: ProjectionFullSourceItem<'_>,
) -> Result<Option<SearchDocument>> {
    if let ProjectionFullSourceItem::GraphNodeCandidate { node_id } = input {
        let rows = context.query_with_params(
            READ,
            &BTreeMap::from([("id".into(), Value::Int(node_id.try_into().unwrap()))]),
        )?;
        return Ok(rows.output.rows.first().map(document));
    }
    // This fixture's projection contains Memory nodes only. Foreground writers
    // mutate the explicitly excluded relational counter table.
    Ok(None)
}

fn feed() -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: nz(1_000_000),
        items: nz(FEED_ITEMS),
        bytes: nz(64 << 20),
    }
}

pub fn begin<'a>(
    database: &mut Database,
    consumer: &'a SearchProjectionConsumer,
    scoped: bool,
) -> ProjectionConsumerStagingSession<'a> {
    database
        .begin_search_projection_consumer_catch_up_in(
            consumer,
            ProjectionConsumerCatchUpConfig {
                reads: PreparedReadLimits {
                    dependency_entries: nz(16_384),
                    dependency_bytes: nz(4 << 20),
                    statement_rows: nz(1),
                    statement_payload_bytes: nz(8192),
                },
                feed: feed(),
                mapping: ProjectionMappingLimits {
                    calls: nz(100_000),
                    queries: nz(100_000),
                    reserved_rows: nz(100_000),
                    reserved_payload_bytes: nz(64 << 20),
                    queries_per_call: nz(1),
                    input_bytes: nz(1 << 20),
                    continuation_bytes: nz(128),
                },
                sorting: SearchUnorderedMutationLimits {
                    max_buffer_records: nz(256),
                    max_buffer_payload_bytes: nz(1 << 20),
                    ..Default::default()
                },
                task: RuntimeTaskContext::default(),
            },
            delta,
            if scoped {
                ProjectionSourceScope::Domains {
                    node_labels: vec!["Memory".into()],
                    relational_tables: vec![],
                }
            } else {
                ProjectionSourceScope::Database
            },
        )
        .unwrap()
}

pub fn prepare(
    mut session: ProjectionConsumerStagingSession<'_>,
    scoped: bool,
) -> (ProjectionCoveredConsumerUpdate, usize) {
    let quantum = ProjectionFeedLimits {
        work: nz(256),
        items: nz(64),
        bytes: nz(1 << 20),
    };
    let mut staging_calls = 0;
    let mut complete = false;
    for _ in 0..STAGING_ADVANCE_CALL_LIMIT {
        staging_calls += 1;
        if session.advance(quantum, Default::default()).unwrap()
            == ProjectionStagingSessionStep::Complete
        {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "staging exceeded {STAGING_ADVANCE_CALL_LIMIT} calls at source epoch {}",
        session.source_epoch()
    );
    let mut coverage = session
        .verify_complete_source_in(
            ProjectionCoverageConfig {
                feed: feed(),
                source: Default::default(),
                mapping: ProjectionBaselineMappingLimits {
                    calls: nz(100_000),
                    queries: nz(100_000),
                    reserved_rows: nz(100_000),
                    reserved_payload_bytes: nz(64 << 20),
                    queries_per_call: nz(1),
                },
                comparison: SearchBaselineComparisonLimits {
                    lookups: nz(100_000),
                    compressed_segment_bytes: NonZeroU64::new(64 << 20).unwrap(),
                    expanded_segment_bytes: NonZeroU64::new(64 << 20).unwrap(),
                    // Each lookup prepays the full segment allowance, even
                    // when repeated documents share a much smaller segment.
                    total_compressed_bytes: NonZeroU64::new(512 << 30).unwrap(),
                    total_expanded_bytes: NonZeroU64::new(512 << 30).unwrap(),
                },
            },
            full,
            if scoped {
                ProjectionSourceScope::Domains {
                    node_labels: vec!["Memory".into()],
                    relational_tables: vec![],
                }
            } else {
                ProjectionSourceScope::Database
            },
        )
        .unwrap();
    for _ in 0..100_000 {
        if coverage.advance(quantum, Default::default()).unwrap()
            == ProjectionCoverageStep::Complete
        {
            return (coverage.finish().unwrap(), staging_calls);
        }
    }
    panic!("coverage exceeded its bounded step count");
}

pub fn legacy(database: &mut Database, consumer: &mut SearchProjectionConsumer) {
    database
        .catch_up_search_projection_consumer(consumer, 100_000, 100_000, 1, |_, batch| {
            Ok(SearchProjectionRelationalDelta {
                processed_primary_key_count: batch
                    .relational_primary_key_changes()
                    .iter()
                    .map(|table| table.primary_keys.len())
                    .sum(),
                ..Default::default()
            })
        })
        .unwrap();
}

pub fn setup(
    root: &Path,
    documents: usize,
    payload_bytes: usize,
    writers: usize,
    retention_entries: usize,
) -> (Database, SearchProjectionConsumer) {
    let mut database = Database::open_with_config(
        root.join("source"),
        DatabaseConfig {
            automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
            // Keep this finite measurement's complete backlog available. The
            // default 4096-entry window can expire while a slow control builds.
            max_search_projection_change_log_entries: Some(retention_entries),
            ..Default::default()
        },
    )
    .unwrap();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            background_task_limit: Some(nz(1)),
            memory_budget_bytes: Some(512 << 20),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(nz(8), None, None),
            RuntimeMemorySnapshot::from_limits(Some(2 << 30), Some(2 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(4, 1),
    );
    governor.pin_resources();
    database.set_runtime_governor(governor);
    database
        .query_sql("CREATE TABLE foreground (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    database
        .query_sql("CREATE TABLE maintenance_trigger (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    database
        .query_sql("INSERT INTO maintenance_trigger (id,value) VALUES (0,0)")
        .unwrap();
    let mut seed = database.begin_transaction().unwrap();
    for id in 0..writers {
        seed.query_sql_with_params(
            "INSERT INTO foreground (id,value) VALUES ($1,0)",
            &[Value::Int(id.try_into().unwrap())],
        )
        .unwrap();
    }
    for id in 0..documents {
        seed.query_with_params(
            "CREATE (:Memory {id:$id,content:$content})",
            &BTreeMap::from([
                ("id".into(), Value::String(format!("m{id}"))),
                ("content".into(), Value::String("x".repeat(payload_bytes))),
            ]),
        )
        .unwrap();
    }
    seed.commit().unwrap();
    database.checkpoint().unwrap();
    let consumer = database
        .create_search_projection_consumer(
            SearchProjectionConsumerId::new("publication-qualification-v1").unwrap(),
            root.join("consumer"),
            SearchProjectionConsumerOptions::new(NonZeroU64::new(1_000_000).unwrap()),
            |snapshot, index| {
                for row in snapshot
                    .query("MATCH (n:Memory) RETURN n.id AS id,n.content AS content")?
                    .rows
                    .iter()
                {
                    index.upsert(document(row))?;
                }
                Ok(())
            },
        )
        .unwrap();
    (database, consumer)
}
