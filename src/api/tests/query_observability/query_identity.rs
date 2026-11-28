// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::telemetry::{QueryTelemetry, TelemetrySink};
use crate::{ConcurrentDatabase, PlanCacheStats, QueryRequest};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct IdentityTelemetry(Mutex<Vec<(String, bool, usize)>>);

impl TelemetrySink for IdentityTelemetry {
    fn record_query(&self, event: QueryTelemetry<'_>) {
        self.0.lock().unwrap().push((
            event.query_digest.to_owned(),
            event.success,
            event.row_count,
        ));
    }
}

fn fixture(cache_entries: usize, summary_capacity: usize) -> (Database, Arc<IdentityTelemetry>) {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(cache_entries),
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 16,
        statement_summary_capacity: summary_capacity,
        ..DatabaseConfig::default()
    });
    database.query("CREATE INDEX ON :IdentityItem(id)").unwrap();
    for id in 1..=2 {
        database
            .query_with_params(
                "CREATE (:IdentityItem {id: $id, value: $value})",
                &BTreeMap::from([
                    ("id".to_owned(), Value::Int(id)),
                    ("value".to_owned(), Value::Int(id * 10)),
                ]),
            )
            .unwrap();
    }
    let telemetry = Arc::new(IdentityTelemetry::default());
    database.set_telemetry_sink(Some(telemetry.clone()));
    (database, telemetry)
}

fn exercise(
    mut query: impl FnMut(QueryRequest<'_>) -> (Result<QueryOutput>, PlanCacheStats),
    mutation: bool,
    cache_entries: usize,
    telemetry: &IdentityTelemetry,
) -> [hawdb_query::QueryIdentity; 2] {
    let queries = if mutation {
        [
            "MATCH (n:IdentityItem {id: $id}) SET n.value = $value RETURN n.value AS value",
            " match (n:IdentityItem { id : $id }) set n.value = $value return n.value as value; ",
        ]
    } else {
        [
            "MATCH (n:IdentityItem {id: $id}) RETURN n.value AS value",
            " match (n:IdentityItem { id : $id }) return n.value as value; ",
        ]
    };
    let identities = queries.map(|text| hawdb_query::QueryIdentity::new("cypher", text));
    assert_eq!(identities[0].query_digest(), identities[1].query_digest());
    assert_ne!(
        identities[0].query_text_hash(),
        identities[1].query_text_hash()
    );
    let mut previous_stats: Option<PlanCacheStats> = None;
    for (index, text) in queries.into_iter().enumerate() {
        let id = index as i64 + 1;
        let parameters = BTreeMap::from([
            ("id".to_owned(), Value::Int(id)),
            ("value".to_owned(), Value::Int(id * 100)),
        ]);
        let (output, stats) = query(QueryRequest::new(text).with_params(&parameters));
        let output = output.unwrap();
        if let Some(previous) = previous_stats {
            assert_eq!(
                stats.hits,
                previous.hits + u64::from(!mutation && cache_entries > 0),
                "unexpected cache reuse: mutation={mutation}, cache_entries={cache_entries}"
            );
            if mutation {
                assert_eq!(stats.bypasses, previous.bypasses + 1);
            }
        }
        previous_stats = Some(stats);
        assert_eq!(
            output.rows,
            vec![BTreeMap::from([(
                "value".to_owned(),
                Value::Int(id * if mutation { 100 } else { 10 }),
            )])]
        );
    }
    let parameters = BTreeMap::from([
        ("id".to_owned(), Value::Int(1)),
        ("value".to_owned(), Value::Int(999)),
    ]);
    // Refuse after a plan exists, then fail during preparation. Neither error
    // belongs in the completed slow-query ring; both belong in the summary.
    query(
        QueryRequest::new(queries[1])
            .with_params(&parameters)
            .with_output_limits(QueryStreamOptions {
                max_rows: Some(0),
                max_payload_bytes: None,
            }),
    )
    .0
    .expect_err("output budget must refuse a nonempty result");
    query(QueryRequest::new(queries[0]))
        .0
        .expect_err("missing binding must fail preparation");
    let recovered = query(QueryRequest::new(queries[1]).with_params(&parameters))
        .0
        .unwrap();
    assert_eq!(
        recovered.rows,
        vec![BTreeMap::from([(
            "value".to_owned(),
            Value::Int(if mutation { 999 } else { 10 })
        )])],
    );
    let digest = identities[0].query_digest().to_owned();
    assert_eq!(
        *telemetry.0.lock().unwrap(),
        vec![
            (digest.clone(), true, 1),
            (digest.clone(), true, 1),
            (digest.clone(), false, 0),
            (digest.clone(), false, 0),
            (digest, true, 1),
        ]
    );
    identities
}

fn assert_observations(
    mut query_sql: impl FnMut(&str, &[Value]) -> Result<QueryOutput>,
    identities: &[hawdb_query::QueryIdentity; 2],
    summary_capacity: usize,
) {
    let parameters = [Value::String(identities[0].query_digest().to_owned())];
    let slow = query_sql(
        "SELECT query_digest, query_text_hash, row_count FROM system.slow_queries \
         WHERE query_digest = $1 ORDER BY sequence",
        &parameters,
    )
    .unwrap();
    assert_eq!(slow.rows.len(), 3);
    for (row, identity) in slow
        .rows
        .iter()
        .zip([&identities[0], &identities[1], &identities[1]])
    {
        assert_eq!(row.get("row_count"), Some(&Value::Int(1)));
        assert_eq!(
            row.get("query_digest"),
            Some(&Value::String(identity.query_digest().to_owned()))
        );
        assert_eq!(
            row.get("query_text_hash"),
            Some(&Value::String(identity.query_text_hash().to_owned()))
        );
    }
    let summary = query_sql(
        "SELECT execution_count, success_count, error_count, sample_query_text_hash, last_success, last_error \
         FROM system.statement_summary WHERE digest = $1",
        &parameters,
    )
    .unwrap();
    assert_eq!(summary.rows.len(), usize::from(summary_capacity > 0));
    if summary_capacity == 0 {
        return;
    }
    let row = &summary.rows[0];
    assert_eq!(row.get("execution_count"), Some(&Value::Int(5)));
    assert_eq!(row.get("success_count"), Some(&Value::Int(3)));
    assert_eq!(row.get("error_count"), Some(&Value::Int(2)));
    assert_eq!(row.get("last_success"), Some(&Value::Bool(true)));
    assert_eq!(row.get("last_error"), Some(&Value::Null));
    assert_eq!(
        row.get("sample_query_text_hash"),
        Some(&Value::String(identities[0].query_text_hash().to_owned()))
    );
}

#[test]
fn ordinary_query_identity_survives_rebinding_disabled_cache_and_failure() {
    for (cache_entries, summary_capacity) in [(0, 0), (0, 16), (8, 0), (8, 16)] {
        for mutation in [false, true] {
            let (mut database, telemetry) = fixture(cache_entries, summary_capacity);
            let identities = exercise(
                |request| {
                    let result = database.query_request(request);
                    (result, database.plan_cache_stats().unwrap())
                },
                mutation,
                cache_entries,
                &telemetry,
            );
            assert_observations(
                |sql, parameters| database.query_sql_with_params(sql, parameters),
                &identities,
                summary_capacity,
            );
        }
    }
}

fn concurrent_cache_stats(database: &ConcurrentDatabase) -> PlanCacheStats {
    let mut stats = None;
    // Inspect the same owner cache used for publication preparation. A read
    // transaction may own a separate cache, and querying SQL would emit events.
    database
        .with_autocommit_exclusive(|owner| {
            stats = Some(owner.plan_cache_stats()?);
            Ok(QueryOutput::from_query_rows(Vec::new().into()))
        })
        .unwrap();
    stats.unwrap()
}

#[test]
fn concurrent_query_identity_survives_rebinding_disabled_cache_and_failure() {
    for (cache_entries, summary_capacity) in [(0, 0), (0, 16), (8, 0), (8, 16)] {
        for mutation in [false, true] {
            let (database, telemetry) = fixture(cache_entries, summary_capacity);
            let database = database.into_concurrent();
            let identities = exercise(
                |request| {
                    let result = database.query_request(request);
                    (result, concurrent_cache_stats(&database))
                },
                mutation,
                cache_entries,
                &telemetry,
            );
            assert_observations(
                |sql, parameters| database.query_sql_with_params(sql, parameters),
                &identities,
                summary_capacity,
            );
        }
    }
}
