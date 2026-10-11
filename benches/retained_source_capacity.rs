// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb::{Database, RetainedColumnValues, RetainedQueryOptions, RetainedQueryStatus, Value};
use std::collections::BTreeMap;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().ok_or("missing row count")?.parse()?;
    let path = args
        .next()
        .ok_or("missing backend path or '-' for memory")?;
    if !(1..=10_000).contains(&rows) || args.next().is_some() {
        return Err("expected 1..=10000 rows and a backend path".into());
    }
    if path != "-" && std::path::Path::new(&path).exists() {
        return Err("file backend requires a new path".into());
    }
    let backend = if path == "-" { "memory" } else { "file" };
    let mut db = if path == "-" {
        Database::new()
    } else {
        Database::open(path)?
    };
    db.query("CREATE NODE TABLE Item")?;
    db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")?;
    let fixture = (0..rows)
        .map(|score| {
            Ok(Value::Map(BTreeMap::from([(
                "score".into(),
                Value::Int(i64::try_from(score)?),
            )])))
        })
        .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Item {score: row.score})",
        &BTreeMap::from([("rows".into(), Value::List(fixture))]),
    )?;
    let snapshots = (0..4)
        .map(|_| db.begin_read_transaction())
        .collect::<Result<Vec<_>, _>>()?;
    let query = "MATCH (n:Item) WHERE n.score >= $min RETURN n.score AS score";
    let params = BTreeMap::from([("min".into(), Value::Int(0))]);
    let mut creation_ns = Vec::new();
    let mut preflight_rows = Vec::new();
    let mut source_capacity_bytes = Vec::new();
    let mut checksums = Vec::new();
    for snapshot in snapshots {
        let started = Instant::now();
        let mut cursor =
            snapshot.into_retained_query(query, &params, RetainedQueryOptions::default())?;
        creation_ns.push(u64::try_from(started.elapsed().as_nanos())?);
        let profile = cursor.profile();
        assert_eq!(profile.visited_rows, 0);
        assert_eq!(profile.source_constructed_bytes, 0);
        assert_eq!(profile.source_pinned_rows, rows);
        preflight_rows.push(profile.source_preflight_rows);
        source_capacity_bytes.push(profile.source_pinned_capacity_bytes);
        let mut emitted = 0usize;
        let mut checksum = 0u64;
        while let Some(batch) = cursor.next_batch()? {
            let RetainedColumnValues::Int64(values) = batch.column(0)? else {
                return Err("expected integer score column".into());
            };
            for selected in batch.selected_rows() {
                let value = values[usize::try_from(*selected)?];
                assert_eq!(value, i64::try_from(emitted)?);
                checksum = checksum
                    .checked_add(u64::try_from(value)?)
                    .ok_or("sum overflow")?;
                emitted += 1;
            }
        }
        assert_eq!(emitted, rows);
        assert_eq!(cursor.status(), RetainedQueryStatus::Completed);
        assert_eq!(cursor.profile().source_pinned_capacity_bytes, 0);
        checksums.push(checksum);
        cursor.close();
    }
    let resources = db
        .retained_result_snapshot()
        .ok_or("missing resource owner")?;
    assert_eq!(resources.retained_bytes, 0);
    assert_eq!(resources.buffer_owners, 0);
    assert_eq!(resources.view_handles, 0);
    println!(
        "{}",
        serde_json::json!({
            "status": "ok",
            "rows": rows,
            "snapshots": 4,
            "maximum_live_cursors": 1,
            "backend": backend,
            "creation_ns": creation_ns,
            "preflight_rows": preflight_rows,
            "source_capacity_bytes": source_capacity_bytes,
            "checksums": checksums,
            "prefetch": 0,
            "durability": "SyncOnEveryWrite",
            "final_retained_bytes": resources.retained_bytes,
            "final_buffer_owners": resources.buffer_owners,
            "final_view_handles": resources.view_handles,
            "scope": "Four precreated snapshots with one live cursor at a time. Setup and full ordered consumption outside creation timers. No Arrow API or whole-query memory claim."
        })
    );
    Ok(())
}
