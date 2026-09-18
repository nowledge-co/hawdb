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

use hawdb::{
    Database, DatabaseConfig, DatabaseReadTransaction, QueryOutput, RelationalIndexMode,
    RelationalRowPageCompactionConfig, StorageResidencyMode, Value,
};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const ROW_PAGE_COMPACTION_PROTOCOL: &str = "hawdb-row-page-compaction-fuzz-v1";
type Model = Vec<BTreeMap<i64, String>>;

/// A replayable state machine with an independent row model and pinned views.
pub fn run_row_page_compaction_case(seed: u64) -> Result<JsonValue, String> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "hawdb-row-page-fuzz-{}-{seed}-{nonce}",
        std::process::id(),
    ));
    let result = run_case(&path, seed).map_err(|error| format!("row-page seed {seed}: {error}"));
    let _ = fs::remove_dir_all(path);
    result
}

fn run_case(path: &Path, seed: u64) -> Result<JsonValue, Box<dyn Error>> {
    let tables = 4 + (seed % 3) as usize;
    let mut model = vec![BTreeMap::new(); tables];
    let mut db = Database::open_with_config(
        path,
        DatabaseConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..Default::default()
        },
    )?;
    for table in 0..tables {
        db.query_sql(&format!(
            "CREATE TABLE documents_{table} (id BIGINT PRIMARY KEY, body TEXT NOT NULL)"
        ))?;
        write_row(
            &mut db,
            &mut model,
            table,
            1,
            format!("seed-{seed}-{}", "x".repeat(8192)),
        )?;
    }
    db.checkpoint()?;
    drop(db);
    let out_of_core = seed & 1 != 0;
    let config = DatabaseConfig {
        storage_residency_mode: if out_of_core {
            StorageResidencyMode::OutOfCore
        } else {
            StorageResidencyMode::Materialized
        },
        relational_index_mode: if out_of_core {
            RelationalIndexMode::Authoritative
        } else {
            RelationalIndexMode::Shadow
        },
        ..Default::default()
    };
    let mut db = Database::open_with_config(path, config.clone())?;
    let mut state = seed.wrapping_add(1);
    let mut relocated_pages = 0;
    let mut mutation_counts = [0usize; 3];
    for cycle in 0..2 {
        let pinned = db.begin_read_transaction();
        let pinned_model = model.clone();
        // A shrinking hot set guarantees sparse generations, independently of
        // the later random row mutations and inline/overflow choices.
        for hot_start in 1..tables {
            for table in hot_start..tables {
                write_row(
                    &mut db,
                    &mut model,
                    table,
                    1,
                    format!("hot-{cycle}-{hot_start}"),
                )?;
            }
            db.checkpoint()?;
        }
        for step in 0..12 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let table = (state.rotate_right(21) as usize) % tables;
            let id = 1 + (state.rotate_right(9) % 3) as i64;
            // Rotate the operation to guarantee every mutation class per case.
            let operation = (step + (seed % 3) as usize) % 3;
            mutation_counts[operation] += 1;
            match operation {
                0 => {
                    let body = format!(
                        "value-{cycle}-{step}-{}",
                        "p".repeat((state % 10000) as usize)
                    );
                    write_row(&mut db, &mut model, table, id, body)?;
                }
                1 => {
                    db.query_sql(&format!("DELETE FROM documents_{table} WHERE id = {id}"))?;
                    model[table].remove(&id);
                }
                _ => {
                    db.query_sql(&format!("DELETE FROM documents_{table} WHERE id = {id}"))?;
                    model[table].remove(&id);
                    write_row(&mut db, &mut model, table, id, format!("reinserted-{step}"))?;
                }
            }
            if step % 3 == 2 {
                let mut rewrite = RelationalRowPageCompactionConfig::default();
                rewrite.rewrite.max_live_ratio_percent = 1 + (state % 100) as u8;
                relocated_pages += db
                    .compact_relational_row_pages(rewrite)?
                    .relocated_pages_written;
                verify_database(&mut db, &model)?;
                verify_pinned(&pinned, &pinned_model)?;
            }
        }
        let before = db.storage_residency_report().relational_rows;
        let mut bounded = RelationalRowPageCompactionConfig::default();
        bounded.rewrite.max_live_ratio_percent = 100;
        bounded.rewrite.max_rewrite_bytes = NonZeroU64::new(1).unwrap();
        if db.compact_relational_row_pages(bounded).is_ok() {
            return Err("rewrite byte limit unexpectedly succeeded".into());
        }
        if db
            .storage_residency_report()
            .relational_rows
            .base_generation
            != before.base_generation
        {
            return Err("failed rewrite changed the selected generation".into());
        }
        bounded.rewrite.max_rewrite_bytes = NonZeroU64::new(64 * 1024 * 1024).unwrap();
        let report = db.compact_relational_row_pages(bounded)?;
        relocated_pages += report.relocated_pages_written;
        if report.allocated_pages != report.root_pages {
            return Err("full compaction retained dead physical slots".into());
        }
        verify_database(&mut db, &model)?;
        verify_pinned(&pinned, &pinned_model)?;
        db.scrub_storage()?;
        drop(pinned);
        db.checkpoint()?;
        let allocation = db.storage_residency_report().relational_rows;
        let physical_bytes =
            fs::read_dir(path)?.try_fold(0u64, |bytes, entry| -> std::io::Result<u64> {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                Ok(bytes
                    + if name.starts_with("relational-row-pages-") && name.ends_with(".pages.hawdb")
                    {
                        entry.metadata()?.len()
                    } else {
                        0
                    })
            })?;
        if physical_bytes != allocation.live_page_bytes {
            return Err(format!(
                "reclaimed {physical_bytes} bytes, live {}",
                allocation.live_page_bytes
            )
            .into());
        }
        drop(db);
        db = Database::open_with_config(path, config.clone())?;
        verify_database(&mut db, &model)?;
        db.scrub_storage()?;
    }
    Ok(json!({
        "protocol": ROW_PAGE_COMPACTION_PROTOCOL,
        "seed": seed,
        "success": true,
        "out_of_core": out_of_core,
        "tables": tables,
        "mutation_counts": mutation_counts,
        "relocated_pages": relocated_pages,
        "reopens": 2,
    }))
}

fn write_row(
    db: &mut Database,
    model: &mut Model,
    table: usize,
    id: i64,
    body: String,
) -> Result<(), Box<dyn Error>> {
    let sql = if model[table].contains_key(&id) {
        format!("UPDATE documents_{table} SET body = $2 WHERE id = $1")
    } else {
        format!("INSERT INTO documents_{table} (id, body) VALUES ($1, $2)")
    };
    db.query_sql_with_params(&sql, &[Value::Int(id), Value::String(body.clone())])?;
    model[table].insert(id, body);
    Ok(())
}

fn verify_database(db: &mut Database, model: &Model) -> Result<(), Box<dyn Error>> {
    for (table, rows) in model.iter().enumerate() {
        verify_rows(
            db.query_sql(&format!(
                "SELECT id, body FROM documents_{table} ORDER BY id"
            ))?,
            rows,
        )?;
    }
    Ok(())
}

fn verify_pinned(db: &DatabaseReadTransaction, model: &Model) -> Result<(), Box<dyn Error>> {
    for (table, rows) in model.iter().enumerate() {
        verify_rows(
            db.query_sql(&format!(
                "SELECT id, body FROM documents_{table} ORDER BY id"
            ))?,
            rows,
        )?;
    }
    Ok(())
}

fn verify_rows(
    output: QueryOutput,
    expected: &BTreeMap<i64, String>,
) -> Result<(), Box<dyn Error>> {
    let expected = expected
        .iter()
        .map(|(id, body)| {
            BTreeMap::from([
                ("id".to_string(), Value::Int(*id)),
                ("body".to_string(), Value::String(body.clone())),
            ])
        })
        .collect::<Vec<_>>();
    if output.rows != expected {
        return Err("row-page state differs from the independent row model".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_page_compaction_state_machine_matches_pins_and_reopen() {
        for seed in [7, 18] {
            let report = run_row_page_compaction_case(seed).unwrap();
            assert_eq!(report["success"], true);
            assert!(report["relocated_pages"].as_u64().unwrap() > 0);
            assert_eq!(report["mutation_counts"], json!([8, 8, 8]));
        }
    }
}
