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

use hawdb::{Database, DatabaseConfig, RecoveryMode, StorageResidencyMode, Value};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const WAL_TAIL_RECOVERY_PROTOCOL: &str = "hawdb-wal-tail-recovery-fuzz-v1";

/// Replay a truncation case from fresh state, comparing the entire recovered
/// graph with the prefix observed before an unacknowledged multi-record batch.
pub fn run_wal_tail_recovery_case(seed: u64) -> Result<JsonValue, String> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "hawdb-wal-tail-fuzz-{}-{seed}-{nonce}",
        std::process::id()
    ));
    let result = run_case(&path, seed).map_err(|error| format!("WAL tail seed {seed}: {error}"));
    let _ = fs::remove_dir_all(path);
    result
}

fn run_case(path: &Path, seed: u64) -> Result<JsonValue, Box<dyn Error>> {
    let config = DatabaseConfig {
        storage_residency_mode: if seed & 16 == 0 {
            StorageResidencyMode::Materialized
        } else {
            StorageResidencyMode::OutOfCore
        },
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(path, config.clone())?;
    db.query("CREATE (:Memory {id: 1, payload: 'checkpoint'})")?;
    let checkpoint = seed & 8 != 0;
    if checkpoint {
        db.checkpoint()?;
    }
    db.query("CREATE (:Memory {id: 2, payload: 'acknowledged'})")?;
    let query = "MATCH (m:Memory) RETURN m.id AS id, m.payload AS payload ORDER BY id";
    let expected = db.query(query)?.rows;
    let expected_epoch = db.commit_epoch();
    let wal = fs::read_dir(path)?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let generation = path
                .file_name()?
                .to_str()?
                .strip_prefix("wal.")?
                .strip_suffix(".hawdb")?
                .parse::<u64>()
                .ok()?;
            Some((generation, path))
        })
        .max_by_key(|(generation, _)| *generation)
        .ok_or("missing WAL fixture")?
        .1;
    let prefix = fs::read(&wal)?;
    let payload_len = 32 * 1024 + (seed.rotate_right(13) as usize % (64 * 1024));
    let payload = "x".repeat(payload_len);
    let mut tx = db.begin_transaction();
    for index in 0..(2 + seed % 3) {
        tx.query_with_params(
            "CREATE (:Memory {id: $id, payload: $payload})",
            &BTreeMap::from([
                ("id".to_string(), Value::Int(100 + index as i64)),
                ("payload".to_string(), Value::String(payload.clone())),
            ]),
        )?;
    }
    tx.commit()?;
    drop(db);
    let complete = fs::read(&wal)?;
    let record_bytes = complete
        .len()
        .checked_sub(prefix.len())
        .ok_or("WAL shrank during append")?;
    if record_bytes < 32 * 1024 {
        return Err("fixture did not produce a fragmented batch".into());
    }
    let next_block = 28 + ((prefix.len() - 28) / (32 * 1024) + 1) * (32 * 1024);
    let cut = match seed % 8 {
        0 => prefix.len() + 1,
        1 => prefix.len() + 7,
        2 => prefix.len() + 14,
        3 => prefix.len() + 15,
        4 => next_block - 1,
        5 => next_block + 1,
        6 => complete.len() - 1,
        _ => prefix.len() + 1 + (seed.rotate_right(27) as usize % (record_bytes - 1)),
    };
    // Only bytes in the not-yet-acknowledged batch are removed. The expected
    // result comes from the pre-batch query, independently of decoder framing.
    let torn = &complete[..cut];
    fs::write(&wal, torn)?;
    if Database::open_with_config(path, config.clone()).is_ok() {
        return Err("strict open accepted the incomplete batch".into());
    }
    if fs::read(&wal)? != torn {
        return Err("strict open modified the WAL".into());
    }
    let mut repaired = Database::open_with_config(
        path,
        DatabaseConfig {
            recovery_mode: RecoveryMode::AutoRepairTornTail,
            ..config.clone()
        },
    )?;
    if repaired.query(query)?.rows != expected || repaired.commit_epoch() != expected_epoch {
        return Err("repair did not recover exactly the acknowledged prefix".into());
    }
    let report = repaired.storage_recovery_report();
    if !report.torn_tail_repaired || report.discarded_wal_tail_bytes != (cut - prefix.len()) as u64
    {
        return Err("repair report disagrees with the injected tail".into());
    }
    if fs::read(&wal)? != prefix {
        return Err("repair changed the acknowledged WAL prefix".into());
    }
    let audits = fs::read_dir(path.join("doctor"))?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            path.to_string_lossy()
                .ends_with(".repair.applied.json")
                .then_some(path)
        })
        .collect::<Vec<_>>();
    if audits.len() != 1 {
        return Err("missing unique applied audit".into());
    }
    let audit: JsonValue = serde_json::from_slice(&fs::read(&audits[0])?)?;
    let quarantine = audit["quarantine_file"]
        .as_str()
        .ok_or("missing quarantine identity")?;
    if fs::read(path.join("doctor/quarantine").join(quarantine))? != torn {
        return Err("quarantine did not retain exact damaged WAL bytes".into());
    }
    repaired.query("CREATE (:Memory {id: 3, payload: 'after-repair'})")?;
    let after_repair = repaired.query(query)?.rows;
    drop(repaired);
    let mut reopened = Database::open_with_config(path, config)?;
    if reopened.query(query)?.rows != after_repair || reopened.commit_epoch() != expected_epoch + 1
    {
        return Err("post-repair append did not survive strict reopen".into());
    }
    Ok(json!({
        "protocol": WAL_TAIL_RECOVERY_PROTOCOL,
        "seed": seed,
        "checkpoint": checkpoint,
        "payload_bytes": payload_len,
        "prefix_bytes": prefix.len(),
        "batch_bytes": record_bytes,
        "cut_offset": cut,
        "recovered_commit_epoch": expected_epoch,
        "success": true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_wal_tail_recovery_covers_fragment_boundaries_and_residency_modes() {
        for seed in 0..32 {
            let report = run_wal_tail_recovery_case(seed).unwrap();
            assert_eq!(report["success"], true, "seed {seed}");
        }
    }

    #[test]
    fn wal_tail_replay_is_identical_from_fresh_state() {
        assert_eq!(
            run_wal_tail_recovery_case(19).unwrap(),
            run_wal_tail_recovery_case(19).unwrap()
        );
    }
}
