use serde_json::{json, Value as JsonValue};
use skein::{
    AppendGeneratedRow, AppendOrderMode, AppendTableSchema, AppendTransaction, AppendWrite,
    Database, RelationalColumnSchema, RelationalKey, RelationalRow, RelationalScalarType,
    RelationalValue,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const APPEND_STATE_MACHINE_PROTOCOL: &str = "skein-append-state-machine-fuzz-v2";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelRow {
    sequence: i64,
    payload: String,
}

#[derive(Default)]
struct AppendModel {
    partitions: BTreeMap<String, Vec<ModelRow>>,
}

impl AppendModel {
    fn watermark(&self, partition: &str) -> i64 {
        self.partitions
            .get(partition)
            .and_then(|rows| rows.last())
            .map_or(-1, |row| row.sequence)
    }

    fn append(&mut self, partition: &str, rows: Vec<ModelRow>) {
        self.partitions
            .entry(partition.to_string())
            .or_default()
            .extend(rows);
    }

    fn global_watermark(&self) -> i64 {
        self.partitions
            .values()
            .flat_map(|rows| rows.iter())
            .map(|row| row.sequence)
            .max()
            .unwrap_or(0)
    }

    fn tail(&self, partition: &str, after: Option<i64>, max_rows: usize) -> Vec<ModelRow> {
        self.partitions
            .get(partition)
            .into_iter()
            .flatten()
            .filter(|row| after.is_none_or(|value| row.sequence > value))
            .take(max_rows)
            .cloned()
            .collect()
    }
}

pub fn run_append_state_machine_case(seed: u64, steps: usize) -> Result<JsonValue, String> {
    if steps == 0 {
        return Err("append state-machine case requires at least one step".to_string());
    }
    let path = unique_path(seed);
    let result = run_case_at_path(&path, seed, steps);
    let _ = fs::remove_dir_all(&path);
    result
}

fn run_case_at_path(path: &Path, seed: u64, steps: usize) -> Result<JsonValue, String> {
    let mut database = Database::open(path).map_err(|error| error.to_string())?;
    database
        .append_transaction(AppendTransaction {
            writes: vec![
                AppendWrite::CreateTable {
                    schema: append_schema(),
                },
                AppendWrite::CreateTable {
                    schema: generated_append_schema(),
                },
            ],
        })
        .map_err(|error| error.to_string())?;
    let mut model = AppendModel::default();
    let mut generated_model = AppendModel::default();
    let mut rng = DeterministicRng::new(seed);
    let mut action_counts = BTreeMap::<&'static str, usize>::new();

    for step in 0..steps {
        let action = rng.next_u64() % 15;
        let result = match action {
            0 | 1 => {
                count_action(&mut action_counts, "valid_append");
                append_valid(&mut database, &mut model, &mut rng, step)
            }
            2 => {
                count_action(&mut action_counts, "duplicate_rejection");
                reject_duplicate(&mut database, &model, &mut rng)
            }
            3 => {
                count_action(&mut action_counts, "out_of_order_rejection");
                reject_out_of_order(&mut database, &model, &mut rng)
            }
            4 => {
                count_action(&mut action_counts, "arity_rejection");
                reject_invalid(
                    &mut database,
                    AppendTransaction {
                        writes: vec![AppendWrite::Append {
                            table: "events".to_string(),
                            rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                                "partition-0000".to_string(),
                            )])],
                        }],
                    },
                    "wrong row arity",
                )
            }
            5 => {
                count_action(&mut action_counts, "type_rejection");
                reject_invalid(
                    &mut database,
                    AppendTransaction {
                        writes: vec![AppendWrite::Append {
                            table: "events".to_string(),
                            rows: vec![RelationalRow::new(vec![
                                RelationalValue::BigInt(7),
                                RelationalValue::BigInt(7),
                                RelationalValue::Text("wrong-partition-type".to_string()),
                            ])],
                        }],
                    },
                    "wrong partition-key type",
                )
            }
            6 => {
                count_action(&mut action_counts, "unknown_table_rejection");
                reject_invalid(
                    &mut database,
                    AppendTransaction {
                        writes: vec![AppendWrite::Append {
                            table: "missing".to_string(),
                            rows: vec![row("partition-0000", 0, "missing")],
                        }],
                    },
                    "unknown table",
                )
            }
            7 => {
                count_action(&mut action_counts, "checkpoint_reopen");
                database.checkpoint().map_err(|error| error.to_string())?;
                drop(database);
                database = Database::open(path).map_err(|error| error.to_string())?;
                Ok(())
            }
            8 => {
                count_action(&mut action_counts, "wal_reopen");
                drop(database);
                database = Database::open(path).map_err(|error| error.to_string())?;
                Ok(())
            }
            9 => {
                count_action(&mut action_counts, "bounded_tail");
                verify_random_tail(&database, &model, &mut rng)
            }
            10 => {
                count_action(&mut action_counts, "payload_budget_rejection");
                verify_payload_budget(&database, &model, &mut rng)
            }
            11 | 12 => {
                count_action(&mut action_counts, "generated_append");
                append_generated_valid(&mut database, &mut generated_model, &mut rng, step)
            }
            13 => {
                count_action(&mut action_counts, "generated_override_rejection");
                reject_invalid(
                    &mut database,
                    AppendTransaction {
                        writes: vec![AppendWrite::Append {
                            table: "generated_events".to_string(),
                            rows: vec![row("partition-0000", 1, "override")],
                        }],
                    },
                    "generated order override",
                )
            }
            _ => {
                count_action(&mut action_counts, "generated_arity_rejection");
                reject_invalid(
                    &mut database,
                    AppendTransaction {
                        writes: vec![AppendWrite::AppendGenerated {
                            table: "generated_events".to_string(),
                            rows: vec![AppendGeneratedRow::new(vec![RelationalValue::Text(
                                "partition-0000".to_string(),
                            )])],
                        }],
                    },
                    "generated row arity",
                )
            }
        };
        result.map_err(|error| format!("step {step} action {action}: {error}"))?;
        verify_all_partitions(&database, &model)
            .map_err(|error| format!("step {step} state mismatch: {error}"))?;
        verify_generated_partitions(&database, &generated_model)
            .map_err(|error| format!("step {step} generated state mismatch: {error}"))?;
    }

    database.checkpoint().map_err(|error| error.to_string())?;
    drop(database);
    let database = Database::open(path).map_err(|error| error.to_string())?;
    verify_all_partitions(&database, &model)
        .map_err(|error| format!("final checkpoint/reopen mismatch: {error}"))?;
    verify_generated_partitions(&database, &generated_model)
        .map_err(|error| format!("final generated checkpoint/reopen mismatch: {error}"))?;
    Ok(json!({
        "protocol": APPEND_STATE_MACHINE_PROTOCOL,
        "seed": seed,
        "steps": steps,
        "row_count": model.partitions.values().map(Vec::len).sum::<usize>(),
        "generated_row_count": generated_model.partitions.values().map(Vec::len).sum::<usize>(),
        "partition_count": model.partitions.len(),
        "action_counts": action_counts,
        "success": true,
    }))
}

fn append_valid(
    database: &mut Database,
    model: &mut AppendModel,
    rng: &mut DeterministicRng,
    step: usize,
) -> Result<(), String> {
    let partition = partition_name((rng.next_u64() % 4) as usize);
    let count = 1 + (rng.next_u64() % 4) as usize;
    let first = model.watermark(&partition) + 1;
    let model_rows = (0..count)
        .map(|offset| {
            let sequence = first + offset as i64;
            ModelRow {
                sequence,
                payload: format!("seed-{step:04}-sequence-{sequence:08}"),
            }
        })
        .collect::<Vec<_>>();
    database
        .append_transaction(AppendTransaction {
            writes: vec![AppendWrite::Append {
                table: "events".to_string(),
                rows: model_rows
                    .iter()
                    .map(|model_row| row(&partition, model_row.sequence, &model_row.payload))
                    .collect(),
            }],
        })
        .map_err(|error| format!("valid append was rejected: {error}"))?;
    model.append(&partition, model_rows);
    Ok(())
}

fn append_generated_valid(
    database: &mut Database,
    model: &mut AppendModel,
    rng: &mut DeterministicRng,
    step: usize,
) -> Result<(), String> {
    let count = 1 + (rng.next_u64() % 4) as usize;
    let first = model.global_watermark() + 1;
    let input = (0..count)
        .map(|offset| {
            let sequence = first + offset as i64;
            let partition = partition_name((rng.next_u64() % 4) as usize);
            let payload = format!("generated-{step:04}-sequence-{sequence:08}");
            (partition, ModelRow { sequence, payload })
        })
        .collect::<Vec<_>>();
    let result = database
        .append_transaction_with_result(AppendTransaction {
            writes: vec![AppendWrite::AppendGenerated {
                table: "generated_events".to_string(),
                rows: input
                    .iter()
                    .map(|(partition, row)| {
                        AppendGeneratedRow::new(vec![
                            RelationalValue::Text(partition.clone()),
                            RelationalValue::Text(row.payload.clone()),
                        ])
                    })
                    .collect(),
            }],
        })
        .map_err(|error| format!("valid generated append was rejected: {error}"))?;
    let actual_keys = result
        .mutations
        .iter()
        .flat_map(|outcome| outcome.generated_order_keys.iter())
        .cloned()
        .collect::<Vec<_>>();
    let expected_keys = (first..first + count as i64)
        .map(|sequence| RelationalKey(vec![RelationalValue::BigInt(sequence)]))
        .collect::<Vec<_>>();
    if actual_keys != expected_keys {
        return Err(format!(
            "generated assignments differ: expected={expected_keys:?} actual={actual_keys:?}"
        ));
    }
    for (partition, row) in input {
        model.append(&partition, vec![row]);
    }
    Ok(())
}

fn reject_duplicate(
    database: &mut Database,
    model: &AppendModel,
    rng: &mut DeterministicRng,
) -> Result<(), String> {
    let partition = partition_name((rng.next_u64() % 4) as usize);
    let sequence = model.watermark(&partition).max(0);
    reject_invalid(
        database,
        AppendTransaction {
            writes: vec![AppendWrite::Append {
                table: "events".to_string(),
                rows: vec![
                    row(&partition, sequence, "duplicate-a"),
                    row(&partition, sequence, "duplicate-b"),
                ],
            }],
        },
        "duplicate order key",
    )
}

fn reject_out_of_order(
    database: &mut Database,
    model: &AppendModel,
    rng: &mut DeterministicRng,
) -> Result<(), String> {
    let partition = partition_name((rng.next_u64() % 4) as usize);
    let watermark = model.watermark(&partition);
    let rows = if watermark >= 0 {
        vec![row(&partition, watermark, "stale")]
    } else {
        vec![row(&partition, 1, "newer"), row(&partition, 0, "older")]
    };
    reject_invalid(
        database,
        AppendTransaction {
            writes: vec![AppendWrite::Append {
                table: "events".to_string(),
                rows,
            }],
        },
        "out-of-order key",
    )
}

fn reject_invalid(
    database: &mut Database,
    transaction: AppendTransaction,
    label: &str,
) -> Result<(), String> {
    let before = database.commit_epoch();
    if database.append_transaction(transaction).is_ok() {
        return Err(format!("{label} was accepted"));
    }
    if database.commit_epoch() != before {
        return Err(format!("{label} changed the commit epoch"));
    }
    Ok(())
}

fn verify_random_tail(
    database: &Database,
    model: &AppendModel,
    rng: &mut DeterministicRng,
) -> Result<(), String> {
    let partition = partition_name((rng.next_u64() % 4) as usize);
    let watermark = model.watermark(&partition);
    let after = (watermark >= 0).then(|| {
        let distance = (rng.next_u64() % 6) as i64;
        watermark.saturating_sub(distance)
    });
    let max_rows = 1 + (rng.next_u64() % 8) as usize;
    verify_tail(database, "events", model, &partition, after, max_rows)
}

fn verify_payload_budget(
    database: &Database,
    model: &AppendModel,
    rng: &mut DeterministicRng,
) -> Result<(), String> {
    let Some((partition, _)) = model
        .partitions
        .iter()
        .filter(|(_, rows)| !rows.is_empty())
        .nth((rng.next_u64() as usize) % model.partitions.len().max(1))
    else {
        return Ok(());
    };
    let result =
        database.read_append_partition_bounded("events", &partition_key(partition), None, 1, 0);
    if result.is_ok() {
        return Err("zero payload budget accepted a non-empty tail".to_string());
    }
    Ok(())
}

fn verify_all_partitions(database: &Database, model: &AppendModel) -> Result<(), String> {
    for index in 0..4 {
        let partition = partition_name(index);
        let max_rows = model
            .partitions
            .get(&partition)
            .map_or(1, |rows| rows.len() + 1);
        verify_tail(database, "events", model, &partition, None, max_rows)?;
    }
    Ok(())
}

fn verify_generated_partitions(database: &Database, model: &AppendModel) -> Result<(), String> {
    for index in 0..4 {
        let partition = partition_name(index);
        let max_rows = model
            .partitions
            .get(&partition)
            .map_or(1, |rows| rows.len() + 1);
        verify_tail(
            database,
            "generated_events",
            model,
            &partition,
            None,
            max_rows,
        )?;
    }
    Ok(())
}

fn verify_tail(
    database: &Database,
    table: &str,
    model: &AppendModel,
    partition: &str,
    after: Option<i64>,
    max_rows: usize,
) -> Result<(), String> {
    let after_key = after.map(|sequence| RelationalKey(vec![RelationalValue::BigInt(sequence)]));
    let actual = database
        .read_append_partition(
            table,
            &partition_key(partition),
            after_key.as_ref(),
            max_rows,
        )
        .map_err(|error| error.to_string())?
        .rows
        .iter()
        .map(decode_model_row)
        .collect::<Result<Vec<_>, _>>()?;
    let expected = model.tail(partition, after, max_rows);
    if actual != expected {
        return Err(format!(
            "table {table} partition {partition} tail differs: after={after:?} max_rows={max_rows} expected={expected:?} actual={actual:?}"
        ));
    }
    Ok(())
}

fn decode_model_row(row: &skein::AppendTableRow) -> Result<ModelRow, String> {
    match row.row.values() {
        [RelationalValue::Text(_), RelationalValue::BigInt(sequence), RelationalValue::Text(payload)] => {
            Ok(ModelRow {
                sequence: *sequence,
                payload: payload.clone(),
            })
        }
        values => Err(format!("unexpected append row values: {values:?}")),
    }
}

fn append_schema() -> AppendTableSchema {
    AppendTableSchema {
        name: "events".to_string(),
        columns: vec![
            column("stream", RelationalScalarType::Text),
            column("sequence", RelationalScalarType::BigInt),
            column("payload", RelationalScalarType::Text),
        ],
        partition_key: vec!["stream".to_string()],
        order_key: vec!["sequence".to_string()],
        order_mode: Default::default(),
    }
}

fn generated_append_schema() -> AppendTableSchema {
    AppendTableSchema {
        name: "generated_events".to_string(),
        order_mode: AppendOrderMode::CommitSequence,
        ..append_schema()
    }
}

fn column(name: &str, scalar_type: RelationalScalarType) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type,
        nullable: false,
        default: None,
    }
}

fn row(partition: &str, sequence: i64, payload: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(partition.to_string()),
        RelationalValue::BigInt(sequence),
        RelationalValue::Text(payload.to_string()),
    ])
}

fn partition_key(partition: &str) -> RelationalKey {
    RelationalKey(vec![RelationalValue::Text(partition.to_string())])
}

fn partition_name(index: usize) -> String {
    format!("partition-{index:04}")
}

fn count_action(counts: &mut BTreeMap<&'static str, usize>, action: &'static str) {
    *counts.entry(action).or_default() += 1;
}

fn unique_path(seed: u64) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "skein-append-state-machine-{}-{seed}-{timestamp}",
        std::process::id()
    ))
}

#[derive(Clone, Copy)]
struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0xa076_1d64_78bd_642f)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0 = self.0.wrapping_mul(0x2545_f491_4f6c_dd1d);
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_state_machine_matches_reference_model_across_reopen() {
        let report = run_append_state_machine_case(7, 128).expect("run append state machine");

        assert_eq!(report["success"], true);
        assert_eq!(report["steps"], 128);
    }

    #[test]
    fn append_state_machine_is_replayable_for_another_seed() {
        let report = run_append_state_machine_case(0xfeed_beef, 128)
            .expect("run second append state machine");

        assert_eq!(report["success"], true);
    }
}
