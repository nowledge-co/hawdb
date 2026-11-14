// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::case::{Case, Mode, Operation, Program, FORMAT_VERSION};
use crate::engine::Observation;
use std::fmt::Write;

pub fn parse(text: &str) -> Result<Case, String> {
    let mut case = Case {
        format_version: FORMAT_VERSION,
        seed: 0,
        index: 0,
        mode: Mode::Optimistic,
        initial: Default::default(),
        transactions: std::array::from_fn(|_| Program {
            operations: Vec::new(),
            rollback: false,
        }),
        schedule: Vec::new(),
    };
    let mut finished = [false; 2];
    let mut metadata = [false; 2];
    let mut mode_seen = false;
    for (index, line) in text.lines().enumerate() {
        let fields: Vec<_> = line
            .split('#')
            .next()
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        if fields.is_empty() {
            continue;
        }
        let error = |message: &str| format!("line {}: {message}", index + 1);
        let number = |field: &str| {
            field
                .parse::<i64>()
                .map_err(|_| error("expected a signed 64-bit integer"))
        };
        let command = fields[0].to_ascii_uppercase();
        if command == "ROW" {
            if fields.len() != 3 || !case.schedule.is_empty() {
                return Err(error("use ROW <id> <value> before transaction steps"));
            }
            if case
                .initial
                .insert(number(fields[1])?, number(fields[2])?)
                .is_some()
            {
                return Err(error("duplicate fixture primary key"));
            }
            continue;
        }
        if command == "MODE" {
            if fields.len() != 2 || !case.schedule.is_empty() || mode_seen {
                return Err(error(
                    "MODE OPTIMISTIC|PESSIMISTIC must appear once before transaction steps",
                ));
            }
            mode_seen = true;
            case.mode = match fields[1].to_ascii_uppercase().as_str() {
                "OPTIMISTIC" => Mode::Optimistic,
                "PESSIMISTIC" => Mode::Pessimistic,
                _ => return Err(error("expected MODE OPTIMISTIC or MODE PESSIMISTIC")),
            };
            continue;
        }
        if command == "SEED" || command == "INDEX" {
            let slot = usize::from(command == "INDEX");
            if fields.len() != 2 || !case.schedule.is_empty() || metadata[slot] {
                return Err(error(
                    "SEED/INDEX must appear once before transaction steps",
                ));
            }
            metadata[slot] = true;
            let value = fields[1]
                .parse()
                .map_err(|_| error("expected an unsigned 64-bit integer"))?;
            if slot == 0 {
                case.seed = value;
            } else {
                case.index = value;
            }
            continue;
        }
        let tx = match command.as_str() {
            "T1" => 0,
            "T2" => 1,
            _ => return Err(error("expected ROW, SEED, INDEX, MODE, T1 or T2")),
        };
        if finished[tx] || fields.len() < 2 {
            return Err(error("missing operation or transaction already finished"));
        }
        let op = fields[1].to_ascii_uppercase();
        let operation = match (op.as_str(), fields.len()) {
            ("READ", 2) => Some(Operation::Read),
            ("READ_FOR_SHARE", 2) => Some(Operation::LockingRead { exclusive: false }),
            ("READ_FOR_UPDATE", 2) => Some(Operation::LockingRead { exclusive: true }),
            ("GRAPH_READ", 2) => Some(Operation::GraphRead),
            ("GRAPH_UPDATE", 4) => Some(Operation::GraphUpdate { id: number(fields[2])?, value: number(fields[3])? }),
            ("UPDATE", 4) => Some(Operation::Update { id: number(fields[2])?, value: number(fields[3])? }),
            ("INSERT", 4) => Some(Operation::Insert { id: number(fields[2])?, value: number(fields[3])? }),
            ("DELETE", 3) => Some(Operation::Delete { id: number(fields[2])? }),
            ("BROAD_UPDATE", 4) => Some(Operation::BroadUpdate { minimum: number(fields[2])?, value: number(fields[3])? }),
            ("COMMIT" | "ROLLBACK", 2) => {
                finished[tx] = true;
                case.transactions[tx].rollback = op == "ROLLBACK";
                None
            }
            _ => return Err(error("use READ, READ_FOR_SHARE, READ_FOR_UPDATE, GRAPH_READ, GRAPH_UPDATE <id> <value>, UPDATE <id> <value>, INSERT <id> <value>, DELETE <id>, BROAD_UPDATE <minimum> <value>, COMMIT or ROLLBACK")),
        };
        if let Some(operation) = operation {
            case.transactions[tx].operations.push(operation);
        }
        case.schedule.push(tx);
        if case.schedule.len() > 128 || case.initial.len() > 64 {
            return Err(error("case exceeds row or schedule limit"));
        }
    }
    if !finished.iter().all(|done| *done) {
        return Err("each transaction must end with COMMIT or ROLLBACK".into());
    }
    case.validate()?;
    Ok(case)
}

fn operation(op: &Operation) -> String {
    match op {
        Operation::Read => "READ".into(),
        Operation::Update { id, value } => format!("UPDATE {id} {value}"),
        Operation::Insert { id, value } => format!("INSERT {id} {value}"),
        Operation::Delete { id } => format!("DELETE {id}"),
        Operation::BroadUpdate { minimum, value } => format!("BROAD_UPDATE {minimum} {value}"),
        Operation::LockingRead { exclusive: false } => "READ_FOR_SHARE".into(),
        Operation::LockingRead { exclusive: true } => "READ_FOR_UPDATE".into(),
        Operation::GraphRead => "GRAPH_READ".into(),
        Operation::GraphUpdate { id, value } => format!("GRAPH_UPDATE {id} {value}"),
    }
}

pub fn format(case: &Case) -> String {
    let mut text = format!(
        "# HawDB MVCC case v1; both snapshots are captured before step 1.\nSEED {}\nINDEX {}\n",
        case.seed, case.index
    );
    if case.mode == Mode::Pessimistic {
        text.push_str("MODE PESSIMISTIC\n");
    }
    for (id, value) in &case.initial {
        let _ = writeln!(text, "ROW {id} {value}");
    }
    text.push('\n');
    let mut positions = [0; 2];
    for &tx in &case.schedule {
        let program = &case.transactions[tx];
        let op = program
            .operations
            .get(positions[tx])
            .map(operation)
            .unwrap_or_else(|| {
                if program.rollback {
                    "ROLLBACK".into()
                } else {
                    "COMMIT".into()
                }
            });
        let _ = writeln!(text, "T{} {op}", tx + 1);
        positions[tx] += 1;
    }
    text
}

pub fn explain(case: &Case, observation: &Observation) -> String {
    let mut text = format!("MVCC replay | seed={} case={}\nSnapshots: T1 and T2 capture the initial fixture before step 1.\nInitial rows: {:?}\n\n", case.seed, case.index, case.initial);
    let mut positions = [0; 2];
    for (step, &tx) in case.schedule.iter().enumerate() {
        if observation.failure.as_ref().is_some_and(|f| step > f.step) {
            break;
        }
        let program = &case.transactions[tx];
        let failed = observation.failure.as_ref().is_some_and(|f| step == f.step);
        let status = if failed { "FAIL" } else { "PASS" };
        let op = program.operations.get(positions[tx]);
        let name = op.map(operation).unwrap_or_else(|| {
            if program.rollback {
                "ROLLBACK".into()
            } else {
                "COMMIT".into()
            }
        });
        let _ = writeln!(text, "{status} {:03} T{} {name}", step + 1, tx + 1);
        if let Some(op) = op {
            let (sql, parameters) = op.sql();
            let language = if op.is_graph() { "Cypher" } else { "SQL" };
            let _ = writeln!(text, "  {language}: {sql}\n  parameters: {parameters:?}");
        }
        for event in observation
            .trace
            .iter()
            .filter(|event| event["step"].as_u64() == Some(step as u64))
        {
            for (key, label) in [
                ("expected_wait", "expected to wait"),
                ("expected_outcome", "expected outcome"),
                ("outcome", "actual outcome"),
                ("error", "error"),
                ("expected_read", "expected rows"),
                ("read", "actual rows"),
                ("expected_terminal", "expected outcome"),
                ("terminal", "actual outcome"),
                ("expected_visible", "expected visible"),
                ("visible", "actual visible"),
                ("expected_visible_graph", "expected visible nodes"),
                ("visible_graph", "actual visible nodes"),
            ] {
                if let Some(value) = event.get(key) {
                    let _ = writeln!(text, "  {label}: {value}");
                }
            }
        }
        if failed {
            if let Some(failure) = &observation.failure {
                let _ = writeln!(text, "  {}: {}", failure.kind, failure.detail);
            }
        } else {
            text.push_str("  pinned snapshot: unchanged\n");
        }
        positions[tx] += 1;
    }
    if let Some(failure) = &observation.failure {
        let _ = writeln!(
            text,
            "\nFAIL: {} at step {}",
            failure.kind,
            failure.step + 1
        );
    } else {
        let _ = writeln!(
            text,
            "\nPASS: all {} steps match the reference model",
            case.schedule.len()
        );
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case::generate;

    #[test]
    fn text_roundtrip_preserves_generated_programs_and_schedule() {
        for index in 0..128 {
            let case = generate(u64::MAX, index);
            let decoded = parse(&format(&case)).unwrap();
            assert_eq!(
                serde_json::to_value(case).unwrap(),
                serde_json::to_value(decoded).unwrap()
            );
        }
    }

    #[test]
    fn line_errors_reject_unknown_operations_and_finished_transactions() {
        for input in [
            "ROW 1 2\nROW 1 3",
            "T1 COMMIT\nT1 READ",
            "T1 UPDATE x 2",
            "T1 SELECT",
            "T1 READ extra",
        ] {
            assert!(parse(input).unwrap_err().starts_with("line "));
        }
        assert!(parse("T1 READ").unwrap_err().contains("must end"));
    }
}
