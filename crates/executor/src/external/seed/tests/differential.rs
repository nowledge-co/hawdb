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

use super::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn index(&mut self, length: usize) -> usize {
        (self.next() % length as u64) as usize
    }
}

fn check_plan(rng: &mut Rng, seed: u64, case: usize) {
    let mut physical = VectorPhysicalPlan::Filter { fields: vec![] };
    let mut dimension = 0;
    let mut limit = None;
    // Expected facts are recorded while wrapping the tree, not recovered using
    // either production traversal or a second recursive plan interpreter.
    for _ in 0..rng.index(12) {
        physical = match rng.index(4) {
            0 => {
                dimension = rng.index(9);
                VectorPhysicalPlan::VectorCandidateScan {
                    source: VectorCandidateSource::Scalar,
                    embedding_dimension: dimension,
                    candidate_limit: rng.index(17),
                    input: Box::new(physical),
                }
            }
            1 => {
                dimension = rng.index(9);
                VectorPhysicalPlan::RawVectorRerank {
                    embedding_dimension: dimension,
                    input: Box::new(physical),
                }
            }
            2 => VectorPhysicalPlan::ResidualFilter {
                fields: vec!["space_id".to_string()],
                initial_candidate_limit: rng.index(17),
                input: Box::new(physical),
            },
            _ => {
                let top_k = rng.index(17);
                limit = Some(top_k);
                VectorPhysicalPlan::TopK {
                    limit: top_k,
                    input: Box::new(physical),
                }
            }
        };
    }
    assert_eq!(
        vector_plan_embedding_dimension(&physical),
        dimension,
        "seed={seed} plan={case}"
    );
    assert_eq!(
        vector_plan_top_k(&physical),
        limit,
        "seed={seed} plan={case}"
    );
    let samples = [
        (Value::Int(0), 0u32),
        (Value::Int(1), 0x3f80_0000),
        (Value::Int(-1), 0xbf80_0000),
        (Value::Int(16_777_217), 0x4b80_0000),
        (Value::Int(i64::MIN), 0xdf00_0000),
        (Value::Int(i64::MAX), 0x5f00_0000),
        (Value::Float(-0.0), 0x8000_0000),
        (Value::Float(0.5), 0x3f00_0000),
        (Value::Float(f64::from(f32::from_bits(1))), 1),
        (Value::Float(f64::from(f32::MAX)), 0x7f7f_ffff),
        (Value::Float(f64::from_bits(1)), 0),
        (Value::Float(-f64::from_bits(1)), 0x8000_0000),
    ];
    let mut values = vec![];
    let mut bits = vec![];
    for _ in 0..dimension {
        let (value, bit) = &samples[rng.index(samples.len())];
        values.push(value.clone());
        bits.push(*bit);
    }
    let actual = vector_embedding_parameter(
        &BTreeMap::from([("embedding".to_string(), Value::List(values))]),
        "embedding",
        &physical,
    )
    .unwrap();
    assert_eq!(
        actual
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        bits,
        "seed={seed} embedding={case}"
    );
}

fn check_stream(rng: &mut Rng, seed: u64, iteration: usize, mode: usize) {
    let mut case = Case::default();
    let top_k = rng.index(8) + 1;
    let limit = (rng.index(2) == 0).then(|| rng.index(8) + 1);
    let rows = top_k.min(limit.unwrap_or(usize::MAX));
    let batch_rows = rng.index(4) + 1;
    let count = if mode == 0 { rng.index(rows + 1) } else { rows };
    let external_ids = rng.index(2) == 0;
    let block_bytes = 4096 * (rng.index(2) + 1);
    let working_intent = [
        None,
        Some(0),
        Some(1),
        Some(1024),
        Some(4096),
        Some(u64::MAX),
    ][rng.index(6)];
    let working_bytes = match working_intent {
        None | Some(u64::MAX) => block_bytes,
        Some(0) => 1,
        Some(value) => value as usize,
    };
    let reserved_bytes = block_bytes + working_bytes;
    let requested_workers = rng.index(17);
    let admitted_workers = if mode == 13 {
        None
    } else {
        Some(rng.index(8) + 1)
    };
    let priority = rng.next() as u8;
    case.plan = plan(2, top_k);
    case.output_limit = limit;
    case.external_ids = external_ids;
    case.memory.blocking_operator_bytes = nz(block_bytes);
    case.memory.batch_rows = nz(batch_rows);
    case.profile = VectorExecutionResourceProfile {
        priority,
        max_parallelism: requested_workers,
        max_working_memory_bytes: working_intent,
    };
    case.admitted_parallelism = admitted_workers;
    case.response = Ok(output(count));
    let (expected_error, calls, reports) = match mode {
        0 | 14 => (None, 1, 1),
        1 => {
            case.consumer_stop = true;
            (None, 1, 1)
        }
        2 => {
            case.consumer_error = true;
            (Some("consumer failure"), 1, 1)
        }
        3 => {
            case.response = Ok(output(rows + 1));
            (Some("exceeding result row budget"), 1, 0)
        }
        4 => {
            let mut response = output(1);
            response.rows[0].id = "x".repeat(block_bytes + 1);
            case.response = Ok(response);
            (Some("exceeding result memory budget"), 1, 0)
        }
        5 => {
            case.memory.query_memory_bytes = nz(reserved_bytes - 1);
            case.cancel_before = true;
            (Some("exceeding query_memory_bytes"), 0, 0)
        }
        6 => {
            case.memory.query_memory_bytes = nz(reserved_bytes);
            (Some("exceeding query_memory_bytes"), 1, 1)
        }
        7 => {
            case.cancel_before = true;
            (Some("external read task stopped: cancelled"), 0, 0)
        }
        8 => {
            case.cancel_after = true;
            case.response = Ok(output(rows + 1));
            (Some("external read task stopped: cancelled"), 1, 0)
        }
        9 => {
            case.cancel_after = true;
            case.response = Err(HawDBError::Execution("host failure".into()));
            (Some("host failure"), 1, 0)
        }
        10 => {
            case.parameters = BTreeMap::from([(
                "embedding".into(),
                Value::List(vec![Value::Float(f64::INFINITY)]),
            )]);
            case.cancel_before = true;
            (Some("must contain finite numbers"), 0, 0)
        }
        11 => {
            case.plan = VectorPhysicalPlan::Filter { fields: vec![] };
            case.parameters.clear();
            case.output_limit = Some(0);
            (Some("missing TopK"), 0, 0)
        }
        12 => {
            case.output_limit = Some(0);
            case.parameters.clear();
            case.cancel_before = true;
            (None, 0, 0)
        }
        13 => {
            case.cancel_before = true;
            (None, 1, 1)
        }
        15 => {
            let mut response = output(0);
            response.rows = Vec::with_capacity(block_bytes);
            case.response = Ok(response);
            (Some("exceeding result memory budget"), 1, 0)
        }
        _ => unreachable!(),
    };
    let outcome = run(case);
    let identity = format!("seed={seed} stream={iteration} mode={mode}");
    match expected_error {
        Some(message) => assert!(
            outcome
                .result
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains(message),
            "{identity}: {outcome:?}"
        ),
        None => assert_eq!(
            outcome.result.as_ref().unwrap(),
            &if mode == 1 {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            },
            "{identity}"
        ),
    }
    assert_eq!(outcome.calls.len(), calls, "{identity}");
    assert_eq!(outcome.reports.len(), reports, "{identity}");
    if let Some(request) = outcome.calls.first() {
        assert_eq!(
            request,
            &Request {
                embedding: vec![0x3f80_0000, 0x8000_0000],
                filters: BTreeMap::from([("space_id".to_string(), "space-1".to_string())]),
                plan: plan(2, top_k),
                priority,
                parallelism: requested_workers.max(1).min(admitted_workers.unwrap_or(1)),
                working_bytes,
                rows,
                result_bytes: block_bytes,
                has_context: admitted_workers.is_some(),
                reserved_bytes,
            },
            "{identity}"
        );
    }
    if reports == 1 {
        assert_eq!(outcome.reports[0], output(count).report, "{identity}");
    }
    let emitted = match mode {
        0 | 13 | 14 => count,
        1 | 2 => count.min(batch_rows),
        _ => 0,
    };
    let expected_rows: Vec<_> = (0..emitted)
        .map(|index| {
            let mut row = BTreeMap::from([
                ("id".to_string(), Value::String(format!("id-{index}"))),
                ("score".to_string(), Value::Float(index as f64 * 0.25)),
            ]);
            if external_ids && index % 2 == 0 {
                row.insert(
                    "external_id".to_string(),
                    Value::String(format!("external-{index}")),
                );
            }
            row
        })
        .collect();
    assert_eq!(
        outcome.batches,
        expected_rows
            .chunks(batch_rows)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>(),
        "{identity}"
    );
    assert!(
        outcome
            .emit_reservations
            .iter()
            .all(|bytes| *bytes == reserved_bytes),
        "{identity}"
    );
}

fn campaign(seeds: u64) {
    let mut plan_cases = 0;
    let mut stream_cases = 0;
    for seed in 0..seeds {
        let mut rng = Rng(seed);
        for index in 0..64 {
            check_plan(&mut rng, seed, index);
            plan_cases += 1;
            check_stream(&mut rng, seed, index, index % 16);
            stream_cases += 1;
        }
    }
    eprintln!("vector-seed-differential-v1 seeds={seeds} plan_cases={plan_cases} stream_cases={stream_cases}");
}

#[test]
fn vector_seed_differential_smoke() {
    campaign(8);
}

#[test]
#[ignore = "bounded local differential campaign"]
fn vector_seed_differential_campaign() {
    campaign(256);
}
