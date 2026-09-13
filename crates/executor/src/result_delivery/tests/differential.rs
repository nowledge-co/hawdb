use super::*;
use std::cell::RefCell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    Rows,
    Payload,
    Account,
    Root,
    Consumer,
    Cancelled,
}

fn failure(error: SkeinError) -> Failure {
    let SkeinError::Execution(message) = error else {
        panic!("unexpected error class: {error:?}");
    };
    if message.starts_with("read query returned more than") {
        Failure::Rows
    } else if message.starts_with("read query payload would exceed") {
        Failure::Payload
    } else if message.starts_with("query memory account query result") {
        Failure::Account
    } else if message.starts_with("query memory ledger would use") {
        Failure::Root
    } else if message == "consumer rejected row" {
        Failure::Consumer
    } else if message == "runtime task stopped: cancelled" {
        Failure::Cancelled
    } else {
        panic!("unexpected error: {message}");
    }
}

// This oracle walks owned values independently of the binding accounting helpers.
fn value_sizes(value: &Value) -> (usize, usize) {
    let (payload, heap) = match value {
        Value::Null => (0, 0),
        Value::Bool(_) => (1, 0),
        Value::Int(_) | Value::Float(_) => (8, 0),
        Value::String(value) => (value.len(), value.len()),
        Value::Binary(value) => (value.len(), value.len()),
        Value::Uuid(_) => (16, 16),
        Value::List(values) => {
            let sizes: Vec<_> = values.iter().map(value_sizes).collect();
            (
                sizes.iter().map(|size| size.0).sum(),
                size_of::<Vec<Value>>() + sizes.iter().map(|size| size.1).sum::<usize>(),
            )
        }
        Value::Map(values) => row_sizes(values),
    };
    (payload, size_of::<Value>() + heap)
}

fn row_sizes(row: &Row) -> (usize, usize) {
    let sizes: Vec<_> = row
        .iter()
        .map(|(name, value)| {
            let (payload, memory) = value_sizes(value);
            (
                name.len() + payload,
                name.len() + memory + 3 * size_of::<(String, Value)>(),
            )
        })
        .collect();
    (
        sizes.iter().map(|size| size.0).sum(),
        size_of::<Row>() + sizes.iter().map(|size| size.1).sum::<usize>(),
    )
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Receipt {
    failure: Option<Failure>,
    rows: usize,
    payload: usize,
    delivered: Vec<Row>,
    used: usize,
    peak: usize,
}

#[derive(Clone, Copy)]
struct Scenario {
    mode: usize,
    limits: OutputLimits,
    account_budget: usize,
    root_budget: usize,
    other_bytes: usize,
    interruption: usize,
}

impl Scenario {
    fn callback_fails(self, ordinal: usize) -> bool {
        matches!(self.interruption, 1 | 2) && ordinal == self.interruption - 1
    }
}

fn expected(input: &[Row], scenario: Scenario) -> Receipt {
    let mut result = Receipt {
        used: scenario.other_bytes,
        peak: scenario.other_bytes,
        ..Receipt::default()
    };
    for row in input {
        let (payload, memory) = row_sizes(row);
        let next_payload = result.payload + payload;
        let rejection = if scenario
            .limits
            .max_rows
            .is_some_and(|max| result.rows == max)
        {
            Some(Failure::Rows)
        } else if scenario
            .limits
            .max_payload_bytes
            .is_some_and(|max| next_payload > max)
        {
            Some(Failure::Payload)
        } else if result.used - scenario.other_bytes + memory > scenario.account_budget {
            Some(Failure::Account)
        } else if result.used + memory > scenario.root_budget {
            Some(Failure::Root)
        } else {
            None
        };
        if let Some(rejection) = rejection {
            result.failure = Some(rejection);
            return result;
        }
        result.used += memory;
        result.peak = result.peak.max(result.used);
        if scenario.mode != 2 {
            let rejected = scenario.callback_fails(result.delivered.len());
            result.delivered.push(row.clone());
            if scenario.mode == 1 {
                result.used -= memory;
            }
            if rejected {
                result.failure = Some(Failure::Consumer);
                return result;
            }
        }
        result.rows += 1;
        result.payload = next_payload;
    }
    if scenario.mode == 2 {
        for row in input {
            if scenario.interruption == 3 {
                result.failure = Some(Failure::Cancelled);
                return result;
            }
            let rejected = scenario.callback_fails(result.delivered.len());
            result.delivered.push(row.clone());
            if rejected || scenario.interruption == 4 {
                result.failure = Some(if rejected {
                    Failure::Consumer
                } else {
                    Failure::Cancelled
                });
                return result;
            }
        }
        result.used = scenario.other_bytes;
    }
    result
}

fn actual(input: &[Row], scenario: Scenario) -> Receipt {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(scenario.root_budget).unwrap());
    let other = ledger.account(
        QueryMemoryClass::BlockingState,
        "other operator",
        NonZeroUsize::new(scenario.root_budget).unwrap(),
    );
    let other_lease = other.reserve(scenario.other_bytes).unwrap();
    let context = RuntimeTaskContext::default();
    let delivered = RefCell::new(Vec::new());
    let mut callback = |row| {
        let mut delivered = delivered.borrow_mut();
        let rejected = scenario.callback_fails(delivered.len());
        delivered.push(row);
        if scenario.interruption == 4 {
            context.cancellation().cancel();
        }
        if rejected {
            Err(SkeinError::Execution("consumer rejected row".to_string()))
        } else {
            Ok(())
        }
    };
    let mode = [
        ConsumerMemoryMode::Retained,
        ConsumerMemoryMode::ReleasedAfterCall,
        ConsumerMemoryMode::DeferredUntilValidated,
    ][scenario.mode];
    let mut output = QueryOutputAccumulator::new(
        scenario.limits,
        NonZeroUsize::new(scenario.account_budget).unwrap(),
        &ledger,
        mode,
        &mut callback,
    )
    .unwrap();
    let mut error = None;
    for row in input {
        if let Err(rejected) = output.emit(Binding::values(row.clone())) {
            error = Some(failure(rejected));
            break;
        }
        // Deferred delivery must not invoke the consumer during admission.
        if scenario.mode == 2 {
            assert!(delivered.borrow().is_empty());
        }
    }
    if error.is_none() {
        if scenario.interruption == 3 {
            context.cancellation().cancel();
        }
        error = output.finish_delivery(Some(&context)).err().map(failure);
    }
    let snapshot = ledger.snapshot();
    let result = Receipt {
        failure: error,
        rows: output.metrics().rows,
        payload: output.metrics().payload_bytes,
        delivered: delivered.borrow().clone(),
        used: snapshot.used_bytes,
        peak: snapshot.peak_bytes,
    };
    assert_eq!(snapshot.account_count, 2);
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, scenario.other_bytes);
    drop(other_lease);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    result
}

fn generated_rows(seed: usize, count: usize) -> Vec<Row> {
    (0..count)
        .map(|ordinal| {
            let n = seed
                .wrapping_mul(6364136223846793005_u64 as usize)
                .wrapping_add(ordinal * 1447);
            let value = match (seed + ordinal) % 9 {
                0 => Value::Null,
                1 => Value::Bool(n.is_multiple_of(2)),
                2 => Value::Int(n as i64),
                3 => Value::Float(if n.is_multiple_of(2) {
                    -0.0
                } else {
                    f64::INFINITY
                }),
                4 => Value::String("a\u{65e5}\u{1f980}".repeat(n % 31)),
                5 => Value::Binary(vec![n as u8; n % 257]),
                6 => Value::Uuid(Default::default()),
                7 => Value::List(vec![
                    Value::String("nested".repeat(n % 7)),
                    Value::Int(i64::MIN),
                ]),
                _ => Value::Map(BTreeMap::from([(
                    "child".to_string(),
                    Value::List(vec![Value::Bool(true), Value::Binary(vec![0; n % 17])]),
                )])),
            };
            BTreeMap::from([(format!("value_{ordinal}"), value)])
        })
        .collect()
}

fn campaign(seeds: usize) -> usize {
    let mut cases = 0;
    for seed in 0..seeds {
        for count in [0, 1, 2, 7] {
            let input = generated_rows(seed, count);
            let first = input
                .first()
                .map(row_sizes)
                .unwrap_or((0, size_of::<Row>()));
            let total_payload = input.iter().map(|row| row_sizes(row).0).sum();
            for mode in 0..3 {
                for policy in 0..10 {
                    for interruption in 0..5 {
                        let mut scenario = Scenario {
                            mode,
                            limits: OutputLimits::default(),
                            account_budget: 1 << 20,
                            root_budget: 1 << 20,
                            other_bytes: 0,
                            interruption,
                        };
                        match policy {
                            0 => {}
                            1 => scenario.limits.max_rows = Some(0),
                            2 => scenario.limits.max_rows = Some(1),
                            3 => {
                                scenario.limits = OutputLimits {
                                    max_rows: Some(count),
                                    max_payload_bytes: Some(total_payload),
                                }
                            }
                            4 => scenario.limits.max_payload_bytes = Some(first.0),
                            5 => {
                                scenario.limits.max_payload_bytes = Some(first.0.saturating_sub(1))
                            }
                            6 => scenario.account_budget = first.1,
                            7 => scenario.account_budget = first.1 - 1,
                            8 => {
                                scenario.root_budget = first.1 + 3;
                                scenario.other_bytes = 4;
                            }
                            _ => {
                                scenario.limits = OutputLimits {
                                    max_rows: Some(0),
                                    max_payload_bytes: Some(0),
                                }
                            }
                        }
                        assert_eq!(actual(&input, scenario), expected(&input, scenario), "seed={seed} count={count} mode={mode} policy={policy} interruption={interruption}");
                        cases += 1;
                    }
                }
            }
        }
    }
    cases
}

#[test]
fn result_delivery_differential_smoke() {
    assert_eq!(campaign(2), 1200);
}

#[test]
#[ignore = "full local result delivery differential campaign"]
fn result_delivery_differential_campaign() {
    let cases = campaign(128);
    assert_eq!(cases, 76800);
    eprintln!("result delivery campaign: 128 seeds, {cases} cases");
}

#[test]
fn deferred_interruption_drains_pending_rows_and_preserves_lease_until_drop_or_retry() {
    for cancel in [false, true] {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(4096).unwrap());
        let context = RuntimeTaskContext::default();
        let calls = std::cell::Cell::new(0);
        let mut callback = |_| {
            calls.set(calls.get() + 1);
            if cancel {
                context.cancellation().cancel();
                Ok(())
            } else {
                Err(SkeinError::Execution("consumer rejected row".to_string()))
            }
        };
        let mut output = QueryOutputAccumulator::new(
            OutputLimits::default(),
            NonZeroUsize::new(4096).unwrap(),
            &ledger,
            ConsumerMemoryMode::DeferredUntilValidated,
            &mut callback,
        )
        .unwrap();
        output.emit(binding("first")).unwrap();
        output.emit(binding("second")).unwrap();
        let before = ledger.snapshot().used_bytes;
        assert!(before > 0);
        assert!(output.finish_delivery(Some(&context)).is_err());
        assert_eq!(calls.get(), 1);
        assert_eq!(ledger.snapshot().used_bytes, before);
        output.finish_delivery(Some(&context)).unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(output.metrics().rows, 2);
    }
}

#[test]
fn callback_unwind_releases_all_result_leases() {
    for mode in [
        ConsumerMemoryMode::Retained,
        ConsumerMemoryMode::ReleasedAfterCall,
        ConsumerMemoryMode::DeferredUntilValidated,
    ] {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(4096).unwrap());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut callback = |_| -> Result<()> { panic!("consumer unwound") };
            let mut output = QueryOutputAccumulator::new(
                OutputLimits::default(),
                NonZeroUsize::new(4096).unwrap(),
                &ledger,
                mode,
                &mut callback,
            )
            .unwrap();
            output.emit(binding("row")).unwrap();
            output.finish_delivery(None).unwrap();
        }));
        assert!(panic.is_err());
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
