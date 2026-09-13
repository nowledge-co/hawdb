use super::*;
use crate::ExecutionMemoryConfig;
use skein_core::Value;
use std::collections::BTreeMap;

mod differential;

fn binding(value: &str) -> Binding {
    Binding::values(BTreeMap::from([(
        "value".to_string(),
        Value::String(value.to_string()),
    )]))
}

#[test]
fn released_consumer_memory_is_not_retained_between_rows() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits::default();
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::ReleasedAfterCall,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("first")).unwrap();
    output.emit(binding("second")).unwrap();

    assert_eq!(output.metrics().rows, 2);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(output);
    assert_eq!(rows, 2);
}

#[test]
fn retained_consumer_memory_lives_until_accumulator_drop() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits::default();
    let mut consumer = |_| Ok(());
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::Retained,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("retained")).unwrap();
    assert!(ledger.snapshot().used_bytes > 0);

    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn output_limits_stop_before_calling_the_consumer() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits {
        max_rows: Some(1),
        max_payload_bytes: None,
    };
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::DeferredUntilValidated,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("first")).unwrap();
    let error = output.emit(binding("second")).unwrap_err();

    assert!(error.to_string().contains("max_read_result_rows 1"));
    assert_eq!(output.metrics().rows, 1);
    drop(output);
    assert_eq!(rows, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn bounded_consumer_flushes_only_after_validation() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits {
        max_rows: Some(2),
        max_payload_bytes: None,
    };
    let mut rows = Vec::new();
    let mut consumer = |row| {
        rows.push(row);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::DeferredUntilValidated,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("first")).unwrap();
    output.emit(binding("second")).unwrap();
    assert!(ledger.snapshot().used_bytes > 0);
    output.finish_delivery(None).unwrap();
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(output);

    assert_eq!(rows.len(), 2);
}

#[test]
fn payload_limit_discards_rows_deferred_across_calls() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let first = binding("first");
    let first_payload_bytes = map_payload_bytes(&first.values);
    let limits = OutputLimits {
        max_rows: None,
        max_payload_bytes: Some(first_payload_bytes),
    };
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::DeferredUntilValidated,
        &mut consumer,
    )
    .unwrap();

    output.emit(first).unwrap();
    let error = output.emit(binding("second")).unwrap_err();

    assert!(error.to_string().contains(&format!(
        "max_read_result_payload_bytes {first_payload_bytes}"
    )));
    drop(output);
    assert_eq!(rows, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn payload_limit_failure_releases_transient_result_memory() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits {
        max_rows: None,
        max_payload_bytes: Some(0),
    };
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::ReleasedAfterCall,
        &mut consumer,
    )
    .unwrap();

    let error = output.emit(binding("too-large")).unwrap_err();

    assert!(error
        .to_string()
        .contains("max_read_result_payload_bytes 0"));
    assert_eq!(output.metrics().rows, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(output);
    assert_eq!(rows, 0);
}
