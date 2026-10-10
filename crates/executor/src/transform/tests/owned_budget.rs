// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::binding::binding_memory_bytes;

fn input(size: usize, count: usize) -> Vec<Binding> {
    (0..count)
        .map(|_| {
            Binding::values(BTreeMap::from([
                ("value".into(), Value::Int(1)),
                ("blob".into(), Value::String("x".repeat(size))),
            ]))
        })
        .collect()
}

fn oversized_row(kernel: Kernel) {
    let mut source = Source::new(input(8192, 1), 1);
    let mut calls = 0;
    let error = with_context(16, 64 * 1024, |context| {
        kernel.execute(&mut source, context, None, &mut |_| {
            calls += 1;
            Ok(BatchControl::Continue)
        })
    })
    .expect_err(
        "a public kernel must reject an oversized owned row without panicking or emitting it",
    );
    assert!(
        error.to_string().contains("batch_payload_bytes"),
        "{error:?}"
    );
    assert_eq!(calls, 0);
}

fn byte_boundary(kernel: Kernel) {
    for stop in [false, true] {
        let rows = input(2048, 3);
        assert!(binding_memory_bytes(&rows[0]) <= 4096);
        assert!(2 * binding_memory_bytes(&rows[0]) > 4096);
        let mut source = Source::new(rows, 3);
        let mut delivered = 0;
        let result = with_context(16, 64 * 1024, |context| {
            kernel.execute(&mut source, context, None, &mut |batch| {
                assert!(
                    batch.iter().map(binding_memory_bytes).sum::<usize>() <= 4096,
                    "the byte boundary must flush before pushing the next owned row"
                );
                assert_eq!(batch.len(), 1);
                delivered += batch.len();
                Ok(if stop {
                    BatchControl::Stop
                } else {
                    BatchControl::Continue
                })
            })
        })
        .unwrap();
        assert_eq!(
            result,
            if stop {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            }
        );
        assert_eq!(delivered, if stop { 1 } else { 3 });
    }
}

#[test]
fn public_transform_owned_filter_refuses_oversized_rows() {
    oversized_row(Kernel::Filter);
}

#[test]
fn public_transform_owned_limit_refuses_oversized_rows() {
    oversized_row(Kernel::Limit {
        offset: 0,
        limit: None,
    });
}

#[test]
fn public_transform_owned_filter_flushes_by_bytes_and_propagates_stop() {
    byte_boundary(Kernel::Filter);
}

#[test]
fn public_transform_owned_limit_flushes_by_bytes_and_propagates_stop() {
    byte_boundary(Kernel::Limit {
        offset: 0,
        limit: None,
    });
}

fn cancel_after_continue(kernel: Kernel, output_batch_rows: usize, payload_size: usize) {
    let mut source = Source::new(input(payload_size, 3), 3);
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let mut calls = 0;
    let mut delivered = 0;
    let result = with_context(output_batch_rows, 64 * 1024, |context| {
        kernel.execute(
            &mut source,
            BatchExecutionContext {
                task_context: Some(&task),
                ..context
            },
            None,
            &mut |batch| {
                calls += 1;
                delivered += batch.len();
                token.cancel();
                Ok(BatchControl::Continue)
            },
        )
    });
    let error =
        result.expect_err("a cancelled public kernel must refuse the remaining source batch");
    assert!(error.to_string().contains("cancelled"), "{error:?}");
    assert_eq!(calls, 1, "no callback after the first callback cancels");
    assert_eq!(delivered, 1, "no row after the first callback cancels");
    assert_eq!(
        source.calls, 1,
        "cancellation occurs within a single source batch"
    );
}

#[test]
fn public_transform_owned_filter_cancel_after_continue_at_row_flush() {
    cancel_after_continue(Kernel::Filter, 1, 0);
}

#[test]
fn public_transform_owned_filter_cancel_after_continue_at_byte_flush() {
    cancel_after_continue(Kernel::Filter, 16, 2048);
}

#[test]
fn public_transform_owned_limit_cancel_after_continue_at_row_flush() {
    cancel_after_continue(
        Kernel::Limit {
            offset: 0,
            limit: None,
        },
        1,
        0,
    );
}

#[test]
fn public_transform_owned_limit_cancel_after_continue_at_byte_flush() {
    cancel_after_continue(
        Kernel::Limit {
            offset: 0,
            limit: None,
        },
        16,
        2048,
    );
}
