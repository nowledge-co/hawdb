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
