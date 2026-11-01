// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn values(count: usize) -> Vec<Binding> {
    (0..count)
        .map(|id| Binding::values(BTreeMap::from([("value".into(), Value::Int(id as i64))])))
        .collect()
}

#[test]
fn limit_transfer_admits_the_selected_payload_and_rejects_a_one_byte_deficit() {
    let rows = values(5);
    let bytes = rows[1..4].iter().map(binding_memory_bytes).sum::<usize>();
    assert!(
        bytes < 4096,
        "exercise a transfer smaller than the configured batch ceiling"
    );
    for budget in [bytes - 1, bytes] {
        let mut source = Source::new(rows.clone(), 5);
        let mut output = Vec::new();
        let result = with_context(16, budget, |context| {
            Kernel::Limit {
                offset: 1,
                limit: Some(3),
            }
            .execute(&mut source, context, None, &mut |batch| {
                output.extend(batch.into_iter().map(|row| row.values["value"].clone()));
                Ok(BatchControl::Continue)
            })
        });
        if budget == bytes {
            result.unwrap();
            assert_eq!(output, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        } else {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("query_memory_bytes"));
            assert!(
                output.is_empty(),
                "reserve before emitting the selected batch"
            );
        }
    }
}

#[test]
fn skipped_payload_and_exhausted_offsets_do_not_reserve_an_output_batch_ceiling() {
    let mut input = values(3);
    input[0]
        .values
        .insert("skipped_blob".into(), Value::String("x".repeat(8192)));
    let bytes = binding_memory_bytes(&input[1]);
    let mut source = Source::new(input, 3);
    let mut output = Vec::new();
    with_context(16, bytes, |context| {
        Kernel::Limit {
            offset: 1,
            limit: Some(1),
        }
        .execute(&mut source, context, None, &mut |batch| {
            output.extend(batch);
            Ok(BatchControl::Continue)
        })
    })
    .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].values["value"], Value::Int(1));
    let mut source = Source::new(values(3), 3);
    with_context(16, 1, |context| {
        Kernel::Limit {
            offset: usize::MAX,
            limit: Some(1),
        }
        .execute(&mut source, context, None, &mut |_| {
            panic!("exhausted offset emitted output")
        })
    })
    .unwrap();
}
