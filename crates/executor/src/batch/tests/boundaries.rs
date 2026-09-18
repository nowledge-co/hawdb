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
use crate::store::SourceScanCandidateRow;

#[test]
fn preparation_borrows_the_original_plan_and_preserves_storage_capability() {
    let memory = ExecutionMemoryConfig::default();
    for out_of_core in [false, true] {
        let store = store::ReadFixture {
            out_of_core,
            ..store::ReadFixture::default()
        };
        for (plan, supported) in fixtures::operators() {
            let prepared = PreparedPhysicalPlan::prepare(&plan, &store, &memory);
            assert!(std::ptr::eq(prepared.plan(), &plan));
            assert_eq!(prepared.batch().is_some(), supported);
            if let Some(batch) = prepared.batch() {
                assert!(std::ptr::eq(batch.plan(), &plan));
            }
            assert_eq!(
                prepared.execution_mode(),
                if supported {
                    PreparedExecutionMode::Batch
                } else {
                    PreparedExecutionMode::Materialized
                },
            );
            assert_eq!(
                prepared.storage_capability(),
                if out_of_core {
                    PreparedStorageCapability::OutOfCore
                } else {
                    PreparedStorageCapability::InMemory
                },
            );
            assert_eq!(
                prepared.required_memory(),
                estimated_execution_memory(&plan, &memory)
            );
        }
    }
}

fn binding(value: Value) -> Binding {
    Binding {
        values: BTreeMap::from([("value".into(), value)]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    }
}

#[test]
fn byte_admission_checks_the_entire_batch_before_any_output() {
    let small = binding(Value::Int(1));
    let large = binding(Value::String("x".repeat(4096)));
    let limit = binding_memory_bytes(&small);
    assert!(binding_memory_bytes(&large) > limit);
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(limit * 4).unwrap());
    let account = ledger.account(
        QueryMemoryClass::PipelineBatch,
        "byte admission",
        NonZeroUsize::new(limit).unwrap(),
    );
    // Two valid rows already require splitting, but a later invalid row must
    // still fail before the first valid prefix is delivered.
    let error = emit_byte_bounded_batches(
        vec![small.clone(), small, large],
        limit,
        &account,
        &QueryExecutionObserver::default(),
        &mut |_| panic!("a batch with an oversized later row emitted a partial prefix"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("batch_payload_bytes"));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn split_batches_keep_live_charges_and_release_on_stop_or_error() {
    let row = binding(Value::Int(1));
    let bytes = binding_memory_bytes(&row);
    for exit in 0..3 {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(bytes * 3).unwrap());
        let account = ledger.account(
            QueryMemoryClass::PipelineBatch,
            "split output",
            NonZeroUsize::new(bytes).unwrap(),
        );
        let observer = QueryExecutionObserver::default();
        let mut calls = 0;
        let result = emit_byte_bounded_batches(
            vec![row.clone(); 3],
            bytes,
            &account,
            &observer,
            &mut |batch| {
                calls += 1;
                assert_eq!(batch.len(), 1);
                assert_eq!(ledger.snapshot().used_bytes, bytes);
                match exit {
                    0 => Ok(BatchControl::Continue),
                    1 => Ok(BatchControl::Stop),
                    _ => Err(HawDBError::Semantic("consumer sentinel".into())),
                }
            },
        );
        assert_eq!(calls, if exit == 0 { 3 } else { 1 });
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(ledger.snapshot().peak_bytes, bytes);
        if exit == 2 {
            assert_eq!(
                result.unwrap_err(),
                HawDBError::Semantic("consumer sentinel".into())
            );
        } else {
            assert_eq!(
                result.unwrap(),
                if exit == 0 {
                    BatchControl::Continue
                } else {
                    BatchControl::Stop
                }
            );
        }
    }
}

#[test]
fn byte_admission_respects_the_query_root_before_calling_the_consumer() {
    let row = binding(Value::Int(1));
    let bytes = binding_memory_bytes(&row);
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(bytes - 1).unwrap());
    let account = ledger.account(
        QueryMemoryClass::PipelineBatch,
        "root admission",
        NonZeroUsize::new(bytes).unwrap(),
    );
    assert!(emit_byte_bounded_batches(
        vec![row],
        bytes,
        &account,
        &QueryExecutionObserver::default(),
        &mut |_| panic!("an unadmitted row reached the consumer"),
    )
    .is_err());
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn source_candidates_must_match_the_canonical_node_label_and_properties() {
    with_context(None, |base| {
        let mut catalog = Catalog::default();
        let source = catalog.get_or_create_label("Source");
        let other = catalog.get_or_create_label("Other");
        let predicate = Predicate::PropertyEq {
            variable: "s".into(),
            property: "version".into(),
            value: Value::Int(1),
        };
        let row = SourceScanCandidateRow {
            node_id: 7,
            properties: BTreeMap::from([("version".into(), Value::Int(1))]),
        };
        for case in 0..4 {
            let node = NodeRecord {
                id: NodeId(7),
                labels: [if case == 2 { other } else { source }]
                    .into_iter()
                    .collect(),
                properties: if case == 3 {
                    BTreeMap::new()
                } else {
                    row.properties.clone()
                },
            };
            let store = store::ReadFixture {
                nodes: if case == 1 {
                    Vec::new()
                } else {
                    vec![node.clone()]
                },
                source_candidates: Some(vec![row.clone()]),
                ..store::ReadFixture::default()
            };
            let context = BatchReadContext {
                catalog: &catalog,
                store: &store,
                ..base
            };
            let mut actual = Vec::new();
            let result = scan::stream_source_segment_scan_batches(
                "s",
                &predicate,
                context,
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    actual.extend(batch.into_iter().map(|binding| binding.nodes["s"].clone()));
                    Ok(BatchControl::Continue)
                },
            );
            assert_eq!(store.source_reads.get(), 1);
            if case == 0 {
                result.unwrap();
                assert_eq!(actual, vec![node]);
            } else {
                assert!(matches!(
                    result.unwrap_err(),
                    HawDBError::StorageIntegrity(_)
                ));
                assert!(actual.is_empty(), "unverified canonical candidate emitted");
            }
            assert_eq!(base.memory_ledger.snapshot().used_bytes, 0);
        }
    });
}

#[test]
fn source_scratch_is_admitted_before_io() {
    with_context(None, |base| {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1).unwrap());
        let store = store::ReadFixture {
            source_candidates: Some(Vec::new()),
            ..store::ReadFixture::default()
        };
        let predicate = Predicate::PropertyEq {
            variable: "s".into(),
            property: "version".into(),
            value: Value::Int(1),
        };
        assert!(scan::stream_source_segment_scan_batches(
            "s",
            &predicate,
            BatchReadContext {
                store: &store,
                memory_ledger: &ledger,
                ..base
            },
            ExecutionLimit::unlimited(),
            &mut |_| panic!("unadmitted source scan emitted output"),
        )
        .is_err());
        assert_eq!(store.source_reads.get(), 0);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    });
}
