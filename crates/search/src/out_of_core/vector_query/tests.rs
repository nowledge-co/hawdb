// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};

fn fixture(rows: usize, dimension: usize) -> (SearchSegmentDescriptorEntry, Vec<u8>, Vec<String>) {
    let ids = (0..rows)
        .map(|row| format!("doc:{row:04}"))
        .collect::<Vec<_>>();
    let embedding = vec![1.0f32; dimension];
    let rows = ids
        .iter()
        .map(|id| (id.as_str(), embedding.as_slice()))
        .collect::<Vec<_>>();
    let mut bytes = Vec::new();
    vector_segment::encode_into(&rows, &mut bytes, || Ok(())).unwrap();
    let document = |id: &str| SearchDocument {
        id: id.into(),
        title: String::new(),
        content: String::new(),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    let (first, last) = (document(&ids[0]), document(&ids[ids.len() - 1]));
    let mut segment =
        SearchSegmentDescriptorEntry::from_documents(0, &[&first, &last], &BTreeSet::new());
    segment.document_count = ids.len();
    (segment, bytes, ids)
}

#[test]
fn query_vectors_stream_in_bounded_runs_and_refund_admission() {
    let dimension = 2048;
    let (segment, bytes, ids) = fixture(1024, dimension);
    assert!(bytes.len() > 8 << 20);
    for (budget, succeeds) in [(16 << 10, false), (128 << 10, false), (1 << 20, true)] {
        let budget = NonZeroUsize::new(budget).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let memory = ledger.account(QueryMemoryClass::ExternalRead, "vector query", budget);
        let task = crate::RuntimeTaskContext::default();
        let query = QueryContext {
            memory: &memory,
            task: &task,
            execution: None,
            io_scope: None,
        };
        let read = |offset: u64, output: &mut [u8]| -> Result<()> {
            let start = offset as usize;
            output.copy_from_slice(&bytes[start..start + output.len()]);
            Ok(())
        };
        let mut seen = 0;
        let result = vector_segment::visit_selected(
            &mut AdmittedSource::new(read, query).unwrap(),
            vector_segment::Expected {
                segment: &segment,
                payload_len: bytes.len() as u64,
                count: ids.len(),
                vector_ordinal_base: 0,
                dimension: Some(dimension),
            },
            |_| true,
            |document| {
                assert_eq!(document.id, ids[seen]);
                assert_eq!(document.embedding.len(), dimension);
                assert!(document.embedding.iter().all(|value| *value == 1.0));
                seen += 1;
                Ok(())
            },
        );
        if succeeds {
            let stats = result.unwrap();
            assert_eq!(stats.bytes, bytes.len() as u64);
            assert!(stats.peak_bytes < 1 << 20, "{stats:?}");
            assert_eq!(seen, ids.len());
        } else {
            assert!(
                matches!(result, Err(HawDBError::ResourceBudgetExceeded(_))),
                "{result:?}"
            );
            assert_eq!(seen, 0);
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn query_vectors_observe_cancellation_and_refund_admission() {
    let (segment, bytes, ids) = fixture(4, 2);
    let budget = NonZeroUsize::new(1 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(QueryMemoryClass::ExternalRead, "vector cancel", budget);
    let cancellation = crate::RuntimeCancellationToken::new();
    let task = crate::RuntimeTaskContext::without_deadline(cancellation.clone());
    assert!(cancellation.cancel());
    let query = QueryContext {
        memory: &memory,
        task: &task,
        execution: None,
        io_scope: None,
    };
    let result = vector_segment::visit_selected(
        &mut AdmittedSource::new(
            |offset: u64, output: &mut [u8]| {
                let start = offset as usize;
                output.copy_from_slice(&bytes[start..start + output.len()]);
                Ok(())
            },
            query,
        )
        .unwrap(),
        vector_segment::Expected {
            segment: &segment,
            payload_len: bytes.len() as u64,
            count: ids.len(),
            vector_ordinal_base: 0,
            dimension: Some(2),
        },
        |_| true,
        |_| panic!("cancelled reads must not decode rows"),
    );
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
