// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use std::io::Cursor;

fn fixture(rows: usize) -> (SearchSegmentDescriptorEntry, SearchOutOfCoreSegmentLayout) {
    let document = |id| SearchDocument {
        id,
        title: String::new(),
        content: String::new(),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    let first = document("doc:0000".into());
    let last = document(format!("doc:{:04}", rows - 1));
    let mut segment =
        SearchSegmentDescriptorEntry::from_documents(0, &[&first, &last], &BTreeSet::new());
    segment.document_count = rows;
    let range = SearchOutOfCoreRange {
        offset: 0,
        length: 0,
        checksum: 0,
        entry_count: rows,
    };
    (
        segment,
        SearchOutOfCoreSegmentLayout {
            segment_id: 0,
            vector_ordinal_base: 0,
            metadata: range,
            vectors: range,
        },
    )
}

#[test]
fn query_vectors_stream_beyond_working_budget_and_refund_decoder_refusals() {
    let (segment, layout) = fixture(1024);
    let dimension = 2048;
    let embedding = std::iter::repeat_n("1", dimension)
        .collect::<Vec<_>>()
        .join(",");
    let ids = (0..1024)
        .map(|row| format!("doc:{row:04}"))
        .collect::<Vec<_>>();
    let mut text = String::from("HAWDB_SEARCH_VECTOR_SEGMENT_V1\n");
    for (row, id) in ids.iter().enumerate() {
        text.push_str(&format!(
            "vector\t{row}\t{}\t{embedding}\n",
            crate::encode_string(id)
        ));
    }
    assert!(text.len() > 3 << 20);
    let bytes = crate::encode_search_snapshot_text(&text).unwrap();
    for (budget, succeeds) in [(64 << 10, false), (1 << 20, false), (3 << 20, true)] {
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
        let mut seen = 0;
        let result = hydration::read_validated_with_account(
            Cursor::new(&bytes),
            bytes.len() as u64,
            checksum_bytes(&bytes),
            text.len() as u64,
            Some(hydration::DecodeAdmission {
                memory: &memory,
                task: &task,
                max_header_bytes: 4096,
            }),
            |text| {
                visit_records(
                    text,
                    RecordContext {
                        segment: &segment,
                        layout: &layout,
                        dimension: Some(dimension),
                        limit: NonZeroU64::new(16 << 10).unwrap(),
                        query,
                    },
                    |document| {
                        assert_eq!(document.id, ids[seen]);
                        assert_eq!(document.embedding.len(), dimension);
                        assert!(document.embedding.iter().all(|value| *value == 1.0));
                        seen += 1;
                        Ok(())
                    },
                )
            },
        );
        if succeeds {
            assert_eq!(result.unwrap(), text.len() as u64);
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
fn query_vector_records_match_legacy_grammar_and_reject_corrupt_layouts() {
    let (segment, layout) = fixture(2);
    let budget = NonZeroUsize::new(64 << 10).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(QueryMemoryClass::ExternalRead, "vector grammar", budget);
    let task = crate::RuntimeTaskContext::default();
    for (ending, second) in [
        ("\n", "1,2"),
        ("\r\n", "1,2"),
        ("", "1,2"),
        ("\r\r\n", "1,2"),
        ("\n", "NaN,2"),
        ("\n", "1"),
        ("\n", "bad,2"),
        ("\n", ""),
    ] {
        let text = format!(
            "HAWDB_SEARCH_VECTOR_SEGMENT_V1\nvector\t0\t{}\t1,2\nvector\t1\t{}\t{second}{ending}",
            crate::encode_string("doc:0000"),
            crate::encode_string("doc:0001")
        );
        let expected = decode_vector_segment(&text, &segment, 2, 0, Some(2), usize::MAX, |_| true);
        let result = visit_records(
            &mut Cursor::new(text.as_bytes()),
            RecordContext {
                segment: &segment,
                layout: &layout,
                dimension: Some(2),
                limit: NonZeroU64::new(4096).unwrap(),
                query: QueryContext {
                    memory: &memory,
                    task: &task,
                    execution: None,
                    io_scope: None,
                },
            },
            |_| Ok(()),
        );
        assert_eq!(
            result.is_ok(),
            expected.is_ok(),
            "{ending:?} {second}: {result:?}"
        );
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    for (ordinal, id) in [(7, "doc:0001"), (1, "doc:0000"), (1, "outside")] {
        let text = format!(
            "HAWDB_SEARCH_VECTOR_SEGMENT_V1\nvector\t0\t{}\t1,2\nvector\t{ordinal}\t{}\t1,2\n",
            crate::encode_string("doc:0000"),
            crate::encode_string(id)
        );
        let result = visit_records(
            &mut Cursor::new(text.as_bytes()),
            RecordContext {
                segment: &segment,
                layout: &layout,
                dimension: Some(2),
                limit: NonZeroU64::new(4096).unwrap(),
                query: QueryContext {
                    memory: &memory,
                    task: &task,
                    execution: None,
                    io_scope: None,
                },
            },
            |_| Ok(()),
        );
        assert!(matches!(result, Err(HawDBError::Storage(_))));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
