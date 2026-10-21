// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use std::io::Cursor;

fn document(id: String, content: String) -> SearchDocument {
    SearchDocument {
        id,
        title: "graph".into(),
        content,
        embedding: Some(vec![1.0, 2.0]),
        metadata: BTreeMap::from([("kind".into(), "Memory".into())]),
    }
}
fn descriptor(documents: &[SearchDocument]) -> SearchSegmentDescriptorEntry {
    SearchSegmentDescriptorEntry::from_documents(
        0,
        &documents.iter().collect::<Vec<_>>(),
        &BTreeSet::new(),
    )
}

#[test]
fn query_hydration_streams_and_removed_documents_keep_their_funding() {
    let documents = (0..1024)
        .map(|i| document(format!("doc:{i:04}"), "x".repeat(2048)))
        .collect::<Vec<_>>();
    let segment = descriptor(&documents);
    let mut text = String::from("HAWDB_SEARCH_SEGMENT_V1\n");
    for document in &documents {
        text.push_str(&crate::encode_search_document_line(document));
    }
    assert!(text.len() > 4 << 20);
    let encoded = crate::encode_search_snapshot_text(&text).unwrap();
    let ids = BTreeSet::from([documents[0].id.clone(), documents[1023].id.clone()]);
    for (bytes, succeeds) in [(64 << 10, false), (1 << 20, false), (3 << 20, true)] {
        let grant = NonZeroUsize::new(bytes).unwrap();
        let ledger = QueryMemoryLedger::new(grant);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "hydration query", grant);
        let task = RuntimeTaskContext::default();
        let query = QueryContext {
            memory: &account,
            task: &task,
            execution: None,
            io_scope: None,
        };
        let result = read_validated_with_account(
            Cursor::new(&encoded),
            encoded.len() as u64,
            checksum_bytes(&encoded),
            text.len() as u64,
            Some(DecodeAdmission {
                memory: &account,
                task: &task,
                max_header_bytes: 4096,
            }),
            |text| {
                select_records(
                    text,
                    &segment,
                    &ids,
                    HydrationBudget {
                        limit: 16384,
                        used: 7,
                    },
                    16384,
                    query,
                )
            },
        );
        if succeeds {
            let Selection {
                documents: selected,
                _memory,
                ..
            } = result.unwrap();
            assert_eq!(selected.len(), 2);
            let mut values = Documents {
                values: BTreeMap::new(),
                _memory: account.reserve(0).unwrap(),
            };
            for row in selected {
                values._memory.grow(MAP_ENTRY_BYTES + row.id.len()).unwrap();
                values.values.insert(row.id.clone(), row);
            }
            drop(_memory);
            let mut loaded = Loaded {
                storage: Storage::Query(values),
            };
            let row = loaded.remove(&documents[0].id).unwrap();
            assert_eq!(row.document, documents[0]);
            drop(loaded);
            assert_eq!(
                ledger.snapshot().used_bytes,
                row._memory.as_ref().unwrap().bytes()
            );
            drop(row);
        } else {
            assert!(matches!(result, Err(HawDBError::ResourceBudgetExceeded(_))));
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn query_hydration_preserves_wire_grammar_budgets_and_unselected_integrity() {
    let documents = [
        document("a".into(), "内容".into()),
        document("b".into(), "tail".into()),
    ];
    let segment = descriptor(&documents);
    let ids = BTreeSet::from(["a".into()]);
    let grant = NonZeroUsize::new(4 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(grant);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "hydration grammar", grant);
    let task = RuntimeTaskContext::default();
    let query = QueryContext {
        memory: &account,
        task: &task,
        execution: None,
        io_scope: None,
    };
    let first = crate::encode_search_document_line(&documents[0]);
    let second = crate::encode_search_document_line(&documents[1]);
    let second = second.strip_suffix('\n').unwrap();
    let variants = [
        second.to_owned(),
        format!("{second}\n"),
        format!("{second}\r\n"),
        format!("{second}\r\r\n"),
        "doc\t62\t\t\tNaN\t6b=76;6b=77".into(),
        "doc\t62\t\t\tbad\t".into(),
        "doc\t62\t\tff\t\t".into(),
        "doc\t62\t\t0\t\t".into(),
        "doc\t62\t\tGG\t\t".into(),
        "doc\t61\t\t\t\t".into(),
        "doc\t7a\t\t\t\t".into(),
        "doc\t62\t\t\t\tbroken".into(),
    ];
    for tail in variants {
        let text = format!("HAWDB_SEARCH_SEGMENT_V1\n{first}{tail}");
        for budget in [
            HydrationBudget {
                limit: 16384,
                used: 0,
            },
            HydrationBudget { limit: 1, used: 0 },
        ] {
            let expected = select_documents(&mut Cursor::new(&text), &segment, &ids, budget);
            let actual = select_records(
                &mut Cursor::new(&text),
                &segment,
                &ids,
                budget,
                16384,
                query,
            );
            assert_eq!(actual.is_ok(), expected.is_ok(), "{tail:?}");
            if let (Ok(actual), Ok(expected)) = (actual, expected) {
                let rows = actual
                    .documents
                    .iter()
                    .map(|row| crate::encode_search_document_line(row))
                    .collect::<Vec<_>>();
                let expected = expected
                    .documents
                    .iter()
                    .map(crate::encode_search_document_line)
                    .collect::<Vec<_>>();
                assert_eq!(rows, expected);
            }
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
    let text = format!("HAWDB_SEARCH_SEGMENT_V1\n{first}{second}\n");
    let encoded = crate::encode_search_snapshot_text(&text).unwrap();
    let result = read_validated_with_account(
        Cursor::new(&encoded),
        encoded.len() as u64,
        checksum_bytes(&encoded) ^ 1,
        text.len() as u64,
        Some(DecodeAdmission {
            memory: &account,
            task: &task,
            max_header_bytes: 4096,
        }),
        |text| {
            select_records(
                text,
                &segment,
                &ids,
                HydrationBudget {
                    limit: 16384,
                    used: 0,
                },
                16384,
                query,
            )
        },
    );
    assert!(matches!(result, Err(HawDBError::Storage(_))));
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let blocker = account.reserve(grant.get() - 1).unwrap();
    assert!(matches!(
        decode_record(first.trim_end(), query),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    assert_eq!(ledger.snapshot().used_bytes, blocker.bytes());
    drop(blocker);
    task.cancellation().cancel();
    assert!(select_records(
        &mut Cursor::new(""),
        &segment,
        &ids,
        HydrationBudget {
            limit: 16384,
            used: 0
        },
        16384,
        query
    )
    .is_err());
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
