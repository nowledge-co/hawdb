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
use crate::RuntimeCancellationToken;
use std::collections::BTreeMap;

fn budget() -> SearchProjectionScanBudget {
    SearchProjectionScanBudget {
        max_rows: 16,
        max_payload_bytes: 4096,
        max_encoded_record_bytes: 4096,
    }
}

fn document(id: &str) -> SearchDocument {
    SearchDocument {
        id: id.into(),
        title: "full title".into(),
        content: "complete body".into(),
        embedding: Some(vec![1.0, -0.0]),
        metadata: BTreeMap::from([("space_id".into(), "team".into())]),
    }
}

#[test]
fn resident_scan_exhausts_all_fields_and_preflights_before_callbacks() {
    let mut index = SearchIndex::in_memory();
    let documents = [document("unmatched:a"), document("unmatched:b")];
    for document in &documents {
        index.upsert(document.clone()).unwrap();
    }
    let task = RuntimeTaskContext::default();
    let mut observed = Vec::new();
    let report = index
        .visit_projection_documents(budget(), &task, &mut |document| {
            observed.push(document.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(observed, documents);
    assert_eq!(report.document_count, 2);
    assert_eq!(
        report.payload_bytes,
        documents
            .iter()
            .map(|document| payload_bytes(document).unwrap())
            .sum::<usize>()
    );
    assert_eq!(report.artifact_generation, None);
    for limited in [
        SearchProjectionScanBudget {
            max_rows: 1,
            ..budget()
        },
        SearchProjectionScanBudget {
            max_payload_bytes: report.payload_bytes - 1,
            ..budget()
        },
    ] {
        let mut calls = 0;
        let error = index
            .visit_projection_documents(limited, &task, &mut |_| {
                calls += 1;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(error, HawDBError::ReadBudgetExceeded { .. }));
        assert_eq!(calls, 0);
    }
}

#[test]
fn resident_scan_empty_cancelled_and_visitor_failure_do_not_certify_partial_results() {
    let mut index = SearchIndex::in_memory();
    assert_eq!(
        index
            .visit_projection_documents(budget(), &RuntimeTaskContext::default(), &mut |_| panic!(
                "empty scan callback"
            ))
            .unwrap()
            .document_count,
        0
    );
    index.upsert(document("a")).unwrap();
    index.upsert(document("b")).unwrap();
    let cancellation = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    assert!(index
        .visit_projection_documents(budget(), &task, &mut |_| panic!("cancelled scan callback"))
        .is_err());
    let mut calls = 0;
    let error = index
        .visit_projection_documents(budget(), &RuntimeTaskContext::default(), &mut |_| {
            calls += 1;
            Err(HawDBError::Execution("visitor stopped".into()))
        })
        .unwrap_err();
    assert_eq!(calls, 1);
    assert!(matches!(error, HawDBError::Execution(message) if message == "visitor stopped"));
}

#[test]
fn resident_scan_terminal_cancellation_refuses_success_after_last_callback() {
    let mut index = SearchIndex::in_memory();
    index.upsert(document("a")).unwrap();
    let cancellation = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(cancellation.clone());
    let mut calls = 0;
    assert!(index
        .visit_projection_documents(budget(), &task, &mut |_| {
            calls += 1;
            cancellation.cancel();
            Ok(())
        })
        .is_err());
    assert_eq!(calls, 1);
    let expired = RuntimeTaskContext::new(
        RuntimeCancellationToken::new(),
        Some(hawdb_core::time::Instant::now()),
    );
    assert!(index
        .visit_projection_documents(budget(), &expired, &mut |_| panic!("expired scan callback"))
        .is_err());
}
