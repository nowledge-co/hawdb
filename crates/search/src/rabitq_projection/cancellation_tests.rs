// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::resident_documents::DocumentView;
use std::cell::Cell;

struct CancellingView {
    documents: BTreeMap<String, SearchDocument>,
    cancellation: hawdb_core::RuntimeCancellationToken,
    reads: Cell<usize>,
}

impl DocumentView for CancellingView {
    fn len(&self) -> usize {
        self.documents.len()
    }

    fn get(&self, id: &str) -> Option<&SearchDocument> {
        self.reads.set(self.reads.get() + 1);
        self.cancellation.cancel();
        self.documents.get(id)
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&String, &SearchDocument)> + Clone {
        self.documents.iter()
    }

    fn range_documents<'a>(
        &'a self,
        first: &'a str,
        last: &'a str,
    ) -> impl Iterator<Item = (&'a String, &'a SearchDocument)> {
        self.documents.range::<str, _>((
            std::ops::Bound::Included(first),
            std::ops::Bound::Included(last),
        ))
    }
}

#[test]
fn cancellation_during_embedding_fetch_does_not_finish_a_projection() {
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(cancellation.clone());
    let view = CancellingView {
        documents: (0..3)
            .map(|number| {
                let id = format!("memory:{number}");
                (
                    id.clone(),
                    SearchDocument {
                        id,
                        title: String::new(),
                        content: String::new(),
                        embedding: Some(vec![1.0; 8]),
                        metadata: BTreeMap::new(),
                    },
                )
            })
            .collect(),
        cancellation,
        reads: Cell::new(0),
    };
    let result = RaBitQCandidateProjection::build_from_view(
        &view,
        ProjectionIdentity::new(1),
        Default::default(),
        hawdb_core::VectorDistance::Euclidean,
        Some(&task),
    );
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(view.reads.get(), 1);
}
