// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::resident_documents::DocumentView;
use std::cell::Cell;

struct CancellingView {
    documents: BTreeMap<String, SearchDocument>,
    cancellation: hawdb_core::RuntimeCancellationToken,
    reads: Cell<usize>,
    iterations: Cell<usize>,
    cancel_during_iteration: bool,
}

impl CancellingView {
    fn new(
        count: usize,
        cancellation: hawdb_core::RuntimeCancellationToken,
        cancel_during_iteration: bool,
    ) -> Self {
        Self {
            documents: (0..count)
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
            iterations: Cell::new(0),
            cancel_during_iteration,
        }
    }
}

impl DocumentView for CancellingView {
    fn len(&self) -> usize {
        self.documents.len()
    }

    fn get(&self, id: &str) -> Option<&SearchDocument> {
        self.reads.set(self.reads.get() + 1);
        if !self.cancel_during_iteration {
            self.cancellation.cancel();
        }
        self.documents.get(id)
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&String, &SearchDocument)> + Clone {
        self.documents.iter().inspect(|_| {
            self.iterations.set(self.iterations.get() + 1);
            if self.cancel_during_iteration {
                self.cancellation.cancel();
            }
        })
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
    let view = CancellingView::new(3, cancellation, false);
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

#[test]
fn cancellation_during_capacity_admission_stops_before_scanning_the_domain() {
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(cancellation.clone());
    let view = CancellingView::new(32, cancellation, true);
    let result = RaBitQCandidateProjection::in_memory_capacity_bound_with_context(
        &view,
        ProjectionIdentity::new(1),
        Default::default(),
        Some(&task),
    );
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(view.iterations.get(), 1);
    assert_eq!(view.reads.get(), 0);

    let retry = RuntimeTaskContext::without_deadline(hawdb_core::RuntimeCancellationToken::new());
    let bound = RaBitQCandidateProjection::in_memory_capacity_bound_with_context(
        &view.documents,
        ProjectionIdentity::new(1),
        Default::default(),
        Some(&retry),
    )
    .unwrap();
    assert!(bound > 0 && bound < usize::MAX);
    assert_eq!(
        bound,
        RaBitQCandidateProjection::in_memory_capacity_bound(
            &view.documents,
            ProjectionIdentity::new(1),
            Default::default(),
        )
    );
}
