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

//! Canonical body checksums retain typed admission errors through serde.
use super::*;
use crate::lexical_projection::QueryContext;
use serde::ser::SerializeSeq;
use std::cell::RefCell;

#[derive(Serialize)]
struct Body<'a> {
    format: &'a str,
    generation: u64,
    analyzer_digest: u64,
    entries: Entries<'a>,
}
struct Entries<'a> {
    entries: &'a [SearchMutationRunEntry],
    context: QueryContext<'a>,
    error: &'a RefCell<Option<HawDBError>>,
}
#[derive(Serialize)]
struct Entry<'a> {
    document_id: &'a str,
    target_segment_id: u64,
    operation: &'a SearchMutationOperation,
    retraction: Retraction<'a>,
}
#[derive(Serialize)]
struct Retraction<'a> {
    documents_digest: u64,
    lexical_document_len: u64,
    unique_terms: terms::WithContext<'a>,
}
impl Serialize for Entries<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.entries.len()))?;
        for entry in self.entries {
            sequence.serialize_element(&Entry {
                document_id: &entry.document_id,
                target_segment_id: entry.target_segment_id,
                operation: &entry.operation,
                retraction: Retraction {
                    documents_digest: entry.retraction.documents_digest,
                    lexical_document_len: entry.retraction.lexical_document_len,
                    unique_terms: terms::WithContext {
                        terms: &entry.retraction.unique_terms,
                        context: self.context,
                        error: self.error,
                    },
                },
            })?;
        }
        sequence.end()
    }
}

pub(super) fn checksum(body: &SearchMutationRunBody, context: QueryContext<'_>) -> Result<u64> {
    context.checkpoint()?;
    let error = RefCell::new(None);
    let value = Body {
        format: &body.format,
        generation: body.generation,
        analyzer_digest: body.analyzer_digest,
        entries: Entries {
            entries: &body.entries,
            context,
            error: &error,
        },
    };
    let result = crate::build_control::json::checksum_with_context(&value, Some(context.task));
    context.checkpoint()?;
    if let Some(error) = error.into_inner() {
        return Err(error);
    }
    result
}
