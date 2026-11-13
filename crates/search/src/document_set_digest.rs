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

//! Order-independent identity of a complete search document set.

use crate::document_encoding::encode_search_document_line;
use crate::SearchDocument;
use serde::Serialize;

/// Constant-size identity of a document multiset, independent of visit order.
///
/// Each document contributes the first 128 bits of the SHA-256 of its
/// canonical encoding (ID, title, body, metadata and embedding);
/// contributions are summed modulo 2^128 and the count is kept separately.
/// Two sets with equal digests and counts hold the same documents unless
/// their difference collides, which detects corruption and drift but is not
/// an adversarial collision-resistance claim. Callers must reject duplicate
/// document IDs separately; search generations hold each ID once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchDocumentSetDigest {
    document_count: usize,
    content: u128,
}

impl SearchDocumentSetDigest {
    pub fn add(&mut self, document: &SearchDocument) {
        let line = encode_search_document_line(document);
        let digest = hawdb_integrity::sha256(line.as_bytes());
        let mut prefix = [0u8; 16];
        prefix.copy_from_slice(&digest.as_bytes()[..16]);
        self.content = self.content.wrapping_add(u128::from_be_bytes(prefix));
        self.document_count = self.document_count.saturating_add(1);
    }

    pub fn document_count(&self) -> usize {
        self.document_count
    }

    /// Lowercase hexadecimal content digest, without the document count.
    pub fn content_digest_hex(&self) -> String {
        format!("{:032x}", self.content)
    }
}

impl Serialize for SearchDocumentSetDigest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("SearchDocumentSetDigest", 2)?;
        state.serialize_field("document_count", &self.document_count)?;
        state.serialize_field("content_digest", &self.content_digest_hex())?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn document(id: &str, body: &str, embedding: Option<Vec<f32>>) -> SearchDocument {
        SearchDocument {
            id: id.into(),
            title: id.into(),
            content: body.into(),
            embedding,
            metadata: BTreeMap::from([("space_id".into(), "default".into())]),
        }
    }

    #[test]
    fn digest_is_order_independent_and_detects_content_drift() {
        let a = document("memory:a", "alpha", Some(vec![0.25, 0.5]));
        let b = document("memory:b", "beta", None);
        let mut forward = SearchDocumentSetDigest::default();
        forward.add(&a);
        forward.add(&b);
        let mut reverse = SearchDocumentSetDigest::default();
        reverse.add(&b);
        reverse.add(&a);
        assert_eq!(forward, reverse);
        assert_eq!(forward.document_count(), 2);

        for changed in [
            document("memory:a", "alpha!", Some(vec![0.25, 0.5])),
            document("memory:a", "alpha", Some(vec![0.25, 0.5000001])),
            document("memory:a", "alpha", None),
            document("memory:c", "alpha", Some(vec![0.25, 0.5])),
        ] {
            let mut drifted = SearchDocumentSetDigest::default();
            drifted.add(&changed);
            drifted.add(&b);
            assert_ne!(drifted, forward, "{changed:?}");
        }
        let mut metadata = a.clone();
        metadata.metadata.insert("labels".into(), "[\"x\"]".into());
        let mut drifted = SearchDocumentSetDigest::default();
        drifted.add(&metadata);
        drifted.add(&b);
        assert_ne!(drifted, forward);
    }
}
