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
use crate::lexical_projection::{
    artifact_file, BlockDescriptor, BlockKind, ManifestBody, ManifestEnvelope, TermStatistics,
    ARTIFACT_HEADER, DEFAULT_MAX_MANIFEST_BYTES,
};
use hawdb_integrity::Crc32cHasher;
use serde::ser::Error as _;
use std::cell::Cell;

pub(in crate::lexical_projection) fn manifest(mut terms: Vec<String>) -> ManifestBody {
    terms.sort();
    terms.dedup();
    let header_len = ARTIFACT_HEADER.len() as u64 + 8;
    let blocks = if terms.is_empty() {
        Vec::new()
    } else {
        vec![
            BlockDescriptor {
                block_id: 0,
                kind: BlockKind::Documents,
                min_key: "document-\"\\\n".into(),
                max_key: "document-\"\\\n".into(),
                offset: header_len,
                length: 64,
                checksum: u64::MAX,
                entry_count: 1,
                ordinal_start: 0,
            },
            BlockDescriptor {
                block_id: 1,
                kind: BlockKind::Postings,
                min_key: terms.first().unwrap().clone(),
                max_key: terms.last().unwrap().clone(),
                offset: header_len + 64,
                length: 64,
                checksum: 0,
                entry_count: terms.len() as u32,
                ordinal_start: 0,
            },
        ]
    };
    ManifestBody {
        format: "HAWDB_LEXICAL_MANIFEST_V2".into(),
        layout: "HAWDB_LEXICAL_ORDINAL_V1".into(),
        generation: 7,
        source_graph_commit_epoch: Some(8),
        analyzer_digest: u64::MAX,
        documents_digest: 0,
        artifact_file: artifact_file(7),
        artifact_len: header_len + blocks.len() as u64 * 64,
        artifact_checksum: u64::MAX,
        document_count: u64::from(!terms.is_empty()),
        total_document_len: terms.len() as u64,
        posting_count: terms.len() as u64,
        term_statistics: terms
            .into_iter()
            .map(|term| TermStatistics {
                term,
                document_frequency: 1,
            })
            .collect(),
        blocks,
    }
}

// Preserve the previous allocating wire algorithm as an independent reference.
fn legacy_encode(body: &ManifestBody) -> Vec<u8> {
    let body_bytes = serde_json::to_vec(body).unwrap();
    let mut digest = Crc32cHasher::new();
    digest.update(&body_bytes);
    serde_json::to_vec(&ManifestEnvelope {
        body: body.clone(),
        checksum: digest.finish(),
    })
    .unwrap()
}

fn assert_wire_and_admission(body: &ManifestBody) {
    let expected = legacy_encode(body);
    let exact = expected.len() as u64;
    let actual = encode(body, exact).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(body.encode(DEFAULT_MAX_MANIFEST_BYTES).unwrap(), expected);
    assert_eq!(ManifestBody::decode(&actual).unwrap(), *body);
    for limit in [0, exact - 1] {
        assert_eq!(
            encode(body, limit).unwrap_err().to_string(),
            format!(
                "storage error: lexical projection manifest requires {exact} bytes, exceeding {limit}"
            )
        );
    }
}

#[test]
fn manifest_wire_and_size_admission_match_the_legacy_envelope() {
    for terms in [
        Vec::new(),
        vec!["plain".into()],
        vec![
            "\0\u{0001}\n\r\t\"\\".into(),
            "\u{4e2d}\u{6587}".into(),
            "\u{1f980}".into(),
        ],
        vec!["x".repeat(5202), "\"".repeat(4097)],
    ] {
        let mut body = manifest(terms);
        assert_wire_and_admission(&body);
        body.source_graph_commit_epoch = None;
        assert_wire_and_admission(&body);
    }
}

#[test]
fn decode_preserves_checksum_and_schema_rejection() {
    let body = manifest(vec!["term".into()]);
    let encoded = body.encode(DEFAULT_MAX_MANIFEST_BYTES).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["checksum"] = serde_json::json!(u64::MAX);
    let error = ManifestBody::decode(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(error.to_string().contains("manifest checksum mismatch"));

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["body"]["unexpected"] = serde_json::json!(true);
    let error = ManifestBody::decode(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(error.to_string().contains("unknown field"));

    let mut invalid = body;
    invalid.posting_count += 1;
    assert!(invalid
        .encode(DEFAULT_MAX_MANIFEST_BYTES)
        .unwrap_err()
        .to_string()
        .contains("counts are inconsistent"));
    let error = ManifestBody::decode(&legacy_encode(&invalid)).unwrap_err();
    assert!(error.to_string().contains("counts are inconsistent"));

    let mut previous_layout = manifest(vec!["term".into()]);
    previous_layout.format = "HAWDB_LEXICAL_MANIFEST_V1".into();
    assert!(previous_layout.validate().is_err());
}

#[test]
fn manifest_rejects_document_blocks_after_postings() {
    let mut body = manifest(vec!["term".into()]);
    let mut documents = body.blocks.remove(0);
    let mut postings = body.blocks.remove(0);
    let header_len = ARTIFACT_HEADER.len() as u64 + 8;
    postings.block_id = 0;
    postings.offset = header_len;
    documents.block_id = 1;
    documents.offset = header_len + postings.length;
    body.blocks = vec![postings, documents];

    assert!(body.validate().is_err());
}

struct ChangingBody {
    pass: Cell<usize>,
    final_body: &'static str,
    fail_pass: Option<usize>,
}

impl Serialize for ChangingBody {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let pass = self.pass.get() + 1;
        self.pass.set(pass);
        if self.fail_pass == Some(pass) {
            return Err(S::Error::custom("injected manifest serialization failure"));
        }
        serializer.serialize_str(if pass < 3 {
            "baseline"
        } else {
            self.final_body
        })
    }
}

#[test]
fn serialization_growth_shrinkage_and_errors_fail_closed() {
    for final_body in ["", "a substantially larger body than the sizing pass"] {
        let body = ChangingBody {
            pass: Cell::new(0),
            final_body,
            fail_pass: None,
        };
        assert!(encode(&body, 4096).is_err());
        assert_eq!(body.pass.get(), 3);
    }
    for fail_pass in [1, 2, 3] {
        let body = ChangingBody {
            pass: Cell::new(0),
            final_body: "baseline",
            fail_pass: Some(fail_pass),
        };
        assert!(encode(&body, 4096)
            .unwrap_err()
            .to_string()
            .contains("injected"));
        assert_eq!(body.pass.get(), fail_pass);
    }
    let body = ChangingBody {
        pass: Cell::new(0),
        final_body: "baseline",
        fail_pass: None,
    };
    assert!(encode(&body, 1).is_err());
    assert_eq!(
        body.pass.get(),
        2,
        "over-budget encoding must not reach the output pass"
    );
}

#[test]
fn operation_output_admission_is_exact_and_retained_with_the_bytes() {
    use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};
    let body = manifest(vec!["alpha".into(), "\"".repeat(9000)]);
    let expected = legacy_encode(&body);
    for short in [0, 1] {
        let budget = expected.len() + 4096 - short;
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(budget as u64, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let other = memory.spool.reserve(4096).unwrap();
        let result = encode_with_context(&body, expected.len() as u64, &memory, &task);
        if short == 1 {
            assert!(result.unwrap_err().to_string().contains("query memory"));
        } else {
            let encoded = result.unwrap();
            assert_eq!(encoded.bytes, expected);
            assert_eq!(memory.ledger.snapshot().used_bytes, budget);
            assert!(memory.input.reserve(1).is_err());
            drop(encoded);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, other.bytes());
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn cancellation_in_any_json_pass_releases_output_and_stops_later_passes() {
    use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
    struct CancelBody {
        pass: Cell<usize>,
        cancel_at: usize,
        cancellation: RuntimeCancellationToken,
    }
    impl Serialize for CancelBody {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            let pass = self.pass.get() + 1;
            self.pass.set(pass);
            let result = serializer.serialize_str("unchanged");
            if pass == self.cancel_at {
                self.cancellation.cancel();
            }
            result
        }
    }
    for cancel_at in [1, 2, 3] {
        let cancellation = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(cancellation.clone());
        let memory = BuildMemory::new(&task).unwrap();
        let body = CancelBody {
            pass: Cell::new(0),
            cancel_at,
            cancellation,
        };
        assert!(encode_with_context(&body, 4096, &memory, &task)
            .unwrap_err()
            .to_string()
            .contains("cancel"));
        assert_eq!(body.pass.get(), cancel_at);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
#[ignore = "explicit local manifest encoding differential campaign"]
fn manifest_encoding_differential_campaign() {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let alphabet = [
        'a',
        'Z',
        '0',
        '\"',
        '\\',
        '\0',
        '\n',
        '\u{4e2d}',
        '\u{1f980}',
    ];
    for case in 0..512 {
        let count = next() as usize % 24;
        let mut terms = Vec::with_capacity(count);
        for index in 0..count {
            let mut term = format!("{case}-{index}-");
            for _ in 0..(next() as usize % 96) {
                term.push(alphabet[next() as usize % alphabet.len()]);
            }
            terms.push(term);
        }
        let mut body = manifest(terms);
        body.analyzer_digest = next();
        body.documents_digest = next();
        body.source_graph_commit_epoch = (next() % 2 == 0).then(&mut next);
        assert_wire_and_admission(&body);
    }
}
