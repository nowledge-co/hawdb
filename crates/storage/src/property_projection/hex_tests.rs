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
use crate::hex_test_support::{campaign_inputs, reference_decode};

#[test]
fn persisted_hex_preserves_ascii_pairs_and_unicode_round_trips() {
    for first in 0..128 {
        for second in 0..128 {
            let input = String::from_utf8(vec![first, second]).unwrap();
            assert_eq!(decode_hex(&input, "test").ok(), reference_decode(&input));
        }
    }
    for value in ["", "\0\t\n", "ASCII \u{e9}\u{4e2d}\u{1f980}"] {
        assert_eq!(
            decode_utf8_hex(&encode_hex(value.as_bytes()), "test").unwrap(),
            value
        );
    }
}

#[test]
fn persisted_hex_rejects_non_ascii_without_panicking() {
    for input in ["a\u{e9}a", "\u{1f980}", "00a\u{e9}a", "f", "gg"] {
        let result = std::panic::catch_unwind(|| decode_hex(input, "test"));
        assert!(result.is_ok(), "decoder panicked for {input:?}");
        assert!(matches!(
            result.unwrap(),
            Err(PersistentPropertyProjectionError::Corrupt(_))
        ));
    }
}

fn manifest(
    kind: PersistentPropertyProjectionKind,
    property: String,
) -> PersistentPropertyProjectionManifest {
    PersistentPropertyProjectionManifest {
        generation: ManifestGeneration(7),
        source_commit_epoch: 11,
        artifact_id: ARTIFACT_ID,
        artifact_len: ARTIFACT_HEADER.len() as u64 + 8,
        artifact_digest: ContentDigest(0),
        artifact_sha256: Sha256Digest::from_bytes([0; 32]),
        entry_count: 0,
        block_count: 0,
        definitions: vec![PersistentPropertyProjectionDefinition {
            label_id: LabelId(1),
            property,
            kind,
            complete: true,
        }],
        descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata {
            encoded_len: 1,
            encoded_crc32c: 0,
            encoded_sha256: Sha256Digest::from_bytes([0; 32]),
        },
    }
}

#[test]
fn persisted_hex_public_manifest_rejects_checksum_valid_corruption() {
    let composite =
        persistent_composite_property_identity(&["first".into(), "second".into()]).unwrap();
    for (kind, property, bad_hex, expected_error) in [
        (
            PersistentPropertyProjectionKind::Equality,
            "name".to_string(),
            "a\u{e9}a".to_string(),
            "definition property",
        ),
        (
            PersistentPropertyProjectionKind::CompositeEquality,
            composite,
            encode_hex(
                format!("{COMPOSITE_PROPERTY_IDENTITY_PREFIX}:a\u{e9}a:7365636f6e64").as_bytes(),
            ),
            "composite property identity",
        ),
    ] {
        let manifest = manifest(kind, property.clone());
        let valid = manifest.encode().unwrap();
        assert_eq!(
            PersistentPropertyProjectionManifest::decode(&valid).unwrap(),
            manifest
        );
        let (body, _) = valid.rsplit_once("checksum\t").unwrap();
        let body = body.replace(
            &format!("\t{}\t", encode_hex(property.as_bytes())),
            &format!("\t{bad_hex}\t"),
        );
        let corrupt = format!("{body}checksum\t{}\n", content_digest(body.as_bytes()).0);
        let result =
            std::panic::catch_unwind(|| PersistentPropertyProjectionManifest::decode(&corrupt));
        assert!(result.is_ok(), "manifest decode panicked for {kind:?}");
        let error = result.unwrap().unwrap_err();
        assert!(matches!(
            error,
            PersistentPropertyProjectionError::Corrupt(_)
        ));
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(
            PersistentPropertyProjectionManifest::decode(&valid).unwrap(),
            manifest
        );
    }
}

#[test]
#[ignore = "explicit local property projection hex differential campaign"]
fn persisted_hex_differential_campaign() {
    for (case, input) in campaign_inputs().iter().enumerate() {
        assert_eq!(
            decode_hex(input, "test").ok(),
            reference_decode(input),
            "case {case}"
        );
    }
}
