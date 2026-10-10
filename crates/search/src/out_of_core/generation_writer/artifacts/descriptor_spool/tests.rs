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
use crate::document_encoding::DescriptorEncoding;
use crate::{SearchSegmentDescriptor, SearchSegmentFieldSummary};
use std::collections::BTreeMap;

fn entry(number: u64) -> SearchSegmentDescriptorEntry {
    SearchSegmentDescriptorEntry {
        segment_id: number,
        first_document_id: format!("memory:{number}\tfirst"),
        last_document_id: format!("memory:{number}\nlast"),
        document_count: 2,
        payload_range: Some(SearchSegmentPayloadRange {
            artifact_id: 7,
            offset: number * 32,
            length: 32,
            checksum: 19,
        }),
        metadata: BTreeMap::from([(
            "space_id".into(),
            SearchSegmentFieldSummary {
                present_count: 2,
                values: BTreeSet::from(["default".into(), "escaped\nvalue".into()]),
                ..Default::default()
            },
        )]),
    }
}

#[test]
fn descriptor_spool_preserves_exact_v3_bytes_and_size_admission() {
    for count in [0, 1, 3] {
        let root = super::super::super::tests::test_dir("descriptor_spool_wire");
        std::fs::create_dir(&root).unwrap();
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut spool = DescriptorSpool::new(&root, &memory, &task).unwrap();
        let descriptor = SearchSegmentDescriptor {
            target_documents: SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
            document_count: count * 2,
            segments: (0..count as u64).map(entry).collect(),
        };
        for entry in &descriptor.segments {
            spool.push(entry, &memory, &task, 1024 * 1024).unwrap();
        }
        let reference = DescriptorEncoding::new(&descriptor, 1024 * 1024).unwrap();
        let mut expected = Vec::new();
        reference.write_to(&mut expected).unwrap();
        let mut actual = Vec::new();
        let bytes = spool
            .write_descriptor(&mut actual, count * 2, expected.len() as u64, &task)
            .unwrap();
        assert_eq!(bytes, expected.len() as u64);
        assert_eq!(actual, expected);
        assert!(spool
            .write_descriptor(&mut Vec::new(), count * 2, bytes - 1, &task)
            .unwrap_err()
            .to_string()
            .contains("descriptor requires"));
        drop(spool);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn descriptor_spool_rejects_changed_and_incomplete_records() {
    for damage in ["checksum", "truncated", "appended"] {
        let root = super::super::super::tests::test_dir("descriptor_spool_damage");
        std::fs::create_dir(&root).unwrap();
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut spool = DescriptorSpool::new(&root, &memory, &task).unwrap();
        spool.push(&entry(0), &memory, &task, 1024 * 1024).unwrap();
        spool.writer.flush().unwrap();
        let path = root.join("search-segment-descriptors.spool.hawdb");
        let mut bytes = std::fs::read(&path).unwrap();
        match damage {
            "checksum" => bytes[0] ^= 1,
            "truncated" => {
                bytes.pop();
            }
            "appended" => bytes.push(0),
            _ => unreachable!(),
        }
        std::fs::write(&path, bytes).unwrap();
        let error = spool
            .write_descriptor(&mut Vec::new(), 2, 1024 * 1024, &task)
            .unwrap_err();
        assert!(
            error.to_string().contains(if damage == "checksum" {
                "descriptor spool checksum changed"
            } else {
                "descriptor spool length changed"
            }),
            "{error}"
        );
        drop(spool);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
