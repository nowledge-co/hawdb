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

struct Fixture {
    directory: PathBuf,
    label: LabelId,
    property: String,
    value: Value,
    expected: u64,
}

impl Fixture {
    fn new(property: String, value: Value) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-canonical-bloom-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let label = LabelId(7);
        // Independent buffered canonical-codec reference, computed before
        // observing the production Bloom path's allocations.
        let mut reference = Vec::new();
        reference.extend_from_slice(&label.0.to_le_bytes());
        encode_string(&property, &mut reference).unwrap();
        encode_value(&value, &mut reference, 0).unwrap();
        assert!(reference.len() > 257 * 1024);
        let expected = content_digest(&reference).0;
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::from([label]),
            properties: BTreeMap::from([(property.clone(), value.clone())]),
        };
        let path = directory.join("canonical.hawdb");
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(23),
                [&node],
                std::iter::empty::<&RelRecord>(),
            )
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(0)),
            StoreId(23),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
        let mut selected = Vec::new();
        let (_, control) = reader
            .scan_nodes_by_property_control(label, &property, &value, |node| {
                selected.push(node);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(selected, vec![node]);
        Self {
            directory,
            label,
            property,
            value,
            expected,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn assert_without_large_encoding_buffer(fixture: &Fixture) {
    let observation = crate::test_allocator::AllocationObservation::start();
    let actual = node_property_bloom_key(fixture.label, &fixture.property, &fixture.value).unwrap();
    let allocations = observation.finish();
    assert_eq!(actual, fixture.expected);
    assert_eq!(
        allocations, 0,
        "Bloom hashing must not materialize the complete variable-width key/value encoding"
    );
}

#[test]
fn checkpoint_units_canonical_bloom_memory_wide_name_does_not_materialize_hash_input() {
    let fixture = Fixture::new(format!("{}🦀\0\t\n", "界".repeat(87_723)), Value::Int(19));
    assert_without_large_encoding_buffer(&fixture);
}

#[test]
fn checkpoint_units_canonical_bloom_memory_wide_binary_does_not_materialize_hash_input() {
    let fixture = Fixture::new("payload".into(), Value::Binary(vec![0x9f; 257 * 1024 + 3]));
    assert_without_large_encoding_buffer(&fixture);
}
