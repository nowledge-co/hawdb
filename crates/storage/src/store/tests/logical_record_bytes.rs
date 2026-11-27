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

// Disk-pressure admission from the maintained logical record byte estimate
// decides exactly as the full-walk estimate does (#2032).
#[test]
fn checkpoint_temporary_space_admission_matches_full_walk_estimate() {
    use super::{CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER, MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES};
    use hawdb_storage::pressure::{
        StorageDebtController, StoragePressureReasonCode, StoragePressureSignals,
    };
    let path = unique_test_dir("logical_record_bytes_admission");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .durable
        .as_mut()
        .unwrap()
        .set_wal_available_space_override(u64::MAX);
    for id in 0..64 {
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::Int(id)),
                    ("body", Value::String("x".repeat(2_048))),
                ]),
            )
            .unwrap();
    }
    assert!(!store.is_out_of_core());
    let walked = |store: &GraphStore| {
        store
            .estimated_logical_record_bytes_reference()
            .saturating_add(store.relational_state.estimated_checkpoint_bytes())
            .saturating_mul(CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER)
            .max(MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES)
    };
    let expected = walked(&store);
    assert!(expected > MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES);
    let pressure = store.storage_pressure_signals(None, None);
    assert_eq!(pressure.estimated_checkpoint_temporary_bytes, expected);
    let admission = store.wal_admission_signals(&[]).unwrap();
    assert_eq!(admission.estimated_checkpoint_temporary_bytes, expected);

    // Golden decisions at the free-space threshold.
    for available in [expected - 1, expected, expected + 1] {
        let decide = |estimated_checkpoint_temporary_bytes| {
            StorageDebtController.evaluate(StoragePressureSignals {
                available_free_space_bytes: Some(available),
                estimated_checkpoint_temporary_bytes,
                ..admission
            })
        };
        let maintained = decide(admission.estimated_checkpoint_temporary_bytes);
        assert_eq!(maintained, decide(expected));
        assert_eq!(
            maintained
                .reason_codes
                .contains(&StoragePressureReasonCode::FreeSpaceReserve),
            available < expected
        );
    }

    // End to end: the WAL append refuses below the walked requirement and
    // admits above it.
    let body = || properties([("body", Value::String("y".repeat(2_048)))]);
    store
        .durable
        .as_mut()
        .unwrap()
        .set_wal_available_space_override(expected / 2);
    let error = store
        .create_node(&mut catalog, "Memory", body())
        .unwrap_err()
        .to_string();
    assert!(error.contains("reasons=free_space_reserve"), "{error}");
    store
        .durable
        .as_mut()
        .unwrap()
        .set_wal_available_space_override(expected.saturating_mul(2));
    store.create_node(&mut catalog, "Memory", body()).unwrap();
    assert_eq!(
        store.estimated_logical_record_bytes(),
        store.estimated_logical_record_bytes_reference()
    );
    drop(store);
    fs::remove_dir_all(path).unwrap();
}
