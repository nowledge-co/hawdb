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

#[test]
fn planning_preserves_resident_detached_and_file_backed_overflow_terms() {
    let source = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 1,
            ..RelationalOverflowConfig::default()
        },
    );
    source
        .commit(create_payload_table(), |_, _| Ok(()))
        .unwrap();
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".into(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".into()),
                        RelationalValue::Text("payload".repeat(64)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    let checkpoint = source.encode_checkpoint().unwrap();
    let path = std::env::temp_dir().join(format!(
        "hawdb-admission-size-{}.hawdb",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::write(&path, &checkpoint).unwrap();
    let snapshot = source.snapshot().unwrap();
    let decoded =
        decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint()).unwrap();
    assert_eq!(decoded.state.file_backed_overflow_segment_count(), 1);
    let resident = (std::mem::size_of::<RelationalKey>()
        + std::mem::size_of::<RelationalRow>()
        + std::mem::size_of::<Vec<RelationalValue>>()
        + 2 * "message-1".len()
        + std::mem::size_of::<RelationalOverflowRef>()) as u64;
    for original in [snapshot.value(), &decoded.state] {
        let overflow_bytes = original
            .overflow_segments
            .values()
            .next()
            .unwrap()
            .read()
            .unwrap()
            .as_ref()
            .len() as u64;
        assert_eq!(original.estimated_materialized_row_bytes(), resident);
        assert_eq!(
            original.estimated_checkpoint_bytes(),
            resident + overflow_bytes
        );
        for detached in [false, true] {
            let mut state = original.clone();
            if detached {
                state.omit_materialized_rows();
            }
            let expected = (
                if detached { 0 } else { resident },
                resident + overflow_bytes,
            );
            let mut visits = 0;
            assert_eq!(
                state
                    .estimated_checkpoint_admission_bytes_with_visit(&mut || {
                        visits += 1;
                        Ok::<_, usize>(())
                    })
                    .unwrap(),
                expected
            );
            assert!(visits > 0);
            for denied in 1..=visits {
                let mut reached = 0;
                assert_eq!(
                    state.estimated_checkpoint_admission_bytes_with_visit(&mut || {
                        reached += 1;
                        if reached == denied {
                            Err(denied)
                        } else {
                            Ok(())
                        }
                    }),
                    Err(denied)
                );
                assert_eq!(reached, denied);
            }
            assert_eq!(state.estimated_materialized_row_bytes(), expected.0);
            assert_eq!(state.estimated_checkpoint_bytes(), expected.1);
        }
    }
    drop(decoded);
    std::fs::remove_file(path).unwrap();
}
