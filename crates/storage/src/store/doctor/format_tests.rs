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
use crate::relational::{encode_relational_wal_batch, RelationalTransaction};
use crate::wal::{WalEntry, WalOp};
use hawdb_core::StorageFormatError;
use std::collections::BTreeMap;
use std::io::Write;

fn files(directory: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut output = BTreeMap::new();
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            output.extend(files(&path));
        } else {
            output.insert(path.clone(), std::fs::read(path).unwrap());
        }
    }
    output
}

#[test]
fn incompatible_record_refuses_apply_and_interrupted_repair_without_mutation() {
    for resume in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "hawdb-doctor-format-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = hawdb_core::Catalog::default();
        drop(super::super::GraphStore::open(&path, &mut catalog).unwrap());
        let options = WalDoctorOptions::default();
        let boundary = WalRepairBoundary::load(&path, options).unwrap();
        let wal = boundary.wal_path(&path);
        std::fs::write(
            &wal,
            crate::wal::frame::encode_binary_wal_header(
                boundary.wal_generation,
                boundary.wal_replay_start_lsn,
            ),
        )
        .unwrap();
        let mut record = encode_relational_wal_batch(1, &RelationalTransaction::default()).unwrap();
        let supported = u16::from_le_bytes(record[8..10].try_into().unwrap());
        record[8..10].copy_from_slice(&(supported + 1).to_le_bytes());
        let payload = crate::wal::binary::encode_binary_wal_record(
            &WalEntry {
                lsn: boundary.wal_replay_start_lsn,
                op: WalOp::Relational {
                    record: record.into(),
                },
            },
            1,
        )
        .unwrap();
        let framed = crate::wal::frame::frame_binary_wal_record(
            boundary.wal_generation,
            &payload,
            std::fs::metadata(&wal).unwrap().len(),
        );
        let mut file = std::fs::OpenOptions::new().append(true).open(&wal).unwrap();
        file.write_all(&framed).unwrap();
        let retained = file.metadata().unwrap().len();
        file.write_all(b"torn").unwrap();
        drop(file);
        // Model a plan produced by a reader supporting the newer codec.
        let plan = build_plan(
            &boundary.selector_path,
            &wal,
            &boundary,
            boundary.wal_replay_start_lsn + 1,
            retained,
            retained + 4,
        )
        .unwrap();
        if resume {
            prepare_repair(&path, &wal, &plan).unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&wal)
                .unwrap()
                .set_len(retained)
                .unwrap();
        }
        let before = files(&path);
        let error = DatabaseDoctor::apply_wal_tail_repair(
            &path,
            &plan,
            plan.acknowledge_potential_data_loss(),
            options,
        )
        .unwrap_err();
        assert_eq!(
            error,
            HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
                format: "relational durable codec",
                found: supported + 1,
                supported,
            })
        );
        assert_eq!(files(&path), before);
        std::fs::remove_dir_all(path).unwrap();
    }
}
