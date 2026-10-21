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
use crate::out_of_core::generation_writer::tests::{document, test_dir};
use crate::test_allocation as allocation;
use std::panic::{catch_unwind, AssertUnwindSafe};

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn partitioned_update_retains_decoded_ownership_until_result_drop_or_unwind() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    for unwind in [false, true] {
        let fixture = Fixture(test_dir("partitioned_update_ownership"));
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&fixture.0, Default::default()).unwrap();
        initial.push(document(0)).unwrap();
        let report = initial.finish().unwrap();
        let path = fixture.0.join(OUT_OF_CORE_MANIFEST_FILE);
        let original = fs::read(&path).unwrap();
        // This exercises the metadata join in isolation; it installs no files
        // and does not claim that this synthetic contribution is a ready view.
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&fixture.0, Default::default()).unwrap();
        writer.push(document(1)).unwrap();
        writer.active_manifest_update = Some(ActiveManifestUpdate::Append {
            expected_generation: report.generation,
        });
        let memory = writer.memory.clone();
        drop(memory.spool.reserve(1).unwrap());
        let baseline = memory.ledger.snapshot().used_bytes;
        let input_memory = memory
            .spool
            .reserve(
                SearchOutOfCoreManifestBody::decode_capacity(&original, &writer.task_context)
                    .unwrap(),
            )
            .unwrap();
        let mut candidate = SearchOutOfCoreManifestBody::decode(&original).unwrap();
        candidate.generation = report.generation + 1;
        let result = catch_unwind(AssertUnwindSafe(|| {
            let (prepared, _) =
                allocation::measure(|| prepare(&writer, candidate, input_memory).unwrap());
            assert!(
                allocation::live() > 0,
                "the join must retain decoded allocations"
            );
            assert!(
                allocation::live() <= memory.ledger.snapshot().used_bytes - baseline,
                "joined payload must stay covered before result destruction"
            );
            if unwind {
                panic!("intentional caller unwind after the manifest join");
            }
            drop(prepared);
        }));
        assert_eq!(result.is_err(), unwind);
        let live = allocation::live();
        let admitted = memory.ledger.snapshot().used_bytes - baseline;
        assert!(
            live <= admitted,
            "result destruction left {live} bytes live with only {admitted} admitted"
        );
        assert_eq!(live, 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
        assert_eq!(fs::read(&path).unwrap(), original);
    }
}
