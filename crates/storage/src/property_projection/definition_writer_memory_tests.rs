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

use super::memory_test_support::{admitted, definition};
use super::*;

#[test]
fn real_writer_denial_uses_no_stable_sort_scratch_and_creates_no_artifact() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-projection-definition-plan-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    fs::create_dir(&root).unwrap();
    let path = root.join("projection.hawdb");
    let paths =
        GraphDescriptorTreePaths::new(root.join("descriptors.hawdb"), root.join("root.hawdb"));
    let definitions = (0..4096)
        .map(|i| {
            definition(
                PersistentPropertyProjectionKind::Equality,
                // A fixed permutation prevents the stable sort's already-
                // sorted/reversed shortcut from hiding its scratch allocation.
                format!("property-{:04}", (i * 2053) % 4096),
            )
        })
        .collect();
    let (governor, admission, work) = admitted(4096);
    let writer =
        PersistentPropertyProjectionWriter::new(Default::default()).with_work_context(work.clone());
    let descriptor =
        PersistentPropertyProjectionDescriptorTree::new(paths.clone(), Default::default());
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = writer.write_fallible(
        &path,
        ManifestGeneration(3),
        11,
        definitions,
        std::iter::empty(),
        descriptor,
    );
    let large_allocations = observer.finish();
    assert!(matches!(
        result,
        Err(PersistentPropertyProjectionError::Work(
            CheckpointWorkError::Memory(_)
        ))
    ));
    assert_eq!(
        large_allocations, 0,
        "definition sorting must not allocate stable-sort scratch"
    );
    assert!(!path.exists() && !paths.page_artifact.exists() && !paths.root_manifest.exists());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(writer);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    fs::remove_dir(&root).unwrap();
}
