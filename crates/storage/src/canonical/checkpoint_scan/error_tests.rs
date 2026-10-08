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
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

fn used(task: &RuntimeTaskContext, ceiling: u64) -> u64 {
    match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => ceiling - available_bytes,
        Ok(lease) => {
            drop(lease);
            0
        }
        Err(error) => panic!("unexpected resource error {error}"),
    }
}

#[test]
fn checkpoint_units_scan_memory_error_deep_array_denial_reaches_outer_classifier_and_fully_retries()
{
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-scan-error-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("canonical.hawdb");
    let n = 8193;
    let nodes = (0..n)
        .map(|i| NodeRecord {
            id: NodeId(i as u64),
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
        .write(
            &path,
            ManifestGeneration(18),
            &nodes,
            std::iter::empty::<&RelRecord>(),
        )
        .unwrap();
    let reader = CanonicalSegmentReader::open(
        &path,
        manifest,
        Arc::new(SegmentCache::new(0)),
        StoreId(18),
        NonZeroU64::new(32 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let before = std::fs::read(&path).unwrap();
    let ceiling = 32 * 1024 * 1024;
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let held = task.reserve_working_memory(ceiling - 512 * 1024).unwrap();
    let before_used = used(&task, ceiling);
    let result = work.classify(|work| {
        let mut scan = CheckpointCanonicalIterator::<NodeRecord>::new(&reader, work);
        scan.next().transpose()
    });
    assert!(
        matches!(
            result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ),
        "nested checkpoint errors must preserve outer work classification"
    );
    assert_eq!(used(&task, ceiling), before_used);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert!(!reader.is_poisoned());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    drop(held);
    let mut scan = CheckpointCanonicalIterator::<NodeRecord>::new(&reader, &work);
    for expected in &nodes {
        let actual = scan.next().unwrap().unwrap();
        assert_eq!(*actual, *expected);
    }
    assert!(scan.next().is_none());
    drop(scan);
    assert_eq!(used(&task, ceiling), 0);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!reader.is_poisoned());
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(reader);
    std::fs::remove_dir_all(directory).unwrap();
}
