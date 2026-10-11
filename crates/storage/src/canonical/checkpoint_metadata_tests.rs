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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest,
};
use std::sync::atomic::Ordering;

struct Source {
    directory: PathBuf,
    reader: CanonicalSegmentReader,
}

impl Drop for Source {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn source(warm: bool) -> Source {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-descriptor-budget-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("canonical.hawdb");
    let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig {
        target_segment_bytes: NonZeroU64::MIN,
        max_record_bytes: NonZeroU64::new(512).unwrap(),
    })
    .write(
        &path,
        ManifestGeneration(1),
        (0..1057).map(|id| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::new(),
        }),
        std::iter::empty::<RelRecord>(),
    )
    .unwrap();
    assert_eq!(manifest.node_segment_count, 1057);
    let reader = CanonicalSegmentReader::open(
        &path,
        manifest,
        Arc::new(SegmentCache::new(4 * 1024 * 1024)),
        StoreId(1),
        NonZeroU64::new(512).unwrap(),
    )
    .unwrap();
    if warm {
        assert!(reader.get_node(NodeId(1056)).unwrap().is_some());
    }
    Source { directory, reader }
}

fn governor(bytes: u64) -> RuntimeGovernor {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    governor
}

#[test]
fn checkpoint_units_metadata_point_admits_all_1057_descriptor_validations() {
    for warm in [false, true] {
        let source = source(warm);
        let governor = governor(4 * 1024 * 1024);
        let permit = governor
            .try_admit(
                RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024).with_io_wave_slots(1),
            )
            .unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(1),
            ..Default::default()
        });
        let probe = Arc::new(CheckpointWorkProbe::default());
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(scheduler.clone());
        let decoded = source
            .reader
            .checkpoint_node(NodeId(1056), &work)
            .unwrap()
            .unwrap();
        assert_eq!(
            &*decoded,
            &source.reader.get_node(NodeId(1056)).unwrap().unwrap()
        );
        probe.assert_released(&scheduler);
        let visits = probe.completed.load(Ordering::SeqCst);
        assert!(visits >= 1057, "1057 real descriptors reached point hydration with only {visits} admitted units (warm={warm})");
        drop(decoded);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_metadata_point_denies_4096_bytes_even_with_warm_serving_cache() {
    for warm in [false, true] {
        let source = source(warm);
        let governor = governor(4096);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(4096).with_io_wave_slots(1))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let result = source.reader.checkpoint_node(NodeId(1056), &work);
        let error = match result {
            Ok(_) => panic!(
                "a 1057-entry metadata page escaped a 4096-byte working reservation (warm={warm})"
            ),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("remaining reservation"),
            "{error}"
        );
        assert!(!source.reader.is_poisoned());
        assert!(!source.reader.descriptor_reader.is_poisoned());
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
    }
}
