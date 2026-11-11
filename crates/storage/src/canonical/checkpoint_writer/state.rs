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

//! Own the canonical data file and its unfinished record/segment state.
//! Source iteration and spill ownership remain with the enclosing preparation.

use super::*;

pub(in crate::canonical) struct State {
    file: File,
    artifact_digest: IntegrityHasher,
    artifact_len: u64,
    generation: ManifestGeneration,
    source_commit_epoch: u64,
    segment_id: u64,
    node_segment_count: u64,
    relationship_segment_count: u64,
    node_count: u64,
    relationship_count: u64,
    property_keys: PropertyKeyDictionary,
    accumulator: accumulator::Accumulator,
    descriptor_tree: GraphDescriptorTreeBuilder,
    pending_descriptor: Option<descriptor::Descriptor>,
    payload: Option<Record>,
    record_plan: Option<record::Planning>,
    record_id: Option<u64>,
    record_phase: u8,
    kind: CanonicalSegmentKind,
    config: CanonicalSegmentConfig,
    source_admits: bool,
    finish_phase: u8,
    finished: bool,
    prepared_tree: Option<PreparedGraphDescriptorTree>,
    work: CheckpointWorkContext,
}

impl State {
    pub(in crate::canonical) fn create(
        writer: &CanonicalSegmentWriter,
        path: &Path,
        identity: CanonicalWriteIdentity,
        descriptor_tree: GraphDescriptorTreeBuilder,
    ) -> Result<Self, CanonicalSegmentError> {
        let CanonicalWriteIdentity {
            generation,
            source_commit_epoch,
        } = identity;
        let work = writer.work.clone().unwrap_or_default();
        let _wave = work.io_wave()?;
        let mut file = File::create(path)?;
        let mut artifact_digest = IntegrityHasher::new();
        write_hashed(&mut file, &mut artifact_digest, ARTIFACT_HEADER)?;
        write_hashed(&mut file, &mut artifact_digest, &generation.0.to_le_bytes())?;
        drop(_wave);
        let artifact_len = ARTIFACT_HEADER.len() as u64 + 8;
        let segment_id = 1u64;
        let node_segment_count = 0u64;
        let relationship_segment_count = 0u64;
        let node_count = 0u64;
        let relationship_count = 0u64;
        let property_keys = if writer.source_admits {
            PropertyKeyDictionary::with_work_context(work.clone())
        } else {
            PropertyKeyDictionary::default()
        };

        let accumulator = checkpoint_writer::accumulator::Accumulator::new(
            CanonicalSegmentKind::Nodes,
            generation,
            segment_id,
            writer.config,
            work.clone(),
            writer.source_admits,
        );
        Ok(Self {
            file,
            artifact_digest,
            artifact_len,
            generation,
            source_commit_epoch,
            segment_id,
            node_segment_count,
            relationship_segment_count,
            node_count,
            relationship_count,
            property_keys,
            accumulator,
            descriptor_tree,
            pending_descriptor: None,
            payload: None,
            record_plan: None,
            record_id: None,
            record_phase: 0,
            kind: CanonicalSegmentKind::Nodes,
            config: writer.config,
            source_admits: writer.source_admits,
            finish_phase: 0,
            finished: false,
            prepared_tree: None,
            work,
        })
    }

    pub(in crate::canonical) fn encode_node(
        &mut self,
        node: &NodeRecord,
        mut spills: Option<&mut PropertySpillWriter>,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        self.bind_attempt(work)?;
        if let Some(spills) = spills.as_mut() {
            spills.bind_checkpoint_attempt(work);
        }
        self.select_record(node.id.0, CanonicalSegmentKind::Nodes)?;
        if self.payload.is_none() {
            self.payload = Some(if self.source_admits {
                Record::Checkpoint(record::node_with_preparation(
                    node,
                    spills,
                    &mut self.property_keys,
                    self.config,
                    &mut self.record_plan,
                )?)
            } else {
                Record::Ordinary(encode_node_with_property_spills(
                    node,
                    spills,
                    Some(&mut self.property_keys),
                )?)
            });
        }
        Ok(())
    }

    pub(in crate::canonical) fn encode_relationship(
        &mut self,
        relationship: &RelRecord,
        mut spills: Option<&mut PropertySpillWriter>,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        self.bind_attempt(work)?;
        if let Some(spills) = spills.as_mut() {
            spills.bind_checkpoint_attempt(work);
        }
        self.select_record(relationship.id.0, CanonicalSegmentKind::Relationships)?;
        if self.payload.is_none() {
            self.payload = Some(if self.source_admits {
                Record::Checkpoint(record::relationship_with_preparation(
                    relationship,
                    spills,
                    &mut self.property_keys,
                    self.config,
                    &mut self.record_plan,
                )?)
            } else {
                Record::Ordinary(encode_relationship_with_property_spills(
                    relationship,
                    spills,
                    Some(&mut self.property_keys),
                )?)
            });
        }
        Ok(())
    }

    pub(in crate::canonical) fn bind_attempt(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        // Observe cancellation at every record. The bindings below are
        // pointer comparisons unless the caller started a new attempt.
        work.checkpoint()?;
        self.work.rebind(work);
        self.accumulator.bind_checkpoint_attempt(work);
        self.descriptor_tree.bind_checkpoint_attempt(work);
        if let Some(dictionary) = self.property_keys.checkpoint.as_mut() {
            dictionary.work.rebind(work);
        }
        Ok(())
    }

    fn select_record(
        &mut self,
        id: u64,
        kind: CanonicalSegmentKind,
    ) -> Result<(), CanonicalSegmentError> {
        if self.finished
            || self.finish_phase != 0
            || self.kind != kind
            || self.record_id.is_some_and(|previous| previous != id)
        {
            return Err(CanonicalSegmentError::Corrupt(
                "retained canonical writer source changed".into(),
            ));
        }
        self.record_id = Some(id);
        Ok(())
    }

    pub(in crate::canonical) fn append_node(
        &mut self,
        node: &NodeRecord,
    ) -> Result<(), CanonicalSegmentError> {
        self.prepare_record_segment()?;
        if self.record_phase == 1 {
            self.accumulator
                .add_node_properties(node, Some(&self.work))?;
            self.record_phase = 2;
        }
        self.append_payload(None)
    }

    pub(in crate::canonical) fn append_relationship(
        &mut self,
        relationship: &RelRecord,
    ) -> Result<(), CanonicalSegmentError> {
        self.prepare_record_segment()?;
        self.record_phase = 2;
        self.append_payload(Some((relationship.source.0, relationship.target.0)))
    }

    fn prepare_record_segment(&mut self) -> Result<(), CanonicalSegmentError> {
        if self.record_phase == 0 {
            let id = self.record_id.expect("selected record");
            let length = self.payload.as_ref().expect("encoded record").len();
            if self.pending_descriptor.is_some()
                || (self.accumulator.would_exceed(id, length) && !self.accumulator.is_empty())
            {
                self.flush_segment()?;
            }
            self.record_phase = 1;
        }
        Ok(())
    }

    fn append_payload(
        &mut self,
        endpoints: Option<(u64, u64)>,
    ) -> Result<(), CanonicalSegmentError> {
        self.accumulator.push(
            self.record_id.expect("selected record"),
            self.payload.as_ref().expect("encoded record"),
            endpoints,
        )?;
        self.payload = None;
        self.record_plan = None;
        self.record_id = None;
        self.record_phase = 0;
        Ok(())
    }

    fn flush_segment(&mut self) -> Result<(), CanonicalSegmentError> {
        if self.pending_descriptor.is_none() {
            self.pending_descriptor = Some(if self.source_admits {
                self.accumulator.flush_retained_with_work_context(
                    &mut self.file,
                    &mut self.artifact_digest,
                    self.artifact_len,
                    &self.work,
                )?
            } else {
                // Ordinary callers retain their consuming flush path.
                let replacement = accumulator::Accumulator::new(
                    self.kind,
                    self.generation,
                    self.segment_id,
                    self.config,
                    self.work.clone(),
                    false,
                );
                std::mem::replace(&mut self.accumulator, replacement).flush_with_work_context(
                    &mut self.file,
                    &mut self.artifact_digest,
                    self.artifact_len,
                    Some(&self.work),
                )?
            });
        }
        let descriptor = self.pending_descriptor.as_ref().expect("completed segment");
        self.descriptor_tree.push(
            descriptor.descriptor_tree_key(),
            descriptor.encode_descriptor_tree_value()?,
        )?;
        self.artifact_len = self.artifact_len.saturating_add(descriptor.length.get());
        match self.kind {
            CanonicalSegmentKind::Nodes => {
                self.node_count = self
                    .node_count
                    .saturating_add(u64::from(descriptor.record_count));
                self.node_segment_count =
                    self.node_segment_count.checked_add(1).ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(
                            "canonical node segment count overflow".into(),
                        )
                    })?;
            }
            CanonicalSegmentKind::Relationships => {
                self.relationship_count = self
                    .relationship_count
                    .saturating_add(u64::from(descriptor.record_count));
                self.relationship_segment_count = self
                    .relationship_segment_count
                    .checked_add(1)
                    .ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(
                            "canonical relationship segment count overflow".into(),
                        )
                    })?;
            }
        }
        self.pending_descriptor = None;
        self.segment_id = self.segment_id.saturating_add(1);
        self.accumulator = accumulator::Accumulator::new(
            self.kind,
            self.generation,
            self.segment_id,
            self.config,
            self.work.clone(),
            self.source_admits,
        );
        Ok(())
    }

    pub(in crate::canonical) fn finish_nodes(&mut self) -> Result<(), CanonicalSegmentError> {
        if self.record_id.is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "cannot finish unfinished canonical node".into(),
            ));
        }
        if self.kind == CanonicalSegmentKind::Relationships {
            return Ok(());
        }
        if !self.accumulator.is_empty() || self.pending_descriptor.is_some() {
            self.flush_segment()?;
        }
        self.kind = CanonicalSegmentKind::Relationships;
        self.accumulator = accumulator::Accumulator::new(
            self.kind,
            self.generation,
            self.segment_id,
            self.config,
            self.work.clone(),
            self.source_admits,
        );
        Ok(())
    }

    pub(in crate::canonical) fn finish(
        mut self,
    ) -> Result<PreparedCanonicalSegmentArtifact, CanonicalSegmentError> {
        let work = self.work.clone();
        self.finish_with_work_context(&work)
    }

    pub(in crate::canonical) fn finish_with_work_context(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> Result<PreparedCanonicalSegmentArtifact, CanonicalSegmentError> {
        if self.finished || self.record_id.is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "cannot finish unfinished or completed canonical writer".into(),
            ));
        }
        self.bind_attempt(work)?;
        if self.finish_phase == 0 {
            if !self.accumulator.is_empty() || self.pending_descriptor.is_some() {
                self.flush_segment()?;
            }
            self.finish_phase = 1;
        }
        if self.finish_phase == 1 {
            self.work.checkpoint()?;
            let _wave = self.work.io_wave()?;
            self.file.sync_all()?;
            self.finish_phase = 2;
        }
        if self.finish_phase == 2 {
            self.prepared_tree = Some(self.descriptor_tree.finish_with_work_context(work)?);
            self.finish_phase = 3;
        }
        work.checkpoint()?;
        let mut descriptor_tree = self
            .prepared_tree
            .take()
            .expect("completed canonical descriptor tree");
        descriptor_tree.bind_checkpoint_attempt(work);
        let artifact_integrity = self.artifact_digest.clone().finish();
        self.finished = true;
        Ok(PreparedCanonicalSegmentArtifact {
            work: work.clone(),
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            artifact_id: ARTIFACT_ID,
            artifact_len: self.artifact_len,
            artifact_digest: ContentDigest(artifact_integrity.crc32c.as_u64()),
            artifact_sha256: artifact_integrity.sha256,
            node_count: self.node_count,
            relationship_count: self.relationship_count,
            node_segment_count: self.node_segment_count,
            relationship_segment_count: self.relationship_segment_count,
            property_keys: std::mem::take(&mut self.property_keys).into_keys(),
            manifest: None,
            manifest_consumed: false,
            descriptor_tree,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::CheckpointWorkProbe;
    use hawdb_core::RuntimeTaskContext;
    use std::sync::atomic::Ordering;

    #[test]
    fn owned_writer_keeps_encoded_payload_and_segment_across_append_attempts() {
        let ceiling = 16 * 1024 * 1024;
        let governor = super::super::tests::governor(ceiling);
        let permit = governor
            .try_admit(
                hawdb_qos::RuntimeWorkRequest::background_maintenance(ceiling)
                    .with_io_wave_slots(1),
            )
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let local = super::super::tests::local();
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let directory = std::env::temp_dir().join(format!(
            "hawdb-owned-canonical-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let writer = CanonicalSegmentWriter {
            source_admits: true,
            config: CanonicalSegmentConfig::default(),
            work: Some(work.clone()),
        };
        let generation = ManifestGeneration(17);
        let tree = create_canonical_descriptor_tree(&path, generation, 17, &work, true).unwrap();
        let mut state = State::create(
            &writer,
            &path,
            CanonicalWriteIdentity {
                generation,
                source_commit_epoch: 17,
            },
            tree,
        )
        .unwrap();
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([("payload".into(), Value::Binary(vec![0x97; 512 * 1024]))]),
        };
        state.encode_node(&node, None, &work).unwrap();
        let original_payload = state.payload.as_ref().unwrap().as_ptr();
        let mut completed = false;
        let mut previous = 0;
        for _ in 0..128 {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(2, Ordering::SeqCst);
            local.set_telemetry_sink(Some(probe.clone()));
            let fresh = CheckpointWorkContext::new(permit.bind_task_context(
                RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
            ))
            .with_scheduler(local.clone());
            state.encode_node(&node, None, &fresh).unwrap();
            assert_eq!(state.payload.as_ref().unwrap().as_ptr(), original_payload);
            let result = state.append_node(&node);
            probe.assert_released(&local);
            assert!(probe.completed.load(Ordering::SeqCst) <= 2);
            match result {
                Ok(()) => {
                    completed = true;
                    break;
                }
                Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))) => {}
                other => panic!("unexpected writer append result: {other:?}"),
            }
            assert!(state.accumulator.records.len() >= previous);
            previous = state.accumulator.records.len();
        }
        assert!(
            completed,
            "owned record append restarted under fixed budget"
        );
        local.set_telemetry_sink(None);
        state.bind_attempt(&work).unwrap();
        state.finish_nodes().unwrap();
        let prepared = state.finish().unwrap();
        assert_eq!(prepared.node_count, 1);
        let manifest = prepared.publish_descriptor_tree().unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(17),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
        drop(reader);
        drop(manifest);
        assert_eq!(super::super::tests::used(&task, ceiling), 0);
        drop(writer);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod finish_tests {
    use super::*;
    use crate::background::CheckpointWorkProbe;
    use hawdb_core::RuntimeTaskContext;
    use std::sync::atomic::Ordering;

    #[test]
    fn owned_canonical_finish_retains_file_and_tree_with_two_units_per_attempt() {
        let ceiling = 16 * 1024 * 1024;
        let governor = super::super::tests::governor(ceiling);
        let permit = governor
            .try_admit(
                hawdb_qos::RuntimeWorkRequest::background_maintenance(ceiling)
                    .with_io_wave_slots(1),
            )
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let local = super::super::tests::local();
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let directory = std::env::temp_dir().join(format!(
            "hawdb-owned-canonical-finish-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let writer = CanonicalSegmentWriter {
            source_admits: true,
            config: CanonicalSegmentConfig::default(),
            work: Some(work.clone()),
        };
        let generation = ManifestGeneration(17);
        let tree = create_canonical_descriptor_tree(&path, generation, 17, &work, true).unwrap();
        let mut state = State::create(
            &writer,
            &path,
            CanonicalWriteIdentity {
                generation,
                source_commit_epoch: 17,
            },
            tree,
        )
        .unwrap();
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([("payload".into(), Value::Binary(vec![0x97; 512 * 1024]))]),
        };
        state.encode_node(&node, None, &work).unwrap();
        state.append_node(&node).unwrap();
        let mut completed = None;
        let mut previous_bytes = fs::metadata(&path).unwrap().len();
        let mut attempts = 0;
        for attempt in 1..=128 {
            attempts = attempt;
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(2, Ordering::SeqCst);
            local.set_telemetry_sink(Some(probe.clone()));
            let fresh = CheckpointWorkContext::new(permit.bind_task_context(
                RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
            ))
            .with_scheduler(local.clone());
            let result = state.finish_with_work_context(&fresh);
            probe.assert_released(&local);
            assert!(probe.completed.load(Ordering::SeqCst) <= 2);
            let bytes = fs::metadata(&path).unwrap().len();
            assert!(bytes >= previous_bytes);
            previous_bytes = bytes;
            match result {
                Ok(prepared) => {
                    completed = Some(prepared);
                    break;
                }
                Err(
                    CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))
                    | CanonicalSegmentError::DescriptorTree(GraphDescriptorTreeError::Work(
                        CheckpointWorkError::Stopped(_),
                    )),
                ) => {}
                other => panic!(
                    "attempt={attempt}: {:?}",
                    other.err().map(|error| error.to_string())
                ),
            }
        }
        let mut prepared = completed.expect("fixed two-unit canonical finish must complete");
        assert!(attempts > 8);
        assert_eq!(prepared.node_count, 1);
        assert_eq!(prepared.relationship_count, 0);
        local.set_telemetry_sink(None);
        assert!(state.encode_node(&node, None, &work).is_err());
        assert!(state.finish_with_work_context(&work).is_err());
        prepared.work = work.clone();
        let manifest = prepared.publish_descriptor_tree().unwrap();
        drop(state);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(17),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
        drop(reader);
        drop(manifest);
        assert_eq!(super::super::tests::used(&task, ceiling), 0);
        drop(writer);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn canonical_publication_retains_key_table_after_tree_commits_and_budget_stops() {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-owned-canonical-publication-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let work = CheckpointWorkContext::default();
        let writer = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default());
        let generation = ManifestGeneration(17);
        let tree = create_canonical_descriptor_tree(&path, generation, 17, &work, false).unwrap();
        let mut state = State::create(
            &writer,
            &path,
            CanonicalWriteIdentity {
                generation,
                source_commit_epoch: 17,
            },
            tree,
        )
        .unwrap();
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::new(),
            properties: (0..32)
                .map(|id| (format!("property-{id:02}"), Value::Int(id)))
                .collect(),
        };
        state.encode_node(&node, None, &work).unwrap();
        state.append_node(&node).unwrap();
        let mut prepared = state.finish().unwrap();
        let scheduler = super::super::tests::local();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(1, Ordering::SeqCst);
        let stopped = probe.context(scheduler.clone());
        assert!(matches!(
            prepared.publish_descriptor_tree_with_work_context(&stopped),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        probe.assert_released(&scheduler);
        assert!(!prepared.manifest_consumed);
        let keys = &prepared.manifest.as_ref().unwrap().manifest.property_keys;
        assert_eq!(keys, &node.properties.keys().cloned().collect::<Vec<_>>());
        let backing = keys.as_ptr();
        let (paths, _) =
            PersistentCanonicalSegmentDescriptorTree::for_artifact(&path, generation).into_parts();
        assert!(paths.page_artifact.exists());
        let root_path = paths.root_manifest;
        let root_before = fs::read(&root_path).unwrap();
        let manifest = prepared
            .publish_descriptor_tree_with_work_context(&work)
            .unwrap();
        assert_eq!(manifest.manifest.property_keys.as_ptr(), backing);
        assert_eq!(fs::read(root_path).unwrap(), root_before);
        assert!(prepared
            .publish_descriptor_tree_with_work_context(&work)
            .is_err());
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(17),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
        drop(reader);
        drop(prepared);
        drop(manifest);
        fs::remove_dir_all(directory).unwrap();
    }
}

/// #1651: the writer re-binds its retained builders to the caller's context
/// before every record. Re-binding the same context is now a pointer
/// comparison; these tests check that units, progress, admissions and
/// cancellation cut points are the same as when every record re-binds to a
/// distinct handle over the same task (what every per-record clone did).
#[cfg(test)]
mod rebind_tests {
    use super::*;
    use crate::background::{CheckpointUnitBatchStats, CheckpointWorkProbe};
    use hawdb_core::RuntimeTaskContext;
    use std::sync::atomic::{AtomicU64, Ordering};

    const RECORDS: u64 = 300;

    #[derive(Debug, PartialEq, Eq)]
    struct Counts {
        records: u64,
        started: usize,
        completed: usize,
        progress: u64,
        stats: CheckpointUnitBatchStats,
        stopped: bool,
    }

    fn node(id: u64) -> NodeRecord {
        NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([
                ("name".into(), Value::String(format!("node-{id}"))),
                ("rank".into(), Value::Int(id as i64)),
                ("blob".into(), Value::Binary(vec![id as u8; 48])),
            ]),
        }
    }

    /// Encode and append `RECORDS` nodes, re-binding to `work` itself or to a
    /// fresh handle over it before each record. Returns the unit counts and
    /// how many builder re-bindings actually replaced a context.
    fn run(fork_per_record: bool, cancel_after: Option<usize>) -> (Counts, u64) {
        let ceiling = 16 * 1024 * 1024;
        let governor = super::super::tests::governor(ceiling);
        let permit = governor
            .try_admit(
                hawdb_qos::RuntimeWorkRequest::background_maintenance(ceiling)
                    .with_io_wave_slots(1),
            )
            .unwrap();
        let local = super::super::tests::local();
        let probe = Arc::new(CheckpointWorkProbe::default());
        local.set_telemetry_sink(Some(probe.clone()));
        let progress = Arc::new(AtomicU64::new(0));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone())
        .with_progress_counter(progress.clone());
        let directory = std::env::temp_dir().join(format!(
            "hawdb-canonical-rebind-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let writer = CanonicalSegmentWriter {
            source_admits: true,
            config: CanonicalSegmentConfig::default(),
            work: Some(work.clone()),
        };
        let generation = ManifestGeneration(17);
        let tree = create_canonical_descriptor_tree(&path, generation, 17, &work, true).unwrap();
        let mut state = State::create(
            &writer,
            &path,
            CanonicalWriteIdentity {
                generation,
                source_commit_epoch: 17,
            },
            tree,
        )
        .unwrap();
        // Setup units are not part of the comparison window.
        work.release_unit_batch();
        let base_started = probe.started.load(Ordering::SeqCst);
        let base_completed = probe.completed.load(Ordering::SeqCst);
        let base_progress = progress.load(Ordering::Relaxed);
        let base_stats = work.unit_batch_stats();
        probe.cancel_after.store(
            cancel_after.map_or(0, |cut| base_completed + cut),
            Ordering::SeqCst,
        );
        let rebinds = crate::background::checkpoint_rebinds();
        let mut records = 0;
        let mut stopped = false;
        for id in 1..=RECORDS {
            let record = node(id);
            let attempt = if fork_per_record {
                work.fork_handle()
            } else {
                work.clone()
            };
            let result = state
                .encode_node(&record, None, &attempt)
                .and_then(|()| state.append_node(&record));
            match result {
                Ok(()) => records += 1,
                Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))) => {
                    stopped = true;
                    break;
                }
                Err(other) => panic!("unexpected append result: {other:?}"),
            }
        }
        let rebinds = crate::background::checkpoint_rebinds() - rebinds;
        work.release_unit_batch();
        probe.assert_released(&local);
        let stats = work.unit_batch_stats();
        let counts = Counts {
            records,
            started: probe.started.load(Ordering::SeqCst) - base_started,
            completed: probe.completed.load(Ordering::SeqCst) - base_completed,
            progress: progress.load(Ordering::Relaxed) - base_progress,
            stats: CheckpointUnitBatchStats {
                units: stats.units - base_stats.units,
                finished_units: stats.finished_units - base_stats.finished_units,
                abandoned_units: stats.abandoned_units - base_stats.abandoned_units,
                admissions: stats.admissions - base_stats.admissions,
                refusals: stats.refusals - base_stats.refusals,
                clock_reads: stats.clock_reads - base_stats.clock_reads,
            },
            stopped,
        };
        local.set_telemetry_sink(None);
        drop(state);
        drop(writer);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        fs::remove_dir_all(directory).unwrap();
        (counts, rebinds)
    }

    #[test]
    fn same_context_rebinding_keeps_unit_progress_and_admission_counts() {
        let (shared, shared_rebinds) = run(false, None);
        let (forked, forked_rebinds) = run(true, None);
        assert_eq!(shared, forked);
        assert_eq!(shared.records, RECORDS);
        assert!(!shared.stopped);
        assert_eq!(shared.started, shared.completed);
        assert_eq!(shared.stats.units, shared.started as u64);
        assert_eq!(shared.stats.abandoned_units, 0);
        assert!(shared.progress > 0 && shared.progress <= shared.stats.finished_units);
        assert!(
            shared.started as u64 > 3 * RECORDS,
            "several units per record"
        );
        // A fresh handle per record replaces the writer's context, its
        // accumulator's, descriptor tree's and key dictionary's: at least
        // four rebinds per record. The shared handle replaces none.
        assert_eq!(shared_rebinds, 0);
        assert!(forked_rebinds >= 4 * RECORDS, "{forked_rebinds}");
    }

    #[test]
    fn same_context_rebinding_keeps_cancellation_cut_points() {
        for cut in [1usize, 2, 7, 63, 64, 65, 129, 500] {
            let (shared, _) = run(false, Some(cut));
            let (forked, _) = run(true, Some(cut));
            assert_eq!(shared, forked, "cut={cut}");
            assert!(shared.stopped, "cut={cut}");
            // Cancellation is visible at the next unit: none starts after it.
            assert_eq!(shared.completed, cut, "cut={cut}");
            assert_eq!(shared.started, cut, "cut={cut}");
            assert!(shared.records < RECORDS);
        }
    }
}
