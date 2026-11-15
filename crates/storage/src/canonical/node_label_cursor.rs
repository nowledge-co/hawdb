// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Snapshot-owned label enumeration without decoding node property values.

use super::checkpoint_scan::{AdmittedDescriptor, CheckpointDescriptorCursor};
use super::*;
use crate::background::CheckpointWorkError;
use crate::cache::SegmentBytes;
use hawdb_core::{RuntimeMemoryPermit, RuntimeTaskContext};

#[derive(Debug, Clone, Copy)]
pub(crate) enum CanonicalLabelAction {
    Segment,
    Record { id: NodeId, labels: usize },
    End,
}

#[derive(Debug)]
struct LabelSegment {
    bytes: SegmentBytes,
    descriptor: AdmittedDescriptor,
    records: SegmentRecordCursor,
    // Data drops before its additional retained-source admission.
    _memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

#[derive(Debug)]
pub(crate) struct CanonicalNodeLabelCursor {
    reader: CanonicalSegmentReader,
    descriptors: CheckpointDescriptorCursor,
    pending_descriptor: Option<AdmittedDescriptor>,
    segment: Option<LabelSegment>,
    ended: bool,
}

impl CanonicalNodeLabelCursor {
    pub(crate) fn new(reader: CanonicalSegmentReader) -> Self {
        let descriptors = CheckpointDescriptorCursor::new(&reader, CanonicalSegmentKind::Nodes);
        Self {
            reader,
            descriptors,
            pending_descriptor: None,
            segment: None,
            ended: false,
        }
    }

    /// Peek only fixed-width framing. The caller charges record/label work
    /// before membership checks or acceptance; a yield retains this position.
    pub(crate) fn action(&mut self) -> Result<CanonicalLabelAction, CanonicalSegmentError> {
        if let Some(segment) = &self.segment {
            if let Some(frame) = segment.records.next(&segment.bytes, &segment.descriptor)? {
                let payload = &segment.bytes[frame.payload_start..frame.payload_end];
                let mut labels = SliceCursor::new(payload);
                let count = labels.read_u32()? as usize;
                labels.read_exact(count.checked_mul(4).ok_or_else(|| {
                    CanonicalSegmentError::Corrupt("canonical label extent overflow".into())
                })?)?;
                return Ok(CanonicalLabelAction::Record {
                    id: NodeId(frame.id),
                    labels: count,
                });
            }
            self.segment = None;
        }
        Ok(if self.ended {
            CanonicalLabelAction::End
        } else {
            CanonicalLabelAction::Segment
        })
    }

    /// One charged descriptor decision, including a pruned segment. Descriptor
    /// continuation never rescans an already completed prefix.
    pub(crate) fn load_segment(
        &mut self,
        labels: &[LabelId],
        task: &RuntimeTaskContext,
    ) -> Result<(), CanonicalSegmentError> {
        let result = self.load_segment_inner(Some(labels), task);
        self.reader.poison_on_physical_failure(&result);
        result
    }

    /// Load the next node segment without a label filter. Descriptor and
    /// segment admission are identical to label-filtered enumeration, but no
    /// Bloom pruning is possible because every actual node record is selected.
    pub(crate) fn load_segment_all(
        &mut self,
        task: &RuntimeTaskContext,
    ) -> Result<(), CanonicalSegmentError> {
        let result = self.load_segment_inner(None, task);
        self.reader.poison_on_physical_failure(&result);
        result
    }

    fn load_segment_inner(
        &mut self,
        labels: Option<&[LabelId]>,
        task: &RuntimeTaskContext,
    ) -> Result<(), CanonicalSegmentError> {
        let work = CheckpointWorkContext::new(task.clone());
        if self.pending_descriptor.is_none() {
            self.pending_descriptor = self.descriptors.next(&self.reader, &work)?;
        }
        let Some(descriptor) = self.pending_descriptor.as_ref() else {
            self.ended = true;
            return Ok(());
        };
        if labels.is_some_and(|labels| {
            !labels.iter().any(|label| {
                descriptor
                    .node_label_bloom
                    .might_contain(u64::from(label.0))
            })
        }) {
            self.pending_descriptor = None;
            return Ok(());
        }
        // Descriptor pages and decoded Blooms were admitted before pruning.
        // Keep their owner in the selected segment until all records are consumed.
        let memory = task
            .reserve_working_memory(
                descriptor
                    .length
                    .get()
                    .saturating_add(std::mem::size_of::<LabelSegment>() as u64),
            )
            .map_err(CheckpointWorkError::Memory)?;
        let _io = task
            .acquire_io_wave(std::num::NonZeroUsize::MIN)
            .map_err(CheckpointWorkError::Io)?;
        let bytes = self.reader.read_segment_with_report(descriptor)?.payload;
        let records =
            SegmentRecordCursor::new(&bytes, self.reader.manifest.generation, descriptor)?;
        self.segment = Some(LabelSegment {
            bytes,
            descriptor: self.pending_descriptor.take().expect("selected descriptor"),
            records,
            _memory: memory,
        });
        Ok(())
    }

    /// The caller has already admitted this record's complete label extent.
    pub(crate) fn accept_record(
        &mut self,
        labels: &[LabelId],
    ) -> Result<bool, CanonicalSegmentError> {
        self.accept_record_inner(Some(labels))
    }

    /// Accept the charged current record without decoding properties or
    /// testing label membership.
    pub(crate) fn accept_record_all(&mut self) -> Result<(), CanonicalSegmentError> {
        let selected = self.accept_record_inner(None)?;
        debug_assert!(selected);
        Ok(())
    }

    fn accept_record_inner(
        &mut self,
        labels: Option<&[LabelId]>,
    ) -> Result<bool, CanonicalSegmentError> {
        let segment = self.segment.as_mut().ok_or_else(|| {
            CanonicalSegmentError::Corrupt("label cursor has no selected segment".into())
        })?;
        let frame = segment
            .records
            .next(&segment.bytes, &segment.descriptor)?
            .ok_or_else(|| CanonicalSegmentError::Corrupt("label cursor has no record".into()))?;
        let payload = &segment.bytes[frame.payload_start..frame.payload_end];
        let matches = match labels {
            Some(labels) => node_payload_has_any_label(payload, labels)?,
            None => true,
        };
        segment.records.accept(frame);
        Ok(matches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (std::path::PathBuf, CanonicalSegmentReader) {
        let path = std::env::temp_dir().join(format!(
            "hawdb-label-segments-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let nodes = (0..96)
            .map(|id| NodeRecord {
                id: NodeId(id * 1_000_000),
                labels: if id == 47 {
                    BTreeSet::from([LabelId(3), LabelId(7)])
                } else {
                    BTreeSet::from([LabelId(1)])
                },
                properties: BTreeMap::from([("payload".into(), Value::String("x".repeat(96)))]),
            })
            .collect::<Vec<_>>();
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(256).unwrap(),
        })
        .write(
            &path,
            ManifestGeneration(5),
            &nodes,
            Vec::<RelRecord>::new(),
        )
        .unwrap();
        assert!(manifest.node_segment_count > 8);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(4096)),
            StoreId(25),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        (path, reader)
    }

    #[test]
    fn label_cursor_prunes_segments_and_resumes_without_property_hydration() {
        let (path, reader) = fixture();
        let count = reader.manifest.node_segment_count;
        let mut cursor = CanonicalNodeLabelCursor::new(reader.clone());
        let mut records = 0;
        let mut descriptors = 0;
        let mut found = Vec::new();
        loop {
            match cursor.action().unwrap() {
                CanonicalLabelAction::Segment => {
                    descriptors += 1;
                    cursor
                        .load_segment(&[LabelId(3), LabelId(7)], &RuntimeTaskContext::default())
                        .unwrap();
                }
                CanonicalLabelAction::Record { id, labels } => {
                    // Peeking repeatedly must not consume a paused record.
                    assert!(
                        matches!(cursor.action().unwrap(), CanonicalLabelAction::Record { id: next, labels: n } if next == id && n == labels)
                    );
                    records += 1;
                    if cursor.accept_record(&[LabelId(3), LabelId(7)]).unwrap() {
                        found.push(id);
                    }
                }
                CanonicalLabelAction::End => break,
            }
        }
        assert_eq!(descriptors, count as usize + 1);
        assert_eq!(found, vec![NodeId(47_000_000)]);
        assert!(
            records < 10,
            "unrelated segments must not enumerate records: {records}"
        );
        let mut absent = CanonicalNodeLabelCursor::new(reader);
        loop {
            match absent.action().unwrap() {
                CanonicalLabelAction::Segment => absent
                    .load_segment(&[LabelId(999)], &RuntimeTaskContext::default())
                    .unwrap(),
                CanonicalLabelAction::Record { .. } => panic!("absent label read a segment"),
                CanonicalLabelAction::End => break,
            }
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn label_descriptor_round_trip_and_old_or_truncated_formats_fail_closed() {
        let (path, reader) = fixture();
        let mut descriptors = CanonicalDescriptorCursor::new(
            CanonicalSegmentKind::Nodes,
            reader.manifest.node_segment_count,
        );
        let descriptor = descriptors.next(&reader).unwrap().unwrap();
        assert!(descriptor.node_label_bloom.might_contain(1));
        let encoded = descriptor.encode_descriptor_tree_value().unwrap();
        assert_eq!(
            CanonicalSegmentDescriptor::decode_descriptor_tree_entry(
                &descriptor.descriptor_tree_key(),
                &encoded
            )
            .unwrap(),
            descriptor
        );
        let mut old = encoded.clone();
        old[8..10].copy_from_slice(&1u16.to_le_bytes());
        assert!(CanonicalSegmentDescriptor::decode_descriptor_tree_entry(
            &descriptor.descriptor_tree_key(),
            &old
        )
        .is_err());
        for len in [0, 8, 80, encoded.len() - 1] {
            assert!(CanonicalSegmentDescriptor::decode_descriptor_tree_entry(
                &descriptor.descriptor_tree_key(),
                &encoded[..len]
            )
            .is_err());
        }
        assert!(
            path.exists(),
            "refused development format must retain evidence"
        );
        std::fs::remove_file(path).unwrap();
    }

    #[derive(Debug)]
    struct RefuseIo;
    impl hawdb_core::RuntimeIoWaveController for RefuseIo {
        fn try_acquire(
            &self,
            slots: std::num::NonZeroUsize,
            _context: &RuntimeTaskContext,
        ) -> Result<Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>, hawdb_core::RuntimeIoWaveError>
        {
            Err(hawdb_core::RuntimeIoWaveError::ReservationExceeded {
                requested_slots: slots.get(),
                reserved_slots: 0,
            })
        }
        fn acquire(
            &self,
            slots: std::num::NonZeroUsize,
            _context: &RuntimeTaskContext,
        ) -> Result<Box<dyn hawdb_core::RuntimeIoWavePermit>, hawdb_core::RuntimeIoWaveError>
        {
            Err(hawdb_core::RuntimeIoWaveError::ReservationExceeded {
                requested_slots: slots.get(),
                reserved_slots: 0,
            })
        }
    }

    #[test]
    fn label_cursor_io_refusal_precedes_descriptor_progress_and_keeps_reader_usable() {
        let (path, reader) = fixture();
        let mut cursor = CanonicalNodeLabelCursor::new(reader.clone());
        let task = RuntimeTaskContext::default().with_io_wave_controller(Arc::new(RefuseIo));
        assert!(matches!(
            cursor.load_segment(&[LabelId(1)], &task),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Io(_)))
        ));
        assert!(cursor.segment.is_none());
        assert!(matches!(
            cursor.action().unwrap(),
            CanonicalLabelAction::Segment
        ));
        cursor
            .load_segment(&[LabelId(1)], &RuntimeTaskContext::default())
            .unwrap();
        assert!(matches!(
            cursor.action().unwrap(),
            CanonicalLabelAction::Record { id: NodeId(0), .. }
        ));
        assert!(reader.get_node(NodeId(0)).unwrap().is_some());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn label_cursor_memory_refusal_keeps_the_canonical_reader_usable() {
        let (path, reader) = fixture();
        let mut cursor = CanonicalNodeLabelCursor::new(reader.clone());
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(1, 0));
        assert!(matches!(
            cursor.load_segment(&[LabelId(1)], &task),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(_)))
        ));
        assert!(cursor.segment.is_none());
        assert!(reader.get_node(NodeId(0)).unwrap().is_some());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn label_cursor_admits_descriptors_before_matching_or_pruning_with_cold_or_warm_cache() {
        for warm in [false, true] {
            for selected in [LabelId(1), LabelId(999)] {
                let (path, reader) = fixture();
                if warm {
                    assert!(reader.get_node(NodeId(0)).unwrap().is_some());
                }
                let mut cursor = CanonicalNodeLabelCursor::new(reader.clone());
                let denied = RuntimeTaskContext::default()
                    .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(1, 0));
                for _ in 0..2 {
                    assert!(matches!(
                        cursor.load_segment(&[selected], &denied),
                        Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(_)))
                    ));
                    assert!(cursor.segment.is_none());
                    assert!(cursor.pending_descriptor.is_none());
                }
                // A denied descriptor must not skip the first segment on retry,
                // even when that segment would be discarded by the Bloom filter.
                cursor
                    .load_segment(&[LabelId(1)], &RuntimeTaskContext::default())
                    .unwrap();
                assert!(matches!(
                    cursor.action().unwrap(),
                    CanonicalLabelAction::Record { id: NodeId(0), .. }
                ));
                assert!(reader.get_node(NodeId(0)).unwrap().is_some());
                std::fs::remove_file(path).unwrap();
            }
        }
    }

    #[test]
    fn label_cursor_exhausted_ledger_and_descriptor_lifetime_cover_positive_and_pruned_paths() {
        use hawdb_core::RuntimeMemoryError;
        use hawdb_qos::{
            IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
            RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
        };
        use std::num::NonZeroUsize;
        const CEILING: u64 = 1 << 20;
        fn used(task: &RuntimeTaskContext) -> u64 {
            match task.reserve_working_memory(CEILING) {
                Err(RuntimeMemoryError::ReservationExceeded {
                    available_bytes, ..
                }) => CEILING - available_bytes,
                result => panic!("expected concrete ledger probe: {result:?}"),
            }
        }
        for warm in [false, true] {
            for selected in [false, true] {
                let (path, reader) = fixture();
                if warm {
                    let mut serving = CanonicalDescriptorCursor::new(
                        CanonicalSegmentKind::Nodes,
                        reader.manifest.node_segment_count,
                    );
                    while serving.next(&reader).unwrap().is_some() {}
                }
                let governor = RuntimeGovernor::new(
                    RuntimeGovernorConfig {
                        memory_budget_bytes: Some(CEILING),
                        background_task_limit: Some(NonZeroUsize::MIN),
                        ..RuntimeGovernorConfig::shared_host()
                    },
                    RuntimeResourceSnapshot::from_parts(
                        RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
                        RuntimeMemorySnapshot::from_limits(
                            Some(1 << 30),
                            Some(1 << 30),
                            None,
                            None,
                            None,
                        ),
                    ),
                    IoConcurrencyBudget::new(2, 1),
                );
                let permit = governor
                    .try_admit(
                        RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1),
                    )
                    .unwrap();
                let task = permit.bind_task_context(RuntimeTaskContext::default());
                let probe = task.reserve_working_memory(1).unwrap();
                let lease_metadata = used(&task) - 1;
                drop(probe);
                let blocker = task
                    .reserve_working_memory(CEILING - lease_metadata)
                    .unwrap();
                let label = if selected { LabelId(1) } else { LabelId(999) };
                let mut cursor = CanonicalNodeLabelCursor::new(reader.clone());
                assert!(matches!(
                    cursor.load_segment(&[label], &task),
                    Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(_)))
                ));
                assert!(cursor.segment.is_none());
                drop(blocker);
                assert_eq!(used(&task), 0);
                cursor.load_segment(&[label], &task).unwrap();
                assert_eq!(
                    governor.snapshot().active_background_io_slots,
                    0,
                    "all descriptor and segment waves must end before returning"
                );
                if selected {
                    let segment = cursor.segment.as_ref().unwrap();
                    let bloom_bytes = (segment.descriptor.node_property_bloom.words.len()
                        + segment.descriptor.node_label_bloom.words.len())
                        * 8;
                    assert!(
                        used(&task) >= segment.bytes.len() as u64 + bloom_bytes as u64,
                        "descriptor and payload must both retain their concrete leases"
                    );
                } else {
                    assert!(cursor.segment.is_none());
                    assert_eq!(used(&task), 0, "pruned descriptor leases must be released");
                }
                drop(task);
                drop(permit);
                assert_eq!(governor.snapshot().active_background_tasks, 0);
                assert_eq!(
                    governor.snapshot().admitted_memory_bytes,
                    if selected { CEILING } else { 0 }
                );
                if selected {
                    assert!(matches!(
                        cursor.action().unwrap(),
                        CanonicalLabelAction::Record { id: NodeId(0), .. }
                    ));
                }
                drop(cursor);
                assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
                assert!(reader.get_node(NodeId(0)).unwrap().is_some());
                std::fs::remove_file(path).unwrap();
            }
        }
    }

    fn memory_refusal_matrix(refuse_io: bool) {
        for warm in [false, true] {
            for label in [LabelId(999), LabelId(1)] {
                let (path, reader) = fixture();
                if warm {
                    let mut serving = CanonicalDescriptorCursor::new(
                        CanonicalSegmentKind::Nodes,
                        reader.manifest.node_segment_count,
                    );
                    while serving.next(&reader).unwrap().is_some() {}
                }
                let mut cursor = CanonicalNodeLabelCursor::new(reader.clone());
                let mut task = RuntimeTaskContext::default()
                    .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(1, 0));
                if refuse_io {
                    task = task.with_io_wave_controller(Arc::new(RefuseIo));
                }
                assert!(matches!(
                    cursor.load_segment(&[label], &task),
                    Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(_)))
                ), "descriptor admission must precede decode/pruning and IO; warm={warm}, label={label:?}, refuse_io={refuse_io}");
                assert!(cursor.segment.is_none());
                // No denied traversal can consume a descriptor. An admitted
                // retry must still observe the first complete source record.
                cursor
                    .load_segment(&[LabelId(1)], &RuntimeTaskContext::default())
                    .unwrap();
                assert!(matches!(
                    cursor.action().unwrap(),
                    CanonicalLabelAction::Record { id: NodeId(0), .. }
                ));
                assert!(reader.get_node(NodeId(0)).unwrap().is_some());
                std::fs::remove_file(path).unwrap();
            }
        }
    }

    #[test]
    fn label_cursor_memory_admission_includes_pruned_cold_and_warm_descriptors() {
        memory_refusal_matrix(false);
    }

    #[test]
    fn label_cursor_memory_admission_precedes_descriptor_io_even_when_serving_cache_is_warm() {
        memory_refusal_matrix(true);
    }

    #[test]
    fn label_cursor_selected_segment_corruption_is_a_storage_error() {
        use std::io::{Seek, SeekFrom, Write};
        let (path, reader) = fixture();
        let mut descriptors = CanonicalDescriptorCursor::new(
            CanonicalSegmentKind::Nodes,
            reader.manifest.node_segment_count,
        );
        let descriptor = descriptors.next(&reader).unwrap().unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(descriptor.offset + 32)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        let mut cursor = CanonicalNodeLabelCursor::new(reader);
        assert!(cursor
            .load_segment(&[LabelId(1)], &RuntimeTaskContext::default())
            .is_err());
        assert!(path.exists());
        std::fs::remove_file(path).unwrap();
    }
}
