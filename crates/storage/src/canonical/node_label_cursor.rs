// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Snapshot-owned label enumeration without decoding node property values.

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
    descriptor: CanonicalSegmentDescriptor,
    records: SegmentRecordCursor,
    // Data drops before its additional retained-source admission.
    _memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

#[derive(Debug)]
pub(crate) struct CanonicalNodeLabelCursor {
    reader: CanonicalSegmentReader,
    descriptors: CanonicalDescriptorCursor,
    segment: Option<LabelSegment>,
    ended: bool,
}

impl CanonicalNodeLabelCursor {
    pub(crate) fn new(reader: CanonicalSegmentReader) -> Self {
        let expected = reader.manifest.node_segment_count;
        Self {
            reader,
            descriptors: CanonicalDescriptorCursor::new(CanonicalSegmentKind::Nodes, expected),
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
        let result = (|| {
            // Both the descriptor decision and an optional segment fetch belong
            // to one sequential wave of the original maintenance admission.
            let _io = task
                .acquire_io_wave(std::num::NonZeroUsize::MIN)
                .map_err(CheckpointWorkError::Io)?;
            self.load_segment_inner(labels, task)
        })();
        self.reader.poison_on_physical_failure(&result);
        result
    }

    fn load_segment_inner(
        &mut self,
        labels: &[LabelId],
        task: &RuntimeTaskContext,
    ) -> Result<(), CanonicalSegmentError> {
        let Some(descriptor) = self.descriptors.next(&self.reader)? else {
            self.ended = true;
            return Ok(());
        };
        if !labels.iter().any(|label| {
            descriptor
                .node_label_bloom
                .might_contain(u64::from(label.0))
        }) {
            return Ok(());
        }
        let descriptor_bytes = [
            &descriptor.source_endpoint_bloom,
            &descriptor.target_endpoint_bloom,
            &descriptor.node_property_bloom,
            &descriptor.node_label_bloom,
        ]
        .iter()
        .fold(
            std::mem::size_of::<LabelSegment>() as u64,
            |bytes, bloom| bytes.saturating_add((bloom.words.len() as u64).saturating_mul(8)),
        );
        let memory = task
            .reserve_working_memory(descriptor.length.get().saturating_add(descriptor_bytes))
            .map_err(CheckpointWorkError::Memory)?;
        let bytes = self.reader.read_segment_with_report(&descriptor)?.payload;
        let records =
            SegmentRecordCursor::new(&bytes, self.reader.manifest.generation, &descriptor)?;
        self.segment = Some(LabelSegment {
            bytes,
            descriptor,
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
        let segment = self.segment.as_mut().ok_or_else(|| {
            CanonicalSegmentError::Corrupt("label cursor has no selected segment".into())
        })?;
        let frame = segment
            .records
            .next(&segment.bytes, &segment.descriptor)?
            .ok_or_else(|| CanonicalSegmentError::Corrupt("label cursor has no record".into()))?;
        let payload = &segment.bytes[frame.payload_start..frame.payload_end];
        let mut input = SliceCursor::new(payload);
        let count = input.read_u32()?;
        let mut matches = false;
        for _ in 0..count {
            matches |= labels.binary_search(&LabelId(input.read_u32()?)).is_ok();
        }
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
