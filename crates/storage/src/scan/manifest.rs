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

use super::{
    CandidateCursor, PruningDecision, ScanPredicate, SegmentPruner, SegmentReadRange,
    SegmentSummary,
};
use crate::ContentDigest;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroU64;

/// One independently readable payload range owned by a persisted scan segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentPayloadRange {
    pub artifact_id: u64,
    pub offset: u64,
    pub length: NonZeroU64,
    pub checksum: u64,
}

/// Summary and payload location for one immutable scan segment.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedScanSegment {
    pub summary: SegmentSummary,
    pub payload_range: SegmentPayloadRange,
}

/// A manifest published for exactly one canonical graph epoch.
///
/// Callers must not use this as an eventually-consistent index. A scan may use
/// it only while the reader is pinned to `graph_epoch`; a newer or older graph
/// snapshot must receive an explicit fallback decision.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanSegmentManifest {
    graph_epoch: u64,
    segments: Vec<PersistedScanSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanSegmentManifestError {
    SegmentIdOutOfOrder { expected: u64, actual: u64 },
    OverlappingPayloadRange { artifact_id: u64 },
    PayloadRangeOverflow { segment_id: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanSegmentFallback {
    NoManifest,
    SnapshotEpochMismatch {
        reader_epoch: u64,
        manifest_epoch: u64,
    },
}

#[derive(Debug, Clone)]
pub enum ScanSegmentAccessPlan {
    Read(ReadySegmentScan),
    Fallback(ScanSegmentFallback),
}

#[derive(Debug, Clone)]
pub struct ReadySegmentScan {
    pub graph_epoch: u64,
    pub segments: Vec<PlannedScanSegment>,
    pub skipped_segment_count: usize,
}

#[derive(Debug, Clone)]
pub struct PlannedScanSegment {
    pub segment_id: u64,
    pub payload_range: SegmentReadRange,
    pub pruning: PruningDecision,
    /// Exact local row candidates when the summary provides them. `None` means
    /// the payload range must apply the predicate to all of its rows.
    pub candidates: Option<CandidateCursor>,
}

impl ScanSegmentManifest {
    pub fn new(
        graph_epoch: u64,
        segments: Vec<PersistedScanSegment>,
    ) -> Result<Self, ScanSegmentManifestError> {
        validate_segments(&segments)?;
        Ok(Self {
            graph_epoch,
            segments,
        })
    }

    pub const fn graph_epoch(&self) -> u64 {
        self.graph_epoch
    }

    pub fn segments(&self) -> &[PersistedScanSegment] {
        &self.segments
    }

    pub fn plan_scan(&self, reader_epoch: u64, predicate: &ScanPredicate) -> ScanSegmentAccessPlan {
        if reader_epoch != self.graph_epoch {
            return ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch {
                reader_epoch,
                manifest_epoch: self.graph_epoch,
            });
        }

        let mut skipped_segment_count = 0;
        let segments = self
            .segments
            .iter()
            .filter_map(|segment| {
                let pruning = SegmentPruner::new(&segment.summary).evaluate(predicate);
                if !pruning.should_open_payload() {
                    skipped_segment_count += 1;
                    return None;
                }
                let range = &segment.payload_range;
                Some(PlannedScanSegment {
                    segment_id: segment.summary.segment_id,
                    payload_range: SegmentReadRange::new(
                        range.artifact_id,
                        segment.summary.segment_id,
                        range.offset,
                        range.length,
                    )
                    .with_content_digest(ContentDigest(range.checksum)),
                    candidates: CandidateCursor::from_decision(pruning.clone()),
                    pruning,
                })
            })
            .collect();
        ScanSegmentAccessPlan::Read(ReadySegmentScan {
            graph_epoch: self.graph_epoch,
            segments,
            skipped_segment_count,
        })
    }
}

impl ScanSegmentAccessPlan {
    pub const fn fallback(reason: ScanSegmentFallback) -> Self {
        Self::Fallback(reason)
    }
}

impl Display for ScanSegmentManifestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::SegmentIdOutOfOrder { expected, actual } => {
                write!(
                    formatter,
                    "scan segment manifest expected id {expected}, got {actual}"
                )
            }
            Self::OverlappingPayloadRange { artifact_id } => {
                write!(
                    formatter,
                    "scan segment manifest has overlapping artifact {artifact_id}"
                )
            }
            Self::PayloadRangeOverflow { segment_id } => {
                write!(
                    formatter,
                    "scan segment {segment_id} payload range overflows"
                )
            }
        }
    }
}

impl Error for ScanSegmentManifestError {}

fn validate_segments(segments: &[PersistedScanSegment]) -> Result<(), ScanSegmentManifestError> {
    for (expected, segment) in segments.iter().enumerate() {
        let expected = expected as u64;
        if segment.summary.segment_id != expected {
            return Err(ScanSegmentManifestError::SegmentIdOutOfOrder {
                expected,
                actual: segment.summary.segment_id,
            });
        }
        segment
            .payload_range
            .offset
            .checked_add(segment.payload_range.length.get())
            .ok_or(ScanSegmentManifestError::PayloadRangeOverflow {
                segment_id: segment.summary.segment_id,
            })?;
    }

    let mut ranges = segments
        .iter()
        .map(|segment| {
            let range = segment.payload_range;
            (range.artifact_id, range.offset, range.length.get())
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|range| (range.0, range.1));
    for pair in ranges.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        if previous.0 == current.0 && previous.1.saturating_add(previous.2) > current.1 {
            return Err(ScanSegmentManifestError::OverlappingPayloadRange {
                artifact_id: current.0,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldSummary;
    use hawdb_core::Value;
    use roaring::RoaringTreemap;

    fn segment(id: u64, offset: u64, summary: SegmentSummary) -> PersistedScanSegment {
        PersistedScanSegment {
            summary,
            payload_range: SegmentPayloadRange {
                artifact_id: 7,
                offset,
                length: NonZeroU64::new(32).unwrap(),
                checksum: id,
            },
        }
    }

    #[test]
    fn stale_manifest_returns_fallback_without_payload_ranges() {
        let manifest =
            ScanSegmentManifest::new(4, vec![segment(0, 0, SegmentSummary::new(0, 1))]).unwrap();

        assert!(matches!(
            manifest.plan_scan(5, &ScanPredicate::True),
            ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch {
                reader_epoch: 5,
                manifest_epoch: 4,
            })
        ));
    }

    #[test]
    fn matching_epoch_prunes_before_emitting_payload_range() {
        let mut skipped_summary = SegmentSummary::new(0, 2);
        skipped_summary.insert_field(
            "lifecycle_state",
            FieldSummary::new(2).with_enum_dictionary(crate::EnumDictionaryStats::complete([
                Value::String("indexed".to_string()),
            ])),
        );
        let mut selected_summary = SegmentSummary::new(1, 2);
        selected_summary.insert_field(
            "lifecycle_state",
            FieldSummary::new(2).with_enum_dictionary(crate::EnumDictionaryStats::complete([
                Value::String("parsed".to_string()),
            ])),
        );
        let manifest = ScanSegmentManifest::new(
            6,
            vec![
                segment(0, 0, skipped_summary),
                segment(1, 32, selected_summary),
            ],
        )
        .unwrap();

        let ScanSegmentAccessPlan::Read(plan) = manifest.plan_scan(
            6,
            &ScanPredicate::Eq {
                property: "lifecycle_state".to_string(),
                value: Value::String("parsed".to_string()),
            },
        ) else {
            panic!("matching epoch must use the manifest");
        };
        assert_eq!(plan.skipped_segment_count, 1);
        assert_eq!(plan.segments.len(), 1);
        assert_eq!(plan.segments[0].segment_id, 1);
        assert_eq!(plan.segments[0].payload_range.offset, 32);
    }

    #[test]
    fn exact_summary_keeps_a_bounded_candidate_cursor() {
        let mut rows = RoaringTreemap::new();
        rows.insert(3);
        let mut summary = SegmentSummary::new(0, 5);
        summary.insert_field(
            "id",
            FieldSummary::new(5).with_exact_value(Value::String("source-3".to_string()), rows),
        );
        let manifest = ScanSegmentManifest::new(8, vec![segment(0, 0, summary)]).unwrap();

        let ScanSegmentAccessPlan::Read(mut plan) = manifest.plan_scan(
            8,
            &ScanPredicate::Eq {
                property: "id".to_string(),
                value: Value::String("source-3".to_string()),
            },
        ) else {
            panic!("matching epoch must use the manifest");
        };
        let mut cursor = plan.segments.pop().unwrap().candidates.unwrap();
        assert_eq!(cursor.next_batch(1), vec![3]);
        assert!(cursor.is_empty());
    }

    #[test]
    fn overlapping_ranges_are_rejected_before_publish() {
        let error = ScanSegmentManifest::new(
            1,
            vec![
                segment(0, 0, SegmentSummary::new(0, 1)),
                segment(1, 16, SegmentSummary::new(1, 1)),
            ],
        )
        .unwrap_err();
        assert_eq!(
            error,
            ScanSegmentManifestError::OverlappingPayloadRange { artifact_id: 7 }
        );
    }
}
