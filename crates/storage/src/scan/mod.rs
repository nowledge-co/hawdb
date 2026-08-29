use skein_core::{LabelId, RelTypeId};

mod cursor;
mod manifest;
mod predicate;
mod reader;
mod scheduler;
mod summary;

pub use cursor::CandidateCursor;
pub use manifest::{
    PersistedScanSegment, PlannedScanSegment, ReadySegmentScan, ScanSegmentAccessPlan,
    ScanSegmentFallback, ScanSegmentManifest, ScanSegmentManifestError, SegmentPayloadRange,
};
pub use predicate::{PruningDecision, PruningReason, RangeBound, ScanPredicate, SegmentPruner};
pub use reader::{
    FileSegmentRangeReader, SegmentRangeRead, SegmentRangeReader, SegmentReadControl,
    SegmentReadError, SegmentReadExecutionError, SegmentReadExecutionReport, SegmentReadExecutor,
    SegmentReadPayload, SegmentReadPool, SegmentReadPoolError,
};
pub use scheduler::{SegmentReadRange, SegmentReadSchedule, SegmentReadScheduler, SegmentReadWave};
pub use summary::{
    DateTimeMinMax, EnumDictionaryStats, FieldSummary, MembershipFilterSummary, MembershipVerdict,
    NumericMinMax, ScanScalar, SegmentSummary,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPruningStrategy {
    FullLabelScan,
    ExactCount,
    Empty,
    IdEq,
    IdIn,
    IdRange,
    PropertyEq { property: String },
    PropertyNotEq { property: String },
    PropertyMissingOrNull { property: String },
    PropertyExists { property: String },
    PropertyDefaultIfNullEq { property: String },
    PropertyDefaultIfNullNotEq { property: String },
    PropertyIn { property: String },
    CompositePropertyEq { properties: Vec<String> },
    CompositePropertyRange { properties: Vec<String> },
    PropertyRange { property: String },
    FullText { property: String },
    OrUnion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPruningTargetKind {
    Node,
    Relationship,
}

impl ScanPruningTargetKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Relationship => "relationship",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPruningReport {
    pub target_kind: ScanPruningTargetKind,
    pub label_id: Option<LabelId>,
    pub rel_type_id: Option<RelTypeId>,
    pub strategy: ScanPruningStrategy,
    pub pruned: bool,
    pub exact_empty: bool,
    pub candidate_count_before_pruning: usize,
    pub pruned_candidate_count: usize,
    pub candidate_count_before_filter: usize,
    pub output_count: usize,
    pub filtered_out_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringTreemap;
    use skein_core::Value;

    #[test]
    fn target_kind_has_stable_storage_name() {
        assert_eq!(ScanPruningTargetKind::Node.as_str(), "node");
        assert_eq!(ScanPruningTargetKind::Relationship.as_str(), "relationship");
    }

    #[test]
    fn segment_pruning_skips_disjoint_numeric_range_before_payload_open() {
        let mut summary = SegmentSummary::new(7, 100);
        summary.insert_field(
            "importance",
            FieldSummary::new(100)
                .with_numeric_min_max(0.1, 0.8)
                .unwrap(),
        );

        let decision = SegmentPruner::new(&summary).evaluate(&ScanPredicate::Range {
            property: "importance".to_string(),
            lower: Some(RangeBound::exclusive(Value::Float(0.9))),
            upper: None,
        });

        assert!(matches!(
            decision,
            PruningDecision::Skip {
                reason: PruningReason::RangeDisjoint
            }
        ));
        assert!(!decision.should_open_payload());
    }

    #[test]
    fn exact_unique_lookup_produces_bounded_candidate_cursor() {
        let mut ids = RoaringTreemap::new();
        ids.insert(42);
        let mut summary = SegmentSummary::new(8, 100);
        summary.insert_field(
            "id",
            FieldSummary::new(100).with_exact_value(Value::String("memory-42".to_string()), ids),
        );

        let decision = SegmentPruner::new(&summary).evaluate(&ScanPredicate::Eq {
            property: "id".to_string(),
            value: Value::String("memory-42".to_string()),
        });
        let mut cursor = CandidateCursor::from_decision(decision).unwrap();

        assert_eq!(cursor.remaining(), 1);
        assert_eq!(cursor.next_batch(1), vec![42]);
        assert!(cursor.is_empty());
    }

    #[test]
    fn bloom_negative_skips_while_positive_remains_a_payload_read() {
        let field = FieldSummary::new(2).with_bloom_values(
            [
                Value::String("active".to_string()),
                Value::String("done".to_string()),
            ],
            128,
            3,
        );
        let mut summary = SegmentSummary::new(9, 2);
        summary.insert_field("status", field);
        let pruner = SegmentPruner::new(&summary);

        let absent = pruner.evaluate(&ScanPredicate::Eq {
            property: "status".to_string(),
            value: Value::String("impossible-value".to_string()),
        });
        assert!(matches!(
            absent,
            PruningDecision::Skip {
                reason: PruningReason::MembershipNegative
            }
        ));

        let present = pruner.evaluate(&ScanPredicate::Eq {
            property: "status".to_string(),
            value: Value::String("active".to_string()),
        });
        assert!(present.should_open_payload());
    }

    #[test]
    fn null_and_missing_counters_prune_impossible_predicates() {
        let mut summary = SegmentSummary::new(10, 5);
        summary.insert_field(
            "metadata",
            FieldSummary::new(5).with_presence_counts(5, 0, 0).unwrap(),
        );
        let pruner = SegmentPruner::new(&summary);

        assert!(!pruner
            .evaluate(&ScanPredicate::IsNull {
                property: "metadata".to_string(),
            })
            .should_open_payload());
        assert!(!pruner
            .evaluate(&ScanPredicate::IsMissing {
                property: "metadata".to_string(),
            })
            .should_open_payload());
    }
}
