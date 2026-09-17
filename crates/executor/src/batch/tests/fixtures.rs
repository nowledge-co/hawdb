use skein_core::{RelationshipDirection, Value};
use skein_plan::*;
use std::collections::BTreeMap;

// Explicit expected capability values are independent of the production classifier.
pub(super) fn operators() -> Vec<(PhysicalPlan, bool)> {
    vec![
        (
            PhysicalPlan::CreateNodeLabel {
                label: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateRelationshipType {
                rel_type: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateNodeTable {
                name: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateRelationshipTable {
                name: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateProperty {
                table_kind: skein_plan::SchemaTableKind::Node,
                table: String::new(),
                property: String::new(),
                value_type: skein_plan::SchemaPropertyType::Any,
                nullable: false,
            },
            false,
        ),
        (
            PhysicalPlan::AlterTableState {
                table_kind: skein_plan::SchemaTableKind::Node,
                table: String::new(),
                state: skein_plan::SchemaObjectState::Public,
            },
            false,
        ),
        (
            PhysicalPlan::AlterPropertyState {
                table_kind: skein_plan::SchemaTableKind::Node,
                table: String::new(),
                property: String::new(),
                state: skein_plan::SchemaObjectState::Public,
            },
            false,
        ),
        (
            PhysicalPlan::CreateIndex {
                label: String::new(),
                property: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateCompositeIndex {
                label: String::new(),
                properties: Vec::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateRangeIndex {
                label: String::new(),
                property: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateFullTextIndex {
                label: String::new(),
                property: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateUniqueConstraint {
                label: String::new(),
                property: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateNodePropertyExistsConstraint {
                label: String::new(),
                property: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateRelationshipUniqueConstraint {
                rel_type: String::new(),
                property: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint {
                rel_type: String::new(),
                property: String::new(),
            },
            false,
        ),
        (
            PhysicalPlan::ProjectGraph {
                name: String::new(),
                node_labels: Vec::new(),
                rel_types: Vec::new(),
            },
            false,
        ),
        (
            PhysicalPlan::GraphAlgorithm {
                algorithm: GraphAlgorithmKind::PageRank,
                graph_name: String::new(),
                options: skein_plan::GraphAlgorithmOptions {
                    damping: None,
                    max_iterations: Some(1),
                    max_levels: Some(1),
                },
                score_column: String::new(),
                node_visibility_predicate: None,
            },
            true,
        ),
        (
            PhysicalPlan::VectorSeedScan {
                embedding_parameter: String::new(),
                output_external_id: false,
                metadata_filters: BTreeMap::new(),
                vector_plan: skein_plan::VectorPhysicalPlan::Filter { fields: Vec::new() },
                resource_profile: skein_plan::VectorExecutionResourceProfile {
                    priority: 0,
                    max_parallelism: 1,
                    max_working_memory_bytes: None,
                },
            },
            true,
        ),
        (
            PhysicalPlan::CreateNode {
                label: String::new(),
                properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::MergeNode {
                label: String::new(),
                match_properties: BTreeMap::new(),
                on_create_properties: BTreeMap::new(),
                on_match_assignments: Vec::new(),
                post_merge_assignments: Vec::new(),
            },
            false,
        ),
        (
            PhysicalPlan::MergeRelationship {
                source_label: String::new(),
                source_properties: BTreeMap::new(),
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::MergeMatchedRelationship {
                source_label: String::new(),
                source_properties: BTreeMap::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
                rel_type: String::new(),
                rel_match_properties: BTreeMap::new(),
                on_create_properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label: String::new(),
                source_properties: BTreeMap::new(),
                old_rel_type: String::new(),
                old_rel_properties: BTreeMap::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
                new_rel_type: String::new(),
                new_rel_match_properties: BTreeMap::new(),
                on_create_properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::MergeRelationshipToMatchedTarget {
                source_label: String::new(),
                source_properties: BTreeMap::new(),
                old_rel_type: String::new(),
                old_rel_properties: BTreeMap::new(),
                old_target_label: String::new(),
                old_target_properties: BTreeMap::new(),
                new_target_label: String::new(),
                new_target_properties: BTreeMap::new(),
                new_rel_type: String::new(),
                new_rel_match_properties: BTreeMap::new(),
                on_create_properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::MergeRelationshipFromMatchedTarget {
                old_source_label: String::new(),
                old_source_properties: BTreeMap::new(),
                old_rel_type: String::new(),
                old_rel_properties: BTreeMap::new(),
                old_target_label: String::new(),
                old_target_properties: BTreeMap::new(),
                new_source_label: String::new(),
                new_source_properties: BTreeMap::new(),
                new_rel_type: String::new(),
                new_rel_match_properties: BTreeMap::new(),
                on_create_properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::CreateMatchedRelationship {
                source_label: String::new(),
                source_properties: BTreeMap::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::SetNodeProperty {
                variable: String::new(),
                label: String::new(),
                predicate: None,
                property: String::new(),
                value: SetValue::Value(Value::Null),
            },
            false,
        ),
        (
            PhysicalPlan::SetNodeProperties {
                variable: String::new(),
                label: String::new(),
                predicate: None,
                assignments: Vec::new(),
            },
            false,
        ),
        (
            PhysicalPlan::SetNodePropertiesReturn {
                variable: String::new(),
                label: String::new(),
                predicate: None,
                assignments: Vec::new(),
                returns: SetNodePropertiesReturnMode::Project(Vec::new()),
            },
            false,
        ),
        (
            PhysicalPlan::SetRelationshipProperty {
                source_variable: String::new(),
                source_label: String::new(),
                predicate: None,
                rel_variable: String::new(),
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                rel_predicate: None,
                target_variable: String::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
                property: String::new(),
                value: Value::Null,
            },
            false,
        ),
        (
            PhysicalPlan::SetRelationshipProperties {
                source_variable: String::new(),
                source_label: String::new(),
                predicate: None,
                rel_variable: String::new(),
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                rel_predicate: None,
                target_variable: String::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
                assignments: Vec::new(),
            },
            false,
        ),
        (
            PhysicalPlan::DeleteNode {
                variable: String::new(),
                label: String::new(),
                predicate: None,
                detach: false,
            },
            false,
        ),
        (
            PhysicalPlan::DeleteRelationship {
                source_variable: String::new(),
                source_label: String::new(),
                predicate: None,
                rel_variable: String::new(),
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                rel_predicate: None,
                target_variable: String::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
            },
            false,
        ),
        (
            PhysicalPlan::DeleteRelationshipTargetNodes {
                source_variable: String::new(),
                source_label: String::new(),
                source_predicate: None,
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                target_variable: String::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
                detach: false,
            },
            false,
        ),
        (
            PhysicalPlan::CreateRelationship {
                source_label: String::new(),
                source_properties: BTreeMap::new(),
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                target_label: String::new(),
                target_properties: BTreeMap::new(),
            },
            false,
        ),
        (PhysicalPlan::EmptyExec, true),
        (
            PhysicalPlan::SeqNodeScan {
                variable: String::new(),
                label: String::new(),
            },
            true,
        ),
        (
            PhysicalPlan::NodeProjectionScanExec {
                variable: String::new(),
                label: String::new(),
                access: skein_plan::NodeProjectionAccess::LabelScan,
                required_properties: Vec::new(),
                predicate: None,
                items: Vec::new(),
            },
            true,
        ),
        (
            PhysicalPlan::SourceSegmentScan {
                variable: String::new(),
                predicate: Predicate::ConstantBool(true),
            },
            true,
        ),
        (
            PhysicalPlan::HashJoinExec {
                left_key: HashJoinKey {
                    variable: "a".into(),
                    property: "key".into(),
                },
                right_key: HashJoinKey {
                    variable: "b".into(),
                    property: "key".into(),
                },
                left: Box::new(PhysicalPlan::EmptyExec),
                right: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::NodeCartesianProductExec {
                left: Box::new(PhysicalPlan::EmptyExec),
                right: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::NodeColumnLookupExec {
                variable: String::new(),
                label: String::new(),
                property: String::new(),
                column: String::new(),
                optional: false,
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::IndexNodeSeek {
                variable: String::new(),
                label: String::new(),
                property: String::new(),
                value: Value::Null,
            },
            true,
        ),
        (
            PhysicalPlan::IndexNodeMultiSeek {
                variable: String::new(),
                label: String::new(),
                property: String::new(),
                values: Vec::new(),
            },
            true,
        ),
        (
            PhysicalPlan::IndexNodeUnionSeek {
                variable: String::new(),
                label: String::new(),
                branches: Vec::new(),
            },
            true,
        ),
        (
            PhysicalPlan::IndexNodeCompositeSeek {
                variable: String::new(),
                label: String::new(),
                predicates: Vec::new(),
            },
            true,
        ),
        (
            PhysicalPlan::IndexNodeCompositeRangeSeek {
                variable: String::new(),
                label: String::new(),
                seek: skein_plan::CompositeRangeSeek {
                    index_properties: vec!["id".to_string()],
                    equality_prefix: Vec::new(),
                    range_property: "id".to_string(),
                    lower: None,
                    upper: None,
                },
            },
            true,
        ),
        (
            PhysicalPlan::IndexNodeRangeSeek {
                variable: String::new(),
                label: String::new(),
                property: String::new(),
                lower: None,
                upper: None,
            },
            true,
        ),
        (
            PhysicalPlan::IndexNodeTextSeek {
                variable: String::new(),
                label: String::new(),
                property: String::new(),
                query: String::new(),
            },
            true,
        ),
        (
            PhysicalPlan::AdjacencyExpandExec {
                source_variable: String::new(),
                source_label: String::new(),
                rel_variable: None,
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: String::new(),
                target_label: String::new(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                graph_budget: None,
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::AdjacencyExistsExec {
                source_variable: String::new(),
                rel_type: String::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: String::new(),
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::OptionalDegreeExec {
                source_variable: String::new(),
                rel_type: String::new(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_label: String::new(),
                target_properties: BTreeMap::new(),
                alias: String::new(),
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::OptionalRelationshipCountSumExec {
                variable: String::new(),
                label: String::new(),
                properties: BTreeMap::new(),
                legs: Vec::new(),
                output: String::new(),
            },
            true,
        ),
        (
            PhysicalPlan::NodeCountExec {
                label: String::new(),
                output: String::new(),
            },
            true,
        ),
        (
            PhysicalPlan::RelationshipCountExec {
                rel_type: String::new(),
                output: String::new(),
            },
            true,
        ),
        (
            PhysicalPlan::ThreadRepairStatsExec {
                label: String::new(),
                identity_label: String::new(),
                identity_ref_property: String::new(),
                thread_id_property: String::new(),
                message_rel_type: String::new(),
                message_label: String::new(),
                memory_rel_type: String::new(),
                memory_label: String::new(),
            },
            true,
        ),
        (
            PhysicalPlan::ShortestPathExec {
                source_variable: String::new(),
                source_label: String::new(),
                source_id: Value::Null,
                source_visibility_predicate: None,
                rel_type: String::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: String::new(),
                target_label: String::new(),
                target_id: Value::Null,
                target_visibility_predicate: None,
                min_hops: 1,
                max_hops: 1,
                returns: Vec::new(),
            },
            true,
        ),
        (
            PhysicalPlan::FilterExec {
                predicate: Predicate::ConstantBool(true),
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::ProjectExec {
                items: Vec::new(),
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::AggregateExec {
                group_keys: Vec::new(),
                items: Vec::new(),
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::DistinctExec {
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::SortExec {
                items: Vec::new(),
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::TopNExec {
                items: Vec::new(),
                offset: 1,
                limit: 1,
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
        (
            PhysicalPlan::LimitExec {
                offset: 1,
                limit: None,
                input: Box::new(PhysicalPlan::EmptyExec),
            },
            true,
        ),
    ]
}
