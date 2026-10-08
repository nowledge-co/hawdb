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

//! Logical optimizer operator metadata.

use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalPlanKind {
    CreateSchema,
    AlterSchema,
    CreateIndex,
    CreateConstraint,
    CreateNode,
    MergeNode,
    MergeRelationship,
    SetProperty,
    Delete,
    NodeScan,
    NodeSeek,
    RelationshipExpand,
    OptionalRelationshipExpand,
    ShortestPath,
    Filter,
    Project,
    Aggregate,
    Distinct,
    Sort,
    Limit,
    ProcedureCall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalPlanClass {
    Schema,
    Mutation,
    Access,
    Traversal,
    Relational,
    Procedure,
}

pub trait LogicalPlanNode {
    fn logical_kind(&self) -> LogicalPlanKind;

    fn logical_class(&self) -> LogicalPlanClass {
        self.logical_kind().class()
    }
}

impl LogicalPlanKind {
    pub fn all() -> &'static [Self] {
        const ALL: &[LogicalPlanKind] = &[
            LogicalPlanKind::CreateSchema,
            LogicalPlanKind::AlterSchema,
            LogicalPlanKind::CreateIndex,
            LogicalPlanKind::CreateConstraint,
            LogicalPlanKind::CreateNode,
            LogicalPlanKind::MergeNode,
            LogicalPlanKind::MergeRelationship,
            LogicalPlanKind::SetProperty,
            LogicalPlanKind::Delete,
            LogicalPlanKind::NodeScan,
            LogicalPlanKind::NodeSeek,
            LogicalPlanKind::RelationshipExpand,
            LogicalPlanKind::OptionalRelationshipExpand,
            LogicalPlanKind::ShortestPath,
            LogicalPlanKind::Filter,
            LogicalPlanKind::Project,
            LogicalPlanKind::Aggregate,
            LogicalPlanKind::Distinct,
            LogicalPlanKind::Sort,
            LogicalPlanKind::Limit,
            LogicalPlanKind::ProcedureCall,
        ];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LogicalPlanKind::CreateSchema => "CreateSchema",
            LogicalPlanKind::AlterSchema => "AlterSchema",
            LogicalPlanKind::CreateIndex => "CreateIndex",
            LogicalPlanKind::CreateConstraint => "CreateConstraint",
            LogicalPlanKind::CreateNode => "CreateNode",
            LogicalPlanKind::MergeNode => "MergeNode",
            LogicalPlanKind::MergeRelationship => "MergeRelationship",
            LogicalPlanKind::SetProperty => "SetProperty",
            LogicalPlanKind::Delete => "Delete",
            LogicalPlanKind::NodeScan => "NodeScan",
            LogicalPlanKind::NodeSeek => "NodeSeek",
            LogicalPlanKind::RelationshipExpand => "RelationshipExpand",
            LogicalPlanKind::OptionalRelationshipExpand => "OptionalRelationshipExpand",
            LogicalPlanKind::ShortestPath => "ShortestPath",
            LogicalPlanKind::Filter => "Filter",
            LogicalPlanKind::Project => "Project",
            LogicalPlanKind::Aggregate => "Aggregate",
            LogicalPlanKind::Distinct => "Distinct",
            LogicalPlanKind::Sort => "Sort",
            LogicalPlanKind::Limit => "Limit",
            LogicalPlanKind::ProcedureCall => "ProcedureCall",
        }
    }

    pub fn class(self) -> LogicalPlanClass {
        match self {
            LogicalPlanKind::CreateSchema
            | LogicalPlanKind::AlterSchema
            | LogicalPlanKind::CreateIndex
            | LogicalPlanKind::CreateConstraint => LogicalPlanClass::Schema,
            LogicalPlanKind::CreateNode
            | LogicalPlanKind::MergeNode
            | LogicalPlanKind::MergeRelationship
            | LogicalPlanKind::SetProperty
            | LogicalPlanKind::Delete => LogicalPlanClass::Mutation,
            LogicalPlanKind::NodeScan | LogicalPlanKind::NodeSeek => LogicalPlanClass::Access,
            LogicalPlanKind::RelationshipExpand
            | LogicalPlanKind::OptionalRelationshipExpand
            | LogicalPlanKind::ShortestPath => LogicalPlanClass::Traversal,
            LogicalPlanKind::Filter
            | LogicalPlanKind::Project
            | LogicalPlanKind::Aggregate
            | LogicalPlanKind::Distinct
            | LogicalPlanKind::Sort
            | LogicalPlanKind::Limit => LogicalPlanClass::Relational,
            LogicalPlanKind::ProcedureCall => LogicalPlanClass::Procedure,
        }
    }
}

impl LogicalPlanClass {
    pub fn all() -> &'static [Self] {
        const ALL: &[LogicalPlanClass] = &[
            LogicalPlanClass::Schema,
            LogicalPlanClass::Mutation,
            LogicalPlanClass::Access,
            LogicalPlanClass::Traversal,
            LogicalPlanClass::Relational,
            LogicalPlanClass::Procedure,
        ];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LogicalPlanClass::Schema => "schema",
            LogicalPlanClass::Mutation => "mutation",
            LogicalPlanClass::Access => "access",
            LogicalPlanClass::Traversal => "traversal",
            LogicalPlanClass::Relational => "relational",
            LogicalPlanClass::Procedure => "procedure",
        }
    }
}

impl FromStr for LogicalPlanKind {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|kind| kind.as_str() == value)
            .ok_or("unknown logical plan kind")
    }
}

impl FromStr for LogicalPlanClass {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|class| class.as_str() == value)
            .ok_or("unknown logical plan class")
    }
}

#[cfg(test)]
mod tests {
    use super::{LogicalPlanClass, LogicalPlanKind, LogicalPlanNode};

    #[test]
    fn logical_plan_kind_exposes_stable_strings_and_classes() {
        assert_eq!(LogicalPlanKind::NodeSeek.as_str(), "NodeSeek");
        assert_eq!(LogicalPlanKind::NodeSeek.class(), LogicalPlanClass::Access);
        assert_eq!(
            LogicalPlanKind::RelationshipExpand.class(),
            LogicalPlanClass::Traversal
        );
        assert_eq!(
            LogicalPlanKind::Project.class(),
            LogicalPlanClass::Relational
        );
    }

    #[test]
    fn logical_plan_kind_strings_round_trip_for_diagnostics() {
        for kind in LogicalPlanKind::all() {
            assert_eq!(kind.as_str().parse::<LogicalPlanKind>(), Ok(*kind));
        }
        assert!("UnknownLogical".parse::<LogicalPlanKind>().is_err());
    }

    #[test]
    fn logical_plan_class_strings_round_trip_for_diagnostics() {
        for class in LogicalPlanClass::all() {
            assert_eq!(class.as_str().parse::<LogicalPlanClass>(), Ok(*class));
        }
        assert!("unknown".parse::<LogicalPlanClass>().is_err());
    }

    struct TestLogicalNode;

    impl LogicalPlanNode for TestLogicalNode {
        fn logical_kind(&self) -> LogicalPlanKind {
            LogicalPlanKind::Filter
        }
    }

    #[test]
    fn logical_plan_node_derives_class_from_kind() {
        let node = TestLogicalNode;
        assert_eq!(node.logical_class(), LogicalPlanClass::Relational);
    }
}
