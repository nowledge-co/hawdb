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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Undirected,
}

// Retained query ownership: RelationshipDirection.
crate::impl_retained_heap_for_copy!(RelationshipDirection);

/// The node a Cypher `startNode(r)` or `endNode(r)` call reads: the stored
/// source or target of the relationship, independent of the match direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RelationshipEndpoint {
    Start,
    End,
}

impl RelationshipEndpoint {
    pub fn from_function_name(name: &str) -> Option<Self> {
        [Self::Start, Self::End]
            .into_iter()
            .find(|endpoint| name.eq_ignore_ascii_case(endpoint.function_name()))
    }

    pub const fn function_name(self) -> &'static str {
        match self {
            Self::Start => "startNode",
            Self::End => "endNode",
        }
    }
}

/// A Kùzu-style `label(x)` or openCypher `labels(n)` call over a bound graph
/// variable.
///
/// - `label(n)` is the single label of a node as a STRING; `label(r)` is the
///   relationship type. A node with no label yields NULL, and a node with
///   more than one label is an execution error (use `labels(n)`).
/// - `labels(n)` is the list of a node's labels, in label-id order (the order
///   of the `labels` entry of a returned node value). A relationship is an
///   execution error.
///
/// Both yield NULL for a NULL variable, such as an unmatched OPTIONAL MATCH.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LabelFunction {
    Label,
    Labels,
}

impl LabelFunction {
    pub fn from_function_name(name: &str) -> Option<Self> {
        [Self::Label, Self::Labels]
            .into_iter()
            .find(|function| name.eq_ignore_ascii_case(function.function_name()))
    }

    pub const fn function_name(self) -> &'static str {
        match self {
            Self::Label => "label",
            Self::Labels => "labels",
        }
    }
}
