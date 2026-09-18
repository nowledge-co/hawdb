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

//! Shared graph property-index map aliases.

use crate::{CowSegment, CowSegmentedMap, NodeId, RelId};
use hawdb_core::{LabelId, RelTypeId, Value};
use std::collections::BTreeSet;

pub type CompositePropertyKey = Vec<(String, Value)>;
pub type NodeIdPostingList = CowSegment<BTreeSet<NodeId>>;
pub type RelIdPropertyPostingList = CowSegment<BTreeSet<RelId>>;
pub type NodePropertyIndex = CowSegmentedMap<(LabelId, String, Value), NodeIdPostingList>;
pub type CompositePropertyIndex =
    CowSegmentedMap<(LabelId, CompositePropertyKey), NodeIdPostingList>;
pub type FullTextPropertyIndex = CowSegmentedMap<(LabelId, String, String), NodeIdPostingList>;
pub type RelationshipPropertyIndex =
    CowSegmentedMap<(RelTypeId, String, Value), RelIdPropertyPostingList>;
