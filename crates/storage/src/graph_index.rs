//! Shared graph property-index map aliases.

use crate::{CowSegment, CowSegmentedMap, NodeId, RelId};
use skein_core::{LabelId, RelTypeId, Value};
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
