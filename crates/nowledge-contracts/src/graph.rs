use std::collections::BTreeMap;

use skein_core::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntity {
    pub node_id: u64,
    pub labels: Vec<String>,
    pub external_id: Option<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeGraphPathDirection {
    Outgoing,
    Incoming,
}

impl KnowledgeGraphPathDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphContextPath {
    pub seed_hit_id: String,
    pub hop: usize,
    pub direction: KnowledgeGraphPathDirection,
    pub relationship_id: u64,
    pub relationship_type: String,
    pub relationship_properties: BTreeMap<String, Value>,
    pub source_node_id: u64,
    pub source_labels: Vec<String>,
    pub source_external_id: Option<String>,
    pub target_node_id: u64,
    pub target_labels: Vec<String>,
    pub target_external_id: Option<String>,
}
