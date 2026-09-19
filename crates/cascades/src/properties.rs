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

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct RequiredProperties {
    pub distribution: Distribution,
    pub ordering: Vec<String>,
    pub covering_fields: Vec<String>,
    pub requires_scan_pruning: bool,
    pub requires_raw_vector_rerank: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct PhysicalProperties {
    pub distribution: Distribution,
    pub ordering: Vec<String>,
    pub covering_fields: Vec<String>,
    pub scan_pruning: ScanPruningSupport,
    pub vector_precision: VectorPrecision,
    pub memory_budget: MemoryBudgetClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub enum Distribution {
    #[default]
    Any,
    Single,
    Hash(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum ScanPruningSupport {
    #[default]
    Unknown,
    None,
    Label,
    Segment,
    Index,
    ExactEmpty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum VectorPrecision {
    #[default]
    NotVector,
    ApproximateCandidate,
    RawReranked,
    Exact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum MemoryBudgetClass {
    #[default]
    Unknown,
    Constant,
    RowLinear,
    Blocking,
}

impl PhysicalProperties {
    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        distribution_satisfies(&self.distribution, &required.distribution)
            && ordering_satisfies(&self.ordering, &required.ordering)
            && covering_satisfies(&self.covering_fields, &required.covering_fields)
            && scan_pruning_satisfies(self.scan_pruning, required.requires_scan_pruning)
            && vector_rerank_satisfies(self.vector_precision, required.requires_raw_vector_rerank)
    }
}

impl Distribution {
    pub fn as_str(&self) -> &'static str {
        match self {
            Distribution::Any => "any",
            Distribution::Single => "single",
            Distribution::Hash(_) => "hash",
        }
    }
}

impl ScanPruningSupport {
    pub fn as_str(self) -> &'static str {
        match self {
            ScanPruningSupport::Unknown => "unknown",
            ScanPruningSupport::None => "none",
            ScanPruningSupport::Label => "label",
            ScanPruningSupport::Segment => "segment",
            ScanPruningSupport::Index => "index",
            ScanPruningSupport::ExactEmpty => "exact_empty",
        }
    }

    pub fn is_prunable(self) -> bool {
        matches!(
            self,
            ScanPruningSupport::Label
                | ScanPruningSupport::Segment
                | ScanPruningSupport::Index
                | ScanPruningSupport::ExactEmpty
        )
    }
}

impl VectorPrecision {
    pub fn as_str(self) -> &'static str {
        match self {
            VectorPrecision::NotVector => "not_vector",
            VectorPrecision::ApproximateCandidate => "approximate_candidate",
            VectorPrecision::RawReranked => "raw_reranked",
            VectorPrecision::Exact => "exact",
        }
    }

    pub fn includes_raw_rerank(self) -> bool {
        matches!(self, VectorPrecision::RawReranked | VectorPrecision::Exact)
    }
}

impl MemoryBudgetClass {
    pub fn as_str(self) -> &'static str {
        match self {
            MemoryBudgetClass::Unknown => "unknown",
            MemoryBudgetClass::Constant => "constant",
            MemoryBudgetClass::RowLinear => "row_linear",
            MemoryBudgetClass::Blocking => "blocking",
        }
    }
}

fn distribution_satisfies(actual: &Distribution, required: &Distribution) -> bool {
    match required {
        Distribution::Any => true,
        Distribution::Single => actual == required,
        Distribution::Hash(keys) => {
            matches!(actual, Distribution::Hash(actual_keys) if actual_keys == keys)
        }
    }
}

fn covering_satisfies(actual: &[String], required: &[String]) -> bool {
    required.iter().all(|field| actual.contains(field))
}

fn scan_pruning_satisfies(actual: ScanPruningSupport, required: bool) -> bool {
    !required || actual.is_prunable()
}

fn vector_rerank_satisfies(actual: VectorPrecision, required: bool) -> bool {
    !required || actual.includes_raw_rerank()
}

fn ordering_satisfies(actual: &[String], required: &[String]) -> bool {
    required.is_empty()
        || (actual.len() >= required.len()
            && actual
                .iter()
                .zip(required.iter())
                .all(|(actual, required)| actual == required))
}

#[cfg(test)]
mod tests {
    use super::{
        Distribution, MemoryBudgetClass, PhysicalProperties, RequiredProperties,
        ScanPruningSupport, VectorPrecision,
    };

    #[test]
    fn empty_required_properties_accept_any_plan() {
        let actual = PhysicalProperties {
            distribution: Distribution::Hash(vec!["space_id".to_string()]),
            ordering: vec!["updated_at".to_string()],
            scan_pruning: ScanPruningSupport::Index,
            ..PhysicalProperties::default()
        };

        assert!(actual.satisfies(&RequiredProperties::default()));
    }

    #[test]
    fn ordering_requirement_accepts_prefix_match() {
        let actual = PhysicalProperties {
            distribution: Distribution::Single,
            ordering: vec!["space_id".to_string(), "updated_at".to_string()],
            ..PhysicalProperties::default()
        };
        let required = RequiredProperties {
            distribution: Distribution::Single,
            ordering: vec!["space_id".to_string()],
            ..RequiredProperties::default()
        };

        assert!(actual.satisfies(&required));
    }

    #[test]
    fn embedded_requirements_check_covering_pruning_and_vector_rerank() {
        let actual = PhysicalProperties {
            distribution: Distribution::Single,
            covering_fields: vec!["Memory.id".to_string(), "Memory.title".to_string()],
            scan_pruning: ScanPruningSupport::Index,
            vector_precision: VectorPrecision::RawReranked,
            memory_budget: MemoryBudgetClass::RowLinear,
            ..PhysicalProperties::default()
        };

        assert!(actual.satisfies(&RequiredProperties {
            distribution: Distribution::Single,
            covering_fields: vec!["Memory.id".to_string()],
            requires_scan_pruning: true,
            requires_raw_vector_rerank: true,
            ..RequiredProperties::default()
        }));
        assert!(!actual.satisfies(&RequiredProperties {
            covering_fields: vec!["Memory.embedding".to_string()],
            ..RequiredProperties::default()
        }));
        assert!(
            !PhysicalProperties::default().satisfies(&RequiredProperties {
                requires_scan_pruning: true,
                ..RequiredProperties::default()
            })
        );
    }

    #[test]
    fn distribution_exposes_stable_names() {
        assert_eq!(Distribution::Any.as_str(), "any");
        assert_eq!(Distribution::Single.as_str(), "single");
        assert_eq!(
            Distribution::Hash(vec!["space_id".to_string()]).as_str(),
            "hash"
        );
        assert_eq!(ScanPruningSupport::Index.as_str(), "index");
        assert_eq!(ScanPruningSupport::Segment.as_str(), "segment");
        assert_eq!(VectorPrecision::RawReranked.as_str(), "raw_reranked");
        assert_eq!(MemoryBudgetClass::Blocking.as_str(), "blocking");
    }
}
