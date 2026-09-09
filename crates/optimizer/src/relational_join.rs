//! Relational inner-join graph contracts and validation.

use crate::{
    Distribution, MemoryBudgetClass, PhysicalProperties, PlanCost, PlanCostBreakdown,
    RelationalAccessPathDescriptor, RelationalAccessPathKind, RequiredProperties,
    ScanPruningSupport,
};
use skein_expression::{BindingId, BindingSet};
use std::{collections::BTreeMap, error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalJoinPredicateId(u32);

impl RelationalJoinPredicateId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinAccessPath {
    pub descriptor: RelationalAccessPathDescriptor,
    pub required_bindings: BindingSet,
    pub properties: PhysicalProperties,
    pub applicability: RelationalJoinAccessApplicability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinAccessApplicability {
    Base,
    Probe,
    BaseAndProbe,
}

impl RelationalJoinAccessPath {
    pub fn base(descriptor: RelationalAccessPathDescriptor) -> Self {
        Self::new(
            descriptor,
            BindingSet::new(),
            RelationalJoinAccessApplicability::Base,
        )
    }

    pub fn probe(
        descriptor: RelationalAccessPathDescriptor,
        required_bindings: BindingSet,
    ) -> Self {
        Self::new(
            descriptor,
            required_bindings,
            RelationalJoinAccessApplicability::Probe,
        )
    }

    pub fn base_and_probe(descriptor: RelationalAccessPathDescriptor) -> Self {
        Self::new(
            descriptor,
            BindingSet::new(),
            RelationalJoinAccessApplicability::BaseAndProbe,
        )
    }

    fn new(
        descriptor: RelationalAccessPathDescriptor,
        required_bindings: BindingSet,
        applicability: RelationalJoinAccessApplicability,
    ) -> Self {
        let properties = access_path_properties(&descriptor);
        Self {
            descriptor,
            required_bindings,
            properties,
            applicability,
        }
    }

    pub fn with_properties(mut self, properties: PhysicalProperties) -> Self {
        self.properties = properties;
        self
    }

    pub(crate) fn supports_base(&self) -> bool {
        matches!(
            self.applicability,
            RelationalJoinAccessApplicability::Base
                | RelationalJoinAccessApplicability::BaseAndProbe
        )
    }

    pub(crate) fn supports_probe(&self) -> bool {
        matches!(
            self.applicability,
            RelationalJoinAccessApplicability::Probe
                | RelationalJoinAccessApplicability::BaseAndProbe
        )
    }

    pub(crate) fn validate_usage(&self) -> Result<(), &'static str> {
        match self.applicability {
            RelationalJoinAccessApplicability::Base
            | RelationalJoinAccessApplicability::BaseAndProbe
                if !self.required_bindings.is_empty() =>
            {
                Err("base access path cannot require another binding")
            }
            RelationalJoinAccessApplicability::Probe if self.required_bindings.is_empty() => {
                Err("probe access path must require another binding")
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinRelation {
    pub binding: BindingId,
    pub access_paths: Vec<RelationalJoinAccessPath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinPredicate {
    pub id: RelationalJoinPredicateId,
    pub bindings: BindingSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinGraph {
    pub relations: Vec<RelationalJoinRelation>,
    pub predicates: Vec<RelationalJoinPredicate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalJoinEnumerationConfig {
    pub max_groups: usize,
    pub max_expressions: usize,
}

impl Default for RelationalJoinEnumerationConfig {
    fn default() -> Self {
        Self {
            max_groups: 4_095,
            max_expressions: 32_768,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinStep {
    pub binding: BindingId,
    pub access_path: RelationalJoinAccessPath,
    pub activated_predicates: Vec<RelationalJoinPredicateId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinPlan {
    pub base_binding: BindingId,
    pub base_access_path: RelationalJoinAccessPath,
    pub steps: Vec<RelationalJoinStep>,
    pub cost_breakdown: PlanCostBreakdown,
    pub properties: PhysicalProperties,
}

impl RelationalJoinPlan {
    pub fn cost(&self) -> PlanCost {
        self.cost_breakdown.as_plan_cost()
    }

    pub fn binding_order(&self) -> Vec<BindingId> {
        std::iter::once(self.base_binding)
            .chain(self.steps.iter().map(|step| step.binding))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalInnerJoinEnumeration {
    pub plan: RelationalJoinPlan,
    pub memo_groups: usize,
    pub memo_expressions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalJoinEnumerationError {
    EmptyGraph,
    DuplicateBinding(BindingId),
    DuplicatePredicate(RelationalJoinPredicateId),
    PredicateHasFewerThanTwoBindings(RelationalJoinPredicateId),
    UnknownPredicateBinding {
        predicate: RelationalJoinPredicateId,
        binding: BindingId,
    },
    UnknownAccessBinding {
        relation: BindingId,
        binding: BindingId,
    },
    AccessWithoutPredicate {
        relation: BindingId,
        binding: BindingId,
    },
    SelfDependentAccess(BindingId),
    MissingBaseAccess(BindingId),
    InvalidAccessPath {
        binding: BindingId,
        reason: &'static str,
    },
    GroupBudgetExceeded {
        required_groups: usize,
        max_groups: usize,
    },
    ExpressionBudgetExceeded {
        required_expressions: usize,
        max_expressions: usize,
    },
    Disconnected,
    RequiredPropertiesUnsatisfied,
}

impl fmt::Display for RelationalJoinEnumerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyGraph => write!(formatter, "relational join graph is empty"),
            Self::DuplicateBinding(binding) => {
                write!(formatter, "duplicate relational binding {}", binding.get())
            }
            Self::DuplicatePredicate(predicate) => {
                write!(formatter, "duplicate relational predicate {}", predicate.get())
            }
            Self::PredicateHasFewerThanTwoBindings(predicate) => write!(
                formatter,
                "relational join predicate {} references fewer than two bindings",
                predicate.get()
            ),
            Self::UnknownPredicateBinding { predicate, binding } => write!(
                formatter,
                "relational join predicate {} references unknown binding {}",
                predicate.get(),
                binding.get()
            ),
            Self::UnknownAccessBinding { relation, binding } => write!(
                formatter,
                "relational binding {} access path requires unknown binding {}",
                relation.get(),
                binding.get()
            ),
            Self::AccessWithoutPredicate { relation, binding } => write!(
                formatter,
                "relational binding {} access path depends on binding {} without a connecting predicate",
                relation.get(),
                binding.get()
            ),
            Self::SelfDependentAccess(binding) => write!(
                formatter,
                "relational binding {} access path depends on itself",
                binding.get()
            ),
            Self::MissingBaseAccess(binding) => write!(
                formatter,
                "relational binding {} has no unbound access path",
                binding.get()
            ),
            Self::InvalidAccessPath { binding, reason } => write!(
                formatter,
                "relational binding {} has an invalid access path: {reason}",
                binding.get()
            ),
            Self::GroupBudgetExceeded {
                required_groups,
                max_groups,
            } => write!(
                formatter,
                "relational join enumeration requires {required_groups} groups, exceeding max_groups {max_groups}"
            ),
            Self::ExpressionBudgetExceeded {
                required_expressions,
                max_expressions,
            } => write!(
                formatter,
                "relational join enumeration requires at least {required_expressions} expressions, exceeding max_expressions {max_expressions}"
            ),
            Self::Disconnected => write!(
                formatter,
                "relational join graph has no connected left-deep enumeration"
            ),
            Self::RequiredPropertiesUnsatisfied => write!(
                formatter,
                "no relational join order satisfies the required physical properties"
            ),
        }
    }
}

impl Error for RelationalJoinEnumerationError {}

pub fn enumerate_relational_inner_joins(
    graph: &RelationalJoinGraph,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
) -> Result<RelationalInnerJoinEnumeration, RelationalJoinEnumerationError> {
    validate_graph(graph)?;
    crate::relational_join_hypergraph::enumerate_inner_graph(graph, required_properties, config)
}

fn validate_graph(graph: &RelationalJoinGraph) -> Result<(), RelationalJoinEnumerationError> {
    if graph.relations.is_empty() {
        return Err(RelationalJoinEnumerationError::EmptyGraph);
    }
    let relation_bindings: BindingSet = graph
        .relations
        .iter()
        .map(|relation| relation.binding)
        .collect();
    if relation_bindings.len() != graph.relations.len() {
        let mut seen = BindingSet::new();
        let duplicate = graph
            .relations
            .iter()
            .map(|relation| relation.binding)
            .find(|binding| !seen.insert(*binding))
            .expect("binding count mismatch requires a duplicate");
        return Err(RelationalJoinEnumerationError::DuplicateBinding(duplicate));
    }
    for relation in &graph.relations {
        if !relation
            .access_paths
            .iter()
            .any(RelationalJoinAccessPath::supports_base)
        {
            return Err(RelationalJoinEnumerationError::MissingBaseAccess(
                relation.binding,
            ));
        }
        for access in &relation.access_paths {
            access.descriptor.validate().map_err(|reason| {
                RelationalJoinEnumerationError::InvalidAccessPath {
                    binding: relation.binding,
                    reason,
                }
            })?;
            access.validate_usage().map_err(|reason| {
                RelationalJoinEnumerationError::InvalidAccessPath {
                    binding: relation.binding,
                    reason,
                }
            })?;
            if access.required_bindings.contains(relation.binding) {
                return Err(RelationalJoinEnumerationError::SelfDependentAccess(
                    relation.binding,
                ));
            }
            if let Some(binding) = access
                .required_bindings
                .iter()
                .find(|binding| !relation_bindings.contains(*binding))
            {
                return Err(RelationalJoinEnumerationError::UnknownAccessBinding {
                    relation: relation.binding,
                    binding,
                });
            }
            if let Some(binding) = access.required_bindings.iter().find(|binding| {
                !graph.predicates.iter().any(|predicate| {
                    predicate.bindings.contains(relation.binding)
                        && predicate.bindings.contains(*binding)
                })
            }) {
                return Err(RelationalJoinEnumerationError::AccessWithoutPredicate {
                    relation: relation.binding,
                    binding,
                });
            }
        }
    }

    let mut predicate_ids = BTreeMap::new();
    for predicate in &graph.predicates {
        if predicate_ids.insert(predicate.id, ()).is_some() {
            return Err(RelationalJoinEnumerationError::DuplicatePredicate(
                predicate.id,
            ));
        }
        if predicate.bindings.len() < 2 {
            return Err(
                RelationalJoinEnumerationError::PredicateHasFewerThanTwoBindings(predicate.id),
            );
        }
        if let Some(binding) = predicate
            .bindings
            .iter()
            .find(|binding| !relation_bindings.contains(*binding))
        {
            return Err(RelationalJoinEnumerationError::UnknownPredicateBinding {
                predicate: predicate.id,
                binding,
            });
        }
    }
    Ok(())
}

fn access_path_properties(descriptor: &RelationalAccessPathDescriptor) -> PhysicalProperties {
    let ordering_start = descriptor.equality_prefix_len;
    let ordering_end = ordering_start
        .saturating_add(descriptor.order_prefix_len)
        .min(descriptor.index_columns.len());
    PhysicalProperties {
        distribution: Distribution::Single,
        ordering: descriptor.index_columns[ordering_start..ordering_end].to_vec(),
        covering_fields: if descriptor.covering {
            descriptor.index_columns.clone()
        } else {
            Vec::new()
        },
        scan_pruning: match descriptor.kind {
            RelationalAccessPathKind::FullScan => ScanPruningSupport::None,
            RelationalAccessPathKind::PrimaryKey | RelationalAccessPathKind::Index => {
                ScanPruningSupport::Index
            }
        },
        memory_budget: MemoryBudgetClass::Constant,
        ..PhysicalProperties::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    const A: BindingId = BindingId::new(1);
    const B: BindingId = BindingId::new(2);
    const C: BindingId = BindingId::new(3);

    fn descriptor(
        name: &str,
        kind: RelationalAccessPathKind,
        rows: usize,
        unique: bool,
    ) -> RelationalAccessPathDescriptor {
        let index_columns = if kind != RelationalAccessPathKind::FullScan {
            vec!["id".to_string()]
        } else {
            Vec::new()
        };
        RelationalAccessPathDescriptor {
            kind,
            name: name.to_string(),
            access_columns: index_columns.iter().cloned().collect::<BTreeSet<_>>(),
            equality_prefix_len: index_columns.len(),
            index_columns,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: unique,
            covering: false,
            requires_row_fetch: kind == RelationalAccessPathKind::Index,
            estimated_rows: rows,
        }
    }

    fn scan(rows: usize) -> RelationalJoinAccessPath {
        RelationalJoinAccessPath::base_and_probe(descriptor(
            "__full_scan",
            RelationalAccessPathKind::FullScan,
            rows,
            false,
        ))
    }

    fn probe(name: &str, required: BindingId, rows: usize) -> RelationalJoinAccessPath {
        RelationalJoinAccessPath::probe(
            descriptor(name, RelationalAccessPathKind::Index, rows, rows == 1),
            required.into(),
        )
    }

    fn relation_with_probes(
        binding: BindingId,
        rows: usize,
        probes: Vec<RelationalJoinAccessPath>,
    ) -> RelationalJoinRelation {
        let mut access_paths = vec![scan(rows)];
        access_paths.extend(probes);
        RelationalJoinRelation {
            binding,
            access_paths,
        }
    }

    fn predicate(id: u32, bindings: impl Into<BindingSet>) -> RelationalJoinPredicate {
        RelationalJoinPredicate {
            id: RelationalJoinPredicateId::new(id),
            bindings: bindings.into(),
        }
    }

    #[test]
    fn memo_enumeration_prefers_selective_outer_and_index_probes() {
        let graph = RelationalJoinGraph {
            relations: vec![
                relation_with_probes(A, 1_000, vec![probe("a_by_b", B, 1)]),
                relation_with_probes(B, 100, vec![probe("b_by_a", A, 100), probe("b_by_c", C, 1)]),
                relation_with_probes(C, 1, vec![probe("c_by_b", B, 100)]),
            ],
            predicates: vec![predicate(1, [A, B]), predicate(2, [B, C])],
        };

        let result = enumerate_relational_inner_joins(
            &graph,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.plan.binding_order(), [C, B, A]);
        assert_eq!(
            result.plan.cost(),
            PlanCost {
                estimated_rows: 1,
                cost: 3
            }
        );
        assert_eq!(
            result.plan.steps[0].activated_predicates,
            [RelationalJoinPredicateId::new(2)]
        );
        assert_eq!(
            result.plan.steps[1].activated_predicates,
            [RelationalJoinPredicateId::new(1)]
        );
        assert!(result.memo_groups >= 5);
        assert!(result.memo_expressions > result.memo_groups);
    }

    #[test]
    fn memo_group_contains_equivalent_join_rewrites() {
        let graph = RelationalJoinGraph {
            relations: vec![
                relation_with_probes(A, 10, vec![probe("a_by_b", B, 1), probe("a_by_c", C, 1)]),
                relation_with_probes(B, 10, vec![probe("b_by_a", A, 1), probe("b_by_c", C, 1)]),
                relation_with_probes(C, 10, vec![probe("c_by_a", A, 1), probe("c_by_b", B, 1)]),
            ],
            predicates: vec![
                predicate(1, [A, B]),
                predicate(2, [B, C]),
                predicate(3, [A, C]),
            ],
        };

        let result = enumerate_relational_inner_joins(
            &graph,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.memo_groups, 7);
        assert_eq!(result.memo_expressions, 12);
        assert_eq!(result.plan.binding_order(), [A, B, C]);
    }

    #[test]
    fn required_ordering_is_part_of_the_best_plan_cache_key() {
        let ordered = scan(100).with_properties(PhysicalProperties {
            distribution: Distribution::Single,
            ordering: vec!["created_at".to_string()],
            memory_budget: MemoryBudgetClass::Constant,
            ..PhysicalProperties::default()
        });
        let graph = RelationalJoinGraph {
            relations: vec![
                RelationalJoinRelation {
                    binding: A,
                    access_paths: vec![ordered, probe("a_by_b", B, 1)],
                },
                relation_with_probes(B, 1, vec![probe("b_by_a", A, 1)]),
            ],
            predicates: vec![predicate(1, [A, B])],
        };
        let required = RequiredProperties {
            distribution: Distribution::Single,
            ordering: vec!["created_at".to_string()],
            ..RequiredProperties::default()
        };

        let result = enumerate_relational_inner_joins(
            &graph,
            &required,
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.plan.binding_order(), [A, B]);
        assert_eq!(result.plan.properties.ordering, ["created_at".to_string()]);
    }

    #[test]
    fn base_only_access_paths_are_not_costed_as_inner_probes() {
        let selective_base = RelationalJoinAccessPath::base(descriptor(
            "a_constant_filter",
            RelationalAccessPathKind::Index,
            5,
            false,
        ));
        let graph = RelationalJoinGraph {
            relations: vec![
                RelationalJoinRelation {
                    binding: A,
                    access_paths: vec![scan(100), selective_base, probe("a_by_b", B, 100)],
                },
                relation_with_probes(B, 1, Vec::new()),
            ],
            predicates: vec![predicate(1, [A, B])],
        };

        let result = enumerate_relational_inner_joins(
            &graph,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.plan.binding_order(), [A, B]);
        assert_eq!(
            result.plan.base_access_path.descriptor.name,
            "a_constant_filter"
        );
        assert_eq!(
            result.plan.cost(),
            PlanCost {
                estimated_rows: 5,
                cost: 10
            }
        );
    }

    #[test]
    fn disconnected_graph_does_not_return_a_partial_enumeration() {
        let graph = RelationalJoinGraph {
            relations: vec![
                relation_with_probes(A, 10, Vec::new()),
                relation_with_probes(B, 10, Vec::new()),
            ],
            predicates: Vec::new(),
        };

        assert_eq!(
            enumerate_relational_inner_joins(
                &graph,
                &RequiredProperties::default(),
                RelationalJoinEnumerationConfig::default(),
            ),
            Err(RelationalJoinEnumerationError::Disconnected)
        );
    }

    #[test]
    fn access_dependencies_require_a_matching_join_predicate() {
        let graph = RelationalJoinGraph {
            relations: vec![
                relation_with_probes(A, 10, vec![probe("a_by_b", B, 1)]),
                relation_with_probes(B, 10, Vec::new()),
                relation_with_probes(C, 10, Vec::new()),
            ],
            predicates: vec![predicate(1, [A, C])],
        };

        assert_eq!(
            enumerate_relational_inner_joins(
                &graph,
                &RequiredProperties::default(),
                RelationalJoinEnumerationConfig::default(),
            ),
            Err(RelationalJoinEnumerationError::AccessWithoutPredicate {
                relation: A,
                binding: B,
            })
        );
    }

    #[test]
    fn group_budget_rejects_the_next_actual_connected_group() {
        let graph = RelationalJoinGraph {
            relations: vec![
                relation_with_probes(A, 10, Vec::new()),
                relation_with_probes(B, 10, Vec::new()),
                relation_with_probes(C, 10, Vec::new()),
            ],
            predicates: vec![predicate(1, [A, B]), predicate(2, [B, C])],
        };

        assert_eq!(
            enumerate_relational_inner_joins(
                &graph,
                &RequiredProperties::default(),
                RelationalJoinEnumerationConfig {
                    max_groups: 5,
                    max_expressions: 100,
                },
            ),
            Err(RelationalJoinEnumerationError::GroupBudgetExceeded {
                required_groups: 6,
                max_groups: 5,
            })
        );
    }
}
