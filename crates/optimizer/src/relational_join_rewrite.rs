//! CD-C legality analysis and memo-backed join rewrite enumeration.

use crate::{
    PhysicalProperties, PlanCost, PlanCostBreakdown, RelationalJoinAccessPath,
    RelationalJoinEnumerationConfig, RelationalJoinEnumerationError, RelationalJoinPredicateId,
    RelationalJoinRelation, RequiredProperties,
};
use skein_expression::{prove_null_rejecting, BindingId, BindingSet, BoundPredicate};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalJoinOperatorId(u32);

impl RelationalJoinOperatorId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RelationalJoinOperatorKind {
    Inner,
    LeftOuter,
}

impl RelationalJoinOperatorKind {
    pub const fn is_commutative(self) -> bool {
        matches!(self, Self::Inner)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinOperator {
    pub id: RelationalJoinOperatorId,
    pub kind: RelationalJoinOperatorKind,
    pub predicate_ids: Vec<RelationalJoinPredicateId>,
    pub predicate: BoundPredicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalJoinTree {
    Relation(BindingId),
    Join {
        operator: RelationalJoinOperator,
        left: Box<Self>,
        right: Box<Self>,
    },
}

impl RelationalJoinTree {
    pub fn join(
        operator: RelationalJoinOperator,
        left: RelationalJoinTree,
        right: RelationalJoinTree,
    ) -> Self {
        Self::Join {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinConflictRule {
    pub trigger: BindingSet,
    pub required: BindingSet,
}

impl RelationalJoinConflictRule {
    pub fn is_obeyed_by(&self, bindings: &BindingSet) -> bool {
        !self.trigger.intersects(bindings) || self.required.is_subset(bindings)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinConflictDescriptor {
    pub operator_id: RelationalJoinOperatorId,
    pub declared_kind: RelationalJoinOperatorKind,
    pub effective_kind: RelationalJoinOperatorKind,
    pub initial_left_bindings: BindingSet,
    pub initial_right_bindings: BindingSet,
    pub predicate_bindings: BindingSet,
    pub left_total_eligibility: BindingSet,
    pub right_total_eligibility: BindingSet,
    pub conflict_rules: Vec<RelationalJoinConflictRule>,
    pub predicate_ids: Vec<RelationalJoinPredicateId>,
}

impl RelationalJoinConflictDescriptor {
    pub fn was_null_rejection_simplified(&self) -> bool {
        self.declared_kind == RelationalJoinOperatorKind::LeftOuter
            && self.effective_kind == RelationalJoinOperatorKind::Inner
    }

    pub fn is_applicable(&self, left: &BindingSet, right: &BindingSet) -> bool {
        if left.intersects(right)
            || !self.left_total_eligibility.is_subset(left)
            || !self.right_total_eligibility.is_subset(right)
        {
            return false;
        }
        let available = binding_union(left, right);
        self.conflict_rules
            .iter()
            .all(|rule| rule.is_obeyed_by(&available))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinConflictAnalysis {
    pub root_bindings: BindingSet,
    pub descriptors: BTreeMap<RelationalJoinOperatorId, RelationalJoinConflictDescriptor>,
}

impl RelationalJoinConflictAnalysis {
    pub fn descriptor(
        &self,
        operator: RelationalJoinOperatorId,
    ) -> Option<&RelationalJoinConflictDescriptor> {
        self.descriptors.get(&operator)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinRewriteProblem {
    pub relations: Vec<RelationalJoinRelation>,
    pub initial_tree: RelationalJoinTree,
    pub post_join_filter: Option<BoundPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinRewriteStep {
    pub operator_id: RelationalJoinOperatorId,
    pub operator_kind: RelationalJoinOperatorKind,
    pub binding: BindingId,
    pub access_path: RelationalJoinAccessPath,
    pub predicate_ids: Vec<RelationalJoinPredicateId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinRewritePlan {
    pub base_binding: BindingId,
    pub base_access_path: RelationalJoinAccessPath,
    pub steps: Vec<RelationalJoinRewriteStep>,
    pub cost_breakdown: PlanCostBreakdown,
    pub properties: PhysicalProperties,
}

impl RelationalJoinRewritePlan {
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
pub struct RelationalJoinRewriteEnumeration {
    pub plan: RelationalJoinRewritePlan,
    pub conflict_analysis: RelationalJoinConflictAnalysis,
    pub memo_groups: usize,
    pub memo_expressions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalJoinRewriteError {
    Enumeration(RelationalJoinEnumerationError),
    DuplicateTreeBinding(BindingId),
    DuplicateOperator(RelationalJoinOperatorId),
    PredicateOutsideOperatorSubtree {
        operator: RelationalJoinOperatorId,
        binding: BindingId,
    },
    RelationSetMismatch,
    OperatorCountMismatch {
        relations: usize,
        operators: usize,
    },
    NoLegalRewrite,
}

impl fmt::Display for RelationalJoinRewriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Enumeration(error) => error.fmt(formatter),
            Self::DuplicateTreeBinding(binding) => {
                write!(formatter, "duplicate tree binding {}", binding.get())
            }
            Self::DuplicateOperator(operator) => {
                write!(formatter, "duplicate join operator {}", operator.get())
            }
            Self::PredicateOutsideOperatorSubtree { operator, binding } => write!(
                formatter,
                "join operator {} predicate references binding {} outside its subtree",
                operator.get(),
                binding.get()
            ),
            Self::RelationSetMismatch => write!(
                formatter,
                "join rewrite relation list does not match initial tree bindings"
            ),
            Self::OperatorCountMismatch {
                relations,
                operators,
            } => write!(
                formatter,
                "join tree has {relations} relations but {operators} operators"
            ),
            Self::NoLegalRewrite => write!(formatter, "join rewrite has no legal complete plan"),
        }
    }
}

impl Error for RelationalJoinRewriteError {}

impl From<RelationalJoinEnumerationError> for RelationalJoinRewriteError {
    fn from(error: RelationalJoinEnumerationError) -> Self {
        Self::Enumeration(error)
    }
}

#[derive(Debug, Clone)]
struct OperatorFacts {
    kind: RelationalJoinOperatorKind,
    left_bindings: BindingSet,
    right_bindings: BindingSet,
    predicate_bindings: BindingSet,
}

struct AnalyzedTree {
    bindings: BindingSet,
    operators: Vec<OperatorFacts>,
}

pub fn analyze_relational_join_conflicts(
    tree: &RelationalJoinTree,
    post_join_filter: Option<&BoundPredicate>,
) -> Result<RelationalJoinConflictAnalysis, RelationalJoinRewriteError> {
    let mut descriptors = BTreeMap::new();
    let mut seen_bindings = BindingSet::new();
    let mut seen_operators = BTreeSet::new();
    let analyzed = analyze_tree(
        tree,
        post_join_filter,
        &mut descriptors,
        &mut seen_bindings,
        &mut seen_operators,
    )?;
    if analyzed.operators.len() + 1 != analyzed.bindings.len() {
        return Err(RelationalJoinRewriteError::OperatorCountMismatch {
            relations: analyzed.bindings.len(),
            operators: analyzed.operators.len(),
        });
    }
    Ok(RelationalJoinConflictAnalysis {
        root_bindings: analyzed.bindings,
        descriptors,
    })
}

fn analyze_tree(
    tree: &RelationalJoinTree,
    post_join_filter: Option<&BoundPredicate>,
    descriptors: &mut BTreeMap<RelationalJoinOperatorId, RelationalJoinConflictDescriptor>,
    seen_bindings: &mut BindingSet,
    seen_operators: &mut BTreeSet<RelationalJoinOperatorId>,
) -> Result<AnalyzedTree, RelationalJoinRewriteError> {
    match tree {
        RelationalJoinTree::Relation(binding) => {
            if !seen_bindings.insert(*binding) {
                return Err(RelationalJoinRewriteError::DuplicateTreeBinding(*binding));
            }
            Ok(AnalyzedTree {
                bindings: (*binding).into(),
                operators: Vec::new(),
            })
        }
        RelationalJoinTree::Join {
            operator,
            left,
            right,
        } => {
            if !seen_operators.insert(operator.id) {
                return Err(RelationalJoinRewriteError::DuplicateOperator(operator.id));
            }
            let left = analyze_tree(
                left,
                post_join_filter,
                descriptors,
                seen_bindings,
                seen_operators,
            )?;
            let right = analyze_tree(
                right,
                post_join_filter,
                descriptors,
                seen_bindings,
                seen_operators,
            )?;
            let bindings = binding_union(&left.bindings, &right.bindings);
            let predicate_bindings = operator.predicate.referenced_bindings();
            if let Some(binding) = predicate_bindings
                .iter()
                .find(|binding| !bindings.contains(*binding))
            {
                return Err(
                    RelationalJoinRewriteError::PredicateOutsideOperatorSubtree {
                        operator: operator.id,
                        binding,
                    },
                );
            }
            let effective_kind =
                effective_operator_kind(operator, &right.bindings, post_join_filter);
            let mut rules =
                calculate_conflict_rules(effective_kind, &left.operators, &right.operators);
            rules.sort_by(|left, right| {
                left.trigger
                    .cmp(&right.trigger)
                    .then_with(|| left.required.cmp(&right.required))
            });
            rules.dedup();

            let left_predicate_bindings = binding_intersection(&predicate_bindings, &left.bindings);
            let right_predicate_bindings =
                binding_intersection(&predicate_bindings, &right.bindings);
            let degenerate =
                left_predicate_bindings.is_empty() || right_predicate_bindings.is_empty();
            let initial_total_eligibility = if degenerate {
                bindings.clone()
            } else {
                predicate_bindings.clone()
            };
            let (total_eligibility, remaining_rules) =
                absorb_conflict_rules(initial_total_eligibility, rules);
            let descriptor = RelationalJoinConflictDescriptor {
                operator_id: operator.id,
                declared_kind: operator.kind,
                effective_kind,
                initial_left_bindings: left.bindings.clone(),
                initial_right_bindings: right.bindings.clone(),
                predicate_bindings: predicate_bindings.clone(),
                left_total_eligibility: binding_intersection(&total_eligibility, &left.bindings),
                right_total_eligibility: binding_intersection(&total_eligibility, &right.bindings),
                conflict_rules: remaining_rules,
                predicate_ids: operator.predicate_ids.clone(),
            };
            descriptors.insert(operator.id, descriptor);

            let mut operators = left.operators;
            operators.extend(right.operators);
            operators.push(OperatorFacts {
                kind: effective_kind,
                left_bindings: left.bindings,
                right_bindings: right.bindings,
                predicate_bindings,
            });
            Ok(AnalyzedTree {
                bindings,
                operators,
            })
        }
    }
}

fn effective_operator_kind(
    operator: &RelationalJoinOperator,
    right_bindings: &BindingSet,
    post_join_filter: Option<&BoundPredicate>,
) -> RelationalJoinOperatorKind {
    if operator.kind == RelationalJoinOperatorKind::LeftOuter
        && post_join_filter
            .is_some_and(|filter| prove_null_rejecting(filter, right_bindings).is_proven())
    {
        RelationalJoinOperatorKind::Inner
    } else {
        operator.kind
    }
}

fn calculate_conflict_rules(
    current: RelationalJoinOperatorKind,
    left_operators: &[OperatorFacts],
    right_operators: &[OperatorFacts],
) -> Vec<RelationalJoinConflictRule> {
    let mut rules = Vec::new();
    for nested in left_operators {
        if !is_associative(nested.kind, current) {
            rules.push(RelationalJoinConflictRule {
                trigger: nested.right_bindings.clone(),
                required: predicate_side_or_subtree(
                    &nested.left_bindings,
                    &nested.predicate_bindings,
                ),
            });
        }
        if !is_left_asscom(nested.kind, current) {
            rules.push(RelationalJoinConflictRule {
                trigger: nested.left_bindings.clone(),
                required: predicate_side_or_subtree(
                    &nested.right_bindings,
                    &nested.predicate_bindings,
                ),
            });
        }
    }
    for nested in right_operators {
        if !is_associative(current, nested.kind) {
            rules.push(RelationalJoinConflictRule {
                trigger: nested.left_bindings.clone(),
                required: predicate_side_or_subtree(
                    &nested.right_bindings,
                    &nested.predicate_bindings,
                ),
            });
        }
        if !is_right_asscom(current, nested.kind) {
            rules.push(RelationalJoinConflictRule {
                trigger: nested.right_bindings.clone(),
                required: predicate_side_or_subtree(
                    &nested.left_bindings,
                    &nested.predicate_bindings,
                ),
            });
        }
    }
    rules
}

fn predicate_side_or_subtree(subtree: &BindingSet, predicate_bindings: &BindingSet) -> BindingSet {
    let used = binding_intersection(subtree, predicate_bindings);
    if used.is_empty() {
        subtree.clone()
    } else {
        used
    }
}

fn absorb_conflict_rules(
    mut total_eligibility: BindingSet,
    mut rules: Vec<RelationalJoinConflictRule>,
) -> (BindingSet, Vec<RelationalJoinConflictRule>) {
    loop {
        let expanded = rules
            .iter()
            .filter(|rule| rule.trigger.intersects(&total_eligibility))
            .fold(total_eligibility.clone(), |bindings, rule| {
                binding_union(&bindings, &rule.required)
            });
        if expanded == total_eligibility {
            break;
        }
        total_eligibility = expanded;
    }
    rules.retain(|rule| !rule.required.is_subset(&total_eligibility));
    (total_eligibility, rules)
}

const fn is_associative(
    lower: RelationalJoinOperatorKind,
    upper: RelationalJoinOperatorKind,
) -> bool {
    matches!(lower, RelationalJoinOperatorKind::Inner)
        && matches!(
            upper,
            RelationalJoinOperatorKind::Inner | RelationalJoinOperatorKind::LeftOuter
        )
}

const fn is_left_asscom(
    _lower: RelationalJoinOperatorKind,
    _upper: RelationalJoinOperatorKind,
) -> bool {
    true
}

const fn is_right_asscom(
    lower: RelationalJoinOperatorKind,
    upper: RelationalJoinOperatorKind,
) -> bool {
    matches!(lower, RelationalJoinOperatorKind::Inner)
        && matches!(upper, RelationalJoinOperatorKind::Inner)
}

pub fn enumerate_relational_join_rewrites(
    problem: &RelationalJoinRewriteProblem,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
) -> Result<RelationalJoinRewriteEnumeration, RelationalJoinRewriteError> {
    let analysis = analyze_relational_join_conflicts(
        &problem.initial_tree,
        problem.post_join_filter.as_ref(),
    )?;
    validate_problem_relations(problem, &analysis)?;
    crate::relational_join_hypergraph::enumerate_left_deep_rewrites(
        problem,
        analysis,
        required_properties,
        config,
    )
}

pub(crate) fn validate_problem_relations(
    problem: &RelationalJoinRewriteProblem,
    analysis: &RelationalJoinConflictAnalysis,
) -> Result<(), RelationalJoinRewriteError> {
    if problem.relations.is_empty() {
        return Err(RelationalJoinEnumerationError::EmptyGraph.into());
    }
    let relation_bindings: BindingSet = problem
        .relations
        .iter()
        .map(|relation| relation.binding)
        .collect();
    if relation_bindings.len() != problem.relations.len() {
        let mut seen = BindingSet::new();
        let duplicate = problem
            .relations
            .iter()
            .map(|relation| relation.binding)
            .find(|binding| !seen.insert(*binding))
            .expect("binding count mismatch requires a duplicate");
        return Err(RelationalJoinEnumerationError::DuplicateBinding(duplicate).into());
    }
    if relation_bindings != analysis.root_bindings {
        return Err(RelationalJoinRewriteError::RelationSetMismatch);
    }
    for relation in &problem.relations {
        if !relation
            .access_paths
            .iter()
            .any(|access| access.supports_base())
        {
            return Err(RelationalJoinEnumerationError::MissingBaseAccess(relation.binding).into());
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
                return Err(
                    RelationalJoinEnumerationError::SelfDependentAccess(relation.binding).into(),
                );
            }
            if let Some(binding) = access
                .required_bindings
                .iter()
                .find(|binding| !relation_bindings.contains(*binding))
            {
                return Err(RelationalJoinEnumerationError::UnknownAccessBinding {
                    relation: relation.binding,
                    binding,
                }
                .into());
            }
            if let Some(binding) = access.required_bindings.iter().find(|binding| {
                !analysis.descriptors.values().any(|descriptor| {
                    descriptor.predicate_bindings.contains(relation.binding)
                        && descriptor.predicate_bindings.contains(*binding)
                })
            }) {
                return Err(RelationalJoinEnumerationError::AccessWithoutPredicate {
                    relation: relation.binding,
                    binding,
                }
                .into());
            }
        }
    }
    Ok(())
}

fn binding_union(left: &BindingSet, right: &BindingSet) -> BindingSet {
    left.iter().chain(right.iter()).collect()
}

fn binding_intersection(left: &BindingSet, right: &BindingSet) -> BindingSet {
    left.iter()
        .filter(|binding| right.contains(*binding))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationalAccessPathDescriptor, RelationalAccessPathKind};
    use skein_expression::{BoundScalarExpression, ScalarNullability};
    use std::collections::BTreeSet;

    const A: BindingId = BindingId::new(1);
    const B: BindingId = BindingId::new(2);
    const C: BindingId = BindingId::new(3);

    fn value(binding: BindingId) -> BoundScalarExpression {
        BoundScalarExpression::BindingValue {
            binding,
            nullability: ScalarNullability::MaybeNull,
        }
    }

    fn comparison(left: BindingId, right: BindingId) -> BoundPredicate {
        BoundPredicate::Comparison {
            left: value(left),
            right: value(right),
        }
    }

    fn operator(
        id: u32,
        kind: RelationalJoinOperatorKind,
        left: BindingId,
        right: BindingId,
    ) -> RelationalJoinOperator {
        RelationalJoinOperator {
            id: RelationalJoinOperatorId::new(id),
            kind,
            predicate_ids: vec![RelationalJoinPredicateId::new(id)],
            predicate: comparison(left, right),
        }
    }

    fn scan(rows: usize) -> RelationalJoinAccessPath {
        RelationalJoinAccessPath::base_and_probe(RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: rows,
        })
    }

    fn probe(name: &str, required: BindingId, rows: usize) -> RelationalJoinAccessPath {
        RelationalJoinAccessPath::probe(
            RelationalAccessPathDescriptor {
                kind: RelationalAccessPathKind::Index,
                name: name.to_string(),
                index_columns: vec!["id".to_string()],
                access_columns: BTreeSet::from(["id".to_string()]),
                equality_prefix_len: 1,
                order_prefix_len: 0,
                exclusive_range: false,
                reverse_order: false,
                unique_point: rows == 1,
                covering: false,
                requires_row_fetch: true,
                estimated_rows: rows,
            },
            required.into(),
        )
    }

    fn relation(
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

    fn left_then_inner_tree(top_left: BindingId) -> RelationalJoinTree {
        RelationalJoinTree::join(
            operator(2, RelationalJoinOperatorKind::Inner, top_left, C),
            RelationalJoinTree::join(
                operator(1, RelationalJoinOperatorKind::LeftOuter, A, B),
                RelationalJoinTree::Relation(A),
                RelationalJoinTree::Relation(B),
            ),
            RelationalJoinTree::Relation(C),
        )
    }

    fn problem(
        tree: RelationalJoinTree,
        filter: Option<BoundPredicate>,
        top_left: BindingId,
    ) -> RelationalJoinRewriteProblem {
        let mut a_probes = vec![probe("a_by_b", B, 1)];
        let mut b_probes = vec![probe("b_by_a", A, 1)];
        let c_probes = if top_left == A {
            a_probes.push(probe("a_by_c", C, 1));
            vec![probe("c_by_a", A, 1)]
        } else {
            b_probes.push(probe("b_by_c", C, 1));
            vec![probe("c_by_b", B, 1)]
        };
        RelationalJoinRewriteProblem {
            relations: vec![
                relation(A, 1_000, a_probes),
                relation(B, 100, b_probes),
                relation(C, 1, c_probes),
            ],
            initial_tree: tree,
            post_join_filter: filter,
        }
    }

    #[test]
    fn cdc_blocks_non_associative_left_outer_rewrite_before_memo_insertion() {
        let problem = problem(left_then_inner_tree(B), None, B);
        let result = enumerate_relational_join_rewrites(
            &problem,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.plan.binding_order(), [A, B, C]);
        let top = result
            .conflict_analysis
            .descriptor(RelationalJoinOperatorId::new(2))
            .unwrap();
        assert_eq!(top.left_total_eligibility, BindingSet::from([A, B]));
        assert!(top.conflict_rules.is_empty());
        assert!(!top.is_applicable(&B.into(), &C.into()));
    }

    #[test]
    fn cdc_allows_left_asscom_when_the_top_predicate_uses_the_preserved_side() {
        let problem = problem(left_then_inner_tree(A), None, A);
        let result = enumerate_relational_join_rewrites(
            &problem,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.plan.binding_order(), [C, A, B]);
        assert_eq!(
            result.plan.cost(),
            PlanCost {
                estimated_rows: 1,
                cost: 21,
            }
        );
        assert_eq!(
            result.plan.steps[1].operator_kind,
            RelationalJoinOperatorKind::LeftOuter
        );
    }

    #[test]
    fn proven_post_filter_converts_left_outer_before_conflict_detection() {
        let filter = BoundPredicate::IsNotNull(value(B));
        let problem = problem(left_then_inner_tree(B), Some(filter), B);
        let result = enumerate_relational_join_rewrites(
            &problem,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        let lower = result
            .conflict_analysis
            .descriptor(RelationalJoinOperatorId::new(1))
            .unwrap();
        assert!(lower.was_null_rejection_simplified());
        assert_eq!(result.plan.binding_order(), [C, B, A]);
        assert!(result
            .plan
            .steps
            .iter()
            .all(|step| step.operator_kind == RelationalJoinOperatorKind::Inner));
    }

    #[test]
    fn opaque_post_filter_cannot_authorize_left_outer_conversion() {
        let filter = BoundPredicate::Opaque {
            referenced_bindings: B.into(),
        };
        let problem = problem(left_then_inner_tree(B), Some(filter), B);
        let result = enumerate_relational_join_rewrites(
            &problem,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        let lower = result
            .conflict_analysis
            .descriptor(RelationalJoinOperatorId::new(1))
            .unwrap();
        assert!(!lower.was_null_rejection_simplified());
        assert_eq!(result.plan.binding_order(), [A, B, C]);
    }

    #[test]
    fn null_rejection_uses_the_whole_nullable_subtree_binding_set() {
        let right = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, B, C),
            RelationalJoinTree::Relation(B),
            RelationalJoinTree::Relation(C),
        );
        let tree = RelationalJoinTree::join(
            RelationalJoinOperator {
                id: RelationalJoinOperatorId::new(2),
                kind: RelationalJoinOperatorKind::LeftOuter,
                predicate_ids: vec![RelationalJoinPredicateId::new(2)],
                predicate: comparison(A, B),
            },
            RelationalJoinTree::Relation(A),
            right,
        );
        let filter = BoundPredicate::And(vec![
            BoundPredicate::IsNotNull(value(B)),
            BoundPredicate::IsNotNull(value(C)),
        ]);
        let analysis = analyze_relational_join_conflicts(&tree, Some(&filter)).unwrap();

        let outer = analysis
            .descriptor(RelationalJoinOperatorId::new(2))
            .unwrap();
        assert_eq!(outer.initial_right_bindings, BindingSet::from([B, C]));
        assert!(outer.was_null_rejection_simplified());
    }
}
