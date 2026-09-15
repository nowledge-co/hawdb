use super::super::physical::{
    hash_join_inputs, materialized_equi_join_selectivity, materialized_join_index_access,
    merge_join_inputs, RelationalEquiJoinKeys,
};
use super::*;
use crate::index_runtime::RelationalIndexReadMode;
use skein_optimizer::{RelationalCsgCmpJoinImplementation, RelationalEquiJoinAlgorithm};

pub(super) struct PreparedJoinImplementation {
    pub(super) optimizer: RelationalCsgCmpJoinImplementation,
    left: RelationalAccessCandidate,
    right: RelationalAccessCandidate,
    keys: RelationalEquiJoinKeys,
}

impl PreparedJoinImplementation {
    pub(super) fn physical_node(
        &self,
        operator_id: RelationalOperatorId,
        relations: &[BoundRelation<'_>],
        predicates: &BTreeMap<RelationalJoinPredicateId, SqlPredicate>,
    ) -> Result<RelationalPhysicalJoinNode> {
        let left = physical_relation(relations, self.optimizer.left_binding, self.left.clone());
        let right = physical_relation(relations, self.optimizer.right_binding, self.right.clone());
        let predicates = self
            .optimizer
            .predicate_ids
            .iter()
            .map(|id| {
                predicates.get(id).cloned().ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "CSG-CMP implementation selected unknown predicate {}",
                        id.get()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        match self.optimizer.algorithm {
            RelationalEquiJoinAlgorithm::Hash => RelationalPhysicalJoinNode::hash_join(
                operator_id,
                match self.optimizer.operator_kind {
                    RelationalJoinOperatorKind::Inner => SqlJoinKind::Inner,
                    RelationalJoinOperatorKind::LeftOuter => SqlJoinKind::Left,
                },
                predicates,
                self.keys.clone(),
                self.optimizer.selectivity,
                left,
                right,
            ),
            RelationalEquiJoinAlgorithm::Merge => RelationalPhysicalJoinNode::merge_join(
                operator_id,
                predicates,
                self.keys.clone(),
                self.optimizer.selectivity,
                left,
                right,
            ),
        }
    }
}

fn physical_relation(
    relations: &[BoundRelation<'_>],
    binding: BindingId,
    access: RelationalAccessCandidate,
) -> RelationalPhysicalJoinNode {
    let relation = relation_by_binding(relations, binding);
    RelationalPhysicalJoinNode::relation(
        binding,
        relation.table.name.clone(),
        relation.qualifier.clone(),
        RelationalPhysicalAccess::Base(access),
    )
}

pub(super) fn prepare_join_implementations(
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, impl RelationalQueryStoreReader>,
    relations: &[BoundRelation<'_>],
    prepared: &[PreparedGraphRelation],
    joins: &BoundJoinInputs,
    max_implementations: usize,
) -> std::result::Result<Vec<PreparedJoinImplementation>, RelationalJoinEnumerationError> {
    let mut implementations = Vec::new();
    for operator in &joins.operators {
        let predicates = joins
            .predicates
            .iter()
            .filter(|predicate| operator.operator.predicate_ids.contains(&predicate.id))
            .collect::<Vec<_>>();
        let bindings = predicates
            .iter()
            .flat_map(|predicate| predicate.bindings.iter())
            .collect::<BTreeSet<_>>();
        if bindings.len() != 2 {
            continue;
        }
        let predicate = combine_predicates(
            predicates
                .iter()
                .map(|predicate| predicate.predicate.clone()),
        );
        for right_binding in bindings.iter().copied() {
            if operator.operator.kind == RelationalJoinOperatorKind::LeftOuter
                && right_binding != operator.right_binding
            {
                continue;
            }
            let left_binding = *bindings
                .iter()
                .find(|binding| **binding != right_binding)
                .unwrap();
            let right_relation = relation_by_binding(relations, right_binding);
            let left_relation = relation_by_binding(relations, left_binding);
            let left_prepared = prepared
                .iter()
                .find(|relation| relation.optimizer_relation.binding == left_binding)
                .unwrap();
            let right_prepared = prepared
                .iter()
                .find(|relation| relation.optimizer_relation.binding == right_binding)
                .unwrap();
            let mut add = |algorithm,
                           left: RelationalAccessCandidate,
                           right: RelationalAccessCandidate,
                           keys: RelationalEquiJoinKeys| {
                let selectivity = materialized_equi_join_selectivity(
                    state,
                    read_modes.index,
                    &physical_relation(relations, left_binding, left.clone()),
                    &physical_relation(relations, right_binding, right.clone()),
                    &keys,
                );
                let candidate = PreparedJoinImplementation {
                    optimizer: RelationalCsgCmpJoinImplementation {
                        operator_id: operator.operator.id,
                        operator_kind: operator.operator.kind,
                        predicate_ids: operator.operator.predicate_ids.clone(),
                        left_binding,
                        left_access: RelationalJoinAccessPath::base(left.descriptor.clone()),
                        right_binding,
                        right_access: RelationalJoinAccessPath::base(right.descriptor.clone()),
                        algorithm,
                        selectivity,
                    },
                    left,
                    right,
                    keys,
                };
                if implementations
                    .iter()
                    .any(|existing: &PreparedJoinImplementation| {
                        existing.optimizer == candidate.optimizer
                    })
                {
                    return Ok(());
                }
                let required_expressions = implementations.len().saturating_add(1);
                if required_expressions > max_implementations {
                    return Err(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                        required_expressions,
                        max_expressions: max_implementations,
                    });
                }
                implementations.push(candidate);
                Ok(())
            };
            for (_, right) in &right_prepared.join_accesses {
                if let Some((right, keys)) = hash_join_inputs(
                    &predicate,
                    right,
                    &right_relation.table.name,
                    &right_relation.qualifier,
                ) {
                    for (_, left) in &left_prepared.base_accesses {
                        add(
                            RelationalEquiJoinAlgorithm::Hash,
                            left.clone(),
                            right.clone(),
                            keys.clone(),
                        )?;
                    }
                }
                if operator.operator.kind == RelationalJoinOperatorKind::Inner
                    && matches!(
                        read_modes.index,
                        RelationalIndexReadMode::Materialized | RelationalIndexReadMode::Shadow(_)
                    )
                {
                    // Full forward index scans are standalone inputs, not correlated probes.
                    let ordered = left_prepared
                        .join_accesses
                        .iter()
                        .filter_map(|(_, access)| {
                            materialized_join_index_access(state, access, &left_relation.table.name)
                        });
                    for left in left_prepared
                        .base_accesses
                        .iter()
                        .map(|(_, access)| access.clone())
                        .chain(ordered)
                    {
                        if let Some((right, keys)) =
                            merge_join_inputs(state, &left, right, &right_relation.table.name)
                        {
                            // An index chosen for a different operator must not supply its keys here.
                            let mut equalities = BTreeMap::new();
                            super::super::collect_conjunctive_join_equalities(
                                &predicate,
                                &right_relation.table.name,
                                &right_relation.qualifier,
                                &mut equalities,
                            );
                            if keys
                                .columns
                                .iter()
                                .all(|(column, outer)| equalities.get(column) == Some(outer))
                            {
                                add(RelationalEquiJoinAlgorithm::Merge, left, right, keys)?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(implementations)
}
