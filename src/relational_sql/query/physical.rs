use super::{
    collect_conjunctive_join_equalities, estimate_relational_access_cost,
    estimate_relational_join_cost, estimate_relational_probe_join_cost,
    predicate_is_covered_by_access, projection_contains_aggregate, resolved_access_order_by,
    AdmittedRelationalExecution, Arc, BTreeMap, BTreeSet, BindingId, BoundRow, PlanCostBreakdown,
    QueryMemoryLedger, RelationalAccessPathDescriptor, RelationalAccessPathKind,
    RelationalFieldPlan, RelationalIndexRangeScan, RelationalIndexReadMode,
    RelationalIndexScanDirection, RelationalJoinCardinality, RelationalJoinPlanningOutcome,
    RelationalJoinRightInput, RelationalJoinSelectivity, RelationalKey,
    RelationalOperatorCardinalityProfile, RelationalOperatorId, RelationalOperatorKind,
    RelationalQueryReadModes, RelationalQueryResourceContext, RelationalSqlStageTimings,
    RelationalState, Result, SelectStatement, SkeinError, SqlColumnRef, SqlJoinKind, SqlPredicate,
};

#[derive(Debug, Clone)]
pub(super) enum RelationalBaseAccess {
    PrimaryKey(RelationalKey),
    Index {
        name: String,
        scan: RelationalIndexRangeScan,
    },
    FullScan,
}

#[derive(Debug, Clone)]
pub(super) enum RelationalJoinAccess {
    PrimaryKey(Vec<(String, SqlColumnRef)>),
    Index {
        name: String,
        columns: Vec<(String, SqlColumnRef)>,
    },
    FullScan,
}

#[derive(Debug, Clone)]
pub(super) struct RelationalAccessCandidate {
    pub(super) descriptor: RelationalAccessPathDescriptor,
    pub(super) access: RelationalBaseAccess,
}

#[derive(Debug, Clone)]
pub(super) struct RelationalJoinAccessCandidate {
    pub(super) descriptor: RelationalAccessPathDescriptor,
    pub(super) access: RelationalJoinAccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparedRelationalJoinSelection {
    pub(super) base_binding: BindingId,
    pub(super) join_bindings: Vec<BindingId>,
    pub(super) cost_breakdown: PlanCostBreakdown,
}

#[derive(Debug, Clone)]
pub(super) enum RelationalPhysicalAccess {
    Base(RelationalAccessCandidate),
    Probe(RelationalJoinAccessCandidate),
}

impl RelationalPhysicalAccess {
    pub(super) fn descriptor(&self) -> &RelationalAccessPathDescriptor {
        match self {
            Self::Base(access) => &access.descriptor,
            Self::Probe(access) => &access.descriptor,
        }
    }

    pub(super) fn descriptor_mut(&mut self) -> &mut RelationalAccessPathDescriptor {
        match self {
            Self::Base(access) => &mut access.descriptor,
            Self::Probe(access) => &mut access.descriptor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationalPhysicalOutputBinding {
    pub(super) binding: BindingId,
    pub(super) table: String,
    pub(super) qualifier: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RelationalPhysicalOutputSchema {
    pub(super) bindings: Arc<[RelationalPhysicalOutputBinding]>,
}

impl RelationalPhysicalOutputSchema {
    pub(super) fn relation(binding: BindingId, table: &str, qualifier: &str) -> Self {
        Self {
            bindings: vec![RelationalPhysicalOutputBinding {
                binding,
                table: table.to_string(),
                qualifier: qualifier.to_string(),
            }]
            .into(),
        }
    }

    pub(super) fn join(left: &Self, right: &Self) -> Result<Self> {
        let mut bindings =
            Vec::with_capacity(left.bindings.len().saturating_add(right.bindings.len()));
        bindings.extend(left.bindings.iter().cloned());
        bindings.extend(right.bindings.iter().cloned());
        let mut seen = BTreeSet::new();
        if let Some(duplicate) = bindings
            .iter()
            .find_map(|binding| (!seen.insert(binding.binding)).then_some(binding.binding))
        {
            return Err(SkeinError::Execution(format!(
                "physical join output schema repeats binding {}",
                duplicate.get()
            )));
        }
        Ok(Self {
            bindings: bindings.into(),
        })
    }

    pub(super) fn ensure_matches(&self, row: &BoundRow<'_>) -> Result<()> {
        if self.bindings.len() != row.bindings.len() {
            return Err(SkeinError::Execution(format!(
                "physical join output schema has {} bindings but executor produced {}",
                self.bindings.len(),
                row.bindings.len()
            )));
        }
        if let Some((expected, actual)) =
            self.bindings
                .iter()
                .zip(&row.bindings)
                .find(|(expected, actual)| {
                    expected.binding != actual.binding
                        || expected.table != actual.table
                        || expected.qualifier != actual.qualifier
                })
        {
            return Err(SkeinError::Execution(format!(
                "physical join output schema binding {} is {}, but executor produced {}",
                expected.binding.get(),
                expected.qualifier,
                actual.qualifier
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelationalPhysicalJoinAlgorithm {
    Probe,
    BatchedIndex,
    Merge,
    Hash,
    Materialized,
}

#[derive(Debug, Clone)]
pub(super) struct RelationalEquiJoinKeys {
    pub(super) columns: Vec<(String, SqlColumnRef)>,
}

#[derive(Debug, Clone)]
pub(super) struct RelationalPhysicalRelation {
    pub(super) binding: BindingId,
    pub(super) table: String,
    pub(super) qualifier: String,
    pub(super) access: RelationalPhysicalAccess,
    pub(super) output_schema: RelationalPhysicalOutputSchema,
}

impl RelationalPhysicalRelation {
    pub(super) fn new(
        binding: BindingId,
        table: String,
        qualifier: String,
        access: RelationalPhysicalAccess,
    ) -> Self {
        let output_schema = RelationalPhysicalOutputSchema::relation(binding, &table, &qualifier);
        Self {
            binding,
            table,
            qualifier,
            access,
            output_schema,
        }
    }

    pub(super) fn supports_batched_index_probe(&self) -> bool {
        matches!(
            &self.access,
            RelationalPhysicalAccess::Probe(candidate)
                if matches!(
                    candidate.access,
                    RelationalJoinAccess::PrimaryKey(_) | RelationalJoinAccess::Index { .. }
                )
        )
    }
}

#[derive(Debug, Clone)]
pub(super) enum RelationalPhysicalJoinNode {
    Relation(RelationalPhysicalRelation),
    Join {
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        algorithm: RelationalPhysicalJoinAlgorithm,
        equi_join_keys: Option<RelationalEquiJoinKeys>,
        selectivity: RelationalJoinSelectivity,
        predicates: Vec<SqlPredicate>,
        left: Box<Self>,
        right: Box<Self>,
        output_schema: RelationalPhysicalOutputSchema,
    },
}

pub(super) struct RelationalPhysicalJoinSpec {
    pub(super) operator_id: RelationalOperatorId,
    pub(super) kind: SqlJoinKind,
    pub(super) algorithm: RelationalPhysicalJoinAlgorithm,
    pub(super) equi_join_keys: Option<RelationalEquiJoinKeys>,
    pub(super) selectivity: RelationalJoinSelectivity,
    pub(super) predicates: Vec<SqlPredicate>,
}

impl RelationalPhysicalJoinNode {
    pub(super) fn relation(
        binding: BindingId,
        table: String,
        qualifier: String,
        access: RelationalPhysicalAccess,
    ) -> Self {
        Self::Relation(RelationalPhysicalRelation::new(
            binding, table, qualifier, access,
        ))
    }

    pub(super) fn apply_index_coverage(
        &mut self,
        state: &RelationalState,
        fields: &RelationalFieldPlan,
    ) -> Result<()> {
        match self {
            Self::Relation(relation) => {
                let descriptor = relation.access.descriptor_mut();
                if descriptor.kind != RelationalAccessPathKind::Index {
                    return Ok(());
                }
                let schema = state.table_schema(&relation.table).ok_or_else(|| {
                    SkeinError::Semantic(format!("unknown relational table {}", relation.table))
                })?;
                let covering = fields.index_covers_table(
                    &relation.table,
                    schema,
                    &descriptor.index_columns,
                )?;
                descriptor.covering = covering;
                descriptor.requires_row_fetch = !covering;
                Ok(())
            }
            Self::Join { left, right, .. } => {
                left.apply_index_coverage(state, fields)?;
                right.apply_index_coverage(state, fields)
            }
        }
    }

    pub(super) fn join(
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        predicates: Vec<SqlPredicate>,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        let algorithm = match &right {
            Self::Relation(relation) if relation.supports_batched_index_probe() => {
                RelationalPhysicalJoinAlgorithm::BatchedIndex
            }
            Self::Relation(_) => RelationalPhysicalJoinAlgorithm::Probe,
            Self::Join { .. } => RelationalPhysicalJoinAlgorithm::Materialized,
        };
        Self::join_with_algorithm(
            RelationalPhysicalJoinSpec {
                operator_id,
                kind,
                algorithm,
                equi_join_keys: None,
                selectivity: RelationalJoinSelectivity::Unknown,
                predicates,
            },
            left,
            right,
        )
    }

    pub(super) fn merge_join(
        operator_id: RelationalOperatorId,
        predicates: Vec<SqlPredicate>,
        equi_join_keys: RelationalEquiJoinKeys,
        selectivity: RelationalJoinSelectivity,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        Self::join_with_algorithm(
            RelationalPhysicalJoinSpec {
                operator_id,
                kind: SqlJoinKind::Inner,
                algorithm: RelationalPhysicalJoinAlgorithm::Merge,
                equi_join_keys: Some(equi_join_keys),
                selectivity,
                predicates,
            },
            left,
            right,
        )
    }

    pub(super) fn hash_join(
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        predicates: Vec<SqlPredicate>,
        equi_join_keys: RelationalEquiJoinKeys,
        selectivity: RelationalJoinSelectivity,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        Self::join_with_algorithm(
            RelationalPhysicalJoinSpec {
                operator_id,
                kind,
                algorithm: RelationalPhysicalJoinAlgorithm::Hash,
                equi_join_keys: Some(equi_join_keys),
                selectivity,
                predicates,
            },
            left,
            right,
        )
    }

    pub(super) fn join_with_algorithm(
        spec: RelationalPhysicalJoinSpec,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        let RelationalPhysicalJoinSpec {
            operator_id,
            kind,
            algorithm,
            equi_join_keys,
            selectivity,
            predicates,
        } = spec;
        let output_schema =
            RelationalPhysicalOutputSchema::join(left.output_schema(), right.output_schema())?;
        Ok(Self::Join {
            operator_id,
            kind,
            algorithm,
            equi_join_keys,
            selectivity,
            predicates,
            left: Box::new(left),
            right: Box::new(right),
            output_schema,
        })
    }

    pub(super) fn output_schema(&self) -> &RelationalPhysicalOutputSchema {
        match self {
            Self::Relation(relation) => &relation.output_schema,
            Self::Join { output_schema, .. } => output_schema,
        }
    }

    pub(super) fn first_relation(&self) -> &RelationalPhysicalRelation {
        match self {
            Self::Relation(relation) => relation,
            Self::Join { left, .. } => left.first_relation(),
        }
    }

    pub(super) fn visit_relations<'a>(
        &'a self,
        visit: &mut impl FnMut(&'a RelationalPhysicalRelation),
    ) {
        match self {
            Self::Relation(relation) => visit(relation),
            Self::Join { left, right, .. } => {
                left.visit_relations(visit);
                right.visit_relations(visit);
            }
        }
    }

    pub(super) fn relation_count(&self) -> usize {
        let mut count = 0usize;
        self.visit_relations(&mut |_| count = count.saturating_add(1));
        count
    }

    pub(super) fn visit_join_right_relations<'a>(
        &'a self,
        visit: &mut impl FnMut(&'a RelationalPhysicalRelation),
    ) {
        if let Self::Join { left, right, .. } = self {
            left.visit_join_right_relations(visit);
            right.visit_join_right_relations(visit);
            visit(right.first_relation());
        }
    }

    pub(super) fn materialized_right_count(&self) -> usize {
        match self {
            Self::Relation(_) => 0,
            Self::Join {
                algorithm,
                left,
                right,
                ..
            } => usize::from(matches!(
                algorithm,
                RelationalPhysicalJoinAlgorithm::Merge
                    | RelationalPhysicalJoinAlgorithm::Hash
                    | RelationalPhysicalJoinAlgorithm::Materialized
            ))
            .saturating_add(left.materialized_right_count())
            .saturating_add(right.materialized_right_count()),
        }
    }

    pub(super) fn batched_probe_depth(&self) -> usize {
        match self {
            Self::Relation(_) => 0,
            Self::Join {
                algorithm,
                left,
                right,
                ..
            } => usize::from(*algorithm == RelationalPhysicalJoinAlgorithm::BatchedIndex)
                .saturating_add(left.batched_probe_depth())
                .saturating_add(right.batched_probe_depth()),
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        match self {
            Self::Relation(relation) => {
                let expected = RelationalPhysicalOutputSchema::relation(
                    relation.binding,
                    &relation.table,
                    &relation.qualifier,
                );
                if relation.output_schema != expected {
                    return Err(SkeinError::Execution(format!(
                        "physical relation {} has an inconsistent output schema",
                        relation.qualifier
                    )));
                }
                Ok(())
            }
            Self::Join {
                kind,
                algorithm,
                equi_join_keys,
                left,
                right,
                output_schema,
                ..
            } => {
                left.validate()?;
                right.validate()?;
                let expected_algorithm = match right.as_ref() {
                    Self::Relation(relation) if relation.supports_batched_index_probe() => {
                        RelationalPhysicalJoinAlgorithm::BatchedIndex
                    }
                    Self::Relation(_) => RelationalPhysicalJoinAlgorithm::Probe,
                    Self::Join { .. } => RelationalPhysicalJoinAlgorithm::Materialized,
                };
                match algorithm {
                    RelationalPhysicalJoinAlgorithm::Merge => {
                        if *kind != SqlJoinKind::Inner {
                            return Err(SkeinError::Execution(
                                "merge join supports inner joins only".to_string(),
                            ));
                        }
                        let (Self::Relation(left), Self::Relation(right)) =
                            (left.as_ref(), right.as_ref())
                        else {
                            return Err(SkeinError::Execution(
                                "merge join requires two relation inputs".to_string(),
                            ));
                        };
                        if !matches!(
                            left.access,
                            RelationalPhysicalAccess::Base(RelationalAccessCandidate {
                                access: RelationalBaseAccess::Index { .. },
                                ..
                            })
                        ) || !matches!(
                            right.access,
                            RelationalPhysicalAccess::Base(RelationalAccessCandidate {
                                access: RelationalBaseAccess::Index { .. },
                                ..
                            })
                        ) || equi_join_keys
                            .as_ref()
                            .is_none_or(|keys| keys.columns.is_empty())
                        {
                            return Err(SkeinError::Execution(
                                "merge join has incompatible ordered inputs".to_string(),
                            ));
                        }
                    }
                    RelationalPhysicalJoinAlgorithm::Hash => {
                        if !matches!(kind, SqlJoinKind::Inner | SqlJoinKind::Left) {
                            return Err(SkeinError::Execution(
                                "hash join supports inner and left joins only".to_string(),
                            ));
                        }
                        let (Self::Relation(left), Self::Relation(right)) =
                            (left.as_ref(), right.as_ref())
                        else {
                            return Err(SkeinError::Execution(
                                "hash join requires two relation inputs".to_string(),
                            ));
                        };
                        if !matches!(left.access, RelationalPhysicalAccess::Base(_))
                            || !matches!(
                                right.access,
                                RelationalPhysicalAccess::Base(RelationalAccessCandidate {
                                    access: RelationalBaseAccess::FullScan,
                                    ..
                                })
                            )
                            || equi_join_keys
                                .as_ref()
                                .is_none_or(|keys| keys.columns.is_empty())
                        {
                            return Err(SkeinError::Execution(
                                "hash join has incompatible build input".to_string(),
                            ));
                        }
                    }
                    _ if *algorithm != expected_algorithm || equi_join_keys.is_some() => {
                        return Err(SkeinError::Execution(
                            "physical join algorithm disagrees with its right input".to_string(),
                        ));
                    }
                    _ => {}
                }
                let expected_schema = RelationalPhysicalOutputSchema::join(
                    left.output_schema(),
                    right.output_schema(),
                )?;
                if *output_schema != expected_schema {
                    return Err(SkeinError::Execution(
                        "physical join has an inconsistent output schema".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct RelationalPhysicalJoinPlan {
    pub(super) root: RelationalPhysicalJoinNode,
    pub(super) cost_breakdown: PlanCostBreakdown,
    pub(super) output_schema: RelationalPhysicalOutputSchema,
}

impl RelationalPhysicalJoinPlan {
    pub(super) fn new(root: RelationalPhysicalJoinNode, cost_breakdown: PlanCostBreakdown) -> Self {
        let output_schema = root.output_schema().clone();
        Self {
            root,
            cost_breakdown,
            output_schema,
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.root.validate()?;
        if self.output_schema != *self.root.output_schema() {
            return Err(SkeinError::Execution(
                "physical join plan has an inconsistent root output schema".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn apply_index_coverage(
        &mut self,
        state: &RelationalState,
        fields: &RelationalFieldPlan,
    ) -> Result<()> {
        self.root.apply_index_coverage(state, fields)
    }
}

#[derive(Debug)]
pub(super) struct PreparedRelationalAccessPlan {
    pub(super) base_access: RelationalAccessCandidate,
    pub(super) join_accesses: Vec<RelationalJoinAccessCandidate>,
    pub(super) join_selection: Option<PreparedRelationalJoinSelection>,
    pub(super) physical_join_plan: Option<RelationalPhysicalJoinPlan>,
}

pub(super) fn merge_join_inputs(
    state: &RelationalState,
    base: &RelationalAccessCandidate,
    right: &RelationalJoinAccessCandidate,
    right_table: &str,
) -> Option<(RelationalAccessCandidate, RelationalEquiJoinKeys)> {
    let RelationalBaseAccess::Index { scan, .. } = &base.access else {
        return None;
    };
    let RelationalJoinAccess::Index { columns, .. } = &right.access else {
        return None;
    };
    if columns.is_empty()
        || base.descriptor.kind != RelationalAccessPathKind::Index
        || base.descriptor.reverse_order
        || scan.direction != RelationalIndexScanDirection::Forward
        || right.descriptor.kind != RelationalAccessPathKind::Index
        || right.descriptor.reverse_order
        || right.descriptor.index_columns.len() < columns.len()
        || right.descriptor.index_columns[..columns.len()]
            != columns
                .iter()
                .map(|(column, _)| column.clone())
                .collect::<Vec<_>>()
    {
        return None;
    }
    let left_columns = columns
        .iter()
        .map(|(_, column)| column.name.clone())
        .collect::<Vec<_>>();
    let left_order_start = base.descriptor.equality_prefix_len;
    if base.descriptor.index_columns.len() < left_order_start.saturating_add(left_columns.len())
        || base.descriptor.index_columns[left_order_start..]
            .iter()
            .take(left_columns.len())
            .ne(left_columns.iter())
    {
        return None;
    }
    let right_base = materialized_join_index_access(state, right, right_table)?;
    Some((
        right_base,
        RelationalEquiJoinKeys {
            columns: columns.clone(),
        },
    ))
}

pub(super) fn materialized_join_index_access(
    state: &RelationalState,
    right: &RelationalJoinAccessCandidate,
    right_table: &str,
) -> Option<RelationalAccessCandidate> {
    let RelationalJoinAccess::Index { name, columns } = &right.access else {
        return None;
    };
    Some(RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: right.descriptor.index_columns.clone(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: columns.len(),
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: right.descriptor.covering,
            requires_row_fetch: right.descriptor.requires_row_fetch,
            estimated_rows: state.row_count(right_table).max(1),
        },
        access: RelationalBaseAccess::Index {
            name: name.clone(),
            scan: RelationalIndexRangeScan {
                prefix: RelationalKey(Vec::new()),
                exclusive_bound: None,
                direction: RelationalIndexScanDirection::Forward,
            },
        },
    })
}

pub(super) fn hash_join_inputs(
    predicate: &SqlPredicate,
    right: &RelationalJoinAccessCandidate,
    right_table: &str,
    right_qualifier: &str,
) -> Option<(RelationalAccessCandidate, RelationalEquiJoinKeys)> {
    if !matches!(right.access, RelationalJoinAccess::FullScan) {
        return None;
    }
    let mut columns = BTreeMap::new();
    collect_conjunctive_join_equalities(predicate, right_table, right_qualifier, &mut columns);
    if columns.is_empty() {
        return None;
    }
    Some((
        RelationalAccessCandidate {
            descriptor: right.descriptor.clone(),
            access: RelationalBaseAccess::FullScan,
        },
        RelationalEquiJoinKeys {
            columns: columns.into_iter().collect(),
        },
    ))
}

pub(super) fn materialized_equi_join_selectivity(
    state: &RelationalState,
    index_read_mode: RelationalIndexReadMode<'_>,
    left: &RelationalPhysicalJoinNode,
    right: &RelationalPhysicalJoinNode,
    keys: &RelationalEquiJoinKeys,
) -> RelationalJoinSelectivity {
    let (RelationalPhysicalJoinNode::Relation(left), RelationalPhysicalJoinNode::Relation(right)) =
        (left, right)
    else {
        return RelationalJoinSelectivity::Unknown;
    };
    let left_columns = keys
        .columns
        .iter()
        .map(|(_, column)| column.name.clone())
        .collect::<Vec<_>>();
    let right_columns = keys
        .columns
        .iter()
        .map(|(column, _)| column.clone())
        .collect::<Vec<_>>();
    RelationalJoinSelectivity::equi_join(
        relational_join_distinct_values(state, index_read_mode, left, &left_columns),
        relational_join_distinct_values(state, index_read_mode, right, &right_columns),
    )
}

pub(super) fn relational_join_distinct_values(
    state: &RelationalState,
    index_read_mode: RelationalIndexReadMode<'_>,
    relation: &RelationalPhysicalRelation,
    columns: &[String],
) -> Option<u64> {
    if columns.is_empty() {
        return None;
    }
    let schema = state.table_schema(&relation.table)?;
    let requested_columns = columns.iter().collect::<BTreeSet<_>>();
    // Planning must not scan relational rows to manufacture NDV. Use a fresh
    // persisted prefix statistic when available, or the exact cardinality of
    // a complete non-null unique key; otherwise retain the cost model's
    // documented fallback.
    for definition in schema.required_index_definitions() {
        if definition.columns.len() < columns.len()
            || definition.columns[..columns.len()]
                .iter()
                .collect::<BTreeSet<_>>()
                != requested_columns
        {
            continue;
        }
        if let Some(statistics) =
            index_read_mode.probe_statistics(&relation.table, &definition.name, columns.len())
        {
            return Some(statistics.distinct_non_null_values);
        }
        let complete_non_null_unique_key = definition.role.is_unique()
            && definition.columns.len() == columns.len()
            && columns.iter().all(|column| {
                schema
                    .column_position(column)
                    .is_some_and(|position| !schema.columns[position].nullable)
            });
        if complete_non_null_unique_key {
            return Some(u64::try_from(state.row_count(&relation.table)).unwrap_or(u64::MAX));
        }
    }
    None
}

impl PreparedRelationalAccessPlan {
    pub(super) fn apply_physical_index_coverage(
        &mut self,
        state: &RelationalState,
        fields: &RelationalFieldPlan,
    ) -> Result<()> {
        self.physical_join_plan
            .as_mut()
            .ok_or_else(|| {
                SkeinError::Execution(
                    "cannot apply relational index coverage before physical planning".to_string(),
                )
            })?
            .apply_index_coverage(state, fields)
    }

    pub(super) fn finalize_physical_join_plan(
        &mut self,
        statement: &SelectStatement,
        state: &RelationalState,
        index_read_mode: RelationalIndexReadMode<'_>,
    ) -> Result<()> {
        if self.physical_join_plan.is_some() {
            return Ok(());
        }
        if matches!(
            index_read_mode,
            RelationalIndexReadMode::Materialized | RelationalIndexReadMode::Shadow(_)
        ) && self.join_selection.is_none()
            && statement.joins.len() == 1
            && statement.joins[0].kind == SqlJoinKind::Inner
            && let Some((right_access, merge_keys)) = merge_join_inputs(
                state,
                &self.base_access,
                &self.join_accesses[0],
                &statement.joins[0].table.name,
            )
        {
            let base_qualifier = statement
                .from_alias
                .as_deref()
                .unwrap_or(statement.from.name.as_str());
            let join = &statement.joins[0];
            let right_qualifier = join.alias.as_deref().unwrap_or(join.table.name.as_str());
            let left = RelationalPhysicalJoinNode::relation(
                BindingId::new(0),
                statement.from.name.clone(),
                base_qualifier.to_string(),
                RelationalPhysicalAccess::Base(self.base_access.clone()),
            );
            let right = RelationalPhysicalJoinNode::relation(
                BindingId::new(1),
                join.table.name.clone(),
                right_qualifier.to_string(),
                RelationalPhysicalAccess::Base(right_access.clone()),
            );
            let selectivity = materialized_equi_join_selectivity(
                state,
                index_read_mode,
                &left,
                &right,
                &merge_keys,
            );
            let cost = estimate_relational_join_cost(
                estimate_relational_access_cost(self.base_access.descriptor.estimated_rows),
                estimate_relational_access_cost(right_access.descriptor.estimated_rows),
                RelationalJoinCardinality::Inner,
                RelationalJoinRightInput::Merge,
                selectivity,
            );
            let root = RelationalPhysicalJoinNode::merge_join(
                RelationalOperatorId::from_plan_index(1),
                vec![join.on.clone()],
                merge_keys,
                selectivity,
                left,
                right,
            )?;
            self.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost));
            return Ok(());
        }
        if self.join_selection.is_none()
            && statement.joins.len() == 1
            && matches!(
                statement.joins[0].kind,
                SqlJoinKind::Inner | SqlJoinKind::Left
            )
            && let join = &statement.joins[0]
            && let Some((right_access, equi_join_keys)) = hash_join_inputs(
                &join.on,
                &self.join_accesses[0],
                &join.table.name,
                join.alias.as_deref().unwrap_or(join.table.name.as_str()),
            )
        {
            let base_qualifier = statement
                .from_alias
                .as_deref()
                .unwrap_or(statement.from.name.as_str());
            let right_qualifier = join.alias.as_deref().unwrap_or(join.table.name.as_str());
            let left = RelationalPhysicalJoinNode::relation(
                BindingId::new(0),
                statement.from.name.clone(),
                base_qualifier.to_string(),
                RelationalPhysicalAccess::Base(self.base_access.clone()),
            );
            let right = RelationalPhysicalJoinNode::relation(
                BindingId::new(1),
                join.table.name.clone(),
                right_qualifier.to_string(),
                RelationalPhysicalAccess::Base(right_access.clone()),
            );
            let selectivity = materialized_equi_join_selectivity(
                state,
                index_read_mode,
                &left,
                &right,
                &equi_join_keys,
            );
            let cost = estimate_relational_join_cost(
                estimate_relational_access_cost(self.base_access.descriptor.estimated_rows),
                estimate_relational_access_cost(right_access.descriptor.estimated_rows),
                match join.kind {
                    SqlJoinKind::Inner => RelationalJoinCardinality::Inner,
                    SqlJoinKind::Left => RelationalJoinCardinality::PreserveLeft,
                },
                RelationalJoinRightInput::Hash,
                selectivity,
            );
            let root = RelationalPhysicalJoinNode::hash_join(
                RelationalOperatorId::from_plan_index(1),
                join.kind,
                vec![join.on.clone()],
                equi_join_keys,
                selectivity,
                left,
                right,
            )?;
            self.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost));
            return Ok(());
        }
        let selection = self.join_selection.as_ref();
        let base_binding = selection.map_or(BindingId::new(0), |selection| selection.base_binding);
        let base_qualifier = statement
            .from_alias
            .as_deref()
            .unwrap_or(statement.from.name.as_str());
        let mut root = RelationalPhysicalJoinNode::relation(
            base_binding,
            statement.from.name.clone(),
            base_qualifier.to_string(),
            RelationalPhysicalAccess::Base(self.base_access.clone()),
        );
        let mut cost = estimate_relational_access_cost(self.base_access.descriptor.estimated_rows);
        for (index, (join, access)) in statement.joins.iter().zip(&self.join_accesses).enumerate() {
            let binding = if let Some(selection) = selection {
                *selection.join_bindings.get(index).ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "prepared relational join selection has no binding for join {}",
                        index.saturating_add(1)
                    ))
                })?
            } else {
                let binding = u32::try_from(index.saturating_add(1)).map_err(|_| {
                    SkeinError::Execution(
                        "relational physical join plan exceeds the binding-id range".to_string(),
                    )
                })?;
                BindingId::new(binding)
            };
            let qualifier = join.alias.as_deref().unwrap_or(join.table.name.as_str());
            let right = RelationalPhysicalJoinNode::relation(
                binding,
                join.table.name.clone(),
                qualifier.to_string(),
                RelationalPhysicalAccess::Probe(access.clone()),
            );
            root = RelationalPhysicalJoinNode::join(
                RelationalOperatorId::from_plan_index(index.saturating_add(1)),
                join.kind,
                vec![join.on.clone()],
                root,
                right,
            )?;
            cost = estimate_relational_probe_join_cost(
                cost,
                access.descriptor.estimated_rows,
                match join.kind {
                    SqlJoinKind::Inner => RelationalJoinCardinality::Inner,
                    SqlJoinKind::Left => RelationalJoinCardinality::PreserveLeft,
                },
            );
        }
        let cost_breakdown = selection.map_or(cost, |selection| selection.cost_breakdown);
        self.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost_breakdown));
        Ok(())
    }

    pub(super) fn physical_join_plan(&self) -> Result<&RelationalPhysicalJoinPlan> {
        self.physical_join_plan.as_ref().ok_or_else(|| {
            SkeinError::Execution(
                "prepared relational SELECT has no finalized physical join plan".to_string(),
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreparedRelationalExecutionMode {
    OrderedIndexProjection,
    StreamingProjection,
    BlockingProjection,
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RelationalExecutionMemoryShape {
    /// Maximum concurrently retained executor transfer batches.
    pub(super) pipeline_batch_count: usize,
    /// Maximum concurrently retained blocking operator states.
    pub(super) blocking_operator_count: usize,
}

impl RelationalExecutionMemoryShape {
    pub(super) fn estimated_bytes(self, memory: &skein_executor::ExecutionMemoryConfig) -> usize {
        self.pipeline_batch_count
            .saturating_mul(memory.batch_payload_bytes.get())
            .saturating_add(
                self.blocking_operator_count
                    .saturating_mul(memory.blocking_operator_bytes.get()),
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparedRelationalExecutionDescriptor {
    /// The execution path selected during preparation. Execution must not
    /// independently infer a different path from the SQL statement.
    pub(super) mode: PreparedRelationalExecutionMode,
    /// Plan-derived peak shape resolved against runtime memory ceilings during
    /// admission.
    pub(super) memory_shape: RelationalExecutionMemoryShape,
}

impl PreparedRelationalExecutionDescriptor {
    pub(super) fn prepare(
        select: &SelectStatement,
        access_plan: &PreparedRelationalAccessPlan,
    ) -> Result<Self> {
        let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
        let access_order_by = resolved_access_order_by(select)?;
        let ordered_index_projection = !select.order_by.is_empty()
            && access_order_by.len() == select.order_by.len()
            && access_plan.base_access.descriptor.order_prefix_len == access_order_by.len()
            && select.joins.is_empty()
            && !select.distinct
            && !has_aggregate
            && select.group_by.is_empty()
            && predicate_is_covered_by_access(
                select.selection.as_ref(),
                &access_plan.base_access.descriptor,
                &access_order_by,
                &select.from.name,
                select.from_alias.as_deref().unwrap_or(&select.from.name),
            );
        let mode = if ordered_index_projection {
            PreparedRelationalExecutionMode::OrderedIndexProjection
        } else if has_aggregate || !select.group_by.is_empty() {
            PreparedRelationalExecutionMode::Aggregate
        } else if !select.order_by.is_empty() || select.distinct {
            PreparedRelationalExecutionMode::BlockingProjection
        } else {
            PreparedRelationalExecutionMode::StreamingProjection
        };
        let blocking_operator_count = match mode {
            PreparedRelationalExecutionMode::OrderedIndexProjection
            | PreparedRelationalExecutionMode::StreamingProjection => 0,
            PreparedRelationalExecutionMode::BlockingProjection => {
                usize::from(select.distinct) + usize::from(!select.order_by.is_empty())
            }
            PreparedRelationalExecutionMode::Aggregate => {
                if !select.group_by.is_empty() {
                    2
                } else {
                    1
                }
            }
        }
        .saturating_add(
            access_plan
                .physical_join_plan
                .as_ref()
                .map_or(0, |tree| tree.root.materialized_right_count()),
        );
        Ok(Self {
            mode,
            memory_shape: RelationalExecutionMemoryShape {
                pipeline_batch_count: 1usize.saturating_add(
                    access_plan
                        .physical_join_plan
                        .as_ref()
                        .map_or(0, |tree| tree.root.batched_probe_depth()),
                ),
                blocking_operator_count,
            },
        })
    }

    pub(super) fn admit<'state, 'runtime>(
        self,
        state: &'state RelationalState,
        read_modes: RelationalQueryReadModes<'state>,
        resources: RelationalQueryResourceContext<'runtime>,
    ) -> Result<AdmittedRelationalExecution<'state, 'runtime>> {
        skein_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        let query_memory_budget = crate::executor::enforced_query_memory_budget(
            resources.execution_memory,
            resources.task_context,
        )?;
        let estimated_bytes = self
            .memory_shape
            .estimated_bytes(resources.execution_memory);
        if estimated_bytes > query_memory_budget.get() {
            return Err(SkeinError::Execution(format!(
                "prepared relational query requires {estimated_bytes} estimated bytes, exceeding query_memory_bytes {query_memory_budget}"
            )));
        }
        Ok(AdmittedRelationalExecution {
            state,
            index_read_mode: read_modes.index,
            row_read_mode: read_modes.row,
            limits: resources.limits,
            execution_memory: resources.execution_memory,
            memory_ledger: QueryMemoryLedger::new(query_memory_budget),
            task_context: resources.task_context,
        })
    }
}

#[derive(Debug)]
pub(super) struct PreparedRelationalSelect {
    pub(super) statement: SelectStatement,
    pub(super) access_plan: PreparedRelationalAccessPlan,
    pub(super) join_planning: RelationalJoinPlanningOutcome,
    pub(super) execution: PreparedRelationalExecutionDescriptor,
    pub(super) stage_timings: RelationalSqlStageTimings,
}

impl PreparedRelationalSelect {
    pub(super) fn validate(&self) -> Result<()> {
        if self.statement.joins.len() != self.access_plan.join_accesses.len() {
            return Err(SkeinError::Execution(format!(
                "prepared relational SELECT has {} joins but {} join access paths",
                self.statement.joins.len(),
                self.access_plan.join_accesses.len()
            )));
        }
        if !base_access_matches_descriptor(&self.access_plan.base_access) {
            return Err(SkeinError::Execution(
                "prepared relational SELECT has an inconsistent base access path".to_string(),
            ));
        }
        if self
            .access_plan
            .join_accesses
            .iter()
            .any(|access| !join_access_matches_descriptor(access))
        {
            return Err(SkeinError::Execution(
                "prepared relational SELECT has an inconsistent join access path".to_string(),
            ));
        }
        let physical_plan = self.access_plan.physical_join_plan()?;
        if physical_plan.root.relation_count() != self.statement.joins.len().saturating_add(1) {
            return Err(SkeinError::Execution(format!(
                "physical relational join plan has {} relations for a {}-join SELECT",
                physical_plan.root.relation_count(),
                self.statement.joins.len()
            )));
        }
        let mut bindings = BTreeSet::new();
        let mut duplicate = None;
        physical_plan.root.visit_relations(&mut |relation| {
            if !bindings.insert(relation.binding) {
                duplicate = Some(relation.binding);
            }
        });
        if let Some(binding) = duplicate {
            return Err(SkeinError::Execution(format!(
                "physical relational join plan repeats binding {}",
                binding.get()
            )));
        }
        physical_plan.validate()?;
        validate_prepared_physical_join_plan_accesses(&physical_plan.root, true)?;
        planned_tree_operator_cardinality_profiles(physical_plan)?;
        if let Some(selection) = &self.access_plan.join_selection {
            if selection.join_bindings.len() != self.statement.joins.len() {
                return Err(SkeinError::Execution(format!(
                    "prepared relational join selection has {} bindings for {} joins",
                    selection.join_bindings.len(),
                    self.statement.joins.len()
                )));
            }
            let mut bindings = BTreeSet::from([selection.base_binding]);
            if selection
                .join_bindings
                .iter()
                .any(|binding| !bindings.insert(*binding))
            {
                return Err(SkeinError::Execution(
                    "prepared relational join selection contains duplicate bindings".to_string(),
                ));
            }
            let cost = selection.cost_breakdown;
            let component_total = cost
                .cpu
                .saturating_add(cost.random_io)
                .saturating_add(cost.sequential_io)
                .saturating_add(cost.output_rows);
            if cost.estimated_rows == 0 || cost.cost != component_total {
                return Err(SkeinError::Execution(
                    "prepared relational join selection has an invalid cost breakdown".to_string(),
                ));
            }
        }
        if self.execution
            != PreparedRelationalExecutionDescriptor::prepare(&self.statement, &self.access_plan)?
        {
            return Err(SkeinError::Execution(
                "prepared relational SELECT has an inconsistent execution descriptor".to_string(),
            ));
        }
        Ok(())
    }
}

pub(super) fn validate_prepared_physical_join_plan_accesses(
    node: &RelationalPhysicalJoinNode,
    requires_base: bool,
) -> Result<()> {
    match node {
        RelationalPhysicalJoinNode::Relation(relation) => match &relation.access {
            RelationalPhysicalAccess::Base(access)
                if requires_base && base_access_matches_descriptor(access) =>
            {
                Ok(())
            }
            RelationalPhysicalAccess::Probe(access)
                if !requires_base && join_access_matches_descriptor(access) =>
            {
                Ok(())
            }
            _ => Err(SkeinError::Execution(format!(
                "physical relation {} has an invalid access role",
                relation.qualifier
            ))),
        },
        RelationalPhysicalJoinNode::Join {
            algorithm,
            predicates,
            left,
            right,
            ..
        } => {
            if predicates.is_empty() {
                return Err(SkeinError::Execution(
                    "physical join has no predicate".to_string(),
                ));
            }
            validate_prepared_physical_join_plan_accesses(left, true)?;
            validate_prepared_physical_join_plan_accesses(
                right,
                matches!(
                    algorithm,
                    RelationalPhysicalJoinAlgorithm::Merge
                        | RelationalPhysicalJoinAlgorithm::Hash
                        | RelationalPhysicalJoinAlgorithm::Materialized
                ),
            )
        }
    }
}

pub(super) fn base_access_matches_descriptor(candidate: &RelationalAccessCandidate) -> bool {
    match (&candidate.descriptor.kind, &candidate.access) {
        (RelationalAccessPathKind::PrimaryKey, RelationalBaseAccess::PrimaryKey(key)) => {
            candidate.descriptor.equality_prefix_len == key.0.len()
                && candidate.descriptor.access_columns
                    == candidate.descriptor.index_columns.iter().cloned().collect()
        }
        (RelationalAccessPathKind::Index, RelationalBaseAccess::Index { name, scan }) => {
            candidate.descriptor.name == *name
                && candidate.descriptor.equality_prefix_len == scan.prefix.0.len()
        }
        (RelationalAccessPathKind::FullScan, RelationalBaseAccess::FullScan) => {
            candidate.descriptor.equality_prefix_len == 0
        }
        _ => false,
    }
}

pub(super) fn join_access_matches_descriptor(candidate: &RelationalJoinAccessCandidate) -> bool {
    match (&candidate.descriptor.kind, &candidate.access) {
        (RelationalAccessPathKind::PrimaryKey, RelationalJoinAccess::PrimaryKey(columns)) => {
            candidate.descriptor.equality_prefix_len == columns.len()
                && candidate.descriptor.access_columns
                    == columns.iter().map(|(column, _)| column.clone()).collect()
        }
        (RelationalAccessPathKind::Index, RelationalJoinAccess::Index { name, columns }) => {
            candidate.descriptor.name == *name
                && candidate.descriptor.equality_prefix_len == columns.len()
                && candidate.descriptor.access_columns
                    == columns.iter().map(|(column, _)| column.clone()).collect()
        }
        (RelationalAccessPathKind::FullScan, RelationalJoinAccess::FullScan) => {
            candidate.descriptor.equality_prefix_len == 0
        }
        _ => false,
    }
}

pub(super) fn planned_operator_cardinality_profiles(
    prepared: &PreparedRelationalSelect,
) -> Result<Vec<RelationalOperatorCardinalityProfile>> {
    planned_tree_operator_cardinality_profiles(prepared.access_plan.physical_join_plan()?)
}

pub(super) fn planned_tree_operator_cardinality_profiles(
    tree: &RelationalPhysicalJoinPlan,
) -> Result<Vec<RelationalOperatorCardinalityProfile>> {
    fn plan_node(
        node: &RelationalPhysicalJoinNode,
        profiles: &mut [Option<RelationalOperatorCardinalityProfile>],
    ) -> Result<PlanCostBreakdown> {
        match node {
            RelationalPhysicalJoinNode::Relation(relation) => Ok(estimate_relational_access_cost(
                relation.access.descriptor().estimated_rows,
            )),
            RelationalPhysicalJoinNode::Join {
                operator_id,
                kind,
                algorithm,
                selectivity,
                left,
                right,
                ..
            } => {
                let left_cost = plan_node(left, profiles)?;
                let right_cost = plan_node(right, profiles)?;
                let cardinality = match kind {
                    SqlJoinKind::Inner => RelationalJoinCardinality::Inner,
                    SqlJoinKind::Left => RelationalJoinCardinality::PreserveLeft,
                };
                let cost = estimate_relational_join_cost(
                    left_cost,
                    right_cost,
                    cardinality,
                    match algorithm {
                        RelationalPhysicalJoinAlgorithm::Probe
                        | RelationalPhysicalJoinAlgorithm::BatchedIndex => {
                            RelationalJoinRightInput::Probe
                        }
                        RelationalPhysicalJoinAlgorithm::Merge => RelationalJoinRightInput::Merge,
                        RelationalPhysicalJoinAlgorithm::Hash => RelationalJoinRightInput::Hash,
                        RelationalPhysicalJoinAlgorithm::Materialized => {
                            RelationalJoinRightInput::Materialized
                        }
                    },
                    *selectivity,
                );
                let index = operator_id.get().checked_sub(1).ok_or_else(|| {
                    SkeinError::Execution("physical join has an invalid operator id".to_string())
                })?;
                let slot = profiles.get_mut(index).ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "physical join operator {} is outside the plan profile",
                        operator_id.get()
                    ))
                })?;
                if slot.is_some() {
                    return Err(SkeinError::Execution(format!(
                        "physical join repeats operator {}",
                        operator_id.get()
                    )));
                }
                let access_path = right.first_relation().access.descriptor().clone();
                *slot = Some(RelationalOperatorCardinalityProfile {
                    operator_id: *operator_id,
                    operator: relational_join_operator_kind(*kind, &access_path, *algorithm),
                    table: right.first_relation().table.clone(),
                    access_path,
                    estimated_rows: estimated_rows_as_usize(cost.estimated_rows),
                    actual_rows: None,
                    fully_consumed: false,
                });
                Ok(cost)
            }
        }
    }

    let relation_count = tree.root.relation_count();
    let mut profiles = vec![None; relation_count];
    let base = tree.root.first_relation();
    profiles[0] = Some(RelationalOperatorCardinalityProfile {
        operator_id: RelationalOperatorId::from_plan_index(0),
        operator: match base.access.descriptor().kind {
            RelationalAccessPathKind::FullScan => RelationalOperatorKind::TableFullScan,
            RelationalAccessPathKind::PrimaryKey => RelationalOperatorKind::TablePointGet,
            RelationalAccessPathKind::Index => RelationalOperatorKind::IndexRangeScan,
        },
        table: base.table.clone(),
        access_path: base.access.descriptor().clone(),
        estimated_rows: base.access.descriptor().estimated_rows,
        actual_rows: None,
        fully_consumed: false,
    });
    let cost = plan_node(&tree.root, &mut profiles)?;
    if cost != tree.cost_breakdown {
        return Err(SkeinError::Execution(
            "physical join operator estimates diverge from the selected join cost".to_string(),
        ));
    }
    profiles
        .into_iter()
        .enumerate()
        .map(|(index, profile)| {
            profile.ok_or_else(|| {
                SkeinError::Execution(format!(
                    "physical join plan has no operator profile at index {index}"
                ))
            })
        })
        .collect()
}

pub(super) fn relational_join_operator_kind(
    kind: SqlJoinKind,
    access_path: &RelationalAccessPathDescriptor,
    algorithm: RelationalPhysicalJoinAlgorithm,
) -> RelationalOperatorKind {
    match (kind, access_path.kind, algorithm) {
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Merge) => {
            RelationalOperatorKind::MergeJoin
        }
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Hash) => {
            RelationalOperatorKind::HashJoin
        }
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::BatchedIndex) => {
            RelationalOperatorKind::BatchedIndexNestedLoopJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::BatchedIndex) => {
            RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Merge) => {
            RelationalOperatorKind::NestedLoopLeftJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Hash) => {
            RelationalOperatorKind::HashJoin
        }
        (SqlJoinKind::Inner, RelationalAccessPathKind::FullScan, _)
        | (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Materialized) => {
            RelationalOperatorKind::NestedLoopJoin
        }
        (SqlJoinKind::Left, RelationalAccessPathKind::FullScan, _)
        | (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Materialized) => {
            RelationalOperatorKind::NestedLoopLeftJoin
        }
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Probe) => {
            RelationalOperatorKind::IndexNestedLoopJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Probe) => {
            RelationalOperatorKind::IndexNestedLoopLeftJoin
        }
    }
}

pub(super) fn estimated_rows_as_usize(rows: u64) -> usize {
    usize::try_from(rows).unwrap_or(usize::MAX)
}
