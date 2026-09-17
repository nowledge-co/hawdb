use super::*;
use skein_storage::relational_index_view::{
    RelationalIndexProbeStatistics, RelationalIndexReadViewReport,
};
use skein_storage::{RelationalIndexReadLimits, RelationalIndexShadowError, RelationalValue};
use std::cell::RefCell;

pub(super) fn select(sql: &str) -> SelectStatement {
    match skein_sql::prepare_postgres_sql(sql).unwrap().statement {
        skein_sql::SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    }
}

pub(super) fn statement() -> SelectStatement {
    select("SELECT l.k FROM l JOIN r ON l.k = r.k")
}

pub(super) fn state() -> RelationalState {
    let mut state = RelationalState::default();
    for sql in [
        "CREATE TABLE l (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, n BIGINT)",
        "CREATE TABLE r (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, n BIGINT)",
        "CREATE INDEX l_k ON l (k)",
        "CREATE INDEX r_k ON r (k)",
        "CREATE UNIQUE INDEX l_n ON l (n)",
        "INSERT INTO l (id, k, n) VALUES (1, 2, NULL), (2, 2, 4), (3, 3, 5)",
        "INSERT INTO r (id, k, n) VALUES (1, 2, NULL), (2, 2, 4)",
    ] {
        let tx = crate::compile_relational_statement_sql(sql, &[], &state).unwrap();
        state = state
            .stage_transaction(tx, Default::default(), Default::default())
            .unwrap();
    }
    state
}

pub(super) fn base(rows: usize, indexed: bool) -> RelationalAccessCandidate {
    RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: if indexed {
                RelationalAccessPathKind::Index
            } else {
                RelationalAccessPathKind::FullScan
            },
            name: if indexed { "key_index" } else { "__full_scan" }.into(),
            index_columns: if indexed {
                vec!["k".into()]
            } else {
                Vec::new()
            },
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: usize::from(indexed),
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: indexed,
            estimated_rows: rows,
        },
        access: if indexed {
            RelationalBaseAccess::Index {
                name: "key_index".into(),
                scan: RelationalIndexRangeScan {
                    prefix: RelationalKey(Vec::new()),
                    exclusive_bound: None,
                    direction: RelationalIndexScanDirection::Forward,
                },
            }
        } else {
            RelationalBaseAccess::FullScan
        },
    }
}

pub(super) fn probe(rows: usize, indexed: bool) -> RelationalJoinAccessCandidate {
    let mut descriptor = base(rows, indexed).descriptor;
    if indexed {
        descriptor.access_columns.insert("k".into());
        descriptor.equality_prefix_len = 1;
    }
    RelationalJoinAccessCandidate {
        descriptor,
        access: if indexed {
            RelationalJoinAccess::Index {
                name: "key_index".into(),
                columns: vec![(
                    "k".into(),
                    SqlColumnRef {
                        qualifier: Some("l".into()),
                        name: "k".into(),
                    },
                )],
            }
        } else {
            RelationalJoinAccess::FullScan
        },
    }
}

pub(super) fn access_plan(indexed: bool) -> PreparedRelationalAccessPlan {
    PreparedRelationalAccessPlan {
        base_access: base(3, indexed),
        join_accesses: vec![probe(2, indexed)],
        join_selection: None,
        physical_join_plan: None,
    }
}

#[derive(Default)]
pub(super) struct Reader {
    pub statistics: Option<RelationalIndexProbeStatistics>,
    pub calls: RefCell<Vec<(String, String, usize)>>,
}

impl RelationalIndexStoreReader for Reader {
    fn relational_index_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        self.calls
            .borrow_mut()
            .push((table.into(), index.into(), prefix_len));
        self.statistics
    }
    fn visit_relational_index_read_view_prefix_entries(
        &self,
        _: &str,
        _: &str,
        _: &RelationalKey,
        _: RelationalIndexReadLimits,
        _: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        panic!("planning must not read index entries")
    }
    fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        _: &str,
        _: &str,
        _: &[RelationalKey],
        _: RelationalIndexReadLimits,
        _: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        panic!("planning must not read index entries")
    }
    fn visit_relational_index_read_view_range_entries(
        &self,
        _: &str,
        _: &str,
        _: &RelationalIndexRangeScan,
        _: RelationalIndexReadLimits,
        _: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        panic!("planning must not read index entries")
    }
}

pub(super) type Mode<'a> = RelationalIndexReadMode<'a, Reader>;

pub(super) fn modes(reader: &Reader) -> [Mode<'_>; 5] {
    [
        Mode::Materialized,
        Mode::Shadow(reader),
        Mode::DemandPaged(reader),
        Mode::Authoritative(reader),
        Mode::TransactionWorkspace,
    ]
}

pub(super) fn prepared(
    statement: SelectStatement,
    access_plan: PreparedRelationalAccessPlan,
) -> PreparedRelationalSelect {
    let execution =
        PreparedRelationalExecutionDescriptor::prepare(&statement, &access_plan).unwrap();
    PreparedRelationalSelect {
        statement,
        access_plan,
        join_planning: RelationalJoinPlanningOutcome::explicit_syntax_order(
            vec!["l".into(), "r".into()],
            Default::default(),
        ),
        execution,
        stage_timings: Default::default(),
    }
}

pub(super) struct Rng(pub u64);
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[derive(Clone)]
pub(super) enum Model {
    Leaf {
        id: u32,
        rows: usize,
        indexed: bool,
        probe: bool,
    },
    Join {
        id: usize,
        algorithm: usize,
        outer: bool,
        ndv: Option<u64>,
        left: Box<Model>,
        right: Box<Model>,
    },
}

impl Model {
    pub fn first(&self) -> (u32, usize, bool, bool) {
        match self {
            Self::Leaf {
                id,
                rows,
                indexed,
                probe,
            } => (*id, *rows, *indexed, *probe),
            Self::Join { left, .. } => left.first(),
        }
    }

    pub fn construct(&self, predicate: &SqlPredicate) -> RelationalPhysicalJoinNode {
        match self {
            Self::Leaf {
                id,
                rows,
                indexed,
                probe: is_probe,
            } => RelationalPhysicalJoinNode::relation(
                BindingId::new(*id),
                format!("t{id}"),
                format!("b{id}"),
                if *is_probe {
                    RelationalPhysicalAccess::Probe(probe(*rows, *indexed))
                } else {
                    RelationalPhysicalAccess::Base(base(*rows, *indexed))
                },
            ),
            Self::Join {
                id,
                algorithm,
                outer,
                ndv,
                left,
                right,
            } => {
                let algorithms = [
                    RelationalPhysicalJoinAlgorithm::Probe,
                    RelationalPhysicalJoinAlgorithm::BatchedIndex,
                    RelationalPhysicalJoinAlgorithm::Merge,
                    RelationalPhysicalJoinAlgorithm::Hash,
                    RelationalPhysicalJoinAlgorithm::Materialized,
                ];
                RelationalPhysicalJoinNode::join_with_algorithm(
                    RelationalPhysicalJoinSpec {
                        operator_id: RelationalOperatorId::from_plan_index(*id),
                        kind: if *outer {
                            SqlJoinKind::Left
                        } else {
                            SqlJoinKind::Inner
                        },
                        algorithm: algorithms[*algorithm],
                        equi_join_keys: matches!(algorithm, 2 | 3).then(|| {
                            RelationalEquiJoinKeys {
                                columns: vec![(
                                    "k".into(),
                                    SqlColumnRef {
                                        qualifier: Some(format!("b{}", left.first().0)),
                                        name: "k".into(),
                                    },
                                )],
                            }
                        }),
                        selectivity: ndv.map_or(RelationalJoinSelectivity::Unknown, |n| {
                            RelationalJoinSelectivity::equi_join(Some(n), None)
                        }),
                        predicates: vec![predicate.clone()],
                    },
                    left.construct(predicate),
                    right.construct(predicate),
                )
                .unwrap()
            }
        }
    }

    // Independent arithmetic oracle: never call a production cost or tree walker.
    pub fn reference(&self) -> (u64, PlanCostBreakdown, usize, usize, Vec<u32>) {
        match self {
            Self::Leaf {
                id, rows, indexed, ..
            } => {
                let rows = u64::try_from(*rows).unwrap_or(u64::MAX).max(1);
                let work = if *indexed {
                    [rows.saturating_mul(2), rows.saturating_add(1), rows, rows]
                } else {
                    [rows.saturating_add(4), 0, rows, rows]
                };
                (rows, reference_cost(rows, work), 0, 0, vec![*id])
            }
            Self::Join {
                algorithm,
                outer,
                ndv,
                left,
                right,
                ..
            } => {
                let (lr, lc, lb, lm, mut ids) = left.reference();
                let (rr, rc, rb, rm, right_ids) = right.reference();
                ids.extend(right_ids);
                let pairs = lr.saturating_mul(rr);
                let divisor = ndv.map_or(10, |n| n.max(1).min(lr.max(1)));
                let joined = if *algorithm < 2 {
                    pairs
                } else {
                    pairs.div_ceil(divisor)
                };
                let rows = if *outer { joined.max(lr) } else { joined };
                let join_cpu = match algorithm {
                    0 | 1 => 0,
                    2 => lr.saturating_add(rr).saturating_add(joined),
                    3 => lr
                        .saturating_add(rr.saturating_mul(2))
                        .saturating_add(joined),
                    4 => pairs,
                    _ => unreachable!(),
                };
                let multiplier = if *algorithm < 2 { lr } else { 1 };
                let combine = |l: u64, r: u64| l.saturating_add(r.saturating_mul(multiplier));
                let cost = reference_cost(
                    rows,
                    [
                        combine(lc.cpu, rc.cpu).saturating_add(join_cpu),
                        combine(lc.random_io, rc.random_io),
                        combine(lc.sequential_io, rc.sequential_io),
                        combine(lc.output_rows, rc.output_rows),
                    ],
                );
                (
                    rows,
                    cost,
                    lb + rb + usize::from(*algorithm == 1),
                    lm + rm + usize::from(*algorithm >= 2),
                    ids,
                )
            }
        }
    }

    pub fn expected_profiles(&self, profiles: &mut [Option<RelationalOperatorCardinalityProfile>]) {
        if let Self::Join {
            id,
            algorithm,
            outer,
            left,
            right,
            ..
        } = self
        {
            left.expected_profiles(profiles);
            right.expected_profiles(profiles);
            let (binding, rows, indexed, is_probe) = right.first();
            let descriptor = if is_probe {
                probe(rows, indexed).descriptor
            } else {
                base(rows, indexed).descriptor
            };
            let operator = match (algorithm, outer) {
                (0 | 4, false) => RelationalOperatorKind::NestedLoopJoin,
                (0 | 4, true) => RelationalOperatorKind::NestedLoopLeftJoin,
                (1, false) => RelationalOperatorKind::BatchedIndexNestedLoopJoin,
                (1, true) => RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin,
                (2, _) => RelationalOperatorKind::MergeJoin,
                (3, _) => RelationalOperatorKind::HashJoin,
                _ => unreachable!(),
            };
            profiles[*id] = Some(RelationalOperatorCardinalityProfile {
                operator_id: RelationalOperatorId::from_plan_index(*id),
                operator,
                table: format!("t{binding}"),
                access_path: descriptor,
                estimated_rows: usize::try_from(self.reference().0).unwrap_or(usize::MAX),
                actual_rows: None,
                fully_consumed: false,
            });
        }
    }
}

pub(super) fn model(
    rng: &mut Rng,
    algorithm: usize,
    depth: usize,
    leaf_id: &mut u32,
    operator_id: &mut usize,
) -> Model {
    fn leaf(rng: &mut Rng, indexed: bool, probe: bool, id: &mut u32) -> Model {
        let rows = [0, 1, 2, 9, 47, usize::MAX][rng.next() as usize % 6];
        let model = Model::Leaf {
            id: *id,
            rows,
            indexed,
            probe,
        };
        *id += 1;
        model
    }
    let left = if depth > 0 && !matches!(algorithm, 2 | 3) {
        let next = rng.next() as usize % 5;
        model(rng, next, depth - 1, leaf_id, operator_id)
    } else {
        leaf(rng, algorithm == 2, false, leaf_id)
    };
    let right = if algorithm == 4 {
        model(rng, 0, depth.saturating_sub(1), leaf_id, operator_id)
    } else {
        leaf(rng, matches!(algorithm, 1 | 2), algorithm < 2, leaf_id)
    };
    *operator_id += 1;
    Model::Join {
        id: *operator_id,
        algorithm,
        outer: algorithm != 2 && rng.next().is_multiple_of(2),
        ndv: if rng.next().is_multiple_of(2) {
            None
        } else {
            Some(rng.next() % 64)
        },
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub(super) fn primary_key() -> RelationalAccessCandidate {
    let mut access = base(1, false);
    access.descriptor.kind = RelationalAccessPathKind::PrimaryKey;
    access.descriptor.index_columns = vec!["id".into()];
    access.descriptor.access_columns = BTreeSet::from(["id".into()]);
    access.descriptor.equality_prefix_len = 1;
    access.access =
        RelationalBaseAccess::PrimaryKey(RelationalKey(vec![RelationalValue::BigInt(1)]));
    access
}

// Keep both component arithmetic and scalar weighting independent of production.
fn reference_cost(
    rows: u64,
    [cpu, random_io, sequential_io, output_rows]: [u64; 4],
) -> PlanCostBreakdown {
    PlanCostBreakdown {
        estimated_rows: rows.max(1),
        cost: cpu
            .saturating_add(random_io.saturating_mul(2))
            .saturating_add(sequential_io)
            .saturating_add(output_rows),
        cpu,
        random_io,
        sequential_io,
        output_rows,
    }
}
