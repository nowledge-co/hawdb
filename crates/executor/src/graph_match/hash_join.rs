// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `GraphMatchExec` as a hash join at the meeting node (#1485).
//!
//! A program whose only import is a node `key` can run its steps once, with
//! `key` unbound, instead of once per input row. Every complete match of
//! that build run is hashed on `key`'s node id in the shared admitted hash
//! join, which charges the build to the blocking-operator budget and spills
//! it when the budget is exceeded. Each input row then probes with its
//! `key` and is extended by the build matches of that node. The program's
//! relationships are all bound by the build run, so relationship uniqueness
//! is enforced there, as in the per-row program.
//!
//! The build evaluates only the plan's `build_conjuncts`, a text-order prefix
//! of conjuncts that cannot fail. The other conjuncts run in order on each
//! joined row, so a build match that joins no input row raises no error. An
//! optional program pads an input row that no joined row satisfies, and a
//! NULL `key` matches nothing.

use super::*;
use crate::blocking::{
    AdmittedHashJoin, AdmittedHashJoinAdapter, AdmittedHashJoinCandidate, AdmittedHashJoinControl,
    AdmittedHashJoinRecord, AdmittedHashJoinSide,
};
use hawdb_plan_cypher::{predicate_cannot_fail, GraphMatchHashJoin};
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::BuildHasher;

const OPERATOR: &str = "GraphMatchExec hash join";

/// Bytes one build key id occupies in its set, including hash-table overhead.
const KEY_BYTES: usize = 32;

/// Whether the snapshot holds node `id`, reading only its labels.
fn node_exists(
    store: &dyn GraphExecutionRead,
    runtime: &MatchRuntime<'_>,
    id: NodeId,
) -> Result<bool> {
    let task_context = runtime.context.task_context;
    Ok(projected_node_with_allocation(
        store,
        id,
        None,
        &BTreeSet::new(),
        &mut |bytes| admit_graph_read(runtime.source_account, task_context, bytes),
        task_context,
    )?
    .is_some())
}

/// The per-row program split into its build run and the conjuncts left for
/// each joined row.
struct SplitProgram {
    build: GraphMatchProgram,
    /// Every conjunct in text order, for a deferred evaluation.
    all: Vec<Predicate>,
    /// The other conjuncts in the per-row program's evaluation order.
    residual: Vec<Predicate>,
    residual_placed: usize,
    probe_conjuncts: usize,
}

fn invalid(detail: &str) -> HawDBError {
    HawDBError::Execution(format!("invalid GraphMatchExec hash join: {detail}"))
}

fn split(program: &GraphMatchProgram, join: &GraphMatchHashJoin) -> Result<SplitProgram> {
    let [import] = program.imports.as_slice() else {
        return Err(invalid("the program must import only its key"));
    };
    if import.variable != join.key
        || import.kind != GraphEntityKind::Node
        || program.introduced.contains(&join.key)
    {
        return Err(invalid("the key must be the imported node"));
    }
    let conjuncts = program.predicate_conjuncts();
    let prefix = join.build_conjuncts.len();
    let in_build = |index: usize| index < prefix;
    if join
        .build_conjuncts
        .iter()
        .enumerate()
        .any(|(position, index)| position != *index)
        || prefix > conjuncts.len()
        || join
            .build_conjuncts
            .iter()
            .any(|index| !predicate_cannot_fail(&conjuncts[*index]))
    {
        return Err(invalid("build conjuncts must be a prefix that cannot fail"));
    }
    let mut order = join.residual_order.clone();
    order.sort_unstable();
    if order != (prefix..conjuncts.len()).collect::<Vec<_>>()
        || join.residual_placed > join.residual_order.len()
        || join.probe_conjuncts > join.residual_placed
    {
        return Err(invalid("residual conjuncts must follow the per-row order"));
    }
    // The planner keeps every build conjunct to the pattern's own variables,
    // which the build binds; anything else would read an unbound name.
    debug_assert!(
        conjuncts[..prefix].iter().all(|conjunct| {
            hawdb_plan_cypher::predicate_variables(conjunct)
                .iter()
                .all(|name| name == &join.key || program.introduced.contains(name))
        }),
        "a hash join build conjunct reads a variable outside its pattern"
    );
    let mut build = program.clone();
    build.imports.clear();
    build.hash_join = None;
    build.optional = false;
    build.id_set_conjuncts.clear();
    if !build.introduced.contains(&join.key) {
        build.introduced.push(join.key.clone());
    }
    let kept = conjuncts
        .iter()
        .enumerate()
        .filter(|(index, _)| in_build(*index))
        .map(|(_, conjunct)| conjunct.clone())
        .collect::<Vec<_>>();
    let residual = join
        .residual_order
        .iter()
        .map(|index| conjuncts[*index].clone())
        .collect::<Vec<_>>();
    build.predicate = match kept.len() {
        0 => None,
        1 => kept.into_iter().next(),
        _ => Some(Predicate::And(kept)),
    };
    if program.predicate_steps.is_empty() {
        build.edge_predicates.clear();
    } else {
        build.predicate_steps = join
            .build_conjuncts
            .iter()
            .map(|index| program.predicate_steps[*index])
            .collect();
        // Build conjuncts are a prefix, so their indices are unchanged.
        build.edge_predicates.retain(|index| *index < prefix);
    }
    if build.predicate.is_none() {
        build.predicate_steps.clear();
        build.edge_predicates.clear();
    }
    Ok(SplitProgram {
        build,
        all: conjuncts.to_vec(),
        residual,
        residual_placed: join.residual_placed,
        probe_conjuncts: join.probe_conjuncts,
    })
}

/// The id of `key` in a probe row, and whether it came from an imported
/// column (which may name a node the snapshot lacks) rather than a node an
/// earlier operator bound under the same name.
fn probe_key(row: &Binding, import: &GraphBindingImport) -> Result<Option<(u64, bool)>> {
    match row.values.get(&import.column) {
        Some(Value::Null) => Ok(None),
        Some(value) => entity_id(imported_entity(value, import)?, "_id").map(|id| Some((id, true))),
        None => match row.nodes.get(&import.column) {
            Some(node) => Ok(Some((node.id.0, false))),
            None => Err(HawDBError::Execution(format!(
                "missing graph column '{}' during MATCH",
                import.column
            ))),
        },
    }
}

struct JoinAdapter<'r, 'm, 'o> {
    runtime: &'r MatchRuntime<'m>,
    key: &'r str,
    import: &'r GraphBindingImport,
    all: Vec<&'r Predicate>,
    residual: Vec<&'r Predicate>,
    residual_placed: usize,
    probe_conjuncts: usize,
    introduced: &'r [String],
    optional: bool,
    hash_state: RandomState,
    output: &'o mut AccountedBindingBatch,
    emit: &'o mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    emitted: usize,
    limit: ExecutionLimit,
}

impl JoinAdapter<'_, '_, '_> {
    fn hash(&self, id: u64) -> u64 {
        self.hash_state.hash_one(id)
    }

    fn append(&mut self, row: &Binding) -> Result<AdmittedHashJoinControl> {
        if self.output.push_cloned(row, self.emit)? == BatchControl::Stop {
            return Ok(AdmittedHashJoinControl::Stop);
        }
        self.emitted += 1;
        if self.output.is_full() && self.output.emit(self.emit)? == BatchControl::Stop {
            return Ok(AdmittedHashJoinControl::Stop);
        }
        Ok(if self.limit.is_reached(self.emitted) {
            AdmittedHashJoinControl::Stop
        } else {
            AdmittedHashJoinControl::Continue
        })
    }

    fn holds(&self, conjunct: &Predicate, row: &Binding) -> Result<bool> {
        evaluate_predicate_with_context(
            conjunct,
            self.runtime.context.catalog,
            self.runtime.store,
            row,
            self.runtime.context.observer,
            self.runtime.adjacency_memory(),
            self.runtime.context.task_context,
        )
    }

    fn conjunction_holds(&self, conjuncts: &[&Predicate], row: &Binding) -> Result<bool> {
        evaluate_conjunction_with_context(
            conjuncts,
            self.runtime.context.catalog,
            self.runtime.store,
            row,
            self.runtime.context.observer,
            self.runtime.adjacency_memory(),
            self.runtime.context.task_context,
        )
    }

    /// Whether the conjuncts left for joined rows accept `row`, evaluated as
    /// the per-row program does: those it places before its last step one
    /// at a time, each false or NULL rejecting, and an error deferring to
    /// the whole predicate in text order on this complete match; then those
    /// after its last step as one conjunction.
    fn joined_holds(&self, row: &Binding) -> Result<bool> {
        for (position, conjunct) in self.residual.iter().enumerate() {
            if position >= self.residual_placed {
                return self.conjunction_holds(&self.residual[position..], row);
            }
            match self.holds(conjunct, row) {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(HawDBError::Execution(_)) => {
                    // Cancellation is sticky; report it rather than defer.
                    runtime_checkpoint(self.runtime.context.task_context)?;
                    return self.conjunction_holds(&self.all, row);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }

    /// Whether the conjuncts the per-row program runs first, on the input
    /// row alone, reject `probe`. An error decides nothing here: the joined
    /// rows evaluate them again and defer as the per-row program would.
    fn probe_rejected(&self, probe: &Binding) -> Result<bool> {
        for conjunct in &self.residual[..self.probe_conjuncts] {
            match self.holds(conjunct, probe) {
                Ok(true) => {}
                Ok(false) => return Ok(true),
                Err(HawDBError::Execution(_)) => {
                    runtime_checkpoint(self.runtime.context.task_context)?;
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    /// The input row with this program's variables NULL.
    fn pad(&mut self, probe: &Binding) -> Result<AdmittedHashJoinControl> {
        let _lease = self.runtime.account.reserve(
            binding_memory_bytes(probe).saturating_add(
                self.introduced
                    .iter()
                    .map(|name| name.len() + 64)
                    .sum::<usize>(),
            ),
        )?;
        let mut row = probe.clone();
        for name in self.introduced {
            row.nodes.discard(name);
            row.relationships.discard(name);
            row.values.insert(name.clone(), Value::Null);
        }
        self.append(&row)
    }
}

impl AdmittedHashJoinAdapter for JoinAdapter<'_, '_, '_> {
    fn validate_spill_record(
        &mut self,
        side: AdmittedHashJoinSide,
        hash: u64,
        binding: &Binding,
    ) -> Result<()> {
        let id = match side {
            AdmittedHashJoinSide::Build => binding.nodes.get(self.key).map(|node| node.id.0),
            AdmittedHashJoinSide::Probe => probe_key(binding, self.import)?.map(|(id, _)| id),
        };
        if id.map(|id| self.hash(id)) != Some(hash) {
            return Err(HawDBError::StorageIntegrity(
                "GraphMatchExec hash join spill key does not match its recorded hash".into(),
            ));
        }
        Ok(())
    }

    fn visit_candidate(
        &mut self,
        probe: &Binding,
        build: &Binding,
    ) -> Result<AdmittedHashJoinCandidate> {
        runtime_checkpoint(self.runtime.context.task_context)?;
        let build_id = build.nodes.get(self.key).map(|node| node.id.0);
        if build_id.is_none() || probe_key(probe, self.import)?.map(|(id, _)| id) != build_id {
            return Ok(AdmittedHashJoinCandidate::rejected());
        }
        let _lease = self
            .runtime
            .account
            .reserve(binding_memory_bytes(probe).saturating_add(binding_memory_bytes(build)))?;
        let mut row = probe.clone();
        for (name, node) in build.nodes.iter() {
            row.nodes.insert(name.clone(), node.clone());
        }
        for (name, relationship) in build.relationships.iter() {
            row.relationships.insert(name.clone(), relationship.clone());
        }
        for (name, value) in &build.values {
            row.values.insert(name.clone(), value.clone());
        }
        if !self.residual.is_empty() && !self.joined_holds(&row)? {
            return Ok(AdmittedHashJoinCandidate::rejected());
        }
        let control = self.append(&row)?;
        Ok(AdmittedHashJoinCandidate::matched(control))
    }

    fn visit_unmatched(&mut self, probe: &Binding) -> Result<AdmittedHashJoinControl> {
        if self.optional {
            self.pad(probe)
        } else {
            Ok(AdmittedHashJoinControl::Continue)
        }
    }

    fn requires_complete_probe_match(&self) -> bool {
        self.optional
    }
}

/// Runs `program` as a hash join on `join.key` over `input`.
#[allow(clippy::too_many_arguments)]
pub(super) fn stream_graph_match_hash_join(
    program: &GraphMatchProgram,
    join: &GraphMatchHashJoin,
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    store: &dyn GraphExecutionRead,
    context: BatchExecutionContext<'_>,
    limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let SplitProgram {
        build,
        all,
        residual,
        residual_placed,
        probe_conjuncts,
    } = split(program, join)?;
    validate_program(&build)?;
    let placed = PlacedPredicate::new(&build)?;
    let account = context.operator_account("GraphMatchExec state");
    let source_account = context.source_account_retaining("GraphMatchExec source", account.clone());
    let output_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "GraphMatchExec output",
        context.memory.batch_payload_bytes,
    );
    let mut output = AccountedBindingBatch::with_account(
        "GraphMatchExec",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        output_account,
    );
    let runtime = MatchRuntime {
        program: &build,
        placed,
        deferred: Cell::new(false),
        store,
        context: context.into(),
        account: &account,
        source_account: &source_account,
        indexed: indexed_node::IndexedNodeStats::default(),
        scoring: false,
        candidate: None,
        memberships: membership::MembershipSets::default(),
    };
    let mut table = AdmittedHashJoin::new(
        OPERATOR,
        context.memory,
        context.memory_ledger,
        context.task_context,
    );
    let mut adapter = JoinAdapter {
        runtime: &runtime,
        key: &join.key,
        import: &program.imports[0],
        all: all.iter().collect(),
        residual: residual.iter().collect(),
        residual_placed,
        probe_conjuncts,
        introduced: &program.introduced,
        optional: program.optional,
        hash_state: RandomState::new(),
        output: &mut output,
        emit,
        emitted: 0,
        limit,
    };
    let mut empty = Binding::values(BTreeMap::new());
    let mut build_error = None;
    // The build's key ids: a probe key outside it matches nothing, and is
    // checked against the snapshot instead (see below).
    let mut keys = HashSet::new();
    let mut keys_lease = account.reserve(0)?;
    runtime.visit(0, &mut empty, &mut BTreeSet::new(), &mut |row| {
        let Some(node) = row.nodes.get(&join.key) else {
            build_error = Some(invalid("a build match does not bind its key"));
            return Ok(ScanControl::Stop);
        };
        if keys.insert(node.id.0) {
            keys_lease.grow(KEY_BYTES)?;
        }
        let hash = adapter.hash(node.id.0);
        table.push_build(AdmittedHashJoinRecord {
            hash,
            binding: row.clone(),
        })?;
        Ok(ScanControl::Continue)
    })?;
    if let Some(error) = build_error {
        return Err(error);
    }
    table.finish_build()?;
    let source_control = source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        for row in &batch {
            runtime_checkpoint(context.task_context)?;
            let mut probe = row.clone();
            for name in &program.introduced {
                probe.nodes.discard(name);
                probe.relationships.discard(name);
                probe.values.remove(name);
            }
            let control = match probe_key(&probe, adapter.import)? {
                Some((id, _)) if keys.contains(&id) => {
                    if adapter.probe_rejected(&probe)? {
                        adapter.visit_unmatched(&probe)?
                    } else {
                        table.push_probe(
                            AdmittedHashJoinRecord {
                                hash: adapter.hash(id),
                                binding: probe,
                            },
                            &mut adapter,
                        )?
                    }
                }
                Some((id, from_column)) => {
                    // The per-row program reads every imported node first and
                    // fails when the snapshot lacks it; the build read every
                    // node it holds, so only a key outside it needs the check.
                    if from_column && !node_exists(store, &runtime, NodeId(id))? {
                        return Err(HawDBError::Execution(
                            "bound node disappeared during MATCH".to_string(),
                        ));
                    }
                    adapter.visit_unmatched(&probe)?
                }
                None => adapter.visit_unmatched(&probe)?,
            };
            if control == AdmittedHashJoinControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    })?;
    let control = if source_control == BatchControl::Stop {
        BatchControl::Stop
    } else {
        match table.finish(&mut adapter)? {
            AdmittedHashJoinControl::Continue => BatchControl::Continue,
            AdmittedHashJoinControl::Stop => BatchControl::Stop,
        }
    };
    context
        .observer
        .record_blocking_memory_report(table.report());
    if adapter.output.emit(adapter.emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(control)
}
