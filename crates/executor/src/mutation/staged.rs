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

//! Row-sourced mutation batches staged before the write guard.
//!
//! `LOAD CSV` and `UNWIND` resolve every row to a storage mutation before the
//! database write guard is taken. A `LOAD CSV` list is charged to the
//! statement's query memory ledger, because its file is not resident before
//! the statement runs. An `UNWIND` list keeps the existing contract: its rows
//! are a parameter the caller already holds, bounded by the mutation limits.
//! Matched-relationship endpoints are resolved to internal node ids with one
//! charged pass per endpoint pattern, so the commit under the guard performs
//! point lookups instead of one label scan per row and endpoint. The id lists
//! copied into each row's endpoint filters are charged on both paths, and a
//! batch whose resolved Cartesian products exceed the affected-row limit is
//! refused before any list is copied.

use crate::binding::{map_memory_bytes, value_memory_bytes};
use crate::memory::DEFAULT_EXECUTION_BATCH_ROWS;
use crate::pipeline::runtime_checkpoint;
use crate::store::{admit_graph_read, GraphExecutionRead, ScanControl};
use crate::{QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger};
use hawdb_core::{Catalog, HawDBError, LabelId, Result, RuntimeTaskContext, Value};
use hawdb_storage::mutation::{
    ensure_additional_mutation_limits, remaining_mutation_affected_rows, GraphMutation,
    MutationLimits, NodeSetAssignment, NodeSetValue, PropertyFilter,
};
use hawdb_storage::read_view::GraphReadAllocator;
use hawdb_storage::{NodeId, ProjectedNodeRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

#[cfg(test)]
mod tests;

/// A statement's resolved mutations. A charged batch keeps its resident bytes
/// on the statement's query memory ledger until it is committed or dropped.
#[derive(Debug)]
pub struct StagedMutations {
    mutations: Vec<GraphMutation>,
    ledger: QueryMemoryLedger,
    charge: QueryMemoryLease,
    /// Relationship MERGE endpoint resolution's lease, held until the commit.
    merge_endpoint_charge: Option<QueryMemoryLease>,
    charges_list: bool,
}

/// The ledger charges of a staged batch handed to a commit; keep it alive
/// until storage returns.
#[derive(Debug)]
pub struct StagedCharge {
    _mutations: QueryMemoryLease,
    _merge_endpoints: Option<QueryMemoryLease>,
}

/// What [`StagedMutations::resolve_matched_endpoints`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EndpointResolutionReport {
    /// Distinct `(label, property names)` endpoint patterns.
    pub patterns: usize,
    /// Patterns resolved by one projected label scan.
    pub label_scans: usize,
    /// Patterns resolved through a declared scalar property index.
    pub index_lookups: usize,
    /// Node records produced by those scans and lookups.
    pub nodes_visited: usize,
    /// Endpoint filters rewritten to internal-id matches.
    pub rewritten_endpoints: usize,
}

impl StagedMutations {
    /// A batch whose mutations are charged to `ledger` as they are staged.
    pub(crate) fn charged(ledger: &QueryMemoryLedger) -> Result<Self> {
        Self::new(ledger, true)
    }

    /// A batch whose mutation list is bounded only by the mutation limits, as
    /// `UNWIND` batches were before staging; endpoint resolution still charges
    /// `ledger` for the scans and id maps it builds and for the id lists it
    /// copies into the staged endpoint filters.
    pub(crate) fn uncharged_list(ledger: &QueryMemoryLedger) -> Result<Self> {
        Self::new(ledger, false)
    }

    fn new(ledger: &QueryMemoryLedger, charges_list: bool) -> Result<Self> {
        let charge = ledger
            .account(
                QueryMemoryClass::BlockingState,
                "staged graph mutations",
                ledger.budget_bytes(),
            )
            .reserve(size_of::<Self>())?;
        Ok(Self {
            mutations: Vec::new(),
            ledger: ledger.clone(),
            charge,
            merge_endpoint_charge: None,
            charges_list,
        })
    }

    /// Charges `mutation` before retaining it; the statement fails closed when
    /// the ledger cannot admit it.
    pub(crate) fn push(&mut self, mutation: GraphMutation) -> Result<()> {
        if !self.charges_list {
            self.mutations.push(mutation);
            return Ok(());
        }
        let spare = if self.mutations.len() == self.mutations.capacity() {
            // Vec growth doubles the slot array; charge the new slots up front.
            self.mutations.capacity().max(4) * size_of::<GraphMutation>()
        } else {
            0
        };
        self.charge.grow(
            graph_mutation_memory_bytes(&mutation)
                .saturating_sub(size_of::<GraphMutation>())
                .saturating_add(spare),
        )?;
        self.mutations.push(mutation);
        Ok(())
    }

    pub fn mutations(&self) -> &[GraphMutation] {
        &self.mutations
    }

    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    /// Bytes currently charged to the statement ledger for this batch.
    pub fn charged_bytes(&self) -> usize {
        self.charge.bytes()
    }

    /// Hands the mutations to a commit. Keep the returned lease alive until
    /// the commit returns so the batch stays charged while storage owns it.
    pub fn into_parts(self) -> (Vec<GraphMutation>, StagedCharge) {
        (
            self.mutations,
            StagedCharge {
                _mutations: self.charge,
                _merge_endpoints: self.merge_endpoint_charge,
            },
        )
    }

    /// Resolves every matched-relationship endpoint of the batch against the
    /// snapshot the commit will use: relationship CREATE through
    /// [`Self::resolve_matched_endpoints`] and relationship MERGE through
    /// [`crate::mutation::resolve_relationship_merge_endpoints`]. The two
    /// rewrite disjoint mutation kinds; every row-sourced commit path calls
    /// this so neither form falls back to per-row scans under the guard.
    pub fn resolve_endpoints(
        &mut self,
        catalog: &Catalog,
        store: &dyn GraphExecutionRead,
        limits: MutationLimits,
        memory: &crate::ExecutionMemoryConfig,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<EndpointResolutionReport> {
        let report = self.resolve_matched_endpoints(catalog, store, limits, task_context)?;
        if let Some(charge) = super::preflight::resolve_relationship_merge_endpoints(
            &mut self.mutations,
            catalog,
            store,
            limits,
            memory,
            task_context,
        )? {
            self.merge_endpoint_charge = Some(charge);
        }
        Ok(report)
    }

    /// Rewrites property-equality endpoints of matched-relationship creations
    /// to internal-id matches, with one pass per distinct endpoint pattern.
    ///
    /// Every distinct `(label, property names)` pattern is resolved once,
    /// through a declared scalar property index on a resident store or
    /// otherwise by one projected label scan that decodes only the pattern's
    /// properties. Each endpoint filter `F` then becomes `IdIn(ids) AND F`
    /// (`IdEq` for one id): storage resolves it by point lookups and rechecks
    /// the label and `F` on every candidate under the write guard, so the
    /// commit creates exactly the relationships the original filter would.
    /// Ids are emitted in ascending order, which is the scan order the
    /// Cartesian creation used before.
    ///
    /// Before any row's ids are copied, its resolved endpoint and Cartesian
    /// counts are checked against `limits` in commit order, so a fan-out the
    /// commit would refuse fails here with the same `max_mutation_affected_rows`
    /// or `max_mutation_operations` error instead of allocating the copies. A
    /// row with an empty endpoint copies no ids for either side. The copies'
    /// growth is charged to the statement ledger even for an `UNWIND` batch,
    /// whose list itself stays uncharged.
    ///
    /// Call this with the catalog and store the commit will use: the
    /// autocommit path holds `&mut Database` for the whole statement, and an
    /// explicit transaction resolves against its own working store, so no
    /// other writer can add a node that matches `F` between resolution and
    /// commit. Nodes that stopped matching are dropped by the recheck.
    /// Mutations staged in the same batch never create nodes here: a
    /// row-sourced statement stages a single operation kind.
    pub fn resolve_matched_endpoints(
        &mut self,
        catalog: &Catalog,
        store: &dyn GraphExecutionRead,
        limits: MutationLimits,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<EndpointResolutionReport> {
        let mut report = EndpointResolutionReport::default();
        let index_account = self.ledger.account(
            QueryMemoryClass::BlockingState,
            "matched endpoint resolution",
            self.ledger.budget_bytes(),
        );
        let mut index_charge = index_account.reserve(0)?;
        let mut patterns: BTreeMap<(LabelId, Vec<String>), EndpointPattern> = BTreeMap::new();
        for (index, mutation) in self.mutations.iter().enumerate() {
            if index.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
                runtime_checkpoint(task_context)?;
            }
            let GraphMutation::CreateRelationshipsBetweenMatches(request) = mutation else {
                continue;
            };
            for (label, filter) in [
                (&request.source_label, &request.source_filter),
                (&request.target_label, &request.target_filter),
            ] {
                let Some((label_id, names, values)) = endpoint_key(catalog, label, filter) else {
                    continue;
                };
                let pattern = match patterns.entry((label_id, names)) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        index_charge.grow(
                            entry.key().1.iter().map(String::len).sum::<usize>()
                                + size_of::<((LabelId, Vec<String>), EndpointPattern)>() * 3,
                        )?;
                        entry.insert(EndpointPattern::default())
                    }
                };
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    pattern.matches.entry(values)
                {
                    index_charge.grow(endpoint_entry_bytes(entry.key()))?;
                    entry.insert(Vec::new());
                }
            }
        }
        report.patterns = patterns.len();
        for ((label_id, names), pattern) in &mut patterns {
            let visited = pattern.resolve(
                catalog,
                store,
                &self.ledger,
                (*label_id, names),
                &mut index_charge,
                task_context,
            )?;
            report.nodes_visited += visited.nodes;
            if visited.indexed {
                report.index_lookups += 1;
            } else {
                report.label_scans += 1;
            }
        }
        // Rows already known to create exactly this many relationships (one
        // storage operation each), counted in commit order.
        let mut affected = 0usize;
        // Whether `affected` is exact: an endpoint that is not keyed here is
        // resolved by the commit, so its row count is unknown until then.
        let mut exact = true;
        for (index, mutation) in self.mutations.iter_mut().enumerate() {
            if index.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
                runtime_checkpoint(task_context)?;
            }
            let GraphMutation::CreateRelationshipsBetweenMatches(request) = mutation else {
                continue;
            };
            // The commit skips a request naming an unknown label.
            if [&request.source_label, &request.target_label]
                .into_iter()
                .any(|label| !label.is_empty() && catalog.label_id(label).is_none())
            {
                continue;
            }
            let resolved = [
                endpoint_ids(
                    &patterns,
                    catalog,
                    &request.source_label,
                    &request.source_filter,
                ),
                endpoint_ids(
                    &patterns,
                    catalog,
                    &request.target_label,
                    &request.target_filter,
                ),
            ];
            // Refuse with the commit's own limit errors before copying any id
            // list: the commit bounds each endpoint by the remaining
            // affected rows, then charges the Cartesian product.
            let remaining = remaining_mutation_affected_rows(affected, limits)?;
            if resolved.iter().flatten().any(|ids| ids.len() > remaining) {
                return Err(HawDBError::Execution(format!(
                    "mutation would exceed max_mutation_affected_rows {remaining}"
                )));
            }
            if let [Some(sources), Some(targets)] = &resolved {
                let pairs = sources.len().checked_mul(targets.len()).ok_or_else(|| {
                    HawDBError::Execution("mutation Cartesian product overflow".to_string())
                })?;
                ensure_additional_mutation_limits(affected, affected, pairs, pairs, limits)?;
                affected += pairs;
            } else {
                exact = false;
            }
            // A row with an empty endpoint creates nothing. While every
            // earlier row's count is exact the checks above already raised
            // any error the commit would, so the other side's ids need not be
            // copied.
            let creates_nothing = exact && resolved.iter().flatten().any(|ids| ids.is_empty());
            for (ids, filter) in resolved
                .into_iter()
                .zip([&mut request.source_filter, &mut request.target_filter])
            {
                let Some(ids) = ids else {
                    continue;
                };
                let ids = if creates_nothing {
                    &[][..]
                } else {
                    ids.as_slice()
                };
                let Some(original) = filter.take() else {
                    unreachable!("endpoint_key requires a filter");
                };
                let before = property_filter_memory_bytes(&original);
                let resolved = id_led_conjunction(ids, original);
                // The copies are new memory even when the list itself is a
                // caller-held `UNWIND` parameter, so they are always charged.
                self.charge
                    .grow(property_filter_memory_bytes(&resolved).saturating_sub(before))?;
                *filter = Some(resolved);
                report.rewritten_endpoints += 1;
            }
        }
        Ok(report)
    }
}

#[derive(Debug, Default)]
struct EndpointPattern {
    /// Property values used by the staged rows, mapped to matching node ids.
    matches: BTreeMap<Vec<Value>, Vec<NodeId>>,
}

struct PatternVisit {
    nodes: usize,
    indexed: bool,
}

impl EndpointPattern {
    /// Fills `matches` with one index lookup or one projected label scan.
    fn resolve(
        &mut self,
        catalog: &Catalog,
        store: &dyn GraphExecutionRead,
        ledger: &QueryMemoryLedger,
        (label_id, names): (LabelId, &[String]),
        index_charge: &mut QueryMemoryLease,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<PatternVisit> {
        let required = names.iter().cloned().collect::<BTreeSet<_>>();
        let read_account = ledger.account(
            QueryMemoryClass::ExternalRead,
            "matched endpoint scan",
            ledger.budget_bytes(),
        );
        let use_index =
            !store.is_out_of_core() && catalog.has_scalar_property_index(label_id, &names[0]);
        let mut leading = Vec::new();
        let mut leading_charge = read_account.reserve(0)?;
        if use_index {
            // Keys are sorted, so equal leading values are adjacent.
            for key in self.matches.keys() {
                if leading.last() != Some(&key[0]) {
                    leading_charge.grow(value_memory_bytes(&key[0]))?;
                    leading.push(key[0].clone());
                }
            }
        }
        let mut admit = |bytes| admit_graph_read(&read_account, task_context, bytes);
        let mut allocator = GraphReadAllocator::new(&mut admit, task_context);
        let mut nodes = 0usize;
        let indexed = {
            let matches = &mut self.matches;
            let mut consumer = |node: ProjectedNodeRecord| -> Result<ScanControl> {
                nodes += 1;
                if nodes.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
                    runtime_checkpoint(task_context)?;
                }
                let ProjectedNodeRecord {
                    id, mut properties, ..
                } = node;
                let Some(key) = names
                    .iter()
                    .map(|name| properties.remove(name))
                    .collect::<Option<Vec<_>>>()
                else {
                    return Ok(ScanControl::Continue);
                };
                if let Some(ids) = matches.get_mut(&key) {
                    index_charge.grow(size_of::<NodeId>())?;
                    ids.push(id);
                }
                Ok(ScanControl::Continue)
            };
            let indexed = use_index
                && store
                    .visit_indexed_projected_nodes_by_property_admitted(
                        label_id,
                        &names[0],
                        &leading,
                        &required,
                        &mut allocator,
                        &mut consumer,
                    )?
                    .is_some();
            if !indexed {
                store.visit_projected_nodes_admitted(
                    Some(label_id),
                    &required,
                    &mut allocator,
                    &mut consumer,
                )?;
            }
            indexed
        };
        runtime_checkpoint(task_context)?;
        for ids in self.matches.values_mut() {
            // Commit visits ids in ascending order, as the label scan did.
            ids.sort_unstable();
            ids.dedup();
        }
        Ok(PatternVisit { nodes, indexed })
    }
}

/// The label id, sorted property names and values of an endpoint filter made
/// only of property equalities, as staged by row-sourced mutations.
fn endpoint_key(
    catalog: &Catalog,
    label: &str,
    filter: &Option<PropertyFilter>,
) -> Option<(LabelId, Vec<String>, Vec<Value>)> {
    if label.is_empty() {
        return None;
    }
    let equalities = match filter.as_ref()? {
        PropertyFilter::Eq { .. } => std::slice::from_ref(filter.as_ref()?),
        PropertyFilter::And(filters) if !filters.is_empty() => filters.as_slice(),
        _ => return None,
    };
    let mut names = Vec::with_capacity(equalities.len());
    let mut values = Vec::with_capacity(equalities.len());
    for filter in equalities {
        let PropertyFilter::Eq { property, value } = filter else {
            return None;
        };
        // Distinct names in ascending order give one key per pattern.
        if names.last().is_some_and(|last: &String| last >= property) {
            return None;
        }
        names.push(property.clone());
        values.push(value.clone());
    }
    Some((catalog.label_id(label)?, names, values))
}

/// The resolved ids of a keyed endpoint, or `None` when the commit resolves it.
fn endpoint_ids<'a>(
    patterns: &'a BTreeMap<(LabelId, Vec<String>), EndpointPattern>,
    catalog: &Catalog,
    label: &str,
    filter: &Option<PropertyFilter>,
) -> Option<&'a Vec<NodeId>> {
    let (label_id, names, values) = endpoint_key(catalog, label, filter)?;
    let ids = patterns
        .get(&(label_id, names))
        .and_then(|pattern| pattern.matches.get(&values))
        .expect("every staged endpoint key was registered before resolution");
    Some(ids)
}

/// `IdIn(ids) AND original`, flattening an equality conjunction.
fn id_led_conjunction(ids: &[NodeId], original: PropertyFilter) -> PropertyFilter {
    let ids = match ids {
        [id] => PropertyFilter::IdEq {
            value: Value::Int(id.0 as i64),
        },
        ids => PropertyFilter::IdIn {
            values: ids.iter().map(|id| Value::Int(id.0 as i64)).collect(),
        },
    };
    let mut filters = vec![ids];
    match original {
        PropertyFilter::And(rest) => filters.extend(rest),
        other => filters.push(other),
    }
    PropertyFilter::And(filters)
}

fn endpoint_entry_bytes(values: &[Value]) -> usize {
    size_of::<(Vec<Value>, Vec<NodeId>)>() * 3
        + values.iter().map(value_memory_bytes).sum::<usize>()
}

/// Estimated resident bytes of one staged mutation, including its inline slot.
pub(crate) fn graph_mutation_memory_bytes(mutation: &GraphMutation) -> usize {
    let label_bytes = |label: &str| label.len();
    size_of::<GraphMutation>().saturating_add(match mutation {
        GraphMutation::CreateNode { label, properties } => {
            label_bytes(label).saturating_add(map_memory_bytes(properties))
        }
        GraphMutation::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => label_bytes(label)
            .saturating_add(map_memory_bytes(match_properties))
            .saturating_add(map_memory_bytes(on_create_properties))
            .saturating_add(assignments_memory_bytes(on_match_assignments))
            .saturating_add(assignments_memory_bytes(post_merge_assignments)),
        GraphMutation::CreateRelationshipsBetweenMatches(request) => {
            label_bytes(&request.source_label)
                .saturating_add(label_bytes(&request.target_label))
                .saturating_add(label_bytes(&request.rel_type))
                .saturating_add(map_memory_bytes(&request.rel_properties))
                .saturating_add(optional_filter_memory_bytes(request.source_filter.as_ref()))
                .saturating_add(optional_filter_memory_bytes(request.target_filter.as_ref()))
        }
        // Row-sourced statements stage only the forms above.
        _ => 0,
    })
}

fn assignments_memory_bytes(assignments: &[NodeSetAssignment]) -> usize {
    assignments.iter().fold(0usize, |total, assignment| {
        let value = match &assignment.value {
            NodeSetValue::Value(value)
            | NodeSetValue::Coalesce { default: value }
            | NodeSetValue::PreserveNewerExisting {
                incoming: value, ..
            } => value_memory_bytes(value),
            NodeSetValue::AddInt { .. } | NodeSetValue::DecrementFloorZero => 0,
        };
        total
            .saturating_add(size_of::<NodeSetAssignment>())
            .saturating_add(assignment.property.len())
            .saturating_add(value)
    })
}

fn optional_filter_memory_bytes(filter: Option<&PropertyFilter>) -> usize {
    filter.map_or(0, |filter| {
        size_of::<PropertyFilter>().saturating_add(property_filter_memory_bytes(filter))
    })
}

/// Heap bytes owned by `filter`, excluding its own inline slot.
fn property_filter_memory_bytes(filter: &PropertyFilter) -> usize {
    let values = |values: &[Value]| {
        values
            .iter()
            .map(value_memory_bytes)
            .fold(0usize, usize::saturating_add)
    };
    let bound = |bound: &Option<(Value, bool)>| {
        bound
            .as_ref()
            .map_or(0, |(value, _)| value_memory_bytes(value))
    };
    match filter {
        PropertyFilter::And(filters) | PropertyFilter::Or(filters) => {
            filters.iter().fold(0usize, |total, filter| {
                total
                    .saturating_add(size_of::<PropertyFilter>())
                    .saturating_add(property_filter_memory_bytes(filter))
            })
        }
        PropertyFilter::Not(filter) => {
            size_of::<PropertyFilter>().saturating_add(property_filter_memory_bytes(filter))
        }
        PropertyFilter::IdEq { value } | PropertyFilter::IdNotEq { value } => {
            value_memory_bytes(value)
        }
        PropertyFilter::IdRange { lower, upper } => bound(lower).saturating_add(bound(upper)),
        PropertyFilter::IdIn { values: list } => values(list),
        PropertyFilter::Eq { property, value }
        | PropertyFilter::NotEq { property, value }
        | PropertyFilter::ListContains { property, value } => {
            property.len().saturating_add(value_memory_bytes(value))
        }
        PropertyFilter::IsNull { property } | PropertyFilter::IsNotNull { property } => {
            property.len()
        }
        PropertyFilter::In {
            property,
            values: list,
        } => property.len().saturating_add(values(list)),
        PropertyFilter::ListContainsLower { property, value }
        | PropertyFilter::Contains { property, value }
        | PropertyFilter::StartsWith { property, value }
        | PropertyFilter::EndsWith { property, value } => {
            property.len().saturating_add(value.len())
        }
        PropertyFilter::RegexMatch { property, pattern } => {
            property.len().saturating_add(pattern.as_str().len())
        }
        PropertyFilter::DefaultIfNullOrEq {
            property,
            empty,
            default,
            value,
            ..
        } => property
            .len()
            .saturating_add(value_memory_bytes(empty))
            .saturating_add(value_memory_bytes(default))
            .saturating_add(value_memory_bytes(value)),
        PropertyFilter::Range {
            property,
            lower,
            upper,
        } => property
            .len()
            .saturating_add(bound(lower))
            .saturating_add(bound(upper)),
    }
}
