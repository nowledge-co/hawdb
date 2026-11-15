// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Expressions that read the graph snapshot: pattern predicates, pattern
//! comprehensions and `startNode`/`endNode` of relationships without a bound
//! pattern step.
//!
//! A pattern expression runs its correlated MATCH program once per
//! evaluation, through the same runtime as a MATCH clause over one row. The
//! subquery row holds only the enclosing graph variables and imported values
//! the pattern reads; enclosing columns its body reads are lent as locals, so
//! a large column such as a collected list is never copied per evaluation.
//! A value imported only to name the entity a pattern variable starts from,
//! such as a list element node, reaches the match as that entity's identity,
//! and a seed bound to a graph slot as its record's id, without copying the
//! value (#1615).
//! Records read, subquery rows and the produced list are charged to the
//! evaluation's memory account, every candidate the match visits is charged
//! as list-expression work, and the runtime checks cancellation between
//! candidates. A relationship value must name a stored relationship before
//! its endpoint is read, and that read honors the plan's node visibility.

use super::*;
use crate::graph_match::{
    import_identity, visit_correlated_matches, CorrelatedInput, ImportIdentity, MatchContext,
    ResolvedImport,
};
use crate::store::{GraphExecutionRead, ScanControl};
use hawdb_plan_cypher::Predicate;
use hawdb_plan_cypher::{
    GraphBindingImport, GraphEntityKind, GraphMatchProgram, GraphMatchStep, PatternProjection,
};
use hawdb_storage::read_view::{AdmittedNodeRead, AdmittedRelationshipRead};
use hawdb_storage::{NodeId, RelId, RelRecord};
use std::borrow::Cow;
use std::collections::BTreeSet;

/// Read access to the graph snapshot for one evaluation site.
#[derive(Clone, Copy)]
pub(crate) struct ExpressionGraph<'a> {
    pub(crate) store: &'a dyn GraphExecutionRead,
    pub(crate) context: MatchContext<'a>,
    /// Charged for graph reads and produced lists when the evaluation itself
    /// has no memory account.
    pub(crate) account: Option<&'a QueryMemoryAccount>,
}

impl<'a> ExpressionGraph<'a> {
    pub(crate) fn new(
        store: &'a dyn GraphExecutionRead,
        context: crate::pipeline::BatchExecutionContext<'a>,
    ) -> Self {
        Self {
            store,
            context: context.into(),
            account: None,
        }
    }

    /// The same access, charging `account` when an evaluation has none.
    pub(crate) fn with_account(self, account: &'a QueryMemoryAccount) -> Self {
        Self {
            account: Some(account),
            ..self
        }
    }
}

/// The enclosing scope of a pattern expression.
pub(super) struct Scope<'a, 'w> {
    pub(super) catalog: &'a Catalog,
    pub(super) binding: &'a Binding,
    pub(super) locals: Option<&'a ListLocals<'a>>,
    pub(super) account: Option<&'w QueryMemoryAccount>,
    pub(super) work: &'w ListWork,
}

fn graph_access<'g>(
    what: &str,
    graph: Option<&'g ExpressionGraph<'g>>,
) -> Result<&'g ExpressionGraph<'g>> {
    graph.ok_or_else(|| {
        HawDBError::Execution(format!(
            "{what} require graph access, which this expression position does not provide"
        ))
    })
}

/// The account graph reads and produced values are charged to: the
/// evaluation's, else the evaluation site's. A site without one, such as
/// mutation preflight, gets a private ledger bounded by its read budget.
enum Charged<'a> {
    Shared(&'a QueryMemoryAccount),
    Private(QueryMemoryAccount),
}

impl Charged<'_> {
    fn account(&self) -> &QueryMemoryAccount {
        match self {
            Self::Shared(account) => account,
            Self::Private(account) => account,
        }
    }
}

fn charged<'a>(
    account: Option<&'a QueryMemoryAccount>,
    graph: &'a ExpressionGraph<'a>,
) -> Charged<'a> {
    match account.or(graph.account) {
        Some(account) => Charged::Shared(account),
        None => {
            let budget = graph.context.query_memory_bytes;
            Charged::Private(crate::QueryMemoryLedger::new(budget).account(
                crate::QueryMemoryClass::BlockingState,
                "pattern expression",
                budget,
            ))
        }
    }
}

/// A pattern predicate's truth, or a pattern comprehension's list.
pub(super) fn evaluate<'a>(
    pattern: &'a PatternProjection,
    scope: Scope<'a, '_>,
    graph: Option<&ExpressionGraph<'_>>,
) -> Result<ProjectedValue<'a>> {
    let graph = graph_access("pattern expressions", graph)?;
    let charged = charged(scope.account, graph);
    let account = charged.account();
    crate::pipeline::runtime_checkpoint(graph.context.task_context)?;
    scope.work.charge(1)?;
    let mut row = Binding::values(BTreeMap::new());
    for name in &pattern.graph_variables {
        let copied = match graph_variable_kind(&pattern.program, name) {
            Some(GraphEntityKind::Node) => {
                kind_matches(scope.binding, name, GraphEntityKind::Node)?;
                row.nodes.insert_shared(&scope.binding.nodes, name)
            }
            Some(GraphEntityKind::Relationship) => {
                kind_matches(scope.binding, name, GraphEntityKind::Relationship)?;
                row.relationships
                    .insert_shared(&scope.binding.relationships, name)
            }
            // Read only by the body, which checks what it reads.
            None => {
                row.nodes.insert_shared(&scope.binding.nodes, name)
                    || row
                        .relationships
                        .insert_shared(&scope.binding.relationships, name)
            }
        };
        if copied {
            continue;
        }
        // A variable an unmatched OPTIONAL MATCH left NULL matches nothing.
        if scope.binding.values.get(name) != Some(&Value::Null) {
            return Err(HawDBError::Execution(format!(
                "missing variable '{name}' during pattern evaluation"
            )));
        }
        row.values.insert(name.clone(), Value::Null);
    }
    // An argument read only as an import reaches the match by the identity
    // of the entity it holds, without an owned copy of its value. A value
    // that is not such an entity is copied as before, so the import fails
    // on it where and as it always did.
    let mut identities = Vec::new();
    let mut _arguments = account.reserve(0)?;
    for (column, argument) in &pattern.arguments {
        let value = evaluate_scoped(
            argument,
            scope.catalog,
            scope.binding,
            scope.locals,
            Some(account),
            scope.work,
            Some(graph),
        )?;
        if let Some(import) = identity_import(pattern, &pattern.program, column)
            && let Ok(Some(identity)) = import_identity(value.as_ref(), import)
        {
            identities.push(ImportIdentity {
                column: column.as_str(),
                identity,
            });
            continue;
        }
        let value = value.into_owned_accounted(&mut _arguments)?;
        row.values.insert(column.clone(), value);
    }
    // A seeded pattern matches once from each distinct node among the seed
    // values, with its seeded program; the WHERE still decides every match
    // (#1607). Seed values that are NULL or not nodes cannot equal the node.
    let mut _seeds = account.reserve(0)?;
    let runs = match &pattern.seeds {
        None => vec![(&pattern.program, Cow::Borrowed(&row), identities)],
        Some(seeds) => {
            let by_identity = identity_import(pattern, &seeds.program, &seeds.column).is_some();
            let mut runs = Vec::new();
            let mut seen = BTreeSet::new();
            for expression in &seeds.expressions {
                if by_identity {
                    let Some(id) = seed_identity(expression, &scope, graph, account)? else {
                        continue;
                    };
                    if !seen.insert(id) {
                        continue;
                    }
                    let mut seeded = identities.clone();
                    seeded.push(ImportIdentity {
                        column: seeds.column.as_str(),
                        identity: ResolvedImport {
                            id: id.0,
                            source: None,
                        },
                    });
                    runs.push((&seeds.program, Cow::Borrowed(&row), seeded));
                    continue;
                }
                // A computed seed (such as `startNode(r)`) is charged to this
                // account while it is built; its owned copy is charged to the
                // seeds lease on the same account, so the charge moves rather
                // than transfers between accounts.
                let computed = evaluate_scoped(
                    expression,
                    scope.catalog,
                    scope.binding,
                    scope.locals,
                    Some(account),
                    scope.work,
                    Some(graph),
                )?;
                let payload = crate::binding::value_memory_bytes(computed.as_ref())
                    .saturating_sub(std::mem::size_of::<Value>());
                let value = computed.into_owned();
                _seeds.grow(payload)?;
                let Some(id) = seed_node_id(graph.store, &value)? else {
                    continue;
                };
                if !seen.insert(id) {
                    continue;
                }
                let mut seeded = row.clone();
                seeded.values.insert(seeds.column.clone(), value);
                runs.push((&seeds.program, Cow::Owned(seeded), identities.clone()));
            }
            runs
        }
    };
    with_column_frames(
        &pattern.columns,
        scope.binding,
        scope.locals,
        &mut |locals| {
            let mut found = false;
            let mut output = pattern
                .projection
                .as_ref()
                .map(|_| list::ListBuilder::new(Some(account), scope.work, 0))
                .transpose()?;
            for (program, row, identities) in &runs {
                if found {
                    break;
                }
                visit_correlated_matches(
                    program,
                    graph.store,
                    graph.context,
                    account,
                    CorrelatedInput { row, identities },
                    // Every candidate the match visits is list-expression work,
                    // so a pattern that scans is bounded like one that matches.
                    &|| scope.work.charge(1),
                    &mut |matched| {
                        let evaluate = |expression| {
                            evaluate_scoped(
                                expression,
                                scope.catalog,
                                matched,
                                locals,
                                Some(account),
                                scope.work,
                                Some(graph),
                            )
                        };
                        if let Some(predicate) = &pattern.predicate {
                            match evaluate(predicate)?.as_ref() {
                                Value::Bool(true) => {}
                                Value::Bool(false) | Value::Null => {
                                    return Ok(ScanControl::Continue)
                                }
                                value => {
                                    return Err(HawDBError::Execution(format!(
                                        "pattern WHERE requires a boolean value, got {:?}",
                                        description(value, Some(account))
                                    )))
                                }
                            }
                        }
                        match (&pattern.projection, output.as_mut()) {
                            (Some(projection), Some(output)) => {
                                output.push_projected(evaluate(projection)?)?;
                                Ok(ScanControl::Continue)
                            }
                            // One match decides a pattern predicate.
                            _ => {
                                found = true;
                                Ok(ScanControl::Stop)
                            }
                        }
                    },
                )?;
            }
            Ok(match output {
                Some(output) => output.finish(),
                None => ProjectedValue::scalar(Value::Bool(found)),
            })
        },
    )
}

/// The import of `program` that reads `column`, when the pattern passes that
/// column by identity.
fn identity_import<'p>(
    pattern: &PatternProjection,
    program: &'p GraphMatchProgram,
    column: &str,
) -> Option<&'p GraphBindingImport> {
    if !pattern.identity_columns.iter().any(|name| name == column) {
        return None;
    }
    program
        .imports
        .iter()
        .find(|import| import.column == column)
}

/// The id of the node a seed expression can equal, as [`seed_node_id`] of
/// its value. A seed that names a node bound in the enclosing row's graph
/// slots is that node, already read from this snapshot, so neither its
/// value nor another read of it is needed.
fn seed_identity(
    expression: &ProjectionExpression,
    scope: &Scope<'_, '_>,
    graph: &ExpressionGraph<'_>,
    account: &QueryMemoryAccount,
) -> Result<Option<NodeId>> {
    if let Some(node) = slot_node(expression, scope.binding) {
        return Ok(Some(node.id));
    }
    let value = evaluate_scoped(
        expression,
        scope.catalog,
        scope.binding,
        scope.locals,
        Some(account),
        scope.work,
        Some(graph),
    )?;
    seed_node_id(graph.store, value.as_ref())
}

/// The node bound in `binding`'s graph slots that `expression` evaluates to,
/// when it reads one directly: a node variable, or the endpoint of a
/// relationship that its MATCH step binds beside it. `None` for every other
/// expression, which is evaluated instead.
fn slot_node<'b>(
    expression: &ProjectionExpression,
    binding: &'b Binding,
) -> Option<&'b NodeRecord> {
    match expression {
        ProjectionExpression::Variable { variable } => binding.nodes.get(variable),
        ProjectionExpression::RelationshipEndpoint {
            endpoint,
            variable,
            endpoints: Some(endpoints),
        } => {
            let relationship = binding.relationships.get(variable)?;
            let id = match endpoint {
                hawdb_core::RelationshipEndpoint::Start => relationship.source,
                hawdb_core::RelationshipEndpoint::End => relationship.target,
            };
            endpoints
                .iter()
                .filter_map(|name| binding.nodes.get(name))
                .find(|node| node.id == id)
        }
        _ => None,
    }
}

/// The id of the node a seed value can equal, or `None` when it can equal no
/// node of the graph. A node value is a map holding `_id` and `labels`, the
/// shape a MATCH import accepts; NULL, scalars, lists and other maps never
/// equal one, and neither does a node-shaped map whose id names no node.
fn seed_node_id(store: &dyn GraphExecutionRead, value: &Value) -> Result<Option<NodeId>> {
    let Value::Map(entries) = value else {
        return Ok(None);
    };
    let (Some(Value::Int(id)), true) = (entries.get("_id"), entries.contains_key("labels")) else {
        return Ok(None);
    };
    let Ok(id) = u64::try_from(*id) else {
        return Ok(None);
    };
    let id = NodeId(id);
    Ok(store.node_owned(id)?.map(|_| id))
}

/// Whether the pattern matches `name` as a node or a relationship, if at all.
fn graph_variable_kind(program: &GraphMatchProgram, name: &str) -> Option<GraphEntityKind> {
    program.steps.iter().find_map(|step| match step {
        GraphMatchStep::Node(node) if node.variable == name => Some(GraphEntityKind::Node),
        GraphMatchStep::Expand {
            source,
            relationship,
            target,
            ..
        } => {
            if source == name || target.variable == name {
                Some(GraphEntityKind::Node)
            } else if relationship.as_deref() == Some(name) {
                Some(GraphEntityKind::Relationship)
            } else {
                None
            }
        }
        _ => None,
    })
}

/// Fails when the enclosing row binds `name` to the other kind of entity.
fn kind_matches(binding: &Binding, name: &str, kind: GraphEntityKind) -> Result<()> {
    let other = match kind {
        GraphEntityKind::Node => binding.relationships.contains_key(name),
        GraphEntityKind::Relationship => binding.nodes.contains_key(name),
    };
    if other {
        let expected = match kind {
            GraphEntityKind::Node => "a node",
            GraphEntityKind::Relationship => "a relationship",
        };
        return Err(HawDBError::Execution(format!(
            "pattern variable '{name}' must be bound to {expected}"
        )));
    }
    Ok(())
}

/// Lends each enclosing row column in `columns` as a local, innermost last.
/// A name the row lacks is an enclosing comprehension's own local, which
/// `parent` already provides.
fn with_column_frames<'p, R>(
    columns: &'p [String],
    binding: &'p Binding,
    parent: Option<&'p ListLocals<'p>>,
    visit: &mut dyn FnMut(Option<&ListLocals<'_>>) -> Result<R>,
) -> Result<R> {
    let Some((column, rest)) = columns.split_first() else {
        return visit(parent);
    };
    match binding.values.get(column) {
        Some(value) => {
            let frame = ListLocals {
                name: column,
                value,
                parent,
            };
            with_column_frames(rest, binding, Some(&frame), visit)
        }
        None => with_column_frames(rest, binding, parent, visit),
    }
}

/// `startNode(r)`/`endNode(r)` of a relationship value. The value must name
/// a stored relationship with the same endpoints, so a map that merely has a
/// relationship's keys reads nothing; the node is the stored record's
/// source or target, NULL when `visibility` does not admit it. Under a
/// visibility policy a value naming no stored relationship is NULL as well;
/// without one it fails.
pub(super) fn relationship_value_endpoint<'a>(
    endpoint: hawdb_core::RelationshipEndpoint,
    relationship: &Value,
    visibility: Option<&Predicate>,
    catalog: &Catalog,
    account: Option<&QueryMemoryAccount>,
    graph: Option<&ExpressionGraph<'_>>,
) -> Result<ProjectedValue<'a>> {
    let name = endpoint.function_name();
    let fields = match relationship {
        Value::Null => return Ok(ProjectedValue::scalar(Value::Null)),
        Value::Map(fields) => fields,
        value => {
            return Err(HawDBError::Execution(format!(
                "{name}() requires a relationship, got {:?}",
                description(value, account)
            )))
        }
    };
    let field = |key: &str| match fields.get(key) {
        Some(Value::Int(id)) if *id >= 0 => Some(*id as u64),
        _ => None,
    };
    let not_a_relationship = || {
        HawDBError::Execution(format!(
            "{name}() requires a relationship, got a map that names no stored relationship"
        ))
    };
    let (Some(id), Some(source), Some(target)) =
        (field("_id"), field("source_id"), field("target_id"))
    else {
        return Err(not_a_relationship());
    };
    let what = format!("{name}() of a relationship value");
    let graph = graph_access(&what, graph)?;
    let charged = charged(account, graph);
    let account = charged.account();
    let Some(stored) = stored_relationship(graph, account, RelId(id), NodeId(source))?
        .filter(|stored| stored.target == NodeId(target))
    else {
        // Under a visibility policy an absent relationship reads like one
        // whose endpoint is hidden, so guessed ids cannot probe whether a
        // relationship exists.
        if visibility.is_some() {
            return Ok(ProjectedValue::scalar(Value::Null));
        }
        return Err(not_a_relationship());
    };
    let node = match endpoint {
        hawdb_core::RelationshipEndpoint::Start => stored.source,
        hawdb_core::RelationshipEndpoint::End => stored.target,
    };
    read_node(name, node, visibility, catalog, account, graph)
}

/// The stored relationship `id` when its source is `source`, by a point read
/// of its endpoints and type in the graph snapshot (#1644). The snapshot's
/// point read resolves transaction and uncheckpointed overlays and tombstones
/// exactly as its adjacency does, so this finds the record the source's
/// outgoing adjacency would list under `id`, without visiting the rest.
fn stored_relationship(
    graph: &ExpressionGraph<'_>,
    account: &QueryMemoryAccount,
    id: RelId,
    source: NodeId,
) -> Result<Option<RelRecord>> {
    let task_context = graph.context.task_context;
    let read = graph.store.projected_relationship_with_allocation(
        id,
        &BTreeSet::new(),
        &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
            &mut |bytes| crate::store::admit_graph_read(account, task_context, bytes).map(Some),
            task_context,
        ),
    )?;
    match read {
        AdmittedRelationshipRead::Relationship(relationship) => {
            let (relationship, _allocation) = relationship.into_parts();
            Ok((relationship.source == source).then_some(relationship))
        }
        AdmittedRelationshipRead::Missing => Ok(None),
        // Admission above never declines a read.
        AdmittedRelationshipRead::Stopped => Err(HawDBError::Execution(
            "relationship point read stopped before admission".into(),
        )),
    }
}

/// The node `id` of a relationship bound in the row, as a node value read
/// from the graph snapshot.
pub(super) fn stored_node<'a>(
    name: &str,
    id: NodeId,
    catalog: &Catalog,
    account: Option<&QueryMemoryAccount>,
    graph: Option<&ExpressionGraph<'_>>,
) -> Result<ProjectedValue<'a>> {
    let what = format!("{name}() of a relationship without its pattern endpoints");
    let graph = graph_access(&what, graph)?;
    let charged = charged(account, graph);
    read_node(name, id, None, catalog, charged.account(), graph)
}

/// Reads node `id`, NULL when `visibility` does not admit it. A node missing
/// from the snapshot fails, as a bound node that disappears during MATCH does.
fn read_node<'a>(
    name: &str,
    id: NodeId,
    visibility: Option<&Predicate>,
    catalog: &Catalog,
    account: &QueryMemoryAccount,
    graph: &ExpressionGraph<'_>,
) -> Result<ProjectedValue<'a>> {
    let task_context = graph.context.task_context;
    let mut admit =
        |bytes: usize| crate::store::admit_graph_read(account, task_context, bytes).map(Some);
    let read = graph.store.node_with_allocation(
        id,
        None,
        &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(&mut admit, task_context),
    )?;
    let AdmittedNodeRead::Node(node) = read else {
        return Err(HawDBError::Execution(format!(
            "{name}() endpoint node {} disappeared during the query",
            id.0
        )));
    };
    let (node, _read) = node.into_parts();
    if let Some(visibility) = visibility {
        let mut binding = Binding::values(BTreeMap::new());
        binding.nodes.insert(
            hawdb_plan_cypher::ENDPOINT_VISIBILITY_VARIABLE.to_string(),
            node.clone(),
        );
        let visible = crate::expression::evaluate_predicate_with_context(
            visibility,
            catalog,
            graph.store,
            &binding,
            graph.context.observer,
            crate::store::AdjacencyReadMemory {
                budget_bytes: graph.context.query_memory_bytes.get(),
                account: Some(account),
            },
            task_context,
        )?;
        if !visible {
            return Ok(ProjectedValue::scalar(Value::Null));
        }
    }
    allocated(node_value_bytes(&node, catalog), Some(account), || {
        node_value(&node, catalog)
    })
}
