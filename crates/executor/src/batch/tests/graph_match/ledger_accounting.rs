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

//! Query-level ledger settlement and refusal points for per-row graph reads
//! (#1615). Every row admission, growth and release goes through the ledger;
//! a query fits a budget equal to its observed peak and is refused one byte
//! below it, and the ledger returns to zero on every exit path.

use super::*;
use crate::memory_ledger::QueryMemoryLedgerSnapshot;

const NODES: u64 = 12;

/// Rows mix strings, lists, maps and missing properties so admissions see
/// nested growth, nulls and dropped values.
fn fixture_context<T>(
    memory: &ExecutionMemoryConfig,
    ledger: &QueryMemoryLedger,
    task_context: Option<&RuntimeTaskContext>,
    run: impl FnOnce(BatchReadContext<'_>) -> T,
) -> T {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let rel_type = catalog.get_or_create_rel_type("MENTIONS");
    let nodes = (0..NODES)
        .map(|id| {
            let mut properties = BTreeMap::from([
                ("id".to_string(), Value::Int(id as i64)),
                (
                    "name".to_string(),
                    Value::String("n".repeat(id as usize + 1)),
                ),
                (
                    "tags".to_string(),
                    Value::List((0..id % 4).map(|tag| Value::Int(tag as i64)).collect()),
                ),
            ]);
            if id % 3 != 0 {
                properties.insert(
                    "meta".to_string(),
                    Value::Map(BTreeMap::from([
                        ("depth".to_string(), Value::Int(id as i64)),
                        ("label".to_string(), Value::String("m".repeat(id as usize))),
                    ])),
                );
            }
            NodeRecord {
                id: NodeId(id),
                labels: [label].into_iter().collect(),
                properties,
            }
        })
        .collect();
    let relationships = (0..NODES)
        .flat_map(|id| [(id, (id + 1) % NODES), (id, (id + 5) % NODES)])
        .enumerate()
        .map(|(index, (source, target))| hawdb_storage::RelRecord {
            id: hawdb_storage::RelId(index as u64),
            source: NodeId(source),
            target: NodeId(target),
            rel_type,
            properties: BTreeMap::from([("weight".to_string(), Value::Int(index as i64))]),
        })
        .collect();
    let store = store::ReadFixture {
        nodes,
        relationships,
        definition: Some(hawdb_storage::projection::ProjectedGraphDefinition {
            node_labels: vec!["Memory".into()],
            rel_types: vec!["MENTIONS".into()],
            relationship_predicates: BTreeMap::new(),
        }),
        out_of_core: false,
        ..store::ReadFixture::default()
    };
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external);
    let observer = QueryExecutionObserver::default();
    run(BatchReadContext {
        catalog: &catalog,
        store: &store,
        memory,
        memory_ledger: ledger,
        parameters: &parameters,
        external: &external,
        observer: &observer,
        host_scorer: None,
        task_context,
    })
}

const QUERIES: [&str; 5] = [
    // Expansion with relationship and node property reads.
    "MATCH (a:Memory)-[r:MENTIONS]->(b:Memory) RETURN a.name AS an, b.tags AS bt, r.weight AS w",
    // Two expansions: rows grow and shrink as slots bind and unbind.
    "MATCH (a:Memory)-[:MENTIONS]->(b:Memory)-[:MENTIONS]->(c:Memory) RETURN a.id AS a, c.meta AS cm",
    // Projection growing lists and maps, with nulls from missing properties.
    "MATCH (a:Memory)-[:MENTIONS]->(b:Memory) RETURN [a.name, b.tags, a.meta] AS grown, {meta: a.meta, none: b.missing} AS wrapped",
    // Retained grouping state.
    "MATCH (a:Memory)-[:MENTIONS]->(b:Memory) RETURN a.id AS id, collect(b.name) AS names",
    // Retained distinct state over maps and nulls.
    "MATCH (a:Memory)-[:MENTIONS]->(b:Memory) RETURN DISTINCT b.meta AS meta",
];

fn plan(query: &str) -> PhysicalPlan {
    lower(hawdb_plan_cypher::plan_pipeline_query(query, &BTreeMap::new()).unwrap())
}

fn run(
    plan: &PhysicalPlan,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> (Result<BatchControl>, QueryMemoryLedgerSnapshot) {
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let result = fixture_context(memory, &ledger, task_context, |context| {
        execute_binding_batches(plan, context, ExecutionLimit::unlimited(), emit)
    });
    (result, ledger.snapshot())
}

fn collect(
    plan: &PhysicalPlan,
    memory: &ExecutionMemoryConfig,
) -> (Result<Vec<Binding>>, QueryMemoryLedgerSnapshot) {
    let mut rows = Vec::new();
    let (result, snapshot) = run(plan, memory, None, &mut |batch| {
        rows.extend(batch);
        Ok(BatchControl::Continue)
    });
    (result.map(|_| rows), snapshot)
}

#[test]
fn graph_match_queries_fit_their_peak_and_are_refused_one_byte_below() {
    for query in QUERIES {
        let plan = plan(query);
        let memory = ExecutionMemoryConfig::default();
        let (rows, unbounded) = collect(&plan, &memory);
        let rows = rows.unwrap_or_else(|error| panic!("{query}: {error}"));
        assert!(!rows.is_empty(), "{query}");
        assert_eq!(unbounded.used_bytes, 0, "{query}");
        let peak = unbounded.peak_bytes;
        assert!(peak > 0, "{query}");

        let exact = ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(peak).unwrap(),
            ..memory.clone()
        };
        let (fitted, snapshot) = collect(&plan, &exact);
        assert_eq!(fitted.unwrap(), rows, "{query}");
        assert_eq!(
            (snapshot.used_bytes, snapshot.peak_bytes),
            (0, peak),
            "{query}"
        );

        let below = ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(peak - 1).unwrap(),
            ..memory.clone()
        };
        let (refused, snapshot) = collect(&plan, &below);
        let HawDBError::ResourceBudgetExceeded(error) = refused.unwrap_err() else {
            panic!("{query}: expected a typed memory refusal");
        };
        assert_eq!(error.resource, hawdb_core::ResourceBudgetKind::MemoryBytes);
        assert_eq!(error.budget, peak as u64 - 1, "{query}");
        assert_eq!(snapshot.used_bytes, 0, "{query}");
        assert!(snapshot.peak_bytes < peak, "{query}");
    }
}

#[test]
fn graph_match_ledger_settles_after_mid_stream_cancellation_and_consumer_errors() {
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(1).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    for query in QUERIES {
        let plan = plan(query);

        let token = hawdb_core::RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(token.clone());
        let mut batches = 0;
        let (result, snapshot) = run(&plan, &memory, Some(&task), &mut |_| {
            batches += 1;
            token.cancel();
            Ok(BatchControl::Continue)
        });
        assert_eq!(batches, 1, "{query}");
        assert!(
            result.unwrap_err().to_string().contains("cancelled"),
            "{query}"
        );
        assert_eq!(snapshot.used_bytes, 0, "{query}");
        assert!(snapshot.peak_bytes > 0, "{query}");

        for stop in [false, true] {
            let mut batches = 0;
            let (result, snapshot) = run(&plan, &memory, None, &mut |_| {
                batches += 1;
                if batches < 2 {
                    Ok(BatchControl::Continue)
                } else if stop {
                    Ok(BatchControl::Stop)
                } else {
                    Err(HawDBError::Execution("consumer failure".into()))
                }
            });
            assert_eq!(result.is_err(), !stop, "{query}");
            assert_eq!(snapshot.used_bytes, 0, "{query}");
        }
    }
}
