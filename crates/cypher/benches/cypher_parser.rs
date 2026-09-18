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

use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

const ITERATIONS: usize = 20_000;
const SAMPLES: usize = 11;

fn main() {
    let cases = cases();
    for case in &cases {
        hawdb_cypher::parse(case.query)
            .unwrap_or_else(|error| panic!("{} benchmark query must parse: {error}", case.name));
    }

    let reports = cases.iter().map(benchmark_case).collect::<Vec<_>>();
    println!("cypher_parser {}", json!({ "cases": reports }));
}

fn benchmark_case(case: &Case) -> serde_json::Value {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(
                hawdb_cypher::parse(black_box(case.query))
                    .unwrap_or_else(|error| panic!("{} parse failed: {error}", case.name)),
            );
        }
        samples.push(started.elapsed().as_nanos());
    }
    samples.sort_unstable();
    let median_ns = samples[SAMPLES / 2];
    let query_ns = median_ns / ITERATIONS as u128;
    let bytes_per_second = if query_ns == 0 {
        0
    } else {
        (case.query.len() as u128)
            .saturating_mul(1_000_000_000)
            .checked_div(query_ns)
            .unwrap_or_default()
    };
    json!({
        "name": case.name,
        "category": case.category,
        "input_bytes": case.query.len(),
        "iterations": ITERATIONS,
        "median_ns_per_query": query_ns,
        "bytes_per_second": bytes_per_second,
    })
}

struct Case {
    name: &'static str,
    category: &'static str,
    query: &'static str,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "exact_lookup",
            category: "short_read",
            query: "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title LIMIT 1",
        },
        Case {
            name: "bounded_expand",
            category: "graph_read",
            query: "MATCH (m:Memory)-[:MENTIONS*1..3]->(e:Entity) WHERE m.workspace_id = $workspace_id AND e.name CONTAINS $query RETURN DISTINCT e.id AS id, e.name AS name ORDER BY name ASC LIMIT $limit",
        },
        Case {
            name: "aggregate_page",
            category: "aggregate_read",
            query: "MATCH (m:Memory) WHERE m.workspace_id = $workspace_id AND m.created_at >= $start RETURN m.kind AS kind, count(m) AS count ORDER BY count DESC, kind ASC SKIP $offset LIMIT $limit",
        },
        Case {
            name: "parameterized_create",
            category: "mutation",
            query: "CREATE (:Memory {id: $id, workspace_id: $workspace_id, title: $title, body: $body, created_at: $created_at})",
        },
        Case {
            name: "optimizer_hint",
            category: "hinted_read",
            query: "CYPHER SYSTEM.optimizer_search = 'auto' MATCH (m:Memory) WHERE m.id = $id RETURN m.id AS id, m.title AS title LIMIT 1",
        },
    ]
}
