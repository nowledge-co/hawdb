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

use hawdb_cypher::parse;
use hawdb_optimizer::{
    plan_vector_search, CascadesOptimizer, LogicalPlanRoot, OptimizerCatalog,
    OptimizerCatalogIndexes, OptimizerCatalogStatistics, OptimizerConfig, OptimizerContext,
    QueryFamily, ResourceHints,
};
use hawdb_plan::{plan, VectorCandidateSource, VectorSearchLogicalPlan};

const QUERY: &str = "MATCH (m:Memory) WHERE m.id = 7 RETURN m.title AS title";
const EXPECTED: &str = include_str!("golden/indexed_memory_lookup.golden");
const VECTOR_EXPECTED: &str = include_str!("golden/filtered_vector_pipeline.golden");

#[test]
fn indexed_memory_lookup_matches_planner_golden() {
    let statement = parse(QUERY).expect("golden Cypher must parse");
    let logical = plan(&statement).expect("golden Cypher must plan");
    let logical_root = LogicalPlanRoot::new(logical);
    let lowering_ready_root = logical_root.clone().into_lowering_ready();
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "id".to_string()), 100)],
            [],
        ),
    );
    let physical_root = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_lowering_ready_root_with_catalog(&lowering_ready_root, &catalog);

    let actual = render_planner_golden(
        QUERY,
        logical_root.plan(),
        lowering_ready_root.plan(),
        physical_root.plan(),
        physical_root.trace(),
    );
    assert_eq!(actual.trim_end(), EXPECTED.trim_end());
}

#[test]
fn filtered_vector_pipeline_matches_planner_golden() {
    let logical = VectorSearchLogicalPlan {
        embedding_dimension: 384,
        filter_fields: vec!["space_id".to_string(), "unit_type".to_string()],
        residual_filter_fields: vec!["complex_visibility".to_string()],
        initial_candidate_limit: 10,
        candidate_source: VectorCandidateSource::Quantized,
        candidate_limit: 80,
        top_k: 10,
    };
    let context = OptimizerContext::default()
        .with_query_family(QueryFamily::VectorSearch)
        .with_resource_hints(ResourceHints {
            priority: 128,
            max_memory_bytes: Some(8 * 1024 * 1024),
            max_parallelism: 2,
        });
    let planned = plan_vector_search(&logical, &context).expect("vector golden must plan");
    let actual = format!(
        "[logical]\nembedding_dimension={}\nprefilter_fields={:?}\nresidual_filter_fields={:?}\ninitial_candidate_limit={}\ncandidate_source={}\ncandidate_limit={}\ntop_k={}\n\n\
         [physical]\n{}\n\n\
         [properties]\nprecision={}\nmax_parallelism={}\nmax_memory_bytes={:?}\n",
        logical.embedding_dimension,
        logical.filter_fields,
        logical.residual_filter_fields,
        logical.initial_candidate_limit,
        logical.candidate_source.as_str(),
        logical.candidate_limit,
        logical.top_k,
        planned.plan.operator_pipeline().join("\n"),
        planned.properties.precision.as_str(),
        planned.properties.max_parallelism,
        planned.properties.max_memory_bytes,
    );

    assert_eq!(actual.trim_end(), VECTOR_EXPECTED.trim_end());
}

fn render_planner_golden(
    query: &str,
    logical: &hawdb_plan::LogicalPlan,
    lowering_input: &hawdb_plan::LogicalPlan,
    physical: &hawdb_plan::PhysicalPlan,
    trace: &hawdb_optimizer::OptimizerTrace,
) -> String {
    let stages = trace
        .stage_events
        .iter()
        .map(|stage| {
            let stats = stage.stats();
            format!(
                "{} order={} input={} output={} applied={} skipped={}",
                stage.name(),
                stage.apply_order().as_str(),
                stats.input_count,
                stats.output_count,
                stats.applied_rules,
                stats.skipped_rules
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let cost = trace.selected_plan_cost_breakdown;
    let properties = &trace.selected_plan_properties;

    format!(
        "[cypher]\n{query}\n\n\
         [logical]\n{logical:#?}\n\n\
         [lowering-input]\n{lowering_input:#?}\n\n\
         [physical]\n{}\n\n\
         [stage-trace]\n{stages}\n\n\
         [cost]\nestimated_rows={} total={} cpu={} random_io={} sequential_io={} output_rows={}\n\n\
         [properties]\ndistribution={} ordering={:?} covering_fields={:?} scan_pruning={} vector_precision={} memory_budget={}\n",
        physical.explain(0),
        cost.estimated_rows,
        cost.cost,
        cost.cpu,
        cost.random_io,
        cost.sequential_io,
        cost.output_rows,
        properties.distribution.as_str(),
        properties.ordering,
        properties.covering_fields,
        properties.scan_pruning.as_str(),
        properties.vector_precision.as_str(),
        properties.memory_budget.as_str(),
    )
}
