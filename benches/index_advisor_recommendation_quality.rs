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

//! `IndexAdvisor` recommendation quality against named workload fixtures.
//!
//! This isn't a latency benchmark: `IndexAdvisor::recommend` is a handful
//! of threshold comparisons, not something with a meaningful throughput
//! number. What needs measuring here is *quality* -- whether the default
//! advisor's recommendation on each fixture matches what the workload
//! actually calls for, per the qualitative reasoning in `advisor.rs`'s
//! module docs and the PR5/PR6 benchmarks its thresholds were calibrated
//! from. Each fixture is a named, documented scenario with an expected
//! action; the report is the fraction of fixtures the default advisor
//! gets right, plus the losing cases in full.

use hawdb_vector_projection::{IndexAction, IndexAdvisor, WorkloadSample};
use serde_json::json;

struct Fixture {
    name: &'static str,
    description: &'static str,
    sample: WorkloadSample,
    expected: IndexAction,
}

fn fixtures() -> Vec<Fixture> {
    vec![
        Fixture {
            name: "small_healthy_corpus",
            description: "Small corpus, no delta, fast exact scan -- nothing to do.",
            sample: WorkloadSample::new(1_000).with_p95_search_seconds(0.0002),
            expected: IndexAction::NoActionNeeded,
        },
        Fixture {
            name: "fresh_writes_below_fold_threshold",
            description: "A little write traffic landed in the delta, but not enough to \
                           matter yet (PR5 found delta scan cost roughly doubles latency at \
                           just 1% delta fraction, hence the low 2% default threshold).",
            sample: WorkloadSample::new(10_000).with_delta_fraction(0.01),
            expected: IndexAction::NoActionNeeded,
        },
        Fixture {
            name: "write_heavy_delta_past_threshold",
            description: "Sustained upserts pushed the delta past the fold threshold; \
                           every query is now paying the delta's exact-scan cost.",
            sample: WorkloadSample::new(10_000).with_delta_fraction(0.08),
            expected: IndexAction::FoldDeltaIntoBase,
        },
        Fixture {
            name: "large_slow_corpus_without_hnsw",
            description: "Large corpus, no HNSW built yet, p95 latency already above where \
                           PR6's benchmark found HNSW paying for its build cost and memory.",
            sample: WorkloadSample::new(50_000).with_p95_search_seconds(0.0028),
            expected: IndexAction::ConsiderBuildingHnsw,
        },
        Fixture {
            name: "large_corpus_already_has_hnsw",
            description: "Same shape as the previous fixture, but HNSW is already built -- \
                           recommending it again would be a no-op at best.",
            sample: WorkloadSample::new(50_000)
                .with_p95_search_seconds(0.0028)
                .with_hnsw_available(true),
            expected: IndexAction::NoActionNeeded,
        },
        Fixture {
            name: "large_corpus_but_still_fast",
            description: "Large corpus but latency is still comfortable -- the base's \
                           quantized scan is keeping up, so HNSW's memory and build cost \
                           are not yet justified.",
            sample: WorkloadSample::new(50_000).with_p95_search_seconds(0.0002),
            expected: IndexAction::NoActionNeeded,
        },
        Fixture {
            name: "small_corpus_high_latency",
            description: "Latency is high but the corpus is small -- more likely a \
                           resource-contention or configuration problem than something an \
                           index shape change fixes, so the advisor should not chase it.",
            sample: WorkloadSample::new(200).with_p95_search_seconds(0.01),
            expected: IndexAction::NoActionNeeded,
        },
        Fixture {
            name: "both_delta_and_hnsw_conditions_met",
            description: "A workload that would justify both actions at once; folding is \
                           cheaper and helps every query regardless of which index answers \
                           it, so the advisor should recommend folding first.",
            sample: WorkloadSample::new(50_000)
                .with_delta_fraction(0.1)
                .with_p95_search_seconds(0.0028),
            expected: IndexAction::FoldDeltaIntoBase,
        },
    ]
}

fn main() {
    let advisor = IndexAdvisor::new();
    let results: Vec<serde_json::Value> = fixtures()
        .into_iter()
        .map(|fixture| {
            let recommendation = advisor.recommend(&fixture.sample);
            let correct = recommendation.action == fixture.expected;
            json!({
                "name": fixture.name,
                "description": fixture.description,
                "expected": format!("{:?}", fixture.expected),
                "recommended": format!("{:?}", recommendation.action),
                "confidence": recommendation.confidence,
                "reason": recommendation.reason,
                "correct": correct,
            })
        })
        .collect();

    let correct_count = results
        .iter()
        .filter(|result| result["correct"].as_bool().unwrap_or(false))
        .count();
    let accuracy = correct_count as f64 / results.len() as f64;

    println!(
        "index_advisor_recommendation_quality {}",
        json!({
            "fixture_count": results.len(),
            "correct_count": correct_count,
            "accuracy": accuracy,
            "fixtures": results,
        })
    );

    assert_eq!(
        correct_count,
        fixtures().len(),
        "the default IndexAdvisor should match every fixture's expected action; see stdout for the failing case(s)"
    );
}
