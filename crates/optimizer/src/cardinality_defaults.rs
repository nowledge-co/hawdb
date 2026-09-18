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

//! Compile-time cardinality heuristics used when stronger statistics are absent.
//!
//! These preserve the existing estimates; they are not benchmark-calibrated
//! probabilities or a public tuning API. Equal values with different meanings
//! stay separate so that later calibration is an explicit, reviewable decision.
//! Callers retain their statistics precedence, ceiling division, saturation,
//! NDV caps and minimum-one estimates. Arithmetic identities are not defaults.

/// Unknown predicate shape or binding: retain half of the input rows.
pub(crate) const FILTER_SELECTIVITY_DIVISOR: u64 = 2;

/// Unsupported grouping expression or missing key NDV: estimate one group per
/// four input rows. Known grouping keys instead use their capped NDV product.
pub(crate) const AGGREGATE_GROUPS_DIVISOR: u64 = 4;

/// String predicates lack query-specific statistics. These denominators are
/// capped by the property's NDV, for both nodes and relationships.
pub(crate) const CONTAINS_SELECTIVITY_DIVISOR: u64 = 4;
pub(crate) const STARTS_WITH_SELECTIVITY_DIVISOR: u64 = 8;
pub(crate) const ENDS_WITH_SELECTIVITY_DIVISOR: u64 = 6;

/// Without a null-frequency statistic, estimate rows / min(max(NDV, 1), 10).
/// IS NOT NULL uses the complementary row count with its existing floor.
pub(crate) const NULL_SELECTIVITY_DIVISOR_CAP: u64 = 10;

/// Missing or empty range histogram: retain half of the input population.
pub(crate) const RANGE_SELECTIVITY_DIVISOR: u64 = 2;

/// A sampled histogram uses an add-one prior: one matching pseudo-observation
/// out of two total. Complete histograms do not use this prior.
pub(crate) const SAMPLED_HISTOGRAM_MATCH_PSEUDOCOUNT: u64 = 1;
pub(crate) const SAMPLED_HISTOGRAM_TOTAL_PSEUDOCOUNT: u64 = 2;

/// Until query-specific full-text statistics exist, retain a quarter of the
/// label's rows, before and after projection fusion. Materialization does not
/// change selectivity, and an already-covered predicate is not applied twice.
pub(crate) const FULL_TEXT_SELECTIVITY_DIVISOR: u64 = 4;

/// Materialized/hash/merge joins without usable key NDV retain 10% of candidate
/// pairs. Probe inputs already estimate per-outer-row fanout and do not use it.
pub(crate) const JOIN_SELECTIVITY_DIVISOR: u64 = 10;
