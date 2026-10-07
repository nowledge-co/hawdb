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

/// Materialized/hash/merge joins without usable key NDV retain 10% of candidate
/// pairs. Probe inputs already estimate per-outer-row fanout and do not use it.
pub(crate) const JOIN_SELECTIVITY_DIVISOR: u64 = 10;
