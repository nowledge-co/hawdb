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

use super::super::PhysicalPlan;
use hawdb_plan::LogicalPlan;

pub(super) fn lower(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::ShortestPath {
            source_variable,
            source_label,
            source_id,
            source_visibility_predicate,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            target_visibility_predicate,
            min_hops,
            max_hops,
            returns,
        } => Some(PhysicalPlan::ShortestPathExec {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_id: source_id.clone(),
            source_visibility_predicate: source_visibility_predicate.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_id: target_id.clone(),
            target_visibility_predicate: target_visibility_predicate.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            returns: returns.clone(),
        }),
        _ => None,
    }
}
