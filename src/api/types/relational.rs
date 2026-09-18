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

pub use crate::relational_sql::{
    RelationalJoinPlanningAttempt, RelationalJoinPlanningBudget, RelationalJoinPlanningCost,
    RelationalJoinPlanningDirective, RelationalJoinPlanningFallbackClass,
    RelationalJoinPlanningOutcome, RelationalJoinPlanningReason, RelationalJoinPlanningStatus,
    RelationalJoinPlanningStrategy, RelationalOperatorCardinalityProfile, RelationalOperatorId,
    RelationalOperatorKind, RelationalSqlStageTimings,
};
pub use hawdb_executor::BlockingOperatorMemoryReport;
pub use hawdb_relational::{
    ProfiledRelationalSqlQueryOutput, RelationalSqlIndexReadProfile, RelationalSqlReadProfile,
    RelationalSqlRowReadProfile,
};

#[cfg(test)]
mod tests {
    use super::{
        ProfiledRelationalSqlQueryOutput, RelationalSqlIndexReadProfile, RelationalSqlReadProfile,
        RelationalSqlRowReadProfile,
    };

    #[test]
    fn facade_reexports_owner_read_profile_contracts() {
        let _: fn(
            ProfiledRelationalSqlQueryOutput,
        ) -> hawdb_relational::ProfiledRelationalSqlQueryOutput = |profile| profile;
        let _: fn(RelationalSqlReadProfile) -> hawdb_relational::RelationalSqlReadProfile =
            |profile| profile;
        let _: fn(
            RelationalSqlIndexReadProfile,
        ) -> hawdb_relational::RelationalSqlIndexReadProfile = |profile| profile;
        let _: fn(RelationalSqlRowReadProfile) -> hawdb_relational::RelationalSqlRowReadProfile =
            |profile| profile;
    }
}
