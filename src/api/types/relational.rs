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
