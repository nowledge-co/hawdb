pub use crate::relational_sql::{
    RelationalJoinPlanningAttempt, RelationalJoinPlanningBudget, RelationalJoinPlanningCost,
    RelationalJoinPlanningDirective, RelationalJoinPlanningFallbackClass,
    RelationalJoinPlanningOutcome, RelationalJoinPlanningReason, RelationalJoinPlanningStatus,
    RelationalJoinPlanningStrategy, RelationalOperatorCardinalityProfile, RelationalOperatorId,
    RelationalOperatorKind, RelationalSqlStageTimings,
};
pub use skein_executor::BlockingOperatorMemoryReport;
pub use skein_relational::{
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
        ) -> skein_relational::ProfiledRelationalSqlQueryOutput = |profile| profile;
        let _: fn(RelationalSqlReadProfile) -> skein_relational::RelationalSqlReadProfile =
            |profile| profile;
        let _: fn(
            RelationalSqlIndexReadProfile,
        ) -> skein_relational::RelationalSqlIndexReadProfile = |profile| profile;
        let _: fn(RelationalSqlRowReadProfile) -> skein_relational::RelationalSqlRowReadProfile =
            |profile| profile;
    }
}
