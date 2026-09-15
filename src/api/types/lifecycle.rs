#[cfg(test)]
pub use skein_nowledge_contracts::test_support::lifecycle::*;

#[cfg(test)]
mod facade_tests {
    use super::KnowledgeSkillUsageStatsUpdate;

    #[test]
    fn facade_reexports_lifecycle_models_without_type_conversion() {
        let _: fn(
            KnowledgeSkillUsageStatsUpdate,
        ) -> skein_nowledge_contracts::test_support::lifecycle::KnowledgeSkillUsageStatsUpdate =
            |value| value;
    }
}
