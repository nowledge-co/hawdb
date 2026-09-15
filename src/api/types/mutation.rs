#[cfg(test)]
pub use skein_nowledge_contracts::test_support::mutation::*;

#[cfg(test)]
mod facade_tests {
    use super::KnowledgeEntityCreateRequest;

    #[test]
    fn facade_reexports_mutation_models_without_type_conversion() {
        let _: fn(
            KnowledgeEntityCreateRequest,
        ) -> skein_nowledge_contracts::test_support::mutation::KnowledgeEntityCreateRequest =
            |value| value;
    }
}
