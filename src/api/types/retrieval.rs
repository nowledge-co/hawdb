pub use skein_nowledge_contracts::*;

#[cfg(test)]
mod facade_tests {
    use super::{BackgroundMaintenanceKind, KnowledgeGraphContextPath, KnowledgeRetrievalRequest};

    #[test]
    fn facade_reexports_nowledge_contract_types() {
        let _: fn(
            KnowledgeRetrievalRequest,
        ) -> skein_nowledge_contracts::KnowledgeRetrievalRequest = |value| value;
        let _: fn(
            BackgroundMaintenanceKind,
        ) -> skein_nowledge_contracts::BackgroundMaintenanceKind = |value| value;
        let _: fn(
            KnowledgeGraphContextPath,
        ) -> skein_nowledge_contracts::KnowledgeGraphContextPath = |value| value;
    }
}
