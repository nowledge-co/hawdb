pub use hawdb_nowledge_contracts::public::*;

#[cfg(test)]
mod facade_tests {
    use super::{BackgroundMaintenanceKind, KnowledgeGraphContextPath, KnowledgeRetrievalRequest};

    #[test]
    fn facade_reexports_nowledge_contract_types() {
        let _: fn(
            KnowledgeRetrievalRequest,
        ) -> hawdb_nowledge_contracts::KnowledgeRetrievalRequest = |value| value;
        let _: fn(
            BackgroundMaintenanceKind,
        ) -> hawdb_nowledge_contracts::BackgroundMaintenanceKind = |value| value;
        let _: fn(
            KnowledgeGraphContextPath,
        ) -> hawdb_nowledge_contracts::KnowledgeGraphContextPath = |value| value;
    }
}
