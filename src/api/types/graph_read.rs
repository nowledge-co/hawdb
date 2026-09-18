#[cfg(test)]
pub use hawdb_nowledge_contracts::test_support::graph_read::*;

#[cfg(test)]
mod facade_tests {
    use super::{KnowledgeNeighborDirection, KnowledgeNeighborsRequest};

    #[test]
    fn facade_reexports_graph_read_models_without_type_conversion() {
        let _: fn(
            KnowledgeNeighborsRequest,
        )
            -> hawdb_nowledge_contracts::test_support::graph_read::KnowledgeNeighborsRequest =
            |value| value;
        let _: fn(
            KnowledgeNeighborDirection,
        ) -> hawdb_nowledge_contracts::test_support::graph_read::KnowledgeNeighborDirection =
            |value| value;
    }
}
