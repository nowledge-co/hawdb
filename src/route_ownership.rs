//! Compatibility facade for graph route ownership and readiness contracts.

pub use skein_route_ownership::graph::{
    nowledge_mem_route_ownership_all_legacy, nowledge_mem_route_ownership_all_skein,
    nowledge_mem_route_ownership_for_engine, nowledge_mem_route_ownership_readiness,
    NowledgeMemRouteOwnership, NowledgeMemRouteOwnershipPolicy,
    NowledgeMemRouteOwnershipReadinessReport, NowledgeMemRouteReadEngine,
    NOWLEDGE_MEM_ROUTE_OWNERSHIP_PROTOCOL,
};

#[cfg(test)]
mod facade_tests;
