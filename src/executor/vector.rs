//! Root wiring for the executor-owned vector seed operator.

pub(super) use skein_executor::external::seed::{
    BatchExternalRead, BatchExternalReadAdapter, VectorSeedContext, VectorSeedScanSpec,
};
