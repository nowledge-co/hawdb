#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalSqlStageTimings {
    /// Time spent parsing SQL on a template-cache miss. Cache hits report zero.
    pub parse_nanos: u64,
    /// Time spent resolving the bound-neutral template against current schema state.
    pub bind_nanos: u64,
    /// Time spent selecting access paths, enumerating joins, and lowering execution state.
    pub plan_nanos: u64,
    /// Time spent executing the prepared relational operators.
    pub execute_nanos: u64,
}

impl RelationalSqlStageTimings {
    pub const fn total_nanos(self) -> u64 {
        self.parse_nanos
            .saturating_add(self.bind_nanos)
            .saturating_add(self.plan_nanos)
            .saturating_add(self.execute_nanos)
    }
}

pub(crate) fn elapsed_nanos(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn measure_nanos<T>(nanos: &mut u64, operation: impl FnOnce() -> T) -> T {
    let started = std::time::Instant::now();
    let output = operation();
    *nanos = nanos.saturating_add(elapsed_nanos(started));
    output
}
