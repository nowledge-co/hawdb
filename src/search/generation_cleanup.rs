use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

pub const SEARCH_PROJECTION_CLEANUP_PROTOCOL: &str =
    "skein-search-projection-generation-cleanup-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionCleanupOptions {
    pub max_pending_files: NonZeroUsize,
    pub max_delete_attempts: NonZeroUsize,
}

impl Default for SearchProjectionCleanupOptions {
    fn default() -> Self {
        Self {
            max_pending_files: NonZeroUsize::new(256).expect("cleanup capacity is non-zero"),
            max_delete_attempts: NonZeroUsize::new(256).expect("cleanup delete budget is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionCleanupReport {
    pub protocol: String,
    pub max_pending_files: usize,
    pub max_delete_attempts: usize,
    pub lexical_generation: Option<u64>,
    pub out_of_core_generation: Option<u64>,
    pub rabitq_generation: Option<u64>,
    pub rabitq_remove_all: bool,
    pub pending_before: usize,
    pub scanned_entries: usize,
    pub eligible_files: usize,
    pub delete_attempts: usize,
    pub deleted_files: usize,
    pub already_absent_files: usize,
    pub delete_failures: usize,
    pub deferred_files: usize,
    pub pending_after: usize,
    pub queue_overflow_files: usize,
    pub generation_discovery_failures: usize,
    pub scan_failures: usize,
    pub failure_kinds: BTreeMap<String, usize>,
    pub retry_required: bool,
}

impl Default for SearchProjectionCleanupReport {
    fn default() -> Self {
        Self {
            protocol: SEARCH_PROJECTION_CLEANUP_PROTOCOL.to_string(),
            max_pending_files: SearchProjectionCleanupOptions::default()
                .max_pending_files
                .get(),
            max_delete_attempts: SearchProjectionCleanupOptions::default()
                .max_delete_attempts
                .get(),
            lexical_generation: None,
            out_of_core_generation: None,
            rabitq_generation: None,
            rabitq_remove_all: false,
            pending_before: 0,
            scanned_entries: 0,
            eligible_files: 0,
            delete_attempts: 0,
            deleted_files: 0,
            already_absent_files: 0,
            delete_failures: 0,
            deferred_files: 0,
            pending_after: 0,
            queue_overflow_files: 0,
            generation_discovery_failures: 0,
            scan_failures: 0,
            failure_kinds: BTreeMap::new(),
            retry_required: false,
        }
    }
}

impl SearchProjectionCleanupReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "max_pending_files": self.max_pending_files,
            "max_delete_attempts": self.max_delete_attempts,
            "lexical_generation": self.lexical_generation,
            "out_of_core_generation": self.out_of_core_generation,
            "rabitq_generation": self.rabitq_generation,
            "rabitq_remove_all": self.rabitq_remove_all,
            "pending_before": self.pending_before,
            "scanned_entries": self.scanned_entries,
            "eligible_files": self.eligible_files,
            "delete_attempts": self.delete_attempts,
            "deleted_files": self.deleted_files,
            "already_absent_files": self.already_absent_files,
            "delete_failures": self.delete_failures,
            "deferred_files": self.deferred_files,
            "pending_after": self.pending_after,
            "queue_overflow_files": self.queue_overflow_files,
            "generation_discovery_failures": self.generation_discovery_failures,
            "scan_failures": self.scan_failures,
            "failure_kinds": self.failure_kinds,
            "retry_required": self.retry_required,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct SearchProjectionGenerations {
    pub lexical: Option<u64>,
    pub out_of_core: Option<u64>,
    pub rabitq: Option<u64>,
    pub rabitq_remove_all: bool,
    pub out_of_core_discovery_failed: bool,
}

#[derive(Debug, Default)]
pub(super) struct SearchProjectionCleanupState {
    pending: VecDeque<CleanupCandidate>,
    report: SearchProjectionCleanupReport,
}

impl SearchProjectionCleanupState {
    pub(super) fn report(&self) -> SearchProjectionCleanupReport {
        self.report.clone()
    }

    pub(super) fn run(
        &mut self,
        root: &Path,
        generations: SearchProjectionGenerations,
        options: SearchProjectionCleanupOptions,
    ) -> SearchProjectionCleanupReport {
        self.run_with_remover(root, generations, options, |path| fs::remove_file(path))
    }

    fn run_with_remover<F>(
        &mut self,
        root: &Path,
        generations: SearchProjectionGenerations,
        options: SearchProjectionCleanupOptions,
        mut remove: F,
    ) -> SearchProjectionCleanupReport
    where
        F: FnMut(&Path) -> io::Result<()>,
    {
        let capacity = options.max_pending_files.get();
        let delete_limit = options.max_delete_attempts.get();
        let mut report = SearchProjectionCleanupReport {
            max_pending_files: capacity,
            max_delete_attempts: delete_limit,
            lexical_generation: generations.lexical,
            out_of_core_generation: generations.out_of_core,
            rabitq_generation: generations.rabitq,
            rabitq_remove_all: generations.rabitq_remove_all,
            pending_before: self.pending.len(),
            ..SearchProjectionCleanupReport::default()
        };
        if generations.out_of_core_discovery_failed {
            report.generation_discovery_failures = 1;
            record_failure(&mut report, "out_of_core_manifest_invalid");
        }

        let previous = std::mem::take(&mut self.pending);
        let mut known = BTreeSet::new();
        let mut pending = VecDeque::new();
        for candidate in previous {
            known.insert(candidate.name.clone());
            if !candidate.is_obsolete(generations) {
                continue;
            }
            report.eligible_files = report.eligible_files.saturating_add(1);
            process_candidate(
                root,
                candidate,
                capacity,
                delete_limit,
                &mut pending,
                &mut report,
                &mut remove,
            );
        }

        match fs::read_dir(root) {
            Ok(entries) => {
                for entry in entries {
                    report.scanned_entries = report.scanned_entries.saturating_add(1);
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(error) => {
                            report.scan_failures = report.scan_failures.saturating_add(1);
                            record_failure(&mut report, io_error_code(&error));
                            continue;
                        }
                    };
                    let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                        continue;
                    };
                    let Some(candidate) = CleanupCandidate::parse(name) else {
                        continue;
                    };
                    if known.contains(&candidate.name) || !candidate.is_obsolete(generations) {
                        continue;
                    }
                    report.eligible_files = report.eligible_files.saturating_add(1);
                    process_candidate(
                        root,
                        candidate,
                        capacity,
                        delete_limit,
                        &mut pending,
                        &mut report,
                        &mut remove,
                    );
                }
            }
            Err(error) => {
                report.scan_failures = report.scan_failures.saturating_add(1);
                record_failure(&mut report, io_error_code(&error));
            }
        }

        report.pending_after = pending.len();
        report.deferred_files = report
            .pending_after
            .saturating_add(report.queue_overflow_files);
        report.retry_required = report.pending_after > 0
            || report.queue_overflow_files > 0
            || report.generation_discovery_failures > 0
            || report.scan_failures > 0;
        self.pending = pending;
        self.report = report.clone();
        report
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CleanupArtifactKind {
    Lexical,
    OutOfCore,
    RaBitQ,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CleanupCandidate {
    name: String,
    kind: CleanupArtifactKind,
    generation: u64,
}

impl CleanupCandidate {
    fn parse(name: String) -> Option<Self> {
        const OUT_OF_CORE_PREFIXES: &[&str] = &[
            "search_projection_segments.",
            "search_projection_segment_payloads.",
            "search_projection_metadata_payloads.",
            "search_projection_vector_payloads.",
            "search_projection_out_of_core_layout.",
            "search_lexical.manifest.",
        ];
        if let Some(generation) = parse_generation(&name, "search_lexical.") {
            return Some(Self {
                name,
                kind: CleanupArtifactKind::Lexical,
                generation,
            });
        }
        if let Some(generation) = parse_generation(&name, "search_rabitq.") {
            return Some(Self {
                name,
                kind: CleanupArtifactKind::RaBitQ,
                generation,
            });
        }
        OUT_OF_CORE_PREFIXES.iter().find_map(|prefix| {
            parse_generation(&name, prefix).map(|generation| Self {
                name: name.clone(),
                kind: CleanupArtifactKind::OutOfCore,
                generation,
            })
        })
    }

    fn is_obsolete(&self, generations: SearchProjectionGenerations) -> bool {
        match self.kind {
            CleanupArtifactKind::Lexical => {
                older_than_previous(self.generation, generations.lexical)
            }
            CleanupArtifactKind::OutOfCore => {
                older_than_previous(self.generation, generations.out_of_core)
            }
            CleanupArtifactKind::RaBitQ if generations.rabitq_remove_all => true,
            CleanupArtifactKind::RaBitQ => older_than_previous(self.generation, generations.rabitq),
        }
    }
}

fn process_candidate<F>(
    root: &Path,
    candidate: CleanupCandidate,
    capacity: usize,
    delete_limit: usize,
    pending: &mut VecDeque<CleanupCandidate>,
    report: &mut SearchProjectionCleanupReport,
    remove: &mut F,
) where
    F: FnMut(&Path) -> io::Result<()>,
{
    if report.delete_attempts >= delete_limit {
        defer_candidate(candidate, capacity, pending, report);
        return;
    }
    report.delete_attempts = report.delete_attempts.saturating_add(1);
    match remove(&root.join(&candidate.name)) {
        Ok(()) => report.deleted_files = report.deleted_files.saturating_add(1),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            report.already_absent_files = report.already_absent_files.saturating_add(1);
        }
        Err(error) => {
            report.delete_failures = report.delete_failures.saturating_add(1);
            record_failure(report, io_error_code(&error));
            defer_candidate(candidate, capacity, pending, report);
        }
    }
}

fn defer_candidate(
    candidate: CleanupCandidate,
    capacity: usize,
    pending: &mut VecDeque<CleanupCandidate>,
    report: &mut SearchProjectionCleanupReport,
) {
    if pending.len() < capacity {
        pending.push_back(candidate);
    } else {
        report.queue_overflow_files = report.queue_overflow_files.saturating_add(1);
    }
}

fn parse_generation(name: &str, prefix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?
        .strip_suffix(".skein")?
        .parse()
        .ok()
}

fn older_than_previous(generation: u64, current: Option<u64>) -> bool {
    current.is_some_and(|current| generation < current.saturating_sub(1))
}

fn record_failure(report: &mut SearchProjectionCleanupReport, kind: &str) {
    let count = report.failure_kinds.entry(kind.to_string()).or_default();
    *count = count.saturating_add(1);
}

fn io_error_code(error: &io::Error) -> &'static str {
    match error.kind() {
        io::ErrorKind::NotFound => "not_found",
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::AlreadyExists => "already_exists",
        io::ErrorKind::WouldBlock => "would_block",
        io::ErrorKind::InvalidInput => "invalid_input",
        io::ErrorKind::InvalidData => "invalid_data",
        io::ErrorKind::TimedOut => "timed_out",
        io::ErrorKind::Interrupted => "interrupted",
        io::ErrorKind::Unsupported => "unsupported",
        io::ErrorKind::OutOfMemory => "out_of_memory",
        _ => "other_io_error",
    }
}

#[cfg(test)]
#[path = "generation_cleanup/tests.rs"]
mod tests;
