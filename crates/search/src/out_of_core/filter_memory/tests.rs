use super::*;
use crate::query_memory::QueryMemory;
use crate::RuntimeCancellationToken;
use skein_optimizer::{push_search_predicates, SearchScanPredicateSupport};
use std::cell::{Cell, RefCell};
use std::num::NonZeroU64;

mod fuzz;
mod query;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Evidence {
    merges: usize,
    parses: usize,
}

thread_local! {
    static EVIDENCE: Cell<Evidence> = Cell::default();
    static CANCEL_PARSE: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
}

pub(super) fn record_merge() {
    let mut value = EVIDENCE.get();
    value.merges += 1;
    EVIDENCE.set(value);
}

pub(super) fn record_parse() {
    let mut value = EVIDENCE.get();
    value.parses += 1;
    EVIDENCE.set(value);
}

pub(super) fn after_parse() {
    CANCEL_PARSE.with_borrow_mut(|token| {
        if let Some(token) = token.take() {
            token.cancel();
        }
    });
}

fn take() -> Evidence {
    EVIDENCE.replace(Evidence::default())
}

fn memory(bytes: usize) -> QueryMemory {
    QueryMemory::new(NonZeroU64::new(bytes as u64).unwrap(), None).unwrap()
}

fn filters(key: &str, value: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(key.to_owned(), value.to_owned())])
}

fn input(
    filters: BTreeMap<String, String>,
    access: Option<&SearchAccessControlContext>,
    memory: &QueryMemory,
) -> Result<(Input, SearchPredicatePushdownReport)> {
    Input::new(
        filters,
        access,
        None,
        &memory.working,
        &RuntimeTaskContext::default(),
    )
}

// This checks parity with the previous wrapper, not an independent parser oracle.
fn assert_default_pushdown_parity(
    filters: &BTreeMap<String, String>,
    input: &Input,
    report: &SearchPredicatePushdownReport,
) {
    let (parsed, error) = match SearchPredicateSet::from_metadata_filters(filters) {
        Ok(parsed) => (parsed, None),
        Err(error) => (SearchPredicateSet::unsatisfiable(), Some(error.to_string())),
    };
    let previous = push_search_predicates(&parsed, SearchScanPredicateSupport::default());
    assert_eq!(&input.predicates, previous.pushed());
    assert_eq!(
        report,
        &SearchPredicatePushdownReport {
            input_predicate_count: filters.len(),
            pushed_predicate_count: previous.pushed().predicates().len(),
            residual_predicate_count: previous.residual().predicates().len(),
            unsatisfiable: previous.pushed().is_unsatisfiable(),
            parse_error: error,
            ..SearchPredicatePushdownReport::default()
        }
    );
}

#[test]
fn empty_filters_release_retained_tree_and_need_no_admitted_bytes() {
    let mut requested = filters("removed", "value");
    requested.remove("removed");
    let memory = memory(1);
    take();
    let (input, report) = input(requested, None, &memory).unwrap();
    assert!(input.requested().is_empty());
    assert!(input.predicates.is_empty());
    assert_eq!(report, SearchPredicatePushdownReport::default());
    assert_eq!(
        take(),
        Evidence {
            merges: 0,
            parses: 1
        }
    );
    assert_eq!(memory.ledger.snapshot().peak_bytes, 0);
    drop(input);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn raw_spare_capacity_is_admitted_before_merge_or_parse() {
    let mut key = String::with_capacity(1024);
    key.push_str("space");
    let mut value = String::with_capacity(4096);
    value.push_str("team");
    // Independent of map_bytes: one entry's documented node allowance and the
    // two original allocations, not their visible string lengths.
    let raw = 2048 + key.capacity() + value.capacity();
    let memory = memory(raw - 1);
    let access = SearchAccessControlContext::visibility_scopes(7, "space_id", ["team"]);
    take();
    assert!(input(BTreeMap::from([(key, value)]), Some(&access), &memory).is_err());
    assert_eq!(take(), Evidence::default());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn parser_peak_and_retained_owner_have_independent_exact_boundaries() {
    let raw = 2048 + 8 + 4;
    let retained = size_of::<SearchPredicate>() + 16 + 4 + 5;
    let scratch = 1024 + 24 * 4 + 4 * 8;
    let peak = 137 + raw + retained + scratch;
    for limit in [peak - 1, peak] {
        let memory = memory(limit);
        let other = memory.scores.reserve(137).unwrap();
        let requested = filters("space_id", "team");
        let (key, value) = requested.first_key_value().unwrap();
        let pointers = (key.as_ptr(), value.as_ptr());
        take();
        let result = input(requested, None, &memory);
        assert_eq!(result.is_ok(), limit == peak);
        assert_eq!(
            take(),
            Evidence {
                merges: 0,
                parses: usize::from(limit == peak)
            }
        );
        if let Ok((input, report)) = result {
            assert_default_pushdown_parity(input.requested(), &input, &report);
            let (key, value) = input.requested().first_key_value().unwrap();
            assert_eq!((key.as_ptr(), value.as_ptr()), pointers);
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
            let ledger = memory.ledger.clone();
            drop(other);
            drop(memory);
            assert_eq!(ledger.snapshot().used_bytes, raw + retained);
            drop(input);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        } else {
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            drop(other);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
fn sequential_filter_parsing_reuses_the_largest_scratch_envelope() {
    let raw = 2 * 2048 + 2 + 300;
    let retained = 2 * size_of::<SearchPredicate>() + 2 * 16 + 300 + 2 * 5;
    let scratch = 1024 + 24 * 200 + 4;
    let peak = raw + retained + scratch;
    for limit in [peak - 1, peak] {
        let memory = memory(limit);
        let filters =
            BTreeMap::from([("a".into(), "x".repeat(100)), ("b".into(), "y".repeat(200))]);
        take();
        let result = input(filters, None, &memory);
        assert_eq!(result.is_ok(), limit == peak);
        assert_eq!(take().parses, usize::from(limit == peak));
        if let Ok((input, _)) = &result {
            assert_eq!(input.predicates.predicates().len(), 2);
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
            assert_eq!(memory.ledger.snapshot().used_bytes, raw + retained);
        }
        drop(result);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn acl_copies_are_admitted_before_single_and_multi_value_merges() {
    for scopes in [vec!["team"], vec!["a\0B", "team", "quoted\"\\\u{130}"]] {
        let access = SearchAccessControlContext::visibility_scopes(7, "space_id", scopes);
        let raw = 2048 + 4 + 6;
        let encoded = 2
            + access.allowed_visibility_values.len() * 3
            + access
                .allowed_visibility_values
                .iter()
                .map(|value| value.len() * 6)
                .sum::<usize>();
        let merge = raw
            + 2048
            + if access.allowed_visibility_values.len() == 1 {
                8 + 4
            } else {
                3 * encoded.max(128) + 3 * (8 + 4)
            };
        let short = memory(raw + merge - 1);
        take();
        assert!(input(filters("kind", "memory"), Some(&access), &short).is_err());
        assert_eq!(take(), Evidence::default());
        assert_eq!(short.ledger.snapshot().used_bytes, 0);

        let plenty = memory(1024 * 1024);
        let requested = filters("kind", "memory");
        let (input, report) = input(requested.clone(), Some(&access), &plenty).unwrap();
        assert_eq!(
            take(),
            Evidence {
                merges: 1,
                parses: 1
            }
        );
        assert_eq!(input.requested(), &requested);
        let effective = access.effective_metadata_filters(&requested).unwrap();
        if access.allowed_visibility_values.len() > 1 {
            let old_values = access
                .allowed_visibility_values
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                effective["space_id__in"],
                serde_json::to_string(&old_values).unwrap()
            );
        }
        assert_default_pushdown_parity(&effective, &input, &report);
        drop(input);
        assert_eq!(plenty.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn aliases_and_all_operations_keep_known_predicates_and_default_pushdown() {
    let cases = [
        ("space", "Team", SearchPredicate::eq("space_id", "Team")),
        (
            "temporal",
            " PAST ",
            SearchPredicate::eq("temporal_context", "past"),
        ),
        (
            "kind__in",
            r#"[" Memory ","memory","Thread"]"#,
            SearchPredicate::in_list("kind", ["memory".into(), "thread".into()]),
        ),
        (
            "space_ids__not_in",
            r#"["A","B"]"#,
            SearchPredicate::not_in_list("space_id", ["A".into(), "B".into()]),
        ),
        ("history", "1", SearchPredicate::eq("is_latest", "false")),
        (
            "latest__in",
            r#"["1"," FALSE "]"#,
            SearchPredicate::in_list("is_latest", ["true".into(), "false".into()]),
        ),
        (
            "space__exists",
            "false",
            SearchPredicate::is_missing("space_id"),
        ),
        ("space__missing", "0", SearchPredicate::exists("space_id")),
        (
            "event_date_after",
            "9",
            SearchPredicate::gt("event_end", "9"),
        ),
        (
            "event_date_to",
            "9",
            SearchPredicate::lte("event_start", "9"),
        ),
        (
            "recorded_date_from",
            "9",
            SearchPredicate::gte("created_at", "9"),
        ),
        (
            "recorded_date_before",
            "9",
            SearchPredicate::lt("created_at", "9"),
        ),
        ("plain__gt", "9", SearchPredicate::gt("plain", "9")),
        ("plain__gte", "9", SearchPredicate::gte("plain", "9")),
        ("plain__lt", "9", SearchPredicate::lt("plain", "9")),
        ("plain__lte", "9", SearchPredicate::lte("plain", "9")),
    ];
    for (key, value, expected) in cases {
        let memory = memory(1024 * 1024);
        let (input, report) = input(filters(key, value), None, &memory).unwrap();
        assert_eq!(input.predicates.predicates(), &[expected]);
        assert!(!input.predicates.is_unsatisfiable());
        assert_default_pushdown_parity(input.requested(), &input, &report);
        drop(input);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    for (key, unsatisfiable) in [("space__in", true), ("space__not_in", false)] {
        let memory = memory(8192);
        let (input, report) = input(filters(key, "[]"), None, &memory).unwrap();
        assert!(input.predicates.predicates().is_empty());
        assert_eq!(input.predicates.is_unsatisfiable(), unsatisfiable);
        assert_default_pushdown_parity(input.requested(), &input, &report);
    }
}

#[test]
fn malformed_filters_fail_closed_without_echoing_values_or_leaking_charges() {
    let secret = "private-\0\u{130}".repeat(4096);
    let unexpected_string = serde_json::to_string(&secret).unwrap();
    for (key, value) in [
        ("space__in", unexpected_string.as_str()),
        ("space__in", r#"["ok",9]"#),
        ("space__in", r#"["unterminated"#),
        ("history", "secret-boolean"),
        ("latest__in", r#"["true","secret-boolean"]"#),
        ("space__exists", "secret-boolean"),
        ("event_date__in", "[]"),
        ("history__gt", "secret-boolean"),
    ] {
        let memory = memory(16 * 1024 * 1024);
        let other = memory.scores.reserve(137).unwrap();
        let (input, report) = input(filters(key, value), None, &memory).unwrap();
        assert!(input.predicates.is_unsatisfiable());
        let error = report.parse_error.as_ref().unwrap();
        assert!(error.starts_with(key));
        assert!(!error.contains("private-"));
        assert!(!error.contains("secret-boolean"));
        assert_default_pushdown_parity(input.requested(), &input, &report);
        drop(input);
        drop(report);
        assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn cancellation_before_and_after_parser_releases_every_private_owner() {
    for before in [true, false] {
        let memory = memory(1024 * 1024);
        let token = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(token.clone());
        if before {
            token.cancel();
        } else {
            CANCEL_PARSE.with_borrow_mut(|slot| *slot = Some(token));
        }
        let access =
            SearchAccessControlContext::visibility_scopes(7, "space_id", ["team", "shared"]);
        take();
        let result = Input::new(
            filters("kind", "memory"),
            Some(&access),
            Some(7),
            &memory.working,
            &task,
        );
        assert!(result.unwrap_err().to_string().contains("cancel"));
        assert_eq!(
            take(),
            Evidence {
                merges: usize::from(!before),
                parses: usize::from(!before)
            }
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn acl_validation_and_policy_epoch_rejection_precede_allocating_transforms() {
    for access in [
        SearchAccessControlContext::visibility_scopes(0, "space_id", ["team"]),
        SearchAccessControlContext::visibility_scopes(7, " ", ["team"]),
        SearchAccessControlContext::visibility_scopes(7, "space_id", Vec::<String>::new()),
        SearchAccessControlContext::visibility_scopes(7, "space_id", [" "]),
    ] {
        let memory = memory(1);
        take();
        assert!(input(filters("kind", "memory"), Some(&access), &memory).is_err());
        assert_eq!(take(), Evidence::default());
        assert_eq!(memory.ledger.snapshot().peak_bytes, 0);
        let error = Input::new(
            BTreeMap::new(),
            Some(&access),
            Some(8),
            &memory.working,
            &RuntimeTaskContext::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("does not match"), "{error}");
    }
}

#[test]
fn list_prefix_bound_handles_escapes_chunks_and_overflow() {
    let task = RuntimeTaskContext::default();
    for (value, expected) in [
        ("[]", 1),
        (r#"["a,b","c"]"#, 2),
        (r#"["a\"b,c","d\\","e"]"#, 3),
        (r#"["valid","unterminated,"#, 2),
        (r#"["valid",["bad","nested"]]"#, 3),
    ] {
        assert_eq!(list_slots(value, &task).unwrap(), expected, "{value}");
    }
    let boundary = format!("[\"{}\\\",quoted\",\"second\"]", "x".repeat(4093));
    assert_eq!(list_slots(&boundary, &task).unwrap(), 2);
    assert!(slots::<String>(usize::MAX).is_err());
    assert!(slots::<u8>(isize::MAX as usize + 1).is_err());
    let token = RuntimeCancellationToken::new();
    token.cancel();
    assert!(list_slots(&boundary, &RuntimeTaskContext::without_deadline(token)).is_err());
}

#[test]
fn acl_same_key_replacement_and_alias_intersection_preserve_existing_semantics() {
    let access = SearchAccessControlContext::visibility_scopes(7, "space_id", ["team"]);
    let memory = memory(1024 * 1024);
    let (input, _) = input(filters("space_id", "private"), Some(&access), &memory).unwrap();
    assert_eq!(input.requested()["space_id"], "private");
    assert_eq!(
        input.predicates.predicates(),
        &[SearchPredicate::eq("space_id", "team")]
    );
    drop(input);
    let (input, _) = self::input(filters("space", "private"), Some(&access), &memory).unwrap();
    assert_eq!(
        input.predicates.predicates(),
        &[
            SearchPredicate::eq("space_id", "private"),
            SearchPredicate::eq("space_id", "team")
        ]
    );
    drop(input);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
