use super::*;

#[test]
#[ignore = "local filter/ACL semantics and shared-root admission campaign"]
fn filter_admission_campaign() {
    let mut seed = 0x206f117eu64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let (path, reader) = query::fixture();
    let mut accepted = 0;
    let mut candidates = 0;
    for case in 0..256 {
        let shape = case % 16;
        let (key, value, expected) = match shape {
            0 => (
                "space",
                "team".into(),
                SearchPredicateSet::new(vec![SearchPredicate::eq("space_id", "team")]),
            ),
            1 => (
                "spaces__in",
                r#"["team","shared","team"]"#.into(),
                SearchPredicateSet::new(vec![SearchPredicate::in_list(
                    "space_id",
                    ["team".into(), "shared".into()],
                )]),
            ),
            2 => (
                "space_ids__not_in",
                r#"["private"]"#.into(),
                SearchPredicateSet::new(vec![SearchPredicate::not_in_list(
                    "space_id",
                    ["private".into()],
                )]),
            ),
            3 => (
                "history",
                " TRUE ".into(),
                SearchPredicateSet::new(vec![SearchPredicate::eq("is_latest", "false")]),
            ),
            4 => (
                "space__exists",
                "0".into(),
                SearchPredicateSet::new(vec![SearchPredicate::is_missing("space_id")]),
            ),
            5 => (
                "space__missing",
                "false".into(),
                SearchPredicateSet::new(vec![SearchPredicate::exists("space_id")]),
            ),
            6 => (
                "recorded_date_from",
                "4".into(),
                SearchPredicateSet::new(vec![SearchPredicate::gte("created_at", "4")]),
            ),
            7 => (
                "created_at__lt",
                "4".into(),
                SearchPredicateSet::new(vec![SearchPredicate::lt("created_at", "4")]),
            ),
            8 => (
                "space__in",
                "[]".into(),
                SearchPredicateSet::unsatisfiable(),
            ),
            9 => ("space__not_in", "[]".into(), SearchPredicateSet::empty()),
            10 => (
                "space__in",
                format!("[\"{}\",false]", "x,".repeat(next() as usize % 300)),
                SearchPredicateSet::unsatisfiable(),
            ),
            11 => (
                "history",
                "invalid".into(),
                SearchPredicateSet::unsatisfiable(),
            ),
            12 => (
                "event_date_before",
                "4".into(),
                SearchPredicateSet::new(vec![SearchPredicate::lt("event_start", "4")]),
            ),
            13 => (
                "temporal",
                " PAST ".into(),
                SearchPredicateSet::new(vec![SearchPredicate::eq("temporal_context", "past")]),
            ),
            14 => (
                "absent__in",
                serde_json::to_string(&["a,\0\u{130}", "quote\"\\"]).unwrap(),
                SearchPredicateSet::new(vec![SearchPredicate::in_list(
                    "absent",
                    ["a,\0\u{130}".into(), "quote\"\\".into()],
                )]),
            ),
            _ => (
                "latest__in",
                r#"["1","FALSE"]"#.into(),
                SearchPredicateSet::new(vec![SearchPredicate::in_list(
                    "is_latest",
                    ["true".into(), "false".into()],
                )]),
            ),
        };
        let acl_shape = next() as usize % 3;
        let access = match acl_shape {
            0 => None,
            1 => Some(SearchAccessControlContext::visibility_scopes(
                7,
                "space_id",
                ["team"],
            )),
            _ => Some(SearchAccessControlContext::visibility_scopes(
                7,
                "space_id",
                ["team", "shared", "a\0\"\\\u{130}"],
            )),
        };
        let key_spare = next() as usize % 1024;
        let value_spare = next() as usize % 2048;
        let requested = || {
            let mut owned_key = String::with_capacity(key.len() + key_spare);
            owned_key.push_str(key);
            let mut owned_value = String::with_capacity(value.len() + value_spare);
            owned_value.push_str(&value);
            BTreeMap::from([(owned_key, owned_value)])
        };
        let competing = 137 + next() as usize % 1024;
        let baseline = memory(16 * 1024 * 1024);
        let other = baseline.scores.reserve(competing).unwrap();
        let (owned, mut report) = input(requested(), access.as_ref(), &baseline).unwrap();
        let peak = baseline.ledger.snapshot().peak_bytes;
        let effective = access.as_ref().map_or_else(&requested, |access| {
            access
                .effective_metadata_filters(owned.requested())
                .unwrap()
        });
        assert_default_pushdown_parity(&effective, &owned, &report);
        if access.is_none() {
            assert_eq!(owned.predicates, expected);
        }
        let matches = std::array::from_fn(|number| {
            let user_match = match shape {
                0 => number % 3 == 0,
                1 | 2 => number % 3 != 2,
                3 => number % 2 == 1,
                4 | 8 | 10 | 11 | 14 => false,
                5 | 9 | 15 => true,
                6 => number >= 4,
                7 | 12 => number < 4,
                13 => number % 2 == 0,
                _ => unreachable!(),
            };
            user_match
                && match acl_shape {
                    0 => true,
                    1 => number % 3 == 0,
                    _ => number % 3 != 2,
                }
        });
        candidates += query::assert_candidates(&reader, &owned, &mut report, &baseline, &matches);
        drop(owned);
        drop(other);
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        for limit in [peak, peak - 1] {
            let bounded = memory(limit);
            let other = bounded.scores.reserve(competing).unwrap();
            let result = input(requested(), access.as_ref(), &bounded);
            assert_eq!(result.is_ok(), limit == peak, "case={case} shape={shape}");
            accepted += usize::from(result.is_ok());
            drop(result);
            drop(other);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            assert_eq!(bounded.ledger.snapshot().account_count, 2);
        }
    }
    drop(reader);
    std::fs::remove_dir_all(path).unwrap();
    assert_eq!(accepted, 256);
    assert!(candidates > 0);
    eprintln!("filters seed=0x206f117e cases=256 exact=256 short=256 candidate_scans=256 candidates={candidates}");
}
