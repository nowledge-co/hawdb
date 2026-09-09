use crate::{
    AggregateFunction, AggregateTarget, Aggregation, GraphExpansionBudget, PhysicalPlan, Predicate,
    Projection, ProjectionExpression, SortDirection, SortItem, SortKey,
};
use skein_cypher::RelationshipDirection;
use std::collections::BTreeMap;

struct Fixture {
    plan: PhysicalPlan,
    lines: Vec<(usize, String)>,
}

fn leaf(seed: usize) -> Fixture {
    let variable = format!("n{seed}");
    Fixture {
        plan: PhysicalPlan::SeqNodeScan {
            variable: variable.clone(),
            label: "Memory".to_string(),
        },
        lines: vec![(0, format!("SeqNodeScan variable={variable} label=Memory"))],
    }
}

fn binary(left: Fixture, right: Fixture) -> Fixture {
    let mut lines = vec![(0, "NodeCartesianProductExec".to_string())];
    lines.extend(
        left.lines
            .into_iter()
            .map(|(depth, text)| (depth + 1, text)),
    );
    lines.extend(
        right
            .lines
            .into_iter()
            .map(|(depth, text)| (depth + 1, text)),
    );
    Fixture {
        plan: PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(left.plan),
            right: Box::new(right.plan),
        },
        lines,
    }
}

fn unary(kind: usize, fixture: Fixture, seed: usize) -> Fixture {
    let input = Box::new(fixture.plan);
    let optional = seed.is_multiple_of(2);
    let (direction, arrow) = match seed % 3 {
        0 => (RelationshipDirection::Outgoing, "->"),
        1 => (RelationshipDirection::Incoming, "<-"),
        _ => (RelationshipDirection::Undirected, "-"),
    };
    let projection = || Projection {
        expression: ProjectionExpression::Property {
            variable: "n".to_string(),
            property: "id".to_string(),
        },
        name: "alias-\u{03bb}".to_string(),
    };
    let sort_items = || {
        vec![SortItem {
            key: SortKey::Column("alias".to_string()),
            direction: SortDirection::Desc,
        }]
    };
    let sort_header = "[SortItem { key: Column(\"alias\"), direction: Desc }]";
    let (plan, header) = match kind {
        0 => (
            PhysicalPlan::NodeColumnLookupExec {
                variable: "n".to_string(),
                label: "Memory".to_string(),
                property: "title".to_string(),
                column: "c0".to_string(),
                optional,
                input,
            },
            format!("NodeColumnLookupExec variable=n label=Memory property=title column=c0 optional={optional}"),
        ),
        1 => (
            PhysicalPlan::AdjacencyExpandExec {
                source_variable: "n".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: optional.then(|| "r".to_string()),
                rel_type: "LINK".to_string(),
                rel_properties: BTreeMap::new(),
                direction,
                target_variable: "m".to_string(),
                target_label: "Target".to_string(),
                min_hops: 1,
                max_hops: 3,
                optional,
                graph_budget: optional.then_some(GraphExpansionBudget {
                    candidate_limit: 7,
                    payload_byte_limit: 256,
                }),
                input,
            },
            format!("AdjacencyExpandExec source=n:Memory{} direction={arrow} properties={{}} hops=1..3 optional={optional}{} target=m:Target",
                if optional { " rel=r:LINK" } else { " rel_type=LINK" },
                if optional { " graph_candidate_limit=7 graph_payload_byte_limit=256" } else { "" }),
        ),
        2 => (
            PhysicalPlan::AdjacencyExistsExec {
                source_variable: "n".to_string(),
                rel_type: "LINK".to_string(),
                direction,
                target_variable: "m".to_string(),
                input,
            },
            format!("AdjacencyExistsExec source=n direction={arrow} rel_type=LINK target=m"),
        ),
        3 => (
            PhysicalPlan::OptionalDegreeExec {
                source_variable: "n".to_string(),
                rel_type: "LINK".to_string(),
                rel_properties: BTreeMap::new(),
                direction,
                target_label: "Target".to_string(),
                target_properties: BTreeMap::new(),
                alias: "degree".to_string(),
                input,
            },
            format!("OptionalDegreeExec source=n rel_type=LINK direction={arrow} target=Target alias=degree"),
        ),
        4 => (
            PhysicalPlan::FilterExec {
                predicate: Predicate::ConstantBool(optional),
                input,
            },
            format!("FilterExec predicate=ConstantBool({optional})"),
        ),
        5 => (
            PhysicalPlan::ProjectExec {
                items: vec![projection()],
                input,
            },
            "ProjectExec columns=[alias-\u{03bb}]".to_string(),
        ),
        6 => (
            PhysicalPlan::AggregateExec {
                group_keys: vec![projection()],
                items: vec![Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::All,
                    distinct: false,
                    name: "total".to_string(),
                }],
                input,
            },
            "AggregateExec columns=[alias-\u{03bb}, total]".to_string(),
        ),
        7 => (PhysicalPlan::DistinctExec { input }, "DistinctExec".to_string()),
        8 => (
            PhysicalPlan::SortExec {
                items: sort_items(),
                input,
            },
            format!("SortExec keys={sort_header}"),
        ),
        9 => (
            PhysicalPlan::TopNExec {
                items: sort_items(),
                offset: seed,
                limit: 7,
                input,
            },
            format!("TopNExec keys={sort_header} offset={seed} limit=7"),
        ),
        10 => (
            PhysicalPlan::LimitExec {
                offset: seed,
                limit: optional.then_some(7),
                input,
            },
            format!("LimitExec offset={seed} limit={}", if optional { "Some(7)" } else { "None" }),
        ),
        _ => panic!("unknown fixture kind"),
    };
    let mut lines = vec![(0, header)];
    lines.extend(
        fixture
            .lines
            .into_iter()
            .map(|(depth, text)| (depth + 1, text)),
    );
    Fixture { plan, lines }
}

fn assert_fixture(fixture: &Fixture, indent: usize) {
    // The oracle is an explicit header/depth sequence built with the fixture,
    // independent of production children(), kind() and rendering helpers.
    let expected = fixture
        .lines
        .iter()
        .map(|(depth, header)| format!("{}{header}", " ".repeat(indent + depth * 2)))
        .collect::<Vec<_>>()
        .join("\n");
    let fingerprint = fixture.plan.instance_fingerprint();
    assert_eq!(fixture.plan.explain(indent), expected);
    assert_eq!(fixture.plan.explain(indent), expected);
    assert_eq!(fixture.plan.instance_fingerprint(), fingerprint);
}

#[test]
fn explain_preserves_every_unary_header_and_initial_indent() {
    for kind in 0..11 {
        for seed in 0..6 {
            let fixture = unary(kind, leaf(seed), seed);
            for indent in [0, 1, 7] {
                assert_fixture(&fixture, indent);
            }
        }
    }
}

#[test]
fn explain_preserves_binary_preorder_and_repeated_subtrees() {
    for indent in [0, 3, 16] {
        assert_fixture(
            &binary(
                unary(4, binary(leaf(1), unary(5, leaf(2), 1)), 2),
                unary(6, binary(leaf(3), leaf(3)), 3),
            ),
            indent,
        );
        assert_fixture(&binary(leaf(1), binary(leaf(2), leaf(3))), indent);
        assert_fixture(&binary(binary(leaf(1), leaf(2)), leaf(3)), indent);
    }
}

#[test]
fn explain_preserves_payload_whitespace_without_trailing_newline() {
    let variable = "n\n\tquoted\"\\";
    let label = "Memory-\u{03bb}";
    let plan = PhysicalPlan::SeqNodeScan {
        variable: variable.to_string(),
        label: label.to_string(),
    };
    assert_eq!(
        plan.explain(2),
        format!("  SeqNodeScan variable={variable} label={label}")
    );
    assert_eq!(PhysicalPlan::EmptyExec.explain(0), "EmptyExec");
}

#[test]
#[ignore = "local-only deterministic EXPLAIN shape and payload campaign"]
fn explain_shape_and_payload_campaign() {
    let mut renderings = 0;
    for seed in 0..256 {
        let mut fixture = leaf(seed);
        for depth in 0..(seed % 24 + 1) {
            fixture = unary((seed + depth) % 11, fixture, seed + depth);
            if depth % 7 == 3 {
                fixture = if (seed + depth).is_multiple_of(2) {
                    binary(fixture, leaf(seed + depth + 1))
                } else {
                    binary(leaf(seed + depth + 1), fixture)
                };
            }
        }
        for indent in [0, 1, 9] {
            assert_fixture(&fixture, indent);
            renderings += 2;
        }
    }
    assert_eq!(renderings, 1536);
    eprintln!("EXPLAIN campaign: 256 fixtures, {renderings} byte-exact renderings");
}
