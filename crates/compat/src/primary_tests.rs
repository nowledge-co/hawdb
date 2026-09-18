// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use std::cell::RefCell;

#[derive(Default)]
struct RecordingEngine {
    events: RefCell<Vec<String>>,
    fail_at: Option<String>,
}

impl RecordingEngine {
    fn event(&self, event: String) -> Result<()> {
        self.events.borrow_mut().push(event.clone());
        if self.fail_at.as_ref() == Some(&event) {
            return Err(HawDBError::Execution(
                "injected primary failure".to_string(),
            ));
        }
        Ok(())
    }
}

struct RecordingSession<'a>(&'a mut RecordingEngine);

impl Drop for RecordingSession<'_> {
    fn drop(&mut self) {
        self.0.events.borrow_mut().push("session:drop".to_string());
    }
}

fn output(parameters: &BTreeMap<String, Value>) -> QueryOutput {
    QueryOutput::from_rows(vec![parameters.clone()])
}

impl CompatibilityPrimaryEngine for RecordingEngine {
    type Session<'a> = RecordingSession<'a>;

    fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.event(format!("database:{cypher}"))?;
        Ok(output(parameters))
    }

    fn explain_plan_with_params(
        &self,
        cypher: &str,
        _: &BTreeMap<String, Value>,
    ) -> Result<String> {
        self.event(format!("explain:{cypher}"))?;
        Ok("ExpectedPlan".to_string())
    }

    fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph {
        self.events
            .borrow_mut()
            .push(format!("project:{rel_type:?}"));
        ProjectedGraph::empty()
    }

    fn session(&mut self) -> Self::Session<'_> {
        self.events.borrow_mut().push("session:open".to_string());
        RecordingSession(self)
    }
}

impl CompatibilityPrimarySession for RecordingSession<'_> {
    fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.0.event(format!("session:{cypher}"))?;
        Ok(output(parameters))
    }
}

fn fixture(session: bool) -> CompatibilityFixture {
    let parameters = super::test_support::row([("value", Value::Int(7))]);
    let statement = |name| CypherFixtureStatement::with_parameters(name, parameters.clone());
    let mut check = CypherFixtureCheck::expect_rows(
        "ordered check",
        statement("statement"),
        ExpectedRows::Exact(vec![parameters.clone()]),
    )
    .with_setup_query(statement("setup"))
    .with_effect_query(statement("effect"), ExpectedRows::Exact(vec![parameters]));
    if session {
        check = check.with_session_execution();
    } else {
        check = check.with_plan_contains(vec!["ExpectedPlan".to_string()]);
    }
    CompatibilityFixture {
        name: "primary-boundary".to_string(),
        setup: vec![CypherFixtureStatement::new("fixture-setup")],
        checks: vec![CompatibilityCheck::Cypher(check)],
    }
}

#[test]
fn primary_adapter_preserves_order_and_stops_at_every_failure_boundary() {
    for session in [false, true] {
        let expected = if session {
            vec![
                "database:fixture-setup",
                "session:open",
                "session:setup",
                "session:statement",
                "session:effect",
                "session:drop",
            ]
        } else {
            vec![
                "database:fixture-setup",
                "database:setup",
                "database:statement",
                "explain:statement",
                "database:effect",
            ]
        };
        for failure in std::iter::once(None).chain(
            expected
                .iter()
                .copied()
                .filter(|event| !matches!(*event, "session:open" | "session:drop"))
                .map(Some),
        ) {
            let mut engine = RecordingEngine {
                fail_at: failure.map(str::to_string),
                ..Default::default()
            };
            let result = run_compatibility_fixture(&mut engine, &fixture(session));
            assert_eq!(result.is_ok(), failure.is_none(), "{session:?} {failure:?}");
            let mut expected_events = expected.clone();
            if let Some(failure) = failure {
                let index = expected.iter().position(|event| *event == failure).unwrap();
                expected_events.truncate(index + 1);
                if session && index > 0 {
                    expected_events.push("session:drop");
                }
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("injected primary failure"));
            }
            assert_eq!(
                *engine.events.borrow(),
                expected_events,
                "{session:?} {failure:?}"
            );
        }
    }
}

#[test]
fn primary_session_drops_after_row_validation_rejects_output() {
    for reject_effect in [false, true] {
        let mut fixture = fixture(true);
        let CompatibilityCheck::Cypher(check) = &mut fixture.checks[0] else {
            unreachable!()
        };
        if reject_effect {
            check.effect_expected_rows = Some(ExpectedRows::RowCount(0));
        } else {
            check.expected_rows = ExpectedRows::RowCount(0);
        }
        let mut engine = RecordingEngine::default();
        let error = run_compatibility_fixture(&mut engine, &fixture).unwrap_err();
        assert!(error.to_string().contains("expected 0 rows"));
        let events = engine.events.borrow();
        assert_eq!(events.last().map(String::as_str), Some("session:drop"));
        assert_eq!(
            events.iter().any(|event| event == "session:effect"),
            reject_effect
        );
    }
}

#[test]
fn expected_primary_error_does_not_execute_explain_or_effect() {
    let mut fixture = fixture(false);
    let CompatibilityCheck::Cypher(check) = &mut fixture.checks[0] else {
        unreachable!()
    };
    check.expected_error = Some(ExpectedErrorClass::Execution);
    let mut engine = RecordingEngine {
        fail_at: Some("database:statement".to_string()),
        ..Default::default()
    };
    assert_eq!(
        run_compatibility_fixture(&mut engine, &fixture)
            .unwrap()
            .checks
            .len(),
        1
    );
    assert_eq!(
        *engine.events.borrow(),
        [
            "database:fixture-setup",
            "database:setup",
            "database:statement"
        ]
    );
}

#[test]
fn empty_projection_uses_the_primary_projection_boundary() {
    let fixture = CompatibilityFixture {
        name: "projection-boundary".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::ProjectedGraph(
            ProjectedGraphFixtureCheck {
                name: "empty projection".to_string(),
                rel_type: Some("REL".to_string()),
                expected_node_count: 0,
                expected_edge_count: 0,
                expected_incoming: Vec::new(),
                expected_communities: Vec::new(),
                expected_hierarchical_communities: Vec::new(),
                expected_page_rank_scores: Vec::new(),
                page_rank_top_node: None,
                tolerance: CompatibilityTolerance::default(),
            },
        )],
    };
    let mut engine = RecordingEngine::default();
    assert_eq!(
        run_compatibility_fixture(&mut engine, &fixture)
            .unwrap()
            .checks
            .len(),
        1
    );
    assert_eq!(*engine.events.borrow(), ["project:Some(\"REL\")"]);
}

#[derive(Default)]
struct ContextShadow {
    phases: Vec<ShadowRequestPhase>,
    session_statements: Vec<String>,
}

impl CompatibilityShadowEngine for ContextShadow {
    fn name(&self) -> &str {
        "context-shadow"
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        Ok(output(&statement.parameters))
    }

    fn execute_with_context(
        &mut self,
        statement: &CypherFixtureStatement,
        context: ShadowRequestContext,
    ) -> Result<QueryOutput> {
        assert_eq!(context.fixture, "primary-boundary");
        assert_eq!(
            context.check,
            if context.phase == ShadowRequestPhase::FixtureSetup {
                None
            } else {
                Some("ordered check")
            }
        );
        assert_eq!(context.statement_index, None);
        self.phases.push(context.phase);
        self.execute(statement)
    }

    fn execute_session_with_context(
        &mut self,
        statements: &[CypherFixtureStatement],
        context: ShadowRequestContext,
    ) -> Result<Vec<QueryOutput>> {
        assert_eq!(context.fixture, "primary-boundary");
        assert_eq!(context.check, Some("ordered check"));
        assert_eq!(context.statement_index, None);
        self.phases.push(context.phase);
        self.session_statements = statements
            .iter()
            .map(|statement| statement.cypher.clone())
            .collect();
        self.execute_session(statements)
    }
}

#[test]
fn shadow_dispatch_preserves_session_batch_and_request_context() {
    for session in [false, true] {
        let mut primary = RecordingEngine::default();
        let mut shadow = ContextShadow::default();
        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture(session), &mut shadow)
                .unwrap();
        assert_eq!(report.primary_checks.len(), 1);
        assert_eq!(report.shadow_checks.len(), 1);
        assert_eq!(
            report.shadow_checks[0].status,
            CompatibilityShadowStatus::Matched
        );
        assert_eq!(report.shadow_engine, "context-shadow");
        if session {
            assert_eq!(
                shadow.phases,
                [
                    ShadowRequestPhase::FixtureSetup,
                    ShadowRequestPhase::Session
                ]
            );
            assert_eq!(shadow.session_statements, ["setup", "statement", "effect"]);
        } else {
            assert_eq!(
                shadow.phases,
                [
                    ShadowRequestPhase::FixtureSetup,
                    ShadowRequestPhase::CheckSetup,
                    ShadowRequestPhase::Statement,
                    ShadowRequestPhase::Effect,
                ]
            );
            assert!(shadow.session_statements.is_empty());
        }
    }
}
