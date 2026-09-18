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

pub(super) fn manifest(corpus: &Corpus) -> Check {
    let manifest = &corpus.manifest;
    if manifest.protocol != "hawdb-sql-frontend-corpus-v1"
        || manifest.audited_base_revision.len() != 40
        || !manifest
            .audited_base_revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || manifest.case_count != corpus.cases.len()
        || corpus.cases.is_empty()
    {
        return Err("invalid corpus identity or case count".into());
    }
    let cases: BTreeMap<_, _> = corpus
        .cases
        .iter()
        .map(|case| (case.id.as_str(), case))
        .collect();
    if cases.len() != corpus.cases.len() {
        return Err("duplicate corpus case ID".into());
    }
    coverage(corpus)?;
    let waivers: BTreeMap<_, _> = manifest
        .waivers
        .iter()
        .map(|waiver| (waiver.id.as_str(), waiver))
        .collect();
    if waivers.len() != manifest.waivers.len() {
        return Err("duplicate waiver ID".into());
    }
    let mut referenced = BTreeSet::new();
    for case in &corpus.cases {
        if case.sql.is_empty()
            || case.source.symbol.is_empty()
            || case.source.adaptation.is_empty()
            || (case.source.adaptation.starts_with("verbatim") && case.source.line == 0)
        {
            return Err(format!("{}: incomplete source provenance", case.id));
        }
        let diverges = case.production.accepts() != case.owned.accepts();
        if diverges != case.waiver.is_some() {
            return Err(format!("{}: missing or stale waiver", case.id));
        }
        if let Some(id) = &case.waiver {
            let waiver = waivers
                .get(id.as_str())
                .ok_or_else(|| format!("{}: unknown waiver {id}", case.id))?;
            let direction = if case.production.accepts() {
                "upstream_accepts"
            } else {
                "owned_accepts"
            };
            if waiver.direction != direction
                || !waiver.families.contains(&case.family)
                || !waiver
                    .issue
                    .starts_with("https://github.com/nowledge-co/hawdb/issues/")
                || waiver.reason.len() < 40
            {
                return Err(format!("{}: waiver scope does not match", case.id));
            }
            referenced.insert(id.as_str());
        }
    }
    if referenced != waivers.keys().copied().collect() {
        return Err("unreferenced waiver".into());
    }

    let mut inventoried = BTreeSet::new();
    let mut source_paths = BTreeSet::new();
    for source in &manifest.sources {
        if !source_paths.insert(source.path.as_str()) || source.case_ids.is_empty() {
            return Err("duplicate or empty source inventory".into());
        }
        let bytes =
            source_bytes(&source.path).ok_or_else(|| format!("unknown source {}", source.path))?;
        if format!("{:x}", Sha256::digest(bytes)) != source.sha256 {
            return Err(format!("source inventory needs review: {}", source.path));
        }
        for id in &source.case_ids {
            let case = cases
                .get(id.as_str())
                .ok_or_else(|| format!("missing inventoried case {id}"))?;
            if case.source.path != source.path || !inventoried.insert(id.as_str()) {
                return Err(format!("{id}: mismatched or repeated source inventory"));
            }
        }
    }
    if source_paths != SOURCE_FILES.iter().map(|(path, _)| *path).collect() {
        return Err("incomplete source inventory".into());
    }
    for id in &manifest.local_case_ids {
        let case = cases
            .get(id.as_str())
            .ok_or_else(|| format!("missing local case {id}"))?;
        if case.source.path != "docs/SQL_FRONTEND_CORPUS.md" || !inventoried.insert(id.as_str()) {
            return Err(format!("{id}: invalid local counterpart provenance"));
        }
    }
    if inventoried != cases.keys().copied().collect() {
        return Err("uninventoried corpus case".into());
    }
    Ok(())
}

fn coverage(corpus: &Corpus) -> Check {
    let families = [
        "select",
        "select_graph_table",
        "create_property_graph",
        "create_table",
        "create_index",
        "alter_table",
        "insert",
        "update",
        "delete",
        "explain",
    ];
    if corpus
        .cases
        .iter()
        .any(|case| !families.contains(&case.family.as_str()))
    {
        return Err("unclassified statement family".into());
    }
    let expected = BTreeSet::from(["select", "select_graph_table", "create_property_graph"]);
    if corpus
        .manifest
        .owned_families
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected
        || corpus.manifest.owned_families.len() != expected.len()
    {
        return Err("owned family declaration is incomplete".into());
    }
    for family in expected {
        for accepts in [true, false] {
            if !corpus
                .cases
                .iter()
                .any(|case| case.family == family && case.owned.accepts() == accepts)
            {
                return Err(format!(
                    "missing owned family coverage: {family}, accepts={accepts}"
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn frozen_inputs(corpus: &Corpus) -> Check {
    let workload: serde_json::Value =
        serde_json::from_str(WORKLOAD).map_err(|error| error.to_string())?;
    let statements = workload["statements"]
        .as_array()
        .ok_or("missing frozen statements")?;
    let workload_cases: Vec<_> = corpus
        .cases
        .iter()
        .filter(|case| case.source.adaptation == "frozen_workload")
        .collect();
    if workload_cases.len() != statements.len() {
        return Err("incomplete frozen workload inventory".into());
    }
    for statement in statements {
        let name = statement["name"]
            .as_str()
            .ok_or("missing frozen statement name")?;
        let matched: Vec<_> = workload_cases
            .iter()
            .filter(|case| case.source.reference.as_deref() == Some(name))
            .collect();
        if matched.len() != 1
            || matched[0].sql != statement["sql"].as_str().ok_or("missing frozen SQL")?
        {
            return Err(format!("frozen SQL mismatch: {name}"));
        }
        let Outcome::Accept { parameters } = &matched[0].production else {
            return Err(format!("frozen production statement must prepare: {name}"));
        };
        let expected = statement["parameters"]
            .as_array()
            .ok_or("missing frozen parameters")?;
        if parameters.len() != expected.len() || parameters.iter().copied().ne(1..=expected.len()) {
            return Err(format!("frozen parameter contract mismatch: {name}"));
        }
    }
    let lines: Vec<_> = SCHEMA
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .collect();
    let schema_cases: Vec<_> = corpus
        .cases
        .iter()
        .filter(|case| case.source.adaptation == "frozen_schema")
        .collect();
    if schema_cases.len() != lines.len() {
        return Err("incomplete frozen schema inventory".into());
    }
    for (line_number, sql) in lines {
        let matched: Vec<_> = schema_cases
            .iter()
            .filter(|case| case.source.line == line_number + 1)
            .collect();
        if matched.len() != 1 || matched[0].sql != sql || !matched[0].production.accepts() {
            return Err(format!("frozen schema mismatch: line {}", line_number + 1));
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Literal {
    path: String,
    function: String,
    line: usize,
    sql: String,
    formatted: bool,
    cases: Vec<String>,
    exclusion: String,
}

pub(super) fn literal_inventory(corpus: &Corpus) -> Check {
    let literals: Vec<Literal> =
        serde_json::from_str(LITERALS).map_err(|error| error.to_string())?;
    check_literals(corpus, &literals)
}

fn check_literals(corpus: &Corpus, literals: &[Literal]) -> Check {
    if literals.len() != corpus.manifest.literal_count || literals.is_empty() {
        return Err("incomplete source literal inventory".into());
    }
    let cases: BTreeMap<_, _> = corpus
        .cases
        .iter()
        .map(|case| (case.id.as_str(), case))
        .collect();
    for literal in literals {
        if source_bytes(&literal.path).is_none()
            || literal.line == 0
            || literal.sql.is_empty()
            || literal.cases.is_empty() == literal.exclusion.is_empty()
        {
            return Err("unclassified source literal".into());
        }
        for id in &literal.cases {
            let case = cases
                .get(id.as_str())
                .ok_or_else(|| format!("missing literal case {id}"))?;
            if case.source.path != literal.path
                || case.source.symbol != literal.function
                || case.source.line != literal.line
            {
                return Err(format!("{id}: literal provenance mismatch"));
            }
            if !literal.formatted {
                let sql = if case
                    .source
                    .adaptation
                    .contains("wrap_graph_table_as_from_item")
                {
                    format!("SELECT * FROM {}", literal.sql)
                } else {
                    literal.sql.clone()
                };
                if case.sql != sql {
                    return Err(format!("{id}: literal text mismatch"));
                }
            }
        }
    }
    Ok(())
}

#[test]
fn rejects_missing_coverage_and_inventory() {
    let mut corpus = Corpus::load();
    corpus
        .cases
        .retain(|case| case.family != "select" || case.owned.accepts());
    corpus.manifest.case_count = corpus.cases.len();
    assert!(manifest(&corpus)
        .unwrap_err()
        .contains("missing owned family coverage"));
    let mut corpus = Corpus::load();
    corpus.cases.pop();
    assert!(manifest(&corpus).is_err());
    let mut corpus = Corpus::load();
    corpus.manifest.sources[0].sha256 = "0".repeat(64);
    assert!(manifest(&corpus)
        .unwrap_err()
        .contains("source inventory needs review"));
    let mut corpus = Corpus::load();
    corpus.manifest.sources.pop();
    assert!(manifest(&corpus)
        .unwrap_err()
        .contains("incomplete source inventory"));
    let mut corpus = Corpus::load();
    corpus.cases[0].family = "unclassified".into();
    assert!(manifest(&corpus)
        .unwrap_err()
        .contains("unclassified statement family"));
}

#[test]
fn rejects_missing_or_unclassified_literals() {
    let corpus = Corpus::load();
    let mut literals: Vec<Literal> = serde_json::from_str(LITERALS).unwrap();
    literals.pop();
    assert!(check_literals(&corpus, &literals)
        .unwrap_err()
        .contains("incomplete source literal inventory"));
    let mut literals: Vec<Literal> = serde_json::from_str(LITERALS).unwrap();
    literals
        .iter_mut()
        .find(|literal| literal.cases.is_empty())
        .unwrap()
        .exclusion
        .clear();
    assert!(check_literals(&corpus, &literals)
        .unwrap_err()
        .contains("unclassified source literal"));
}

#[test]
fn rejects_missing_stale_and_unreferenced_waivers() {
    let mut corpus = Corpus::load();
    let case = corpus
        .cases
        .iter_mut()
        .find(|case| case.waiver.is_some())
        .unwrap();
    case.waiver = None;
    assert!(manifest(&corpus)
        .unwrap_err()
        .contains("missing or stale waiver"));
    let mut corpus = Corpus::load();
    let case = corpus
        .cases
        .iter_mut()
        .find(|case| case.waiver.is_some())
        .unwrap();
    case.production = case.owned.clone();
    assert!(manifest(&corpus)
        .unwrap_err()
        .contains("missing or stale waiver"));
    let mut corpus = Corpus::load();
    let mut unused = corpus.manifest.waivers[0].clone();
    unused.id = "unused".into();
    corpus.manifest.waivers.push(unused);
    assert!(manifest(&corpus)
        .unwrap_err()
        .contains("unreferenced waiver"));
}

#[test]
fn rejects_frozen_source_and_parameter_drift() {
    let mut corpus = Corpus::load();
    let case = corpus
        .cases
        .iter_mut()
        .find(|case| case.source.adaptation == "frozen_workload")
        .unwrap();
    case.sql.push(' ');
    assert!(frozen_inputs(&corpus)
        .unwrap_err()
        .contains("frozen SQL mismatch"));
    let mut corpus = Corpus::load();
    let case = corpus
        .cases
        .iter_mut()
        .find(|case| case.source.adaptation == "frozen_workload")
        .unwrap();
    case.production = Outcome::Accept { parameters: vec![] };
    assert!(frozen_inputs(&corpus)
        .unwrap_err()
        .contains("frozen parameter contract mismatch"));
}

#[test]
fn both_frontends_enforce_the_recorded_outcomes_and_error_codes() {
    let corpus = Corpus::load();
    let mut case = corpus
        .cases
        .iter()
        .find(|case| case.production.accepts() && case.owned.accepts())
        .unwrap()
        .clone();
    case.production = Outcome::Reject {
        code: "Parse".into(),
    };
    assert!(check_case(&case).unwrap_err().contains("production"));
    let mut case = corpus
        .cases
        .iter()
        .find(|case| !case.owned.accepts())
        .unwrap()
        .clone();
    case.owned = Outcome::Reject {
        code: "InvalidCharacter".into(),
    };
    assert!(check_case(&case).unwrap_err().contains("owned"));
}
