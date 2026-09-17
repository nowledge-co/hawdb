use skein::{SearchAnalyzerLexicon, SearchDocument, SearchIndex, SearchMode};
use std::collections::BTreeMap;

fn main() -> skein::Result<()> {
    let mut index = SearchIndex::in_memory().with_analyzer_lexicon(nowledge_application_lexicon());

    index
        .upsert(SearchDocument {
            id: "memory-lifecycle".to_string(),
            title: "Crystal memory keeps SYNTHESIZED_FROM evidence".to_string(),
            content: "Episodic provenance preserves raw Thread and SourceChunk records".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let hits = index.search("raw evidence", None, SearchMode::Text, 10)?;
    assert_eq!(hits[0].id, "memory-lifecycle");
    Ok(())
}

fn nowledge_application_lexicon() -> SearchAnalyzerLexicon {
    SearchAnalyzerLexicon::default()
        .with_normalized_alias_rule(["crystal"], ["crystallized memory", "synthesized memory"])
        .with_normalized_alias_rule(["crystallization", "crystallized"], ["crystal"])
        .with_normalized_alias_rule(["synthesized", "synthesis"], ["crystal"])
        .with_normalized_alias_rule(["synthesized from"], ["crystal", "sourced from"])
        .with_normalized_alias_rule(["episodic", "episodic provenance"], ["raw evidence"])
        .with_normalized_alias_rule(["raw evidence"], ["episodic provenance"])
        .with_normalized_alias_rule(["source provenance"], ["sourced from"])
        .with_normalized_alias_rule(["sourced from"], ["source provenance"])
        .with_normalized_alias_rule(["entity mention", "memory mention"], ["mentions"])
        .with_normalized_alias_rule(["mentions"], ["entity mention", "memory mention"])
        .with_normalized_alias_rule(["evolves"], ["memory evolution"])
        .with_normalized_alias_rule(["memory evolution", "evolution edge"], ["evolves"])
        .with_normalized_alias_rule(["ai summary"], ["community summary"])
        .with_normalized_alias_rule(
            ["community summary", "summarized community"],
            ["ai summary"],
        )
}
