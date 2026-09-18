use super::*;

#[test]
fn reads_communities_for_nowledge_summary_list_shapes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Community {id: 'community_a', community_id: 1, name: 'Alpha', description: 'alpha description', ai_summary: 'alpha summary', member_count: 5, updated_at: 10})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_b', community_id: 2, name: 'Beta', description: 'beta description', ai_summary: '', member_count: 10, updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_c', community_id: 3, name: 'Gamma', description: 'gamma description', ai_summary: 'gamma summary', member_count: 3, updated_at: 30})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_negative', community_id: -1, name: 'Negative', ai_summary: 'negative summary', member_count: 100})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let summary_only = db
        .query_communities_via_cypher(&KnowledgeCommunityListRequest {
            require_summary: true,
            require_non_negative_community_id: false,
            order: KnowledgeCommunityListOrder::MemberCountDesc,
            limit: 2,
        })
        .unwrap();
    assert_eq!(summary_only.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(summary_only.matched_count, 3);
    assert_eq!(summary_only.returned_count, 2);
    assert_eq!(
        summary_only.rows[0].id.as_deref(),
        Some("community_negative")
    );
    assert_eq!(summary_only.rows[0].member_count, Some(100));
    assert!(summary_only.rows[0].has_summary);
    assert_eq!(summary_only.rows[1].id.as_deref(), Some("community_a"));
    assert_eq!(
        summary_only.rows[1].description,
        Some(Value::String("alpha description".to_string()))
    );
    assert_eq!(
        summary_only.rows[1].ai_summary,
        Some(Value::String("alpha summary".to_string()))
    );
    assert_eq!(summary_only.rows[1].updated_at, Some(Value::Int(10)));

    let presence_ranked = db
        .query_communities_via_cypher(&KnowledgeCommunityListRequest {
            require_summary: false,
            require_non_negative_community_id: true,
            order: KnowledgeCommunityListOrder::SummaryPresenceThenMemberCountDesc,
            limit: 0,
        })
        .unwrap();
    assert_eq!(presence_ranked.matched_count, 3);
    assert_eq!(presence_ranked.returned_count, 3);
    assert_eq!(presence_ranked.rows[0].id.as_deref(), Some("community_a"));
    assert_eq!(presence_ranked.rows[1].id.as_deref(), Some("community_c"));
    assert_eq!(presence_ranked.rows[2].id.as_deref(), Some("community_b"));
    assert!(!presence_ranked.rows[2].has_summary);

    let stats = db.plan_cache_stats();
    let repeated_presence_ranked = db
        .query_communities_via_cypher(&KnowledgeCommunityListRequest {
            require_summary: false,
            require_non_negative_community_id: true,
            order: KnowledgeCommunityListOrder::SummaryPresenceThenMemberCountDesc,
            limit: 0,
        })
        .unwrap();
    assert_eq!(repeated_presence_ranked, presence_ranked);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert_eq!(repeated_stats.hits, stats.hits + 1);
}

#[test]
fn community_list_read_returns_empty_without_community_label() {
    let db = Database::new();

    let output = db
        .query_communities_via_cypher(&KnowledgeCommunityListRequest {
            require_summary: true,
            require_non_negative_community_id: true,
            order: KnowledgeCommunityListOrder::SummaryPresenceThenMemberCountDesc,
            limit: 10,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, db.store.commit_epoch());
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.returned_count, 0);
    assert!(output.rows.is_empty());
}

#[test]
fn reads_community_detail_for_wiki_and_mcp_shapes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Community {id: 'community_a', community_id: 7, name: 'Alpha', description: 'alpha description', ai_summary: 'alpha summary', member_count: 5, updated_at: 10})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_b', community_id: 8, name: 'Beta', description: 'beta description', ai_summary: '', member_count: 3})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let community_id_request = KnowledgeCommunityRequest {
        key: KnowledgeCommunityLookupKey::CommunityId(7),
    };
    let by_community_id = db
        .query_community_via_cypher(&community_id_request)
        .unwrap();
    assert_eq!(by_community_id.graph_commit_epoch, graph_commit_epoch);
    assert!(by_community_id.found);
    let row = by_community_id.row.as_ref().unwrap();
    assert_eq!(row.id.as_deref(), Some("community_a"));
    assert_eq!(row.community_id, Some(7));
    assert_eq!(row.name.as_deref(), Some("Alpha"));
    assert_eq!(
        row.description,
        Some(Value::String("alpha description".to_string()))
    );
    assert_eq!(
        row.ai_summary,
        Some(Value::String("alpha summary".to_string()))
    );
    assert_eq!(row.member_count, Some(5));
    assert_eq!(row.updated_at, Some(Value::Int(10)));
    assert!(row.has_summary);

    let stats = db.plan_cache_stats();
    let repeated_by_community_id = db
        .query_community_via_cypher(&community_id_request)
        .unwrap();
    assert_eq!(repeated_by_community_id, by_community_id);
    let repeated_stats = db.plan_cache_stats();
    assert_eq!(repeated_stats.entries, stats.entries);
    assert_eq!(repeated_stats.misses, stats.misses);
    assert_eq!(repeated_stats.hits, stats.hits + 1);

    let by_id = db
        .query_community_via_cypher(&KnowledgeCommunityRequest {
            key: KnowledgeCommunityLookupKey::Id("community_b".to_string()),
        })
        .unwrap();
    assert!(by_id.found);
    let by_id_row = by_id.row.unwrap();
    assert_eq!(by_id_row.community_id, Some(8));
    assert!(!by_id_row.has_summary);

    let missing = db
        .query_community_via_cypher(&KnowledgeCommunityRequest {
            key: KnowledgeCommunityLookupKey::CommunityId(99),
        })
        .unwrap();
    assert!(!missing.found);
    assert_eq!(missing.row, None);
}

#[test]
fn community_detail_read_rejects_empty_id() {
    let db = Database::new();

    let error = db
        .query_community_via_cypher(&KnowledgeCommunityRequest {
            key: KnowledgeCommunityLookupKey::Id(String::new()),
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty id"));
}
