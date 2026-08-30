use super::*;
use crate::{
    ProjectionGenerationBatchLimits, ProjectionGenerationBegin, ProjectionGenerationDigestBuilder,
    ProjectionGenerationIdentity, ProjectionGenerationMember, ProjectionGenerationReadLimits,
};

#[test]
fn durable_database_exposes_projection_generations_across_reopen() {
    assert!(Database::new().projection_generation_store().is_err());

    let path = unique_test_dir("projection_generation_facade");
    let rows = vec![
        ProjectionGenerationMember {
            collection: "spans".to_string(),
            key: b"span-1".to_vec(),
            payload: b"first".to_vec(),
        },
        ProjectionGenerationMember {
            collection: "spans".to_string(),
            key: b"span-2".to_vec(),
            payload: b"second".to_vec(),
        },
    ];
    let begin = ProjectionGenerationBegin {
        identity: ProjectionGenerationIdentity {
            projection: "session_spans".to_string(),
            owner_key: b"session-1".to_vec(),
            generation: "generation-1".to_string(),
        },
        source_watermark: 42,
        projection_version: 1,
        expected_head: None,
    };

    {
        let database = Database::open(&path).expect("open durable database");
        let store = database
            .projection_generation_store()
            .expect("open projection generation catalog");
        let mut writer = store
            .begin_candidate(begin, ProjectionGenerationBatchLimits::default())
            .expect("begin candidate");
        writer.append_batch(&rows).expect("append candidate");
        let mut digest = ProjectionGenerationDigestBuilder::default();
        for row in &rows {
            digest.update(row).expect("digest candidate member");
        }
        let sealed = writer.seal(digest.finish()).expect("seal candidate");
        store.publish(&sealed).expect("publish candidate");
    }

    {
        let database = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .expect("reopen read-only durable database");
        let store = database
            .projection_generation_store()
            .expect("reopen projection generation catalog");
        let reader = store
            .open_active("session_spans", b"session-1")
            .expect("open active generation");
        let page = reader
            .read_page(None, ProjectionGenerationReadLimits::default())
            .expect("read active generation");
        assert_eq!(page.members, rows);
        assert!(page.next.is_none());
        assert!(store
            .begin_candidate(
                ProjectionGenerationBegin {
                    identity: ProjectionGenerationIdentity {
                        projection: "session_spans".to_string(),
                        owner_key: b"session-1".to_vec(),
                        generation: "generation-2".to_string(),
                    },
                    source_watermark: 43,
                    projection_version: 1,
                    expected_head: Some("generation-1".to_string()),
                },
                ProjectionGenerationBatchLimits::default(),
            )
            .is_err());
    }

    std::fs::remove_dir_all(path).expect("remove durable database fixture");
}
