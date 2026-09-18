use super::*;

const AUGMENTATION_JOB_EXACT_QUERY: &str = "MATCH (j:AugmentationJob) WHERE j.job_id = $job_id \
     RETURN j.job_id AS job_id, id(j) AS node_id, j.job_type AS job_type, \
     j.status AS status, j.progress AS progress, j.message AS message, \
     j.result AS result, j.error_message AS error_message, j.started_at AS started_at, \
     j.completed_at AS completed_at, j.created_at AS created_at \
     ORDER BY node_id ASC LIMIT 1";

const AUGMENTATION_JOB_FILTERED_STARTED_QUERY: &str =
    "MATCH (j:AugmentationJob) WHERE j.status = $status \
     RETURN j.job_id AS job_id, id(j) AS node_id, j.job_type AS job_type, \
     j.status AS status, j.progress AS progress, j.message AS message, \
     j.result AS result, j.error_message AS error_message, j.started_at AS started_at, \
     j.completed_at AS completed_at, j.created_at AS created_at \
     ORDER BY j.started_at DESC, node_id ASC LIMIT $limit";

const AUGMENTATION_JOB_FILTERED_CREATED_QUERY: &str =
    "MATCH (j:AugmentationJob) WHERE j.status = $status \
     RETURN j.job_id AS job_id, id(j) AS node_id, j.job_type AS job_type, \
     j.status AS status, j.progress AS progress, j.message AS message, \
     j.result AS result, j.error_message AS error_message, j.started_at AS started_at, \
     j.completed_at AS completed_at, j.created_at AS created_at \
     ORDER BY j.created_at DESC, node_id ASC LIMIT $limit";

const AUGMENTATION_JOB_ALL_CREATED_QUERY: &str = "MATCH (j:AugmentationJob) \
     RETURN j.job_id AS job_id, id(j) AS node_id, j.job_type AS job_type, \
     j.status AS status, j.progress AS progress, j.message AS message, \
     j.result AS result, j.error_message AS error_message, j.started_at AS started_at, \
     j.completed_at AS completed_at, j.created_at AS created_at \
     ORDER BY j.created_at DESC, node_id ASC LIMIT $limit";

#[test]
fn reads_augmentation_job_status_with_parameterized_cypher() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:AugmentationJob {job_id: 'job_1', job_type: 'pagerank', status: 'running', progress: 42.5, message: 'Working', result: '{}', error_message: '', started_at: 100, completed_at: NULL, created_at: 90})")
        .unwrap();
    let mut read = db.begin_read_transaction();
    let parameters = BTreeMap::from([("job_id".to_string(), Value::String("job_1".to_string()))]);

    let first = read
        .query_with_params_bounded(AUGMENTATION_JOB_EXACT_QUERY, &parameters, Some(1))
        .unwrap();
    let second = read
        .query_with_params_bounded(AUGMENTATION_JOB_EXACT_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(read.commit_epoch(), 1);
    let job = &first.rows[0];
    assert_eq!(job.get("job_id"), Some(&Value::String("job_1".to_string())));
    assert_eq!(
        job.get("job_type"),
        Some(&Value::String("pagerank".to_string()))
    );
    assert_eq!(
        job.get("status"),
        Some(&Value::String("running".to_string()))
    );
    assert_eq!(job.get("progress"), Some(&Value::Float(42.5)));
    assert_eq!(
        job.get("message"),
        Some(&Value::String("Working".to_string()))
    );
    assert_eq!(job.get("result"), Some(&Value::String("{}".to_string())));
    assert_eq!(
        job.get("error_message"),
        Some(&Value::String(String::new()))
    );
    assert_eq!(job.get("started_at"), Some(&Value::Int(100)));
    assert_eq!(job.get("completed_at"), Some(&Value::Null));
    assert_eq!(job.get("created_at"), Some(&Value::Int(90)));
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);

    let missing = BTreeMap::from([("job_id".to_string(), Value::String("missing".to_string()))]);
    assert!(read
        .query_with_params_bounded(AUGMENTATION_JOB_EXACT_QUERY, &missing, Some(1))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn lists_augmentation_jobs_with_named_count_and_page_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:AugmentationJob {job_id: 'old_running', job_type: 'pagerank', status: 'running', progress: 10.0, message: 'old', started_at: 10, created_at: 1})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'new_running', job_type: 'louvain', status: 'running', progress: 20.0, message: 'new', started_at: 30, created_at: 2})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'pending', job_type: 'louvain', status: 'pending', progress: 0.0, message: 'pending', created_at: 3})")
        .unwrap();
    let mut read = db.begin_read_transaction();
    let count_parameters =
        BTreeMap::from([("status".to_string(), Value::String("running".to_string()))]);
    let count_query = "MATCH (j:AugmentationJob) WHERE j.status = $status RETURN count(j) AS total";
    let count = read
        .query_with_params_bounded(count_query, &count_parameters, Some(1))
        .unwrap();
    assert_eq!(count.rows[0].get("total"), Some(&Value::Int(2)));

    let mut page_parameters = count_parameters.clone();
    page_parameters.insert("limit".to_string(), Value::Int(1));
    let page = read
        .query_with_params_bounded(
            AUGMENTATION_JOB_FILTERED_STARTED_QUERY,
            &page_parameters,
            Some(1),
        )
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.rows[0].get("job_id"),
        Some(&Value::String("new_running".to_string()))
    );
    assert_eq!(page.rows[0].get("started_at"), Some(&Value::Int(30)));

    read.query_with_params_bounded(count_query, &count_parameters, Some(1))
        .unwrap();
    read.query_with_params_bounded(
        AUGMENTATION_JOB_FILTERED_STARTED_QUERY,
        &page_parameters,
        Some(1),
    )
    .unwrap();
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 2);
}

#[test]
fn selects_created_at_list_shape_without_dynamic_query_fragments() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'old_done', job_type: 'pagerank', status: 'done', progress: 100.0, message: 'old', created_at: 10})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'new_done', job_type: 'pagerank', status: 'done', progress: 100.0, message: 'new', created_at: 20})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'queued', job_type: 'community', status: 'queued', progress: 0.0, message: 'queued', created_at: 30})")
        .unwrap();
    let mut read = db.begin_read_transaction();

    let filtered_parameters = BTreeMap::from([
        ("status".to_string(), Value::String("done".to_string())),
        ("limit".to_string(), Value::Int(10)),
    ]);
    let filtered = read
        .query_with_params_bounded(
            AUGMENTATION_JOB_FILTERED_CREATED_QUERY,
            &filtered_parameters,
            Some(10),
        )
        .unwrap();
    assert_eq!(filtered.rows.len(), 2);
    assert_eq!(
        filtered.rows[0].get("job_id"),
        Some(&Value::String("new_done".to_string()))
    );
    assert_eq!(
        filtered.rows[1].get("job_id"),
        Some(&Value::String("old_done".to_string()))
    );

    let all_parameters = BTreeMap::from([("limit".to_string(), Value::Int(2))]);
    let all = read
        .query_with_params_bounded(AUGMENTATION_JOB_ALL_CREATED_QUERY, &all_parameters, Some(2))
        .unwrap();
    assert_eq!(all.rows.len(), 2);
    assert_eq!(
        all.rows[0].get("job_id"),
        Some(&Value::String("queued".to_string()))
    );
    assert_eq!(
        all.rows[1].get("job_id"),
        Some(&Value::String("new_done".to_string()))
    );
}

#[test]
fn augmentation_job_read_values_remain_parameters() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'job_1', status: 'running'})")
        .unwrap();
    let mut read = db.begin_read_transaction();
    let parameters = BTreeMap::from([(
        "job_id".to_string(),
        Value::String("job_1') MATCH (n) RETURN n //".to_string()),
    )]);
    assert!(read
        .query_with_params_bounded(AUGMENTATION_JOB_EXACT_QUERY, &parameters, Some(1))
        .unwrap()
        .rows
        .is_empty());
}
