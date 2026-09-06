use super::*;
use crate::FileProjection;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
#[ignore = "local projection search memory and quantized ranking campaign"]
fn projection_search_memory_campaign() {
    let root = std::env::temp_dir().join(format!(
        "skein_projection_memory_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    let mut seed = 0x2065ca11u64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let mut accepted = 0;
    let mut denied = 0;
    let mut compared = 0;
    let mut parallel_scans = 0;
    for case in 0..192 {
        let dimension = [1, 7, 8, 9, 17, 33, 65][case % 7];
        let rows = [0, 1, 2, 31, 32, 33, 63, 64, 65, 129][case % 10];
        let segment_rows = 1 + next() as usize % 33;
        let width = if (case / 6) % 2 == 0 {
            RaBitQBitWidth::One
        } else {
            RaBitQBitWidth::Four
        };
        let config =
            ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(case as u64 + 1))
                .with_segment_rows(segment_rows)
                .with_bit_width(width);
        let mut builder = ProjectionBuilder::new(config.clone()).unwrap();
        let path = root.join(format!("{case}.skein"));
        let mut writer = ProjectionWriter::create(&path, config).unwrap();
        for id in 0..rows {
            let vector = (0..dimension)
                .map(|_| (next() % 129) as f32 / 16.0 - 4.0)
                .collect::<Vec<_>>();
            builder.push(id as u64, &vector).unwrap();
            writer.push(id as u64, &vector).unwrap();
        }
        let projection = builder.finish().unwrap();
        drop(writer.finish().unwrap());
        let file = FileProjection::open(&path).unwrap();
        let query = (0..dimension)
            .map(|_| {
                if case % 17 == 0 {
                    0.0
                } else {
                    (next() % 129) as f32 / 16.0 - 4.0
                }
            })
            .collect::<Vec<_>>();
        let mut allowed = (0..rows as u64)
            .filter(|id| match case % 4 {
                1 => true,
                2 => id % 3 == 1,
                _ => false,
            })
            .collect::<Vec<_>>();
        if case % 11 == 0 && matches!(case % 4, 1 | 2) {
            allowed.push(u64::MAX);
        }
        let allowed = (case % 4 != 0).then_some(allowed.as_slice());
        let limit = [0, 1, 3, 31, rows, usize::MAX][case % 6];
        let expected = oracle(&projection, &query, limit, allowed);
        let eligible = (0..rows as u64)
            .filter(|id| allowed.is_none_or(|ids| ids.contains(id)))
            .count();
        let context = RuntimeTaskContext::default()
            .with_admitted_parallelism(NonZeroUsize::new(2 + case % 2).unwrap());
        let mut options = ProjectionSearchOptions::new()
            .with_kernel(KernelPreference::Scalar)
            .with_max_parallelism(NonZeroUsize::new(4).unwrap())
            .with_task_context(&context);
        if let Some(allowed) = allowed {
            options = options.with_allowed_ids(allowed);
        }
        let mut run =
            |global: usize,
             worker: usize,
             search: &dyn Fn(usize) -> Result<ProjectionSearchOutput>| {
                let minimum = global + worker;
                for budget in [
                    minimum - 1,
                    minimum,
                    global + 2 * worker,
                    global + 3 * worker,
                    64 * 1024 * 1024,
                ] {
                    reset_allocations();
                    let result = search(budget);
                    if budget < minimum {
                        assert!(
                            matches!(result, Err(ProjectionError::ResourceBudgetExceeded { required, available }) if required == minimum && available == budget),
                            "case={case}"
                        );
                        assert_eq!(ALLOCATIONS.get(), Allocations::default(), "case={case}");
                        denied += 1;
                    } else {
                        let output = result.unwrap();
                        assert_eq!(output.hits, expected, "case={case}");
                        let workers = (budget - global)
                            .checked_div(worker)
                            .unwrap_or(0)
                            .min(4)
                            .min(2 + case % 2)
                            .min(projection.manifest.segments.len())
                            .min(allowed.map_or(usize::MAX, |ids| ids.len().div_ceil(1024)));
                        assert_eq!(output.report.worker_count, workers, "case={case}");
                        assert_eq!(
                            output.report.admitted_working_bytes,
                            global + workers * worker,
                            "case={case}"
                        );
                        let scored = if workers == 0 { 0 } else { eligible };
                        assert_eq!(output.report.scored_document_count, scored, "case={case}");
                        assert_eq!(
                            output.report.filtered_document_count,
                            rows - scored,
                            "case={case}"
                        );
                        assert_eq!(output.report.candidate_count, expected.len(), "case={case}");
                        parallel_scans += usize::from(workers > 1);
                        compared += output.hits.len();
                        accepted += 1;
                    }
                }
            };
        let (global, worker) = envelope(&projection, limit, allowed);
        run(global, worker, &|budget| {
            projection.search(&query, limit, options.with_max_working_bytes(budget))
        });
        let (global, worker) = envelope(&file, limit, allowed);
        run(global, worker, &|budget| {
            file.search(&query, limit, options.with_max_working_bytes(budget))
        });
        drop(file);
        fs::remove_file(path).unwrap();
    }
    fs::remove_dir(root).unwrap();
    assert!(parallel_scans > 0);
    eprintln!("projection memory seed=0x2065ca11 cases=192 backends=384 accepted={accepted} denied={denied} compared={compared} parallel={parallel_scans}");
}
