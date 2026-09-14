use super::*;
use crate::analyzer_stream::{visit_token_list, visit_token_list_with_workspace};
use skein_core::RuntimeMemoryReservation;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

fn task(bytes: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0))
        .with_executor_thread_limit(std::num::NonZeroUsize::MIN)
}

#[test]
fn initialization_denial_does_not_enter_the_consumer_or_retain_capacity() {
    let task = task(4 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let entered = AtomicUsize::new(0);
    let error = run(&memory, &task, |_| {
        entered.fetch_add(1, Ordering::Relaxed);
        Ok(())
    })
    .unwrap_err();
    assert!(error.to_string().contains("query_memory_bytes"), "{error}");
    assert_eq!(entered.load(Ordering::Relaxed), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
}

#[test]
fn admitted_tokens_keep_complete_order_and_release_scratch_on_consumer_error() {
    let task = task(64 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let analyzer = crate::SearchAnalyzerLexicon::default();
    for text in [
        "GraphStorage write_ahead_log",
        "\u{4e2d}\u{534e}\u{4eba}\u{6c11}\u{5171}\u{548c}\u{56fd}",
        "Graph_\u{9f98}\u{9750}\u{9f49} MixedCase \u{20000}\u{20001}",
    ] {
        let mut expected = Vec::new();
        visit_token_list(text, &analyzer, |token, occurrence| {
            expected.push((token, occurrence));
            Ok(())
        })
        .unwrap();
        let actual = run(&memory, &task, |workspace| {
            let mut output = Vec::new();
            visit_token_list_with_workspace(
                text,
                &analyzer,
                Some(&workspace),
                |token, occurrence| {
                    output.push((token, occurrence));
                    Ok(())
                },
            )?;
            let retained = memory.ledger.snapshot().used_bytes;
            let mut count = 0;
            let error =
                visit_token_list_with_workspace(text, &analyzer, Some(&workspace), |_, _| {
                    count += 1;
                    if count == 2 {
                        return Err(SkeinError::Execution("consumer stopped".into()));
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(error.to_string().contains("consumer stopped"));
            assert_eq!(memory.ledger.snapshot().used_bytes, retained);
            Ok(output)
        })
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn retained_hmm_capacity_survives_shorter_calls_and_shared_root_denial() {
    let task = task(64 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    run(&memory, &task, |workspace| {
        let text = "\u{9f98}\u{9750}\u{9f49}".repeat(512);
        crate::cjk_tokenizer::visit_chinese_search_tokens_with_workspace(
            &text,
            Some(&workspace),
            |_| Ok(()),
        )?;
        let retained = memory.ledger.snapshot().used_bytes;
        crate::cjk_tokenizer::visit_chinese_search_tokens_with_workspace(
            WARMUP,
            Some(&workspace),
            |_| Ok(()),
        )?;
        assert_eq!(memory.ledger.snapshot().used_bytes, retained);
        let invocation = bounds::invocation(WARMUP.len(), WARMUP.chars().count()).unwrap();
        let snapshot = memory.ledger.snapshot();
        let competing = memory
            .input
            .reserve(snapshot.budget_bytes - retained - invocation + 1)?;
        let before = memory.ledger.snapshot().used_bytes;
        let mut entered = false;
        assert!(
            crate::cjk_tokenizer::visit_chinese_search_tokens_with_workspace(
                WARMUP,
                Some(&workspace),
                |_| {
                    entered = true;
                    Ok(())
                },
            )
            .is_err()
        );
        assert!(!entered);
        assert_eq!(memory.ledger.snapshot().used_bytes, before);
        drop(competing);
        assert!(workspace.admit(WARMUP).is_ok());
        Ok(())
    })
    .unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

struct ExitObserver {
    memory: BuildMemory,
    observed: Arc<AtomicUsize>,
}

impl Drop for ExitObserver {
    fn drop(&mut self) {
        self.observed
            .store(self.memory.ledger.snapshot().used_bytes, Ordering::Relaxed);
    }
}

thread_local! {
    static EXIT: RefCell<Option<ExitObserver>> = const { RefCell::new(None) };
}

#[test]
fn native_join_keeps_the_lease_through_success_error_panic_and_cancellation_tls() {
    for mode in 0..4 {
        let task = task(32 * 1024 * 1024);
        let memory = BuildMemory::new(&task).unwrap();
        let expected = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(AtomicUsize::new(0));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run(&memory, &task, |workspace| {
                expected.store(memory.ledger.snapshot().used_bytes, Ordering::Relaxed);
                EXIT.with(|observer| {
                    *observer.borrow_mut() = Some(ExitObserver {
                        memory: memory.clone(),
                        observed: Arc::clone(&observed),
                    });
                });
                match mode {
                    0 => Ok(()),
                    1 => Err(SkeinError::Execution("analysis stopped".into())),
                    2 => panic!("analysis panic"),
                    _ => {
                        task.cancellation().cancel();
                        workspace.admit(WARMUP).map(drop)
                    }
                }
            })
        }));
        assert_eq!(result.is_err(), mode == 2);
        if let Ok(result) = result {
            assert_eq!(result.is_err(), mode != 0);
        }
        assert!(expected.load(Ordering::Relaxed) > STACK_BYTES);
        assert_eq!(
            observed.load(Ordering::Relaxed),
            expected.load(Ordering::Relaxed)
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn guard_joins_during_parent_unwind_even_when_the_worker_also_panics() {
    for worker_panics in [false, true] {
        let task = task(32 * 1024 * 1024);
        let memory = BuildMemory::new(&task).unwrap();
        let observed = Arc::new(AtomicUsize::new(0));
        let expected = Arc::new(AtomicUsize::new(0));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::thread::scope(|scope| {
                let workspace = Arc::new(Workspace::new(memory.clone(), task.clone()).unwrap());
                let thread_memory = memory.retained.reserve(STACK_BYTES).unwrap();
                let worker_workspace = Arc::clone(&workspace);
                let handle = scope.spawn(|| {
                    worker_workspace.warm_up().unwrap();
                    expected.store(memory.ledger.snapshot().used_bytes, Ordering::Relaxed);
                    EXIT.with(|observer| {
                        *observer.borrow_mut() = Some(ExitObserver {
                            memory: memory.clone(),
                            observed: Arc::clone(&observed),
                        });
                    });
                    // This owner must end before TLS, leaving only the guard.
                    drop(worker_workspace);
                    assert!(!worker_panics, "worker panic during parent unwind");
                });
                let _worker = JoinedWorker {
                    handle: Some(handle),
                    _workspace: workspace,
                    _thread_memory: thread_memory,
                };
                panic!("parent panic");
            });
        }));
        assert!(result.is_err());
        assert_eq!(
            observed.load(Ordering::Relaxed),
            expected.load(Ordering::Relaxed)
        );
        assert!(observed.load(Ordering::Relaxed) > STACK_BYTES);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn cancellation_during_token_consumption_releases_the_remaining_output_and_tls() {
    let task = task(32 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let mut consumed = 0;
    let error = run(&memory, &task, |workspace| {
        crate::cjk_tokenizer::visit_chinese_search_tokens_with_workspace(
            &"\u{9f98}\u{9750}\u{9f49}".repeat(1024),
            Some(&workspace),
            |_| {
                consumed += 1;
                task.cancellation().cancel();
                Ok(())
            },
        )
    })
    .unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!((1..=1024).contains(&consumed));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
