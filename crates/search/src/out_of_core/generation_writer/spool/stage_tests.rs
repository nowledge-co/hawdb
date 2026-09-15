use super::*;
use crate::build_memory::directory;
use crate::test_allocation as allocation;
use skein_core::RuntimeMemoryReservation;
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = crate::out_of_core::generation_writer::tests::test_dir("stage_cleanup");
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn stage_cleanup_admission_precedes_creation_and_keeps_the_parent() {
    let fixture = Fixture::new();
    let bytes = directory::stage_removal_bytes(&fixture.0).unwrap();
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64 - 1, 0));
    let memory = BuildMemory::new(&task).unwrap();
    assert!(StageDirectory::create(&fixture.0, &memory, &task).is_err());
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn stage_cleanup_retains_workspace_through_full_root_cancellation_and_unwind() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    for unwind in [false, true] {
        let fixture = Fixture::new();
        let budget = 8 * 1024 * 1024;
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(budget, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let stage = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
        let path = stage.path.to_path_buf();
        for index in 0..129 {
            fs::write(path.join(format!("generated-{index}.skein")), b"partial").unwrap();
        }
        let reserved = memory.ledger.snapshot().used_bytes;
        let held = memory.input.reserve(budget as usize - reserved).unwrap();
        task.cancellation().cancel();
        if unwind {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _stage = stage;
                panic!("injected writer unwind");
            }));
            assert!(result.is_err());
        } else {
            let ((), peak) = allocation::measure(|| drop(stage));
            assert!(peak <= reserved, "requested {peak}, reserved {reserved}");
            assert_eq!(allocation::live(), 0);
        }
        assert!(!path.exists());
        assert!(fixture.0.exists());
        assert_eq!(memory.ledger.snapshot().used_bytes, held.bytes());
        drop(held);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[cfg(unix)]
#[test]
fn native_stage_cleanup_unlinks_symlinks_without_following_their_targets() {
    let fixture = Fixture::new();
    let outside = fixture.0.join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("retained"), b"outside").unwrap();
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let stage = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
    let path = stage.path.to_path_buf();
    std::os::unix::fs::symlink(&outside, path.join("link")).unwrap();
    drop(stage);
    assert!(!path.exists());
    assert_eq!(fs::read(outside.join("retained")).unwrap(), b"outside");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
