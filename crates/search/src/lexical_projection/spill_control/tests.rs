use super::*;
use crate::build_control::observation;
use crate::build_memory::BuildMemory;

fn admitted(task: &RuntimeTaskContext) -> Control {
    let memory = BuildMemory::new(task).unwrap();
    Control::new(ReservedMemory::new(&memory.spool, 0).unwrap(), task.clone())
}

struct ShortWriter<'a> {
    output: Vec<u8>,
    task: &'a RuntimeTaskContext,
    cancel_at: usize,
    interrupt: bool,
}

impl Write for ShortWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if std::mem::take(&mut self.interrupt) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let len = bytes.len().min(3);
        self.output.extend_from_slice(&bytes[..len]);
        if self.output.len() >= self.cancel_at {
            self.task.cancellation().cancel();
        }
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn record_checks_cross_field_boundaries_and_preserve_short_writes() {
    let task = RuntimeTaskContext::default();
    let control = admitted(&task);
    let mut output = ShortWriter {
        output: Vec::new(),
        task: &task,
        cancel_at: usize::MAX,
        interrupt: true,
    };
    let payload = vec![7; 8193];
    let (result, checks) = observation::measure(|| -> io::Result<()> {
        let mut writer = RecordWriter::new(&mut output, &control)?;
        writer.write_all(&[1, 2, 3])?;
        writer.write_all(&payload)?;
        writer.flush()
    });
    result.unwrap();
    assert_eq!(checks, 4, "entry, interruption retry, 8 KiB, flush");
    assert_eq!(&output.output[..3], &[1, 2, 3]);
    assert_eq!(&output.output[3..], payload);
}

#[test]
fn cancelled_record_writes_stop_at_a_chunk_or_flush_boundary() {
    for cancel_at in [0, 1, 8191, 8192, 8193, 16387] {
        let task = RuntimeTaskContext::default();
        let control = admitted(&task);
        if cancel_at == 0 {
            task.cancellation().cancel();
        }
        let mut output = ShortWriter {
            output: Vec::new(),
            task: &task,
            cancel_at,
            interrupt: false,
        };
        let error = (|| -> io::Result<()> {
            let mut writer = RecordWriter::new(&mut output, &control)?;
            writer.write_all(&vec![7; 16387])?;
            writer.flush()
        })()
        .unwrap_err();
        assert!(error.to_string().contains("cancel"));
        assert!(output.output.len() <= cancel_at.saturating_add(SPILL_IO_BUFFER_BYTES));
        assert!(output.output.iter().all(|&byte| byte == 7));
    }
}

#[test]
fn expired_records_do_not_write_any_bytes() {
    let task = RuntimeTaskContext::with_timeout(std::time::Duration::ZERO);
    let control = admitted(&RuntimeTaskContext::default()).with_task(task);
    let mut output = Vec::<u8>::new();
    let error = match RecordWriter::new(&mut output, &control) {
        Ok(_) => panic!("expired record admitted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("deadline"));
    assert!(output.is_empty());
}

#[test]
fn interrupted_writes_cannot_hide_cancellation() {
    struct Interrupt<'a>(&'a RuntimeTaskContext);
    impl Write for Interrupt<'_> {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            self.0.cancellation().cancel();
            Err(io::ErrorKind::Interrupted.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let task = RuntimeTaskContext::default();
    let control = admitted(&task);
    let mut output = Interrupt(&task);
    let error = RecordWriter::new(&mut output, &control)
        .unwrap()
        .write_all(b"payload")
        .unwrap_err();
    assert!(error.to_string().contains("cancel"));
}

#[test]
fn long_field_reads_check_before_allocation_and_between_chunks() {
    struct Reader<'a> {
        task: &'a RuntimeTaskContext,
        read: usize,
    }
    impl std::io::Read for Reader<'_> {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            output.fill(b'x');
            self.read += output.len();
            self.task.cancellation().cancel();
            Ok(output.len())
        }
    }
    for cancelled in [false, true] {
        let task = RuntimeTaskContext::default();
        if cancelled {
            task.cancellation().cancel();
        }
        let control = admitted(&RuntimeTaskContext::default()).with_task(task.clone());
        let mut reader = Reader {
            task: &task,
            read: 0,
        };
        let error = crate::lexical_projection::spill_memory::read_text(
            &mut reader,
            2 * SPILL_IO_BUFFER_BYTES + 1,
            &control,
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancel"));
        assert_eq!(
            reader.read,
            if cancelled { 0 } else { SPILL_IO_BUFFER_BYTES }
        );
    }
}
