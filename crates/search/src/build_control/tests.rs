use super::*;

struct ShortWriter<'a> {
    output: Vec<u8>,
    task: &'a RuntimeTaskContext,
    cancel_at: usize,
    interrupt: bool,
}

impl Write for ShortWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(bytes.len() <= 8192);
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
fn checked_payload_writes_preserve_bytes_and_checksum_across_short_writes() {
    let payload = (0..32_769).map(|index| index as u8).collect::<Vec<_>>();
    for len in [0, 1, 8191, 8192, 8193, payload.len()] {
        let task = RuntimeTaskContext::default();
        let mut writer = ShortWriter {
            output: Vec::new(),
            task: &task,
            cancel_at: usize::MAX,
            interrupt: true,
        };
        let checksum = write_checksummed(&mut writer, &payload[..len], Some(&task)).unwrap();
        assert_eq!(writer.output, payload[..len]);
        assert_eq!(checksum, crate::checksum_bytes(&payload[..len]));
    }
}

#[test]
fn cancellation_during_payload_writes_returns_no_completed_checksum() {
    let payload = vec![7; 32_769];
    for cancel_at in [0, 1, 8192, payload.len()] {
        let task = RuntimeTaskContext::default();
        if cancel_at == 0 {
            task.cancellation().cancel();
        }
        let mut writer = ShortWriter {
            output: Vec::new(),
            task: &task,
            cancel_at,
            interrupt: false,
        };
        let error = write_checksummed(&mut writer, &payload, Some(&task)).unwrap_err();
        assert!(error.to_string().contains("cancel"));
        assert!(writer.output.len() <= cancel_at + 2);
        assert_eq!(writer.output, payload[..writer.output.len()]);
    }
}
