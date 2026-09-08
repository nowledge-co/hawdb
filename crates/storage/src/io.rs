//! Positioned file reads shared by storage and other internal workspace crates.

use std::fs::File;
use std::io::{self, ErrorKind};

/// Fill a buffer from an absolute file offset, retrying interrupted short reads.
///
/// The range must have a representable exclusive end. On error, the buffer may
/// contain a partial read and must not be consumed as a complete payload.
/// Unix preserves the file cursor; Windows uses `seek_read`, which moves it.
pub fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        read_exact_with(buffer, offset, |buffer, offset| {
            file.read_at(buffer, offset)
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        read_exact_with(buffer, offset, |buffer, offset| {
            file.seek_read(buffer, offset)
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        read_exact_at_fallback(file, buffer, offset)
    }
}

fn read_exact_with(
    mut buffer: &mut [u8],
    mut offset: u64,
    mut read_at: impl FnMut(&mut [u8], u64) -> io::Result<usize>,
) -> io::Result<()> {
    let length = u64::try_from(buffer.len()).map_err(|_| invalid_range())?;
    offset.checked_add(length).ok_or_else(invalid_range)?;
    while !buffer.is_empty() {
        match read_at(buffer, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            Ok(read) => {
                // The complete range was checked before I/O. File reads cannot
                // return more bytes than the remaining buffer can hold.
                offset += read as u64;
                buffer = &mut buffer[read..];
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn invalid_range() -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, "read range end overflows u64")
}

#[cfg(any(test, not(any(unix, windows))))]
fn read_exact_at_fallback(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::Mutex;

    // Cloning a File can share its cursor. Serialize the whole sequence across
    // callers of this helper when the platform has no positioned-read API.
    static POSITIONED_READ_LOCK: Mutex<()> = Mutex::new(());
    let _guard = POSITIONED_READ_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut file = file;
    read_exact_with(buffer, offset, |buffer, offset| {
        file.seek(SeekFrom::Start(offset))?;
        file.read(buffer)
    })
}

#[cfg(test)]
#[path = "io/tests.rs"]
mod tests;
