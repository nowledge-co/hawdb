use crate::error::{Result, SkeinError};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

const READ_BUFFER_BYTES: usize = 8192;

pub(crate) fn read_bounded_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    // Admission and reading must refer to the same opened file, even if its
    // pathname is replaced while a generation is being published.
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    if length > max_bytes {
        return Err(SkeinError::Storage(format!(
            "search artifact {} requires {length} bytes, exceeding {max_bytes}",
            path.display()
        )));
    }
    let length = usize::try_from(length).map_err(|_| {
        SkeinError::Storage(format!(
            "search artifact {} length does not fit in memory",
            path.display()
        ))
    })?;
    #[cfg(test)]
    tests::after_admission(path);
    let bytes = read_admitted_bytes(&mut file, length, path)?;
    #[cfg(test)]
    tests::after_read(path);
    Ok(bytes)
}

fn read_admitted_bytes(reader: &mut impl Read, length: usize, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; READ_BUFFER_BYTES];
    while bytes.len() < length {
        let count = buffer.len().min(length - bytes.len());
        reader.read_exact(&mut buffer[..count])?;
        let needed = bytes.len() + count;
        if needed > bytes.capacity() {
            // Grow with data actually read, without quadratic reallocations or
            // reserving the entire caller budget for a small or truncated file.
            let capacity = needed.max(bytes.capacity().saturating_mul(2)).min(length);
            bytes
                .try_reserve_exact(capacity - bytes.len())
                .map_err(|error| {
                    SkeinError::Storage(format!(
                        "search artifact {} allocation failed: {error}",
                        path.display()
                    ))
                })?;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    // Probe outside the admitted length separately. Growing files must fail,
    // not cause allocation or reading toward the (possibly much larger) limit.
    loop {
        match reader.read(&mut buffer[..1]) {
            Ok(0) => return Ok(bytes),
            Ok(_) => {
                return Err(SkeinError::Storage(format!(
                    "search artifact {} grew beyond its admitted {length} bytes",
                    path.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
