use crate::error::{Result, SkeinError};
use crate::Uuid;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const NANOS_PER_MILLISECOND: u128 = 1_000_000;
const MAX_UNIX_MILLISECONDS: u128 = (1_u128 << 48) - 1;
const MAX_UNIX_NANOS: u128 = MAX_UNIX_MILLISECONDS * NANOS_PER_MILLISECOND + 999_999;
const MINIMUM_MONOTONIC_STEP_NANOS: u128 = 245;

static LAST_UNIX_NANOS: Mutex<Option<u128>> = Mutex::new(None);

pub fn generate_uuidv7() -> Result<Uuid> {
    generate_uuidv7_with(next_system_unix_nanos, |bytes| {
        getrandom::fill(bytes).map_err(|error| {
            SkeinError::Execution(format!("uuidv7 random source unavailable: {error}"))
        })
    })
}

fn next_system_unix_nanos() -> Result<u128> {
    let observed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            SkeinError::Execution(format!("uuidv7 clock is before Unix epoch: {error}"))
        })?
        .as_nanos();
    if observed > MAX_UNIX_NANOS {
        return Err(SkeinError::Execution(
            "uuidv7 clock exceeds the RFC 9562 timestamp range".to_string(),
        ));
    }
    let mut last = LAST_UNIX_NANOS
        .lock()
        .map_err(|_| SkeinError::Execution("uuidv7 clock state is poisoned".to_string()))?;
    let next = match *last {
        Some(previous) => observed.max(previous.saturating_add(MINIMUM_MONOTONIC_STEP_NANOS)),
        None => observed,
    };
    if next > MAX_UNIX_NANOS {
        return Err(SkeinError::Execution(
            "uuidv7 clock exceeds the RFC 9562 timestamp range".to_string(),
        ));
    }
    *last = Some(next);
    Ok(next)
}

fn generate_uuidv7_with(
    clock: impl FnOnce() -> Result<u128>,
    random: impl FnOnce(&mut [u8]) -> Result<()>,
) -> Result<Uuid> {
    let unix_nanos = clock()?;
    if unix_nanos > MAX_UNIX_NANOS {
        return Err(SkeinError::Execution(
            "uuidv7 clock exceeds the RFC 9562 timestamp range".to_string(),
        ));
    }
    let unix_milliseconds = unix_nanos / NANOS_PER_MILLISECOND;
    let sub_millisecond_nanos = unix_nanos % NANOS_PER_MILLISECOND;
    let increased_clock_precision =
        (sub_millisecond_nanos * (1 << 12) / NANOS_PER_MILLISECOND) as u16;

    let mut bytes = [0_u8; 16];
    bytes[..6].copy_from_slice(&unix_milliseconds.to_be_bytes()[10..]);
    bytes[6] = 0x70 | ((increased_clock_precision >> 8) as u8 & 0x0f);
    bytes[7] = increased_clock_precision as u8;
    random(&mut bytes[8..])?;
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    Ok(Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuidv7_uses_postgres_18_timestamp_version_variant_and_entropy_layout() {
        let value = generate_uuidv7_with(
            || Ok(1_735_689_600_123_456_789),
            |bytes| {
                bytes.copy_from_slice(&[0xff; 8]);
                Ok(())
            },
        )
        .expect("generate deterministic UUIDv7");
        let bytes = value.as_bytes();
        assert_eq!(&bytes[..6], &1_735_689_600_123_u64.to_be_bytes()[2..]);
        assert_eq!(bytes[6] >> 4, 7);
        assert_eq!(bytes[6] & 0x0f, 0x07);
        assert_eq!(bytes[7], 0x4f);
        assert_eq!(bytes[8], 0xbf);
        assert_eq!(&bytes[9..], &[0xff; 7]);
    }

    #[test]
    fn uuidv7_generation_propagates_clock_and_randomness_errors() {
        let clock_error = generate_uuidv7_with(
            || Err(SkeinError::Execution("clock unavailable".to_string())),
            |_| Ok(()),
        )
        .expect_err("clock failure must be returned");
        assert_eq!(
            clock_error.to_string(),
            "execution error: clock unavailable"
        );

        let random_error = generate_uuidv7_with(
            || Ok(1),
            |_| Err(SkeinError::Execution("entropy unavailable".to_string())),
        )
        .expect_err("randomness failure must be returned");
        assert_eq!(
            random_error.to_string(),
            "execution error: entropy unavailable"
        );
    }
}
