// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use hawdb::{Database, Value};
use std::{env, error::Error, mem::MaybeUninit, time::Instant};

fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the complete output on success. The
    // return code is checked before reading it, and the pointer does not escape.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: the successful call above initialized the complete value.
    let bytes = u64::try_from(unsafe { usage.assume_init() }.ru_maxrss)?;
    #[cfg(target_os = "macos")]
    let unit_bytes = 1;
    #[cfg(not(target_os = "macos"))]
    let unit_bytes = 1024;
    Ok(bytes
        .checked_mul(unit_bytes)
        .ok_or("RSS byte count overflow")?)
}

fn payload(id: usize, width: usize) -> Result<String, Box<dyn Error>> {
    let mut state = u64::try_from(id)?.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut bytes = Vec::with_capacity(width);
    for _ in 0..width {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push(b' ' + u8::try_from(state % 95)?);
    }
    Ok(String::from_utf8(bytes)?)
}

fn count(db: &mut Database, table: &str) -> Result<Value, Box<dyn Error>> {
    let rows = db.query_sql(&format!("SELECT COUNT(*) AS n FROM {table}"))?;
    Ok(rows.rows[0]["n"].clone())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let rows = args.first().ok_or("expected row count")?.parse::<usize>()?;
    let width = args
        .get(1)
        .ok_or("expected payload width")?
        .parse::<usize>()?;
    let expected = args.get(2).ok_or("expected baseline, admit or refuse")?;
    if !matches!(expected.as_str(), "baseline" | "admit" | "refuse") {
        return Err("expected baseline, admit or refuse".into());
    }
    let expected_count = Value::Int(i64::try_from(rows)?);
    let setup_started = Instant::now();
    let mut db = Database::new();
    db.query_sql(if width == 0 {
        "CREATE TABLE events (id BIGINT PRIMARY KEY)"
    } else {
        "CREATE TABLE events (id BIGINT PRIMARY KEY, body TEXT NOT NULL)"
    })?;
    let batch_rows = if width == 0 { 1_000 } else { 8 };
    for start in (0..rows).step_by(batch_rows) {
        let mut parameters = Vec::new();
        let mut values = Vec::new();
        for id in start..rows.min(start + batch_rows) {
            parameters.push(Value::Int(i64::try_from(id)?));
            if width == 0 {
                values.push(format!("(${})", parameters.len()));
            } else {
                let first = parameters.len();
                parameters.push(Value::String(payload(id, width)?));
                values.push(format!("(${first}, ${})", parameters.len()));
            }
        }
        db.query_sql_with_params(
            &format!(
                "INSERT INTO events ({}) VALUES {}",
                if width == 0 { "id" } else { "id, body" },
                values.join(", ")
            ),
            &parameters,
        )?;
    }
    assert_eq!(count(&mut db, "events")?, expected_count);
    let setup_elapsed_us = setup_started.elapsed().as_micros();
    let peak_before = peak_rss_bytes()?;
    let started = Instant::now();
    let outcome = if expected == "baseline" {
        None
    } else {
        Some(db.query_sql(if width == 0 {
            "CREATE MATERIALIZED VIEW projected AS SELECT id FROM events"
        } else {
            "CREATE MATERIALIZED VIEW projected AS SELECT body AS a, body AS b FROM events"
        }))
    };
    let elapsed_us = started.elapsed().as_micros();
    let peak_after = peak_rss_bytes()?;
    let (status, error) = match outcome {
        None => ("baseline", None),
        Some(Ok(_)) => {
            if expected == "refuse" {
                return Err("expected maintenance admission refusal".into());
            }
            assert_eq!(count(&mut db, "projected")?, expected_count);
            ("admitted", None)
        }
        Some(Err(error)) => {
            let refusal = if width == 0 {
                "materialized view maintenance exceeds the transaction row budget"
            } else {
                "materialized view maintenance exceeds the transaction row or payload budget"
            };
            if expected == "admit" || !error.to_string().contains(refusal) {
                return Err(error.into());
            }
            assert!(db.query_sql("SELECT COUNT(*) FROM projected").is_err());
            ("refused", Some(error.to_string()))
        }
    };
    assert_eq!(count(&mut db, "events")?, expected_count);
    println!(
        "{}",
        serde_json::json!({
            "source_rows": rows, "payload_bytes_each": width, "status": status,
            "setup_elapsed_us": setup_elapsed_us,
            "commit_elapsed_us": elapsed_us, "peak_rss_before_commit_bytes": peak_before,
            "peak_rss_through_commit_bytes": peak_after, "error": error,
        })
    );
    Ok(())
}
