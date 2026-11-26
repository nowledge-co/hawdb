// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Separate processes change implementations while retaining the same database.

#[cfg(feature = "host-zstd")]
mod host;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let [_, mode, path] = args.as_slice() else {
        return Err("usage: persistence create|host|verify PATH".into());
    };
    #[cfg(feature = "host-zstd")]
    let backend = std::sync::Arc::new(host::HostBackend::default());
    if mode == "host" {
        #[cfg(feature = "host-zstd")]
        hawdb::compression::install_compression_backend(backend.clone())?;
        #[cfg(not(feature = "host-zstd"))]
        return Err("host mode requires host-zstd".into());
    }
    #[cfg(feature = "binding-zstd")]
    hawdb_binding_zstd::install();
    let value = "cross-backend checkpoint payload ".repeat(10_000);
    let mut db = hawdb::Database::open(path)?;
    if mode == "create" {
        db.query_sql("CREATE TABLE documents (id BIGINT PRIMARY KEY, body TEXT)")?;
        db.query_sql_with_params(
            "INSERT INTO documents VALUES (1, $1)",
            &[hawdb::Value::String(value.clone())],
        )?;
        db.checkpoint()?;
    }
    let rows = db.query_sql("SELECT body FROM documents WHERE id = 1")?;
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(
        rows.rows[0].get("body"),
        Some(&hawdb::Value::String(value.clone()))
    );
    if mode == "host" || mode == "binding" {
        db.query_sql_with_params(
            "INSERT INTO documents VALUES (2, $1)",
            &[hawdb::Value::String(value.clone())],
        )?;
        db.checkpoint()?;
        #[cfg(feature = "host-zstd")]
        {
            use std::sync::atomic::Ordering;
            assert!(backend.encoders.load(Ordering::SeqCst) > 0);
            assert!(backend.decoders.load(Ordering::SeqCst) > 0);
        }
    } else if mode == "verify" {
        let rows = db.query_sql("SELECT body FROM documents WHERE id = 2")?;
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("body"), Some(&hawdb::Value::String(value)));
    } else if mode != "create" {
        return Err("unknown mode".into());
    }
    Ok(())
}
