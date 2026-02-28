use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::firefox::copy_db_to_temp;
use super::{prtime_to_datetime, unix_seconds_to_datetime, CookieEntry};

fn samesite_name(val: i32) -> &'static str {
    match val {
        0 => "None",
        1 => "Lax",
        2 => "Strict",
        _ => "Unknown",
    }
}

/// Extract cookies from a Firefox `cookies.sqlite` file.
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<CookieEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "cookies.sqlite")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    let table_exists: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='moz_cookies'")?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    // Check for sameSite column (added in newer Firefox)
    let has_samesite: bool = conn
        .prepare("SELECT sameSite FROM moz_cookies LIMIT 0")
        .is_ok();

    let query = if has_samesite {
        "SELECT id, host, name, path, value, \
                creationTime, expiry, lastAccessed, \
                isSecure, isHttpOnly, sameSite \
         FROM moz_cookies \
         ORDER BY creationTime ASC"
    } else {
        "SELECT id, host, name, path, value, \
                creationTime, expiry, lastAccessed, \
                isSecure, isHttpOnly, -1 \
         FROM moz_cookies \
         ORDER BY creationTime ASC"
    };

    let mut stmt = conn.prepare(query)?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, i32>(8)?,
            row.get::<_, i32>(9)?,
            row.get::<_, i32>(10)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (
            id,
            host,
            name,
            path,
            value,
            creation_time_raw,
            expiry_raw,
            last_accessed_raw,
            is_secure,
            is_httponly,
            samesite,
        ) = row?;

        let creation_time = match prtime_to_datetime(creation_time_raw) {
            Some(dt) => dt,
            None => continue,
        };

        // Firefox expiry is Unix seconds (not microseconds)
        let expiry_time = expiry_raw
            .and_then(|t| if t == 0 { None } else { Some(t) })
            .and_then(unix_seconds_to_datetime);

        let last_access_time = last_accessed_raw
            .and_then(|t| if t == 0 { None } else { Some(t) })
            .and_then(prtime_to_datetime);

        entries.push(CookieEntry {
            host,
            name,
            path,
            value: value.unwrap_or_default(),
            creation_time,
            expiry_time,
            last_access_time,
            is_secure: is_secure != 0,
            is_httponly: is_httponly != 0,
            is_persistent: true, // Firefox cookies are always persistent in cookies.sqlite
            same_site: if samesite >= 0 {
                samesite_name(samesite).to_string()
            } else {
                String::new()
            },
            web_browser: "Firefox".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            record_id: id,
        });
    }

    Ok(entries)
}
