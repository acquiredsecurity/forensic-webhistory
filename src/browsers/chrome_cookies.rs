use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::chrome::copy_db_to_temp;
use super::{chrome_time_to_datetime, detect_chromium_browser, BrowserType, CookieEntry};

fn samesite_name(val: i32) -> &'static str {
    match val {
        -1 => "Unspecified",
        0 => "No Restriction",
        1 => "Lax",
        2 => "Strict",
        _ => "Unknown",
    }
}

/// Extract cookies from a Chrome/Chromium `Cookies` SQLite file.
pub fn extract(
    db_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<CookieEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&db_str));

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "Cookies")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    let table_exists: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='cookies'")?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    // Check which columns exist — Chrome schema has changed over versions
    let has_is_persistent: bool = conn
        .prepare("SELECT is_persistent FROM cookies LIMIT 0")
        .is_ok();

    let query = if has_is_persistent {
        "SELECT rowid, host_key, name, path, value, \
                creation_utc, expires_utc, last_access_utc, \
                is_secure, is_httponly, is_persistent, samesite \
         FROM cookies \
         ORDER BY creation_utc ASC"
    } else {
        "SELECT rowid, host_key, name, path, value, \
                creation_utc, expires_utc, last_access_utc, \
                is_secure, is_httponly, 1, samesite \
         FROM cookies \
         ORDER BY creation_utc ASC"
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
            row.get::<_, Option<i32>>(11)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (
            rowid,
            host_key,
            name,
            path,
            value,
            creation_utc,
            expires_utc,
            last_access_utc,
            is_secure,
            is_httponly,
            is_persistent,
            samesite,
        ) = row?;

        let creation_time = match chrome_time_to_datetime(creation_utc) {
            Some(dt) => dt,
            None => continue,
        };

        let expiry_time = expires_utc
            .and_then(|t| if t == 0 { None } else { Some(t) })
            .and_then(chrome_time_to_datetime);
        let last_access_time = last_access_utc
            .and_then(|t| if t == 0 { None } else { Some(t) })
            .and_then(chrome_time_to_datetime);

        entries.push(CookieEntry {
            host: host_key,
            name,
            path,
            value: value.unwrap_or_default(),
            creation_time,
            expiry_time,
            last_access_time,
            is_secure: is_secure != 0,
            is_httponly: is_httponly != 0,
            is_persistent: is_persistent != 0,
            same_site: samesite_name(samesite.unwrap_or(-1)).to_string(),
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            record_id: rowid,
        });
    }

    Ok(entries)
}
