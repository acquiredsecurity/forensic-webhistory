use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::chrome::copy_db_to_temp;
use super::{chrome_time_to_datetime, detect_chromium_browser, BrowserType, LoginEntry};

/// Extract login metadata from a Chrome/Chromium `Login Data` SQLite file.
///
/// IMPORTANT: Only extracts metadata (URLs, usernames, timestamps, usage counts).
/// Password values are NEVER extracted.
pub fn extract(
    db_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<LoginEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&db_str));

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "LoginData")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    let table_exists: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='logins'")?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    // Check which columns exist (older Chromium versions lack date_password_modified)
    let has_date_pw_modified: bool = conn
        .prepare("SELECT date_password_modified FROM logins LIMIT 0")
        .is_ok();

    let query = if has_date_pw_modified {
        "SELECT rowid, origin_url, action_url, username_value, \
                date_created, date_last_used, date_password_modified, times_used \
         FROM logins ORDER BY date_created ASC"
    } else {
        "SELECT rowid, origin_url, action_url, username_value, \
                date_created, date_last_used, NULL, times_used \
         FROM logins ORDER BY date_created ASC"
    };

    let mut stmt = conn.prepare(query)?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, i32>(7)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (
            rowid,
            origin_url,
            action_url,
            username_value,
            date_created,
            date_last_used,
            date_password_modified,
            times_used,
        ) = row?;

        if origin_url.is_empty() {
            continue;
        }

        entries.push(LoginEntry {
            origin_url,
            action_url: action_url.unwrap_or_default(),
            username_value: username_value.unwrap_or_default(),
            date_created: date_created.and_then(chrome_time_to_datetime),
            date_last_used: date_last_used.and_then(chrome_time_to_datetime),
            date_password_modified: date_password_modified.and_then(chrome_time_to_datetime),
            times_used: times_used as u32,
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            record_id: rowid,
        });
    }

    Ok(entries)
}
