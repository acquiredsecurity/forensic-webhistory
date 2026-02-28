use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::chrome::copy_db_to_temp;
use super::{detect_chromium_browser, unix_seconds_to_datetime, AutofillEntry, BrowserType};

/// Extract autofill entries from a Chrome/Chromium `Web Data` SQLite file.
///
/// Note: Chrome autofill timestamps are Unix epoch seconds (NOT Chrome epoch).
pub fn extract(
    db_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<AutofillEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&db_str));

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "WebData")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    let table_exists: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='autofill'")?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        "SELECT rowid, name, value, date_created, date_last_used, count \
         FROM autofill \
         ORDER BY date_last_used DESC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, i32>(5)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (rowid, name, value, date_created, date_last_used, count) = row?;

        if name.is_empty() && value.is_empty() {
            continue;
        }

        let first_used = date_created.and_then(unix_seconds_to_datetime);
        let last_used = date_last_used.and_then(unix_seconds_to_datetime);

        entries.push(AutofillEntry {
            field_name: name,
            value,
            times_used: count as u32,
            first_used,
            last_used,
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            record_id: rowid,
        });
    }

    Ok(entries)
}
