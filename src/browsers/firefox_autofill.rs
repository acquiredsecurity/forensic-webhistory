use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::firefox::copy_db_to_temp;
use super::{prtime_to_datetime, AutofillEntry};

/// Extract form history from a Firefox `formhistory.sqlite` file.
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<AutofillEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "formhistory.sqlite")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    let table_exists: bool = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='moz_formhistory'",
        )?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        "SELECT id, fieldname, value, timesUsed, firstUsed, lastUsed \
         FROM moz_formhistory \
         ORDER BY lastUsed DESC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i32>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (id, fieldname, value, times_used, first_used, last_used) = row?;

        if fieldname.is_empty() && value.is_empty() {
            continue;
        }

        entries.push(AutofillEntry {
            field_name: fieldname,
            value,
            times_used: times_used as u32,
            first_used: first_used.and_then(prtime_to_datetime),
            last_used: last_used.and_then(prtime_to_datetime),
            web_browser: "Firefox".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            record_id: id,
        });
    }

    Ok(entries)
}
