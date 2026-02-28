use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::chrome::copy_db_to_temp;
use super::{chrome_time_to_datetime, detect_chromium_browser, BrowserType, DownloadEntry};

fn download_state_name(state: i32) -> &'static str {
    match state {
        0 => "In Progress",
        1 => "Complete",
        2 => "Cancelled",
        3 => "Interrupted",
        _ => "Unknown",
    }
}

fn danger_type_name(danger: i32) -> &'static str {
    match danger {
        0 => "Not Dangerous",
        1 => "Dangerous File",
        2 => "Dangerous URL",
        3 => "Dangerous Content",
        4 => "Maybe Dangerous Content",
        5 => "Uncommon Content",
        6 => "User Validated",
        7 => "Dangerous Host",
        8 => "Potentially Unwanted",
        9 => "Allowlisted By Policy",
        _ => "Unknown",
    }
}

/// Extract downloads from a Chrome/Chromium `History` SQLite file.
pub fn extract(
    db_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<DownloadEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&db_str));

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "History")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    // Check if downloads table exists
    let table_exists: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='downloads'")?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    // Check if downloads_url_chains table exists
    let chains_exist: bool = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='downloads_url_chains'",
        )?
        .exists([])?;

    let query = if chains_exist {
        "SELECT d.id, d.current_path, d.target_path, \
                d.start_time, d.end_time, d.received_bytes, d.total_bytes, \
                d.state, d.danger_type, d.opened, \
                d.referrer, d.tab_url, d.mime_type, d.original_mime_type, \
                duc.url AS chain_url \
         FROM downloads d \
         LEFT JOIN downloads_url_chains duc ON d.id = duc.id AND duc.chain_index = 0 \
         ORDER BY d.start_time ASC"
    } else {
        "SELECT d.id, d.current_path, d.target_path, \
                d.start_time, d.end_time, d.received_bytes, d.total_bytes, \
                d.state, d.danger_type, d.opened, \
                d.referrer, d.tab_url, d.mime_type, d.original_mime_type, \
                NULL AS chain_url \
         FROM downloads d \
         ORDER BY d.start_time ASC"
    };

    let mut stmt = conn.prepare(query)?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i32>(7)?,
            row.get::<_, i32>(8)?,
            row.get::<_, i32>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
            row.get::<_, Option<String>>(12)?,
            row.get::<_, Option<String>>(13)?,
            row.get::<_, Option<String>>(14)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (
            id,
            current_path,
            target_path,
            start_time_raw,
            end_time_raw,
            received_bytes,
            total_bytes,
            state,
            danger_type,
            opened,
            referrer,
            tab_url,
            mime_type,
            original_mime_type,
            chain_url,
        ) = row?;

        let start_time = match chrome_time_to_datetime(start_time_raw) {
            Some(dt) => dt,
            None => continue,
        };
        let end_time = end_time_raw.and_then(|t| if t == 0 { None } else { Some(t) }).and_then(chrome_time_to_datetime);

        // Use chain_url (actual download URL) if available, fall back to tab_url
        let url = chain_url
            .or_else(|| tab_url.clone())
            .unwrap_or_default();
        if url.is_empty() {
            continue;
        }

        entries.push(DownloadEntry {
            url,
            target_path: target_path.unwrap_or_default(),
            current_path: current_path.unwrap_or_default(),
            start_time,
            end_time,
            received_bytes,
            total_bytes,
            state: download_state_name(state).to_string(),
            danger_type: danger_type_name(danger_type).to_string(),
            mime_type: mime_type.or(original_mime_type).unwrap_or_default(),
            referrer: referrer.unwrap_or_default(),
            tab_url: tab_url.unwrap_or_default(),
            opened: opened != 0,
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            record_id: id,
        });
    }

    Ok(entries)
}
