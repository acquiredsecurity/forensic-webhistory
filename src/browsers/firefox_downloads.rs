use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::firefox::copy_db_to_temp;
use super::{prtime_to_datetime, DownloadEntry};

/// Extract downloads from a Firefox `places.sqlite` file.
///
/// Modern Firefox stores download metadata in `moz_annos` with attributes
/// `downloads/destinationFileURI` and `downloads/metaData`.
/// Legacy Firefox (< 26) uses a `moz_downloads` table.
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<DownloadEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "places.sqlite")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    // Try modern approach first (moz_annos)
    let entries = extract_from_annos(&conn, username, &db_str);
    if let Ok(ref e) = entries {
        if !e.is_empty() {
            return entries;
        }
    }

    // Fallback: try legacy moz_downloads table
    extract_from_legacy(&conn, username, &db_str)
}

fn extract_from_annos(
    conn: &Connection,
    username: &str,
    db_str: &str,
) -> Result<Vec<DownloadEntry>> {
    // Check if moz_annos and moz_anno_attributes exist
    let tables_exist: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='moz_annos'")?
        .exists([])?;
    if !tables_exist {
        return Ok(Vec::new());
    }

    // Gather download destinations: place_id -> destination file URI
    let mut dest_stmt = conn.prepare(
        "SELECT a.place_id, a.content, a.dateAdded \
         FROM moz_annos a \
         JOIN moz_anno_attributes aa ON a.anno_attribute_id = aa.id \
         WHERE aa.name = 'downloads/destinationFileURI'",
    )?;

    let dest_rows = dest_stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<i64>>(2)?,
        ))
    })?;

    let mut destinations = std::collections::HashMap::new();
    for row in dest_rows {
        let (place_id, content, date_added) = row?;
        destinations.insert(place_id, (content.unwrap_or_default(), date_added));
    }

    if destinations.is_empty() {
        return Ok(Vec::new());
    }

    // Gather metadata: place_id -> JSON blob with state, endTime, fileSize
    let mut meta_stmt = conn.prepare(
        "SELECT a.place_id, a.content \
         FROM moz_annos a \
         JOIN moz_anno_attributes aa ON a.anno_attribute_id = aa.id \
         WHERE aa.name = 'downloads/metaData'",
    )?;

    let meta_rows = meta_stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
        ))
    })?;

    let mut metadata = std::collections::HashMap::new();
    for row in meta_rows {
        let (place_id, content) = row?;
        if let Some(json_str) = content {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&json_str) {
                metadata.insert(place_id, val);
            }
        }
    }

    // Get URL and title from moz_places for each download
    let place_ids: Vec<i64> = destinations.keys().copied().collect();
    let mut entries = Vec::new();

    for place_id in place_ids {
        let mut place_stmt = conn.prepare(
            "SELECT url, title FROM moz_places WHERE id = ?1",
        )?;

        let place = place_stmt.query_row([place_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        });

        let (url, _title) = match place {
            Ok(p) => p,
            Err(_) => continue,
        };

        let (dest_uri, date_added) = destinations.get(&place_id).unwrap();

        // Parse destination URI (file:///path/to/file -> /path/to/file)
        let target_path = dest_uri
            .strip_prefix("file:///")
            .or_else(|| dest_uri.strip_prefix("file://"))
            .unwrap_or(dest_uri)
            .to_string();

        let start_time = date_added.and_then(|d| prtime_to_datetime(d));
        let start_time = match start_time {
            Some(dt) => dt,
            None => continue,
        };

        // Extract metadata if available
        let meta = metadata.get(&place_id);
        let end_time = meta
            .and_then(|m| m.get("endTime"))
            .and_then(|v| v.as_i64())
            .and_then(|t| prtime_to_datetime(t * 1000)); // endTime is in ms, convert to µs

        let total_bytes = meta
            .and_then(|m| m.get("fileSize"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let state_num = meta
            .and_then(|m| m.get("state"))
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);

        let state = match state_num {
            0 => "In Progress",
            1 => "Complete",
            2 => "Failed",
            3 => "Cancelled",
            4 => "Paused",
            5 => "Queued",
            6 => "Blocked (Parental)",
            7 => "Scanning",
            _ => "Unknown",
        };

        entries.push(DownloadEntry {
            url,
            target_path,
            current_path: String::new(),
            start_time,
            end_time,
            received_bytes: total_bytes, // Firefox doesn't track partial separately in annos
            total_bytes,
            state: state.to_string(),
            danger_type: String::new(),
            mime_type: String::new(),
            referrer: String::new(),
            tab_url: String::new(),
            opened: false,
            web_browser: "Firefox".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.to_string(),
            record_id: place_id,
        });
    }

    entries.sort_by_key(|e| e.start_time);
    Ok(entries)
}

fn extract_from_legacy(
    conn: &Connection,
    username: &str,
    db_str: &str,
) -> Result<Vec<DownloadEntry>> {
    // Check if legacy moz_downloads table exists
    let table_exists: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='moz_downloads'")?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        "SELECT id, name, source, target, startTime, endTime, \
                currBytes, maxBytes, state \
         FROM moz_downloads \
         ORDER BY startTime ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i32>(8)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (id, _name, source, target, start_time_raw, end_time_raw, curr_bytes, max_bytes, state) =
            row?;

        let url = source.unwrap_or_default();
        if url.is_empty() {
            continue;
        }

        let start_time = match start_time_raw.and_then(prtime_to_datetime) {
            Some(dt) => dt,
            None => continue,
        };
        let end_time = end_time_raw.and_then(prtime_to_datetime);

        let target_path = target.unwrap_or_default();

        let state_name = match state {
            0 => "In Progress",
            1 => "Complete",
            2 => "Failed",
            3 => "Cancelled",
            4 => "Paused",
            _ => "Unknown",
        };

        entries.push(DownloadEntry {
            url,
            target_path,
            current_path: String::new(),
            start_time,
            end_time,
            received_bytes: curr_bytes,
            total_bytes: max_bytes,
            state: state_name.to_string(),
            danger_type: String::new(),
            mime_type: String::new(),
            referrer: String::new(),
            tab_url: String::new(),
            opened: false,
            web_browser: "Firefox".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.to_string(),
            record_id: id,
        });
    }

    Ok(entries)
}
