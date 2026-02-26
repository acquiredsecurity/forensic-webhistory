use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use rusqlite::Connection;
use std::path::Path;
use tempfile::TempDir;

use super::HistoryEntry;

/// Apple Core Data timestamp epoch: 2001-01-01 00:00:00 UTC
/// Safari stores timestamps as seconds (with fractional precision) since this epoch.
/// Conversion: unix_timestamp = core_data_timestamp + 978_307_200
fn safari_time_to_datetime(seconds: f64) -> Option<DateTime<Utc>> {
    if seconds == 0.0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(2001, 1, 1)?.and_hms_opt(0, 0, 0)?;
    // Convert fractional seconds to microseconds for precision
    let micros = (seconds * 1_000_000.0) as i64;
    let dt = epoch + Duration::microseconds(micros);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Extract browsing history from Safari's History.db SQLite file.
///
/// Safari schema:
///   history_items: id, url, domain_expansion, visit_count, daily_visit_counts, ...
///   history_visits: id, history_item (FK), visit_time (Core Data epoch), title, ...
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<HistoryEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    // Safari may lock the database — copy to temp directory first
    let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
    let tmp_db = tmp_dir.path().join("History.db");
    std::fs::copy(db_path, &tmp_db)
        .with_context(|| format!("Failed to copy Safari database: {}", db_str))?;

    // Copy WAL/SHM if present
    for ext in &["-wal", "-shm"] {
        let aux_name = format!("History.db{ext}");
        let aux = db_path.parent().unwrap_or(Path::new(".")).join(&aux_name);
        if aux.exists() {
            let _ = std::fs::copy(&aux, tmp_dir.path().join(&aux_name));
        }
    }

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open Safari database: {}", db_str))?;

    let mut stmt = conn.prepare(
        "SELECT hi.url, hv.title, hv.visit_time, hi.visit_count, hv.id \
         FROM history_items hi \
         JOIN history_visits hv ON hi.id = hv.history_item \
         ORDER BY hv.visit_time ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,          // url
            row.get::<_, Option<String>>(1)?,   // title
            row.get::<_, f64>(2)?,              // visit_time (Core Data seconds)
            row.get::<_, i32>(3)?,              // visit_count
            row.get::<_, i64>(4)?,              // visit id
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (url, title, visit_time_raw, visit_count, id) = row?;

        if url.is_empty() {
            continue;
        }

        let visit_time = match safari_time_to_datetime(visit_time_raw) {
            Some(dt) => dt,
            None => continue,
        };

        entries.push(HistoryEntry {
            url_length: url.len(),
            url,
            title: title.unwrap_or_default(),
            visit_time,
            visit_count: visit_count as u32,
            visited_from: String::new(),
            visit_type: String::new(), // Safari doesn't store transition type
            visit_duration: String::new(),
            web_browser: "Safari".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            typed_count: 0,
            history_file: db_str.clone(),
            record_id: id,
        });
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safari_time_conversion() {
        // 2024-01-15 12:00:00 UTC
        // Core Data epoch = 2001-01-01, Unix epoch = 1970-01-01
        // Offset = 978_307_200 seconds
        // 2024-01-15 12:00:00 = Unix 1705320000
        // Core Data = 1705320000 - 978307200 = 727012800
        let dt = safari_time_to_datetime(727012800.0);
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-01-15");
    }

    #[test]
    fn test_safari_time_zero() {
        assert!(safari_time_to_datetime(0.0).is_none());
    }
}
