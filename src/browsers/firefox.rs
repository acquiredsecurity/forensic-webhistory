use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use rusqlite::Connection;
use std::path::Path;
use tempfile::TempDir;

use super::HistoryEntry;

/// Firefox stores timestamps as PRTime: microseconds since Unix epoch (1970-01-01).
fn prtime_to_datetime(microseconds: i64) -> Option<DateTime<Utc>> {
    if microseconds == 0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::microseconds(microseconds);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

fn visit_type_name(visit_type: i32) -> &'static str {
    match visit_type {
        1 => "Link",
        2 => "Typed",
        3 => "Bookmark",
        4 => "Embed",
        5 => "Redirect (Permanent)",
        6 => "Redirect (Temporary)",
        7 => "Download",
        8 => "Framed Link",
        9 => "Reload",
        _ => "Other",
    }
}

/// Extract browsing history from a Firefox `places.sqlite` file.
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<HistoryEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    // Firefox locks its database — copy to temp directory first
    let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
    let tmp_db = tmp_dir.path().join("places.sqlite");
    std::fs::copy(db_path, &tmp_db)
        .with_context(|| format!("Failed to copy database: {}", db_str))?;

    // Copy WAL/SHM if present
    for ext in &["-wal", "-shm"] {
        let aux_name = format!("places.sqlite{ext}");
        let aux = db_path.parent().unwrap_or(Path::new(".")).join(&aux_name);
        if aux.exists() {
            let _ = std::fs::copy(&aux, tmp_dir.path().join(&aux_name));
        }
    }

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    let mut stmt = conn.prepare(
        "SELECT p.url, p.title, v.visit_date, p.visit_count, \
                v.from_visit, v.visit_type, p.id \
         FROM moz_places p \
         JOIN moz_historyvisits v ON p.id = v.place_id \
         ORDER BY v.visit_date ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,         // url
            row.get::<_, Option<String>>(1)?, // title
            row.get::<_, Option<i64>>(2)?,    // visit_date
            row.get::<_, i32>(3)?,            // visit_count
            row.get::<_, i64>(4)?,            // from_visit
            row.get::<_, i32>(5)?,            // visit_type
            row.get::<_, i64>(6)?,            // id
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (url, title, visit_date, visit_count, _from_visit, visit_type, id) = row?;

        if url.is_empty() {
            continue;
        }

        let visit_time = match visit_date.and_then(prtime_to_datetime) {
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
            visit_type: visit_type_name(visit_type).to_string(),
            visit_duration: String::new(),
            web_browser: "Firefox".to_string(),
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
    fn test_prtime_conversion() {
        // 2020-09-19 in microseconds since Unix epoch
        let dt = prtime_to_datetime(1600480000000000);
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2020-09-19");
    }

    #[test]
    fn test_visit_type_names() {
        assert_eq!(visit_type_name(1), "Link");
        assert_eq!(visit_type_name(2), "Typed");
        assert_eq!(visit_type_name(7), "Download");
        assert_eq!(visit_type_name(99), "Other");
    }
}
