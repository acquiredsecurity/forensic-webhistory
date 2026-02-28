use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use tempfile::TempDir;

use super::{safari_time_to_datetime, HistoryEntry};

/// Extract browsing history from Safari's History.db SQLite file.
///
/// Opens the database read-only directly. Falls back to copying to a temp dir
/// if the direct open fails (e.g., locked by a running browser).
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<HistoryEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    // Try opening read-only directly first (avoids needing copy permissions)
    let (conn, _tmp_dir) = match Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => (c, None),
        Err(_) => {
            // Fallback: copy to temp (handles locked DBs on live systems)
            let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
            let tmp_db = tmp_dir.path().join("History.db");
            std::fs::copy(db_path, &tmp_db)
                .with_context(|| format!("Failed to copy Safari database: {}", db_str))?;
            for ext in &["-wal", "-shm"] {
                let aux_name = format!("History.db{ext}");
                let aux = db_path.parent().unwrap_or(Path::new(".")).join(&aux_name);
                if aux.exists() {
                    let _ = std::fs::copy(&aux, tmp_dir.path().join(&aux_name));
                }
            }
            let c = Connection::open(&tmp_db)
                .with_context(|| format!("Failed to open Safari database: {}", db_str))?;
            (c, Some(tmp_dir))
        }
    };

    let mut stmt = conn.prepare(
        "SELECT hi.url, hv.title, hv.visit_time, hi.visit_count, hv.id \
         FROM history_items hi \
         JOIN history_visits hv ON hi.id = hv.history_item \
         ORDER BY hv.visit_time ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, f64>(2)?,
            row.get::<_, i32>(3)?,
            row.get::<_, i64>(4)?,
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
            visit_type: String::new(),
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
