use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use tempfile::TempDir;

use super::{chrome_time_to_datetime, detect_chromium_browser, BrowserType, HistoryEntry};

/// Core transition type (lower 8 bits of the transition field).
fn transition_name(transition: i32) -> &'static str {
    match transition & 0xFF {
        0 => "Link",
        1 => "Typed",
        2 => "Auto Bookmark",
        3 => "Auto Subframe",
        4 => "Manual Subframe",
        5 => "Generated",
        6 => "Start Page",
        7 => "Form Submit",
        8 => "Reload",
        9 => "Keyword",
        10 => "Keyword Generated",
        _ => "Other",
    }
}

/// Copy a Chrome-style database to a temp directory (Chrome locks its DB).
/// Returns (TempDir, PathBuf to copied DB).
pub fn copy_db_to_temp(db_path: &Path, filename: &str) -> Result<(TempDir, std::path::PathBuf)> {
    let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
    let tmp_db = tmp_dir.path().join(filename);
    std::fs::copy(db_path, &tmp_db)
        .with_context(|| format!("Failed to copy database: {}", db_path.display()))?;

    // Copy WAL/SHM/journal if present
    for ext in &["-wal", "-shm", "-journal"] {
        let aux = db_path.with_extension(&ext[1..]);
        if aux.exists() {
            let _ = std::fs::copy(&aux, tmp_dir.path().join(format!("{filename}{ext}")));
        }
    }

    Ok((tmp_dir, tmp_db))
}

/// Extract browsing history from a Chrome/Chromium `History` SQLite file.
pub fn extract(
    db_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<HistoryEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&db_str));

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "History")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    let mut stmt = conn.prepare(
        "SELECT u.url, u.title, v.visit_time, u.visit_count, \
                v.from_visit, v.transition, u.typed_count, u.id \
         FROM urls u \
         JOIN visits v ON u.id = v.url \
         ORDER BY v.visit_time ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i32>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i32>(5)?,
            row.get::<_, i32>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (url, title, visit_time_raw, visit_count, _from_visit, transition, typed_count, id) =
            row?;

        if url.is_empty() {
            continue;
        }

        let visit_time = match chrome_time_to_datetime(visit_time_raw) {
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
            visit_type: transition_name(transition).to_string(),
            visit_duration: String::new(),
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            typed_count: typed_count as u32,
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
    fn test_chrome_time_conversion() {
        let dt = chrome_time_to_datetime(13245010621000000);
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2020-09-19");
    }

    #[test]
    fn test_transition_names() {
        assert_eq!(transition_name(0), "Link");
        assert_eq!(transition_name(1), "Typed");
        assert_eq!(transition_name(0x00800001), "Typed");
        assert_eq!(transition_name(99), "Other");
    }

    #[test]
    fn test_detect_browser() {
        assert_eq!(
            detect_chromium_browser(
                "/Users/test/AppData/Local/Google/Chrome/User Data/Default/History"
            ),
            BrowserType::Chrome
        );
        assert_eq!(
            detect_chromium_browser(
                "/Users/test/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/History"
            ),
            BrowserType::Brave
        );
        assert_eq!(
            detect_chromium_browser(
                "/Users/test/AppData/Local/Microsoft/Edge/User Data/Default/History"
            ),
            BrowserType::EdgeChromium
        );
    }
}
