use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use rusqlite::Connection;
use std::path::Path;
use tempfile::TempDir;

use super::{BrowserType, HistoryEntry};

/// Chrome/WebKit timestamp epoch: 1601-01-01 00:00:00 UTC
/// Stored as microseconds since this epoch.
fn chrome_time_to_datetime(microseconds: i64) -> Option<DateTime<Utc>> {
    if microseconds == 0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(1601, 1, 1)?
        .and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::microseconds(microseconds);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

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

/// Detect browser type from the file path.
fn detect_browser(path: &str) -> BrowserType {
    let lower = path.to_lowercase();
    if lower.contains("brave") {
        BrowserType::Brave
    } else if lower.contains("opera") {
        BrowserType::Opera
    } else if lower.contains("vivaldi") {
        BrowserType::Vivaldi
    } else if lower.contains("edge") || lower.contains("msedge") {
        BrowserType::EdgeChromium
    } else if lower.contains("chromium") {
        BrowserType::Chromium
    } else {
        BrowserType::Chrome
    }
}

/// Extract browsing history from a Chrome/Chromium `History` SQLite file.
pub fn extract(
    db_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<HistoryEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_browser(&db_str));

    // Chrome locks its database — copy to temp directory first
    let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
    let tmp_db = tmp_dir.path().join("History");
    std::fs::copy(db_path, &tmp_db)
        .with_context(|| format!("Failed to copy database: {}", db_str))?;

    // Copy WAL/SHM/journal if present
    for ext in &["-wal", "-shm", "-journal"] {
        let aux = db_path.with_extension(&ext[1..]);
        if aux.exists() {
            let _ = std::fs::copy(&aux, tmp_dir.path().join(format!("History{ext}")));
        }
    }

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
            row.get::<_, String>(0)?,          // url
            row.get::<_, Option<String>>(1)?,   // title
            row.get::<_, i64>(2)?,              // visit_time
            row.get::<_, i32>(3)?,              // visit_count
            row.get::<_, i64>(4)?,              // from_visit
            row.get::<_, i32>(5)?,              // transition
            row.get::<_, i32>(6)?,              // typed_count
            row.get::<_, i64>(7)?,              // id
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
        // 2020-09-19 03:23:41 UTC in Chrome time
        // Chrome epoch = 1601-01-01, so 2020-09-19 = 13245010621000000 µs
        let dt = chrome_time_to_datetime(13245010621000000);
        assert!(dt.is_some());
        let dt = dt.unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2020-09-19");
    }

    #[test]
    fn test_transition_names() {
        assert_eq!(transition_name(0), "Link");
        assert_eq!(transition_name(1), "Typed");
        assert_eq!(transition_name(0x00800001), "Typed"); // with qualifier bits
        assert_eq!(transition_name(99), "Other");
    }

    #[test]
    fn test_detect_browser() {
        assert_eq!(
            detect_browser("/Users/test/AppData/Local/Google/Chrome/User Data/Default/History"),
            BrowserType::Chrome
        );
        assert_eq!(
            detect_browser(
                "/Users/test/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/History"
            ),
            BrowserType::Brave
        );
        assert_eq!(
            detect_browser("/Users/test/AppData/Local/Microsoft/Edge/User Data/Default/History"),
            BrowserType::EdgeChromium
        );
    }
}
