use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::chrome::copy_db_to_temp;
use super::{chrome_time_to_datetime, detect_chromium_browser, BrowserType, KeywordSearchEntry};

/// Extract keyword search terms from a Chrome/Chromium `History` SQLite file.
pub fn extract(
    db_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<KeywordSearchEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&db_str));

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "History")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    // Check if keyword_search_terms table exists
    let table_exists: bool = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='keyword_search_terms'",
        )?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        "SELECT kst.keyword_id, kst.url_id, kst.term, kst.normalized_term, \
                u.url, u.title, u.last_visit_time \
         FROM keyword_search_terms kst \
         JOIN urls u ON kst.url_id = u.id \
         ORDER BY u.last_visit_time ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<i64>>(6)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (keyword_id, url_id, term, normalized_term, url, title, last_visit_time) = row?;

        if term.is_empty() {
            continue;
        }

        let visit_time = last_visit_time.and_then(chrome_time_to_datetime);

        entries.push(KeywordSearchEntry {
            search_term: term,
            normalized_term: normalized_term.unwrap_or_default(),
            url,
            title: title.unwrap_or_default(),
            visit_time,
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            keyword_id,
            url_id,
        });
    }

    Ok(entries)
}
