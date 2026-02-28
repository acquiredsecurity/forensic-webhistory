use anyhow::{Context, Result};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;

use super::firefox::copy_db_to_temp;
use super::{prtime_to_datetime, BookmarkEntry};

/// Extract bookmarks from a Firefox `places.sqlite` file.
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<BookmarkEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    let (_tmp_dir, tmp_db) = copy_db_to_temp(db_path, "places.sqlite")?;

    let conn = Connection::open(&tmp_db)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    // Check if moz_bookmarks table exists
    let table_exists: bool = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='moz_bookmarks'")?
        .exists([])?;
    if !table_exists {
        return Ok(Vec::new());
    }

    // Build folder lookup: id -> (title, parent_id) for type=2 (folders)
    let mut folder_stmt = conn.prepare(
        "SELECT id, title, parent FROM moz_bookmarks WHERE type = 2",
    )?;
    let folder_rows = folder_stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;

    let mut folders: HashMap<i64, (String, i64)> = HashMap::new();
    for row in folder_rows {
        let (id, title, parent) = row?;
        folders.insert(id, (title.unwrap_or_default(), parent));
    }

    // Extract bookmarks (type=1 = bookmark, not folder/separator)
    let mut stmt = conn.prepare(
        "SELECT b.id, b.title, b.dateAdded, b.lastModified, b.parent, p.url \
         FROM moz_bookmarks b \
         LEFT JOIN moz_places p ON b.fk = p.id \
         WHERE b.type = 1 \
           AND p.url IS NOT NULL \
           AND p.url NOT LIKE 'place:%' \
         ORDER BY b.dateAdded ASC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (id, title, date_added, last_modified, parent_id, url) = row?;

        if url.is_empty() {
            continue;
        }

        let folder_path = build_folder_path(parent_id, &folders);

        entries.push(BookmarkEntry {
            url,
            title: title.unwrap_or_default(),
            date_added: date_added.and_then(prtime_to_datetime),
            date_last_used: last_modified.and_then(prtime_to_datetime),
            folder_path,
            web_browser: "Firefox".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: db_str.clone(),
            record_id: id,
        });
    }

    Ok(entries)
}

/// Walk parent chain to build "Bookmarks Toolbar > Folder > Subfolder" path.
fn build_folder_path(parent_id: i64, folders: &HashMap<i64, (String, i64)>) -> String {
    let mut parts = Vec::new();
    let mut current = parent_id;
    let mut depth = 0;

    while let Some((title, parent)) = folders.get(&current) {
        if !title.is_empty() {
            parts.push(title.clone());
        }
        if *parent == current || *parent == 0 {
            break;
        }
        current = *parent;
        depth += 1;
        if depth > 20 {
            break; // safety
        }
    }

    parts.reverse();
    parts.join(" > ")
}
