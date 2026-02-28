use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

use super::{chrome_time_to_datetime, detect_chromium_browser, BookmarkEntry, BrowserType};

#[derive(Deserialize)]
struct BookmarksRoot {
    roots: std::collections::HashMap<String, BookmarkNode>,
}

#[derive(Deserialize)]
struct BookmarkNode {
    #[serde(default)]
    children: Vec<BookmarkNode>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    url: String,
    #[serde(default, rename = "type")]
    node_type: String,
    #[serde(default)]
    date_added: String,
    #[serde(default)]
    date_last_used: String,
    #[serde(default)]
    id: String,
}

/// Extract bookmarks from a Chrome/Chromium `Bookmarks` JSON file.
pub fn extract(
    file_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<BookmarkEntry>> {
    let file_str = file_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&file_str));

    let data = std::fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read Bookmarks file: {}", file_str))?;

    let root: BookmarksRoot = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse Bookmarks JSON: {}", file_str))?;

    let mut entries = Vec::new();

    for (root_name, node) in &root.roots {
        let folder = match root_name.as_str() {
            "bookmark_bar" => "Bookmarks Bar",
            "other" => "Other Bookmarks",
            "synced" => "Mobile Bookmarks",
            _ => root_name.as_str(),
        };
        walk_bookmarks(
            node,
            folder,
            username,
            &browser,
            &file_str,
            &mut entries,
        );
    }

    entries.sort_by_key(|e| e.date_added);
    Ok(entries)
}

fn walk_bookmarks(
    node: &BookmarkNode,
    folder_path: &str,
    username: &str,
    browser: &BrowserType,
    source_file: &str,
    entries: &mut Vec<BookmarkEntry>,
) {
    if node.node_type == "url" && !node.url.is_empty() {
        let date_added = parse_chrome_time_string(&node.date_added);
        let date_last_used = parse_chrome_time_string(&node.date_last_used);

        let record_id = node.id.parse::<i64>().unwrap_or(0);

        entries.push(BookmarkEntry {
            url: node.url.clone(),
            title: node.name.clone(),
            date_added,
            date_last_used,
            folder_path: folder_path.to_string(),
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: source_file.to_string(),
            record_id,
        });
    }

    for child in &node.children {
        let child_folder = if child.node_type == "folder" {
            if folder_path.is_empty() {
                child.name.clone()
            } else {
                format!("{} > {}", folder_path, child.name)
            }
        } else {
            folder_path.to_string()
        };
        walk_bookmarks(child, &child_folder, username, browser, source_file, entries);
    }
}

/// Parse a Chrome timestamp stored as a string (microseconds since 1601-01-01).
fn parse_chrome_time_string(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if s.is_empty() || s == "0" {
        return None;
    }
    s.parse::<i64>().ok().and_then(chrome_time_to_datetime)
}
