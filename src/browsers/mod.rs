pub mod chrome;
pub mod firefox;
pub mod safari;
pub mod webcache;

use chrono::{DateTime, Utc};

/// A single browser history entry, matching NirSoft BrowsingHistoryView CSV format.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub url: String,
    pub title: String,
    pub visit_time: DateTime<Utc>,
    pub visit_count: u32,
    pub visited_from: String,
    pub visit_type: String,
    pub visit_duration: String,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub url_length: usize,
    pub typed_count: u32,
    pub history_file: String,
    pub record_id: i64,
}

/// Detected browser artifact in a triage directory.
#[derive(Debug, Clone)]
pub struct BrowserArtifact {
    pub browser: BrowserType,
    pub db_path: String,
    pub profile_name: String,
    pub username: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserType {
    Chrome,
    EdgeChromium,
    Brave,
    Opera,
    Vivaldi,
    Chromium,
    Arc,
    Firefox,
    Safari,
    InternetExplorer,
}

impl BrowserType {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Chrome => "Chrome",
            Self::EdgeChromium => "Edge Chromium",
            Self::Brave => "Brave",
            Self::Opera => "Opera",
            Self::Vivaldi => "Vivaldi",
            Self::Chromium => "Chromium",
            Self::Arc => "Arc",
            Self::Firefox => "Firefox",
            Self::Safari => "Safari",
            Self::InternetExplorer => "Internet Explorer 10/11 / Edge",
        }
    }
}

/// Detect the type of web activity from URL, visit type, and title.
fn detect_activity<'a>(url: &str, visit_type: &str, title: &str) -> &'a str {
    let url_lower = url.to_lowercase();
    let title_lower = title.to_lowercase();
    let vtype_lower = visit_type.to_lowercase();

    // Download detection
    if vtype_lower == "download" || vtype_lower.contains("download") {
        return "File Download";
    }

    // Search detection
    if url_lower.contains("search?")
        || url_lower.contains("&q=")
        || url_lower.contains("?q=")
        || url_lower.contains("?query=")
        || url_lower.contains("&query=")
        || url_lower.contains("/search")
        || title_lower.contains(" - google search")
        || title_lower.contains(" - bing")
        || title_lower.contains(" - search")
    {
        return "Web Search";
    }

    // Typed URL
    if vtype_lower == "typed" {
        return "Typed URL";
    }

    "Web Visit"
}

/// Produce a natural-language description of a browser history entry for semantic indexing.
pub fn linearize_entry(entry: &HistoryEntry) -> String {
    let mut parts = Vec::new();

    // Timestamp
    parts.push(format!("[{}]", entry.visit_time.format("%Y-%m-%d %H:%M:%S")));

    // Activity type
    parts.push(
        detect_activity(&entry.url, &entry.visit_type, &entry.title).to_string(),
    );

    // Browser
    parts.push(format!("in {}", entry.web_browser));

    // Title (truncated)
    if !entry.title.is_empty() {
        let title = if entry.title.len() > 150 {
            format!("{}...", &entry.title[..150])
        } else {
            entry.title.clone()
        };
        parts.push(format!("- \"{}\"", title));
    }

    // URL (truncated)
    let url_display = if entry.url.len() > 200 {
        format!("{}...", &entry.url[..200])
    } else {
        entry.url.clone()
    };
    parts.push(format!("({})", url_display));

    // Visit type
    if !entry.visit_type.is_empty() {
        parts.push(format!("| Type: {}", entry.visit_type));
    }

    // User / Profile
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    if !entry.browser_profile.is_empty() {
        parts.push(format!("| Profile: {}", entry.browser_profile));
    }

    parts.join(" ")
}
