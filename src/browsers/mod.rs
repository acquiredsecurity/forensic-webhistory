pub mod chrome;
pub mod firefox;
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
    Firefox,
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
            Self::Firefox => "Firefox",
            Self::InternetExplorer => "Internet Explorer 10/11 / Edge",
        }
    }
}
