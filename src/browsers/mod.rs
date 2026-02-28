pub mod chrome;
pub mod chrome_autofill;
pub mod chrome_bookmarks;
pub mod chrome_cookies;
pub mod chrome_downloads;
pub mod chrome_extensions;
pub mod chrome_keywords;
pub mod chrome_logins;
pub mod firefox;
pub mod firefox_autofill;
pub mod firefox_bookmarks;
pub mod firefox_cookies;
pub mod firefox_downloads;
pub mod firefox_extensions;
pub mod firefox_logins;
pub mod safari;
pub mod webcache;

use chrono::{DateTime, Duration, NaiveDate, Utc};

// ---------------------------------------------------------------------------
// Shared timestamp conversion functions
// ---------------------------------------------------------------------------

/// Chrome/WebKit timestamp epoch: 1601-01-01 00:00:00 UTC
/// Stored as microseconds since this epoch.
pub fn chrome_time_to_datetime(microseconds: i64) -> Option<DateTime<Utc>> {
    if microseconds == 0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(1601, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::microseconds(microseconds);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Firefox stores timestamps as PRTime: microseconds since Unix epoch (1970-01-01).
pub fn prtime_to_datetime(microseconds: i64) -> Option<DateTime<Utc>> {
    if microseconds == 0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::microseconds(microseconds);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Apple Core Data timestamp epoch: 2001-01-01 00:00:00 UTC
/// Safari stores timestamps as seconds (with fractional precision) since this epoch.
pub fn safari_time_to_datetime(seconds: f64) -> Option<DateTime<Utc>> {
    if seconds == 0.0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(2001, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let micros = (seconds * 1_000_000.0) as i64;
    let dt = epoch + Duration::microseconds(micros);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Unix epoch seconds to DateTime (used by Chrome autofill).
pub fn unix_seconds_to_datetime(seconds: i64) -> Option<DateTime<Utc>> {
    if seconds == 0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::seconds(seconds);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Unix epoch milliseconds to DateTime (used by Firefox logins).
pub fn unix_millis_to_datetime(millis: i64) -> Option<DateTime<Utc>> {
    if millis == 0 {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::milliseconds(millis);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Truncate a string to max length, appending "..." if truncated.
pub fn truncate_str(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}...", &s[..max])
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Browser type and artifact type enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

    pub fn is_chromium(&self) -> bool {
        matches!(
            self,
            Self::Chrome
                | Self::EdgeChromium
                | Self::Brave
                | Self::Opera
                | Self::Vivaldi
                | Self::Chromium
                | Self::Arc
        )
    }
}

/// Type of browser artifact being extracted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactType {
    History,
    Downloads,
    KeywordSearches,
    Cookies,
    Autofill,
    Bookmarks,
    LoginData,
    Extensions,
}

impl ArtifactType {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::History => "History",
            Self::Downloads => "Downloads",
            Self::KeywordSearches => "Keyword Searches",
            Self::Cookies => "Cookies",
            Self::Autofill => "Autofill",
            Self::Bookmarks => "Bookmarks",
            Self::LoginData => "Login Data",
            Self::Extensions => "Extensions",
        }
    }

    pub fn file_suffix(&self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Downloads => "downloads",
            Self::KeywordSearches => "keyword_searches",
            Self::Cookies => "cookies",
            Self::Autofill => "autofill",
            Self::Bookmarks => "bookmarks",
            Self::LoginData => "login_data",
            Self::Extensions => "extensions",
        }
    }
}

// ---------------------------------------------------------------------------
// Core data structures
// ---------------------------------------------------------------------------

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
    pub artifact_type: ArtifactType,
    pub db_path: String,
    pub profile_name: String,
    pub username: String,
}

/// A browser download entry.
#[derive(Debug, Clone)]
pub struct DownloadEntry {
    pub url: String,
    pub target_path: String,
    pub current_path: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub received_bytes: i64,
    pub total_bytes: i64,
    pub state: String,
    pub danger_type: String,
    pub mime_type: String,
    pub referrer: String,
    pub tab_url: String,
    pub opened: bool,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub source_file: String,
    pub record_id: i64,
}

/// A keyword/omnibox search term.
#[derive(Debug, Clone)]
pub struct KeywordSearchEntry {
    pub search_term: String,
    pub normalized_term: String,
    pub url: String,
    pub title: String,
    pub visit_time: Option<DateTime<Utc>>,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub source_file: String,
    pub keyword_id: i64,
    pub url_id: i64,
}

/// A browser cookie entry.
#[derive(Debug, Clone)]
pub struct CookieEntry {
    pub host: String,
    pub name: String,
    pub path: String,
    pub value: String,
    pub creation_time: DateTime<Utc>,
    pub expiry_time: Option<DateTime<Utc>>,
    pub last_access_time: Option<DateTime<Utc>>,
    pub is_secure: bool,
    pub is_httponly: bool,
    pub is_persistent: bool,
    pub same_site: String,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub source_file: String,
    pub record_id: i64,
}

/// An autofill/form history entry.
#[derive(Debug, Clone)]
pub struct AutofillEntry {
    pub field_name: String,
    pub value: String,
    pub times_used: u32,
    pub first_used: Option<DateTime<Utc>>,
    pub last_used: Option<DateTime<Utc>>,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub source_file: String,
    pub record_id: i64,
}

/// A browser bookmark entry.
#[derive(Debug, Clone)]
pub struct BookmarkEntry {
    pub url: String,
    pub title: String,
    pub date_added: Option<DateTime<Utc>>,
    pub date_last_used: Option<DateTime<Utc>>,
    pub folder_path: String,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub source_file: String,
    pub record_id: i64,
}

/// Login/credential metadata (NO passwords extracted).
#[derive(Debug, Clone)]
pub struct LoginEntry {
    pub origin_url: String,
    pub action_url: String,
    pub username_value: String,
    pub date_created: Option<DateTime<Utc>>,
    pub date_last_used: Option<DateTime<Utc>>,
    pub date_password_modified: Option<DateTime<Utc>>,
    pub times_used: u32,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub source_file: String,
    pub record_id: i64,
}

/// A browser extension entry.
#[derive(Debug, Clone)]
pub struct ExtensionEntry {
    pub extension_id: String,
    pub name: String,
    pub version: String,
    pub description: String,
    pub enabled: bool,
    pub install_time: Option<DateTime<Utc>>,
    pub update_url: String,
    pub permissions: String,
    pub web_browser: String,
    pub user_profile: String,
    pub browser_profile: String,
    pub source_file: String,
}

// ---------------------------------------------------------------------------
// Activity detection and natural language linearizers
// ---------------------------------------------------------------------------

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

    parts.push(format!(
        "[{}]",
        entry.visit_time.format("%Y-%m-%d %H:%M:%S")
    ));
    parts.push(detect_activity(&entry.url, &entry.visit_type, &entry.title).to_string());
    parts.push(format!("in {}", entry.web_browser));

    if !entry.title.is_empty() {
        parts.push(format!("- \"{}\"", truncate_str(&entry.title, 150)));
    }

    parts.push(format!("({})", truncate_str(&entry.url, 200)));

    if !entry.visit_type.is_empty() {
        parts.push(format!("| Type: {}", entry.visit_type));
    }
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    if !entry.browser_profile.is_empty() {
        parts.push(format!("| Profile: {}", entry.browser_profile));
    }

    parts.join(" ")
}

pub fn linearize_download(entry: &DownloadEntry) -> String {
    let mut parts = Vec::new();
    parts.push(format!(
        "[{}]",
        entry.start_time.format("%Y-%m-%d %H:%M:%S")
    ));
    parts.push("File Download".to_string());
    parts.push(format!("in {}", entry.web_browser));

    let filename = entry
        .target_path
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(&entry.target_path);
    if !filename.is_empty() {
        parts.push(format!("- \"{}\"", filename));
    }
    parts.push(format!("from ({})", truncate_str(&entry.url, 200)));
    if !entry.mime_type.is_empty() {
        parts.push(format!("| MIME: {}", entry.mime_type));
    }
    parts.push(format!("| State: {}", entry.state));
    if entry.total_bytes > 0 {
        parts.push(format!("| Size: {} bytes", entry.total_bytes));
    }
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    parts.join(" ")
}

pub fn linearize_keyword_search(entry: &KeywordSearchEntry) -> String {
    let mut parts = Vec::new();
    if let Some(dt) = entry.visit_time {
        parts.push(format!("[{}]", dt.format("%Y-%m-%d %H:%M:%S")));
    } else {
        parts.push("[Unknown Time]".to_string());
    }
    parts.push("Web Search".to_string());
    parts.push(format!("in {}", entry.web_browser));
    parts.push(format!("- Query: \"{}\"", entry.search_term));
    parts.push(format!("({})", truncate_str(&entry.url, 200)));
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    parts.join(" ")
}

pub fn linearize_cookie(entry: &CookieEntry) -> String {
    let mut parts = Vec::new();
    parts.push(format!(
        "[{}]",
        entry.creation_time.format("%Y-%m-%d %H:%M:%S")
    ));
    parts.push("Cookie Set".to_string());
    parts.push(format!("in {}", entry.web_browser));
    parts.push(format!("- {} on {}{}", entry.name, entry.host, entry.path));
    if entry.is_secure {
        parts.push("| Secure".to_string());
    }
    if entry.is_httponly {
        parts.push("| HttpOnly".to_string());
    }
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    parts.join(" ")
}

pub fn linearize_autofill(entry: &AutofillEntry) -> String {
    let mut parts = Vec::new();
    if let Some(dt) = entry.last_used {
        parts.push(format!("[{}]", dt.format("%Y-%m-%d %H:%M:%S")));
    } else if let Some(dt) = entry.first_used {
        parts.push(format!("[{}]", dt.format("%Y-%m-%d %H:%M:%S")));
    } else {
        parts.push("[Unknown Time]".to_string());
    }
    parts.push("Form Autofill".to_string());
    parts.push(format!("in {}", entry.web_browser));
    parts.push(format!(
        "- Field: \"{}\" = \"{}\"",
        entry.field_name, entry.value
    ));
    parts.push(format!("| Used {} times", entry.times_used));
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    parts.join(" ")
}

pub fn linearize_bookmark(entry: &BookmarkEntry) -> String {
    let mut parts = Vec::new();
    if let Some(dt) = entry.date_added {
        parts.push(format!("[{}]", dt.format("%Y-%m-%d %H:%M:%S")));
    } else {
        parts.push("[Unknown Time]".to_string());
    }
    parts.push("Bookmark Added".to_string());
    parts.push(format!("in {}", entry.web_browser));
    if !entry.title.is_empty() {
        parts.push(format!("- \"{}\"", truncate_str(&entry.title, 150)));
    }
    parts.push(format!("({})", truncate_str(&entry.url, 200)));
    if !entry.folder_path.is_empty() {
        parts.push(format!("| Folder: {}", entry.folder_path));
    }
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    parts.join(" ")
}

pub fn linearize_login(entry: &LoginEntry) -> String {
    let mut parts = Vec::new();
    if let Some(dt) = entry.date_created {
        parts.push(format!("[{}]", dt.format("%Y-%m-%d %H:%M:%S")));
    } else {
        parts.push("[Unknown Time]".to_string());
    }
    parts.push("Login Credential Stored".to_string());
    parts.push(format!("in {}", entry.web_browser));
    parts.push(format!("- Username: \"{}\"", entry.username_value));
    parts.push(format!("on {}", entry.origin_url));
    parts.push(format!("| Used {} times", entry.times_used));
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    parts.join(" ")
}

pub fn linearize_extension(entry: &ExtensionEntry) -> String {
    let mut parts = Vec::new();
    if let Some(dt) = entry.install_time {
        parts.push(format!("[{}]", dt.format("%Y-%m-%d %H:%M:%S")));
    } else {
        parts.push("[Unknown Time]".to_string());
    }
    parts.push("Extension Installed".to_string());
    parts.push(format!("in {}", entry.web_browser));
    parts.push(format!("- \"{}\" v{}", entry.name, entry.version));
    parts.push(format!("({})", entry.extension_id));
    parts.push(format!("| Enabled: {}", entry.enabled));
    if !entry.user_profile.is_empty() {
        parts.push(format!("| User: {}", entry.user_profile));
    }
    parts.join(" ")
}

/// Detect browser type from the file path (shared by all Chrome-based extractors).
pub fn detect_chromium_browser(path: &str) -> BrowserType {
    let lower = path.to_lowercase();
    if lower.contains("brave") {
        BrowserType::Brave
    } else if lower.contains("opera") {
        BrowserType::Opera
    } else if lower.contains("vivaldi") {
        BrowserType::Vivaldi
    } else if lower.contains("edge") || lower.contains("msedge") {
        BrowserType::EdgeChromium
    } else if lower.contains("/arc/") {
        BrowserType::Arc
    } else if lower.contains("chromium") {
        BrowserType::Chromium
    } else {
        BrowserType::Chrome
    }
}
