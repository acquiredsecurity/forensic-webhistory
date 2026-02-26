//! SQLite deleted record carver for browser history databases.
//!
//! Recovers deleted browsing history entries by:
//! 1. Scanning SQLite freelist pages for residual URL data
//! 2. Parsing WAL (Write-Ahead Log) files for uncommitted/deleted entries
//! 3. Raw byte scanning for URL patterns in unallocated space
//!
//! Browser databases frequently contain deleted records because SQLite reuses
//! freed pages lazily — the data persists until overwritten.

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use log::{debug, info, warn};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// A recovered (carved) browsing history entry.
#[derive(Debug, Clone)]
pub struct CarvedEntry {
    pub url: String,
    pub title: String,
    pub visit_time: Option<DateTime<Utc>>,
    pub browser_hint: String,
    pub source: CarveSource,
    pub source_file: String,
}

/// Where the carved data was recovered from.
#[derive(Debug, Clone, PartialEq)]
pub enum CarveSource {
    /// SQLite freelist page
    FreelistPage,
    /// WAL (Write-Ahead Log) file
    WalFile,
    /// Raw byte scan of unallocated space
    RawScan,
}

impl std::fmt::Display for CarveSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CarveSource::FreelistPage => write!(f, "Freelist Page"),
            CarveSource::WalFile => write!(f, "WAL File"),
            CarveSource::RawScan => write!(f, "Raw Scan"),
        }
    }
}

/// Main entry point: carve deleted entries from a browser database file.
pub fn carve(db_path: &Path) -> Result<Vec<CarvedEntry>> {
    let db_str = db_path.to_string_lossy().to_string();
    info!("Carving deleted entries from: {}", db_str);

    let mut entries = Vec::new();
    let mut seen_urls = HashSet::new();

    // 1. Scan freelist pages in the main database
    match carve_freelist_pages(db_path) {
        Ok(carved) => {
            info!("  Freelist carving: {} candidate entries", carved.len());
            for e in carved {
                if seen_urls.insert(e.url.clone()) {
                    entries.push(e);
                }
            }
        }
        Err(e) => warn!("  Freelist carving failed: {}", e),
    }

    // 2. Parse WAL file if present
    let wal_path = db_path.with_extension(
        db_path
            .extension()
            .map(|ext| format!("{}-wal", ext.to_string_lossy()))
            .unwrap_or_else(|| "wal".to_string()),
    );
    // Also try the typical format: "History-wal", "places.sqlite-wal"
    let wal_candidates = vec![
        wal_path,
        db_path.parent().unwrap_or(Path::new(".")).join(format!(
            "{}-wal",
            db_path.file_name().unwrap_or_default().to_string_lossy()
        )),
    ];

    for wal in &wal_candidates {
        if wal.exists() {
            match carve_wal_file(wal, &db_str) {
                Ok(carved) => {
                    info!(
                        "  WAL carving ({}): {} candidate entries",
                        wal.display(),
                        carved.len()
                    );
                    for e in carved {
                        if seen_urls.insert(e.url.clone()) {
                            entries.push(e);
                        }
                    }
                }
                Err(e) => warn!("  WAL carving failed for {}: {}", wal.display(), e),
            }
            break;
        }
    }

    // 3. Raw byte scan of the entire database file
    match carve_raw_urls(db_path) {
        Ok(carved) => {
            info!("  Raw URL scan: {} candidate entries", carved.len());
            for e in carved {
                if seen_urls.insert(e.url.clone()) {
                    entries.push(e);
                }
            }
        }
        Err(e) => warn!("  Raw URL scan failed: {}", e),
    }

    info!(
        "  Total carved: {} unique deleted entries from {}",
        entries.len(),
        db_str
    );
    Ok(entries)
}

/// Read the SQLite header to get page size and freelist info.
struct SqliteHeader {
    page_size: u32,
    freelist_trunk_page: u32,
    freelist_page_count: u32,
    total_pages: u32,
}

fn read_sqlite_header(data: &[u8]) -> Result<SqliteHeader> {
    if data.len() < 100 {
        anyhow::bail!("File too small for SQLite header");
    }

    // Verify SQLite magic
    if &data[0..16] != b"SQLite format 3\0" {
        anyhow::bail!("Not a SQLite database");
    }

    let page_size_raw = u16::from_be_bytes([data[16], data[17]]) as u32;
    let page_size = if page_size_raw == 1 {
        65536
    } else {
        page_size_raw
    };

    let freelist_trunk_page = u32::from_be_bytes([data[32], data[33], data[34], data[35]]);
    let freelist_page_count = u32::from_be_bytes([data[36], data[37], data[38], data[39]]);

    let db_size_pages = u32::from_be_bytes([data[28], data[29], data[30], data[31]]);
    let total_pages = if db_size_pages == 0 {
        (data.len() as u32) / page_size
    } else {
        db_size_pages
    };

    Ok(SqliteHeader {
        page_size,
        freelist_trunk_page,
        freelist_page_count,
        total_pages,
    })
}

/// Carve URL-like strings from SQLite freelist pages.
fn carve_freelist_pages(db_path: &Path) -> Result<Vec<CarvedEntry>> {
    let data = fs::read(db_path).context("Failed to read database file")?;
    let header = read_sqlite_header(&data)?;
    let db_str = db_path.to_string_lossy().to_string();

    debug!(
        "SQLite: page_size={}, freelist_trunk={}, freelist_count={}, total_pages={}",
        header.page_size,
        header.freelist_trunk_page,
        header.freelist_page_count,
        header.total_pages
    );

    if header.freelist_trunk_page == 0 || header.freelist_page_count == 0 {
        debug!("No freelist pages found");
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let mut visited_pages = HashSet::new();
    let mut trunk_page = header.freelist_trunk_page;

    // Walk the freelist trunk chain
    while trunk_page > 0 && trunk_page <= header.total_pages {
        if !visited_pages.insert(trunk_page) {
            break; // Cycle detection
        }

        let offset = ((trunk_page - 1) as usize) * (header.page_size as usize);
        if offset + (header.page_size as usize) > data.len() {
            break;
        }

        let page = &data[offset..offset + header.page_size as usize];

        // First 4 bytes: next trunk page pointer
        let next_trunk = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
        // Next 4 bytes: count of leaf page pointers on this trunk page
        let leaf_count = u32::from_be_bytes([page[4], page[5], page[6], page[7]]);

        // Scan the trunk page itself for URL data
        entries.extend(extract_urls_from_page(
            page,
            &db_str,
            CarveSource::FreelistPage,
        ));

        // Scan each leaf page
        for i in 0..leaf_count.min(((header.page_size - 8) / 4) as u32) {
            let ptr_offset = 8 + (i as usize) * 4;
            if ptr_offset + 4 > page.len() {
                break;
            }
            let leaf_page = u32::from_be_bytes([
                page[ptr_offset],
                page[ptr_offset + 1],
                page[ptr_offset + 2],
                page[ptr_offset + 3],
            ]);

            if leaf_page > 0 && leaf_page <= header.total_pages {
                let leaf_offset = ((leaf_page - 1) as usize) * (header.page_size as usize);
                if leaf_offset + (header.page_size as usize) <= data.len() {
                    let leaf_data = &data[leaf_offset..leaf_offset + header.page_size as usize];
                    entries.extend(extract_urls_from_page(
                        leaf_data,
                        &db_str,
                        CarveSource::FreelistPage,
                    ));
                }
            }
        }

        trunk_page = next_trunk;
    }

    Ok(entries)
}

/// Carve URL data from a WAL (Write-Ahead Log) file.
fn carve_wal_file(wal_path: &Path, source_db: &str) -> Result<Vec<CarvedEntry>> {
    let data = fs::read(wal_path).context("Failed to read WAL file")?;

    if data.len() < 32 {
        anyhow::bail!("WAL file too small");
    }

    // WAL header: magic number check
    let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if magic != 0x377f0682 && magic != 0x377f0683 {
        anyhow::bail!("Invalid WAL magic number: 0x{:08x}", magic);
    }

    let page_size = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
    if page_size == 0 || page_size > 65536 {
        anyhow::bail!("Invalid WAL page size: {}", page_size);
    }

    let mut entries = Vec::new();
    let frame_header_size = 24;
    let wal_header_size = 32;

    // Walk WAL frames: each frame = 24-byte header + page_size bytes
    let mut offset = wal_header_size;
    while offset + frame_header_size + page_size <= data.len() {
        let page_data = &data[offset + frame_header_size..offset + frame_header_size + page_size];
        entries.extend(extract_urls_from_page(
            page_data,
            source_db,
            CarveSource::WalFile,
        ));
        offset += frame_header_size + page_size;
    }

    Ok(entries)
}

/// Scan the raw database file for URL patterns in potentially unallocated space.
fn carve_raw_urls(db_path: &Path) -> Result<Vec<CarvedEntry>> {
    let data = fs::read(db_path).context("Failed to read database file")?;
    let db_str = db_path.to_string_lossy().to_string();
    Ok(extract_urls_from_page(&data, &db_str, CarveSource::RawScan))
}

/// Extract URL strings from a page/buffer of bytes.
/// Looks for common URL prefixes and extracts the full string.
fn extract_urls_from_page(data: &[u8], source_file: &str, source: CarveSource) -> Vec<CarvedEntry> {
    let mut entries = Vec::new();
    let prefixes: &[&[u8]] = &[b"https://", b"http://", b"ftp://", b"file:///"];

    let len = data.len();
    let mut i = 0;

    while i < len.saturating_sub(8) {
        let mut found_prefix = false;

        for prefix in prefixes {
            if i + prefix.len() <= len && &data[i..i + prefix.len()] == *prefix {
                found_prefix = true;
                break;
            }
        }

        if !found_prefix {
            i += 1;
            continue;
        }

        // Verify the byte before the URL start is a non-URL character (boundary check).
        // This prevents matching partial URLs embedded inside other strings.
        if i > 0 {
            let prev = data[i - 1];
            // If previous byte is a normal URL character, skip — we're mid-string
            if (0x21..0x7F).contains(&prev)
                && prev != b'"'
                && prev != b'\''
                && prev != b'<'
                && prev != b'>'
                && prev != b'('
                && prev != b')'
                && prev != b','
            {
                i += 1;
                continue;
            }
        }

        // Found a URL prefix — extract the full URL string
        let start = i;
        let mut end = i;
        while end < len {
            let b = data[end];
            // URL characters: printable ASCII except whitespace and common delimiters
            if !(0x21..=0x7E).contains(&b) || b == b'"' || b == b'\'' || b == b'<' || b == b'>' {
                break;
            }
            end += 1;
        }

        // Trim trailing non-URL garbage (dots, commas, parentheses at the end)
        while end > start {
            let last = data[end - 1];
            if last == b'.' || last == b',' || last == b')' || last == b';' {
                end -= 1;
            } else {
                break;
            }
        }

        let url_bytes = &data[start..end];
        if let Ok(url) = std::str::from_utf8(url_bytes) {
            let url = url.to_string();

            // Filter: must be at least 12 chars and look like a real URL
            if url.len() >= 12 && is_plausible_url(&url) {
                // Try to find a title nearby — but only non-URL text
                let title = find_nearby_title(data, start, end);

                // Try to find a timestamp nearby (only for structured sources, not raw scan)
                let visit_time = if source != CarveSource::RawScan {
                    find_nearby_timestamp(data, start, end)
                } else {
                    None
                };

                entries.push(CarvedEntry {
                    url,
                    title: title.unwrap_or_default(),
                    visit_time,
                    browser_hint: guess_browser_from_url(source_file),
                    source: source.clone(),
                    source_file: source_file.to_string(),
                });
            }
        }

        i = end;
    }

    entries
}

/// Check if a URL looks plausible (not just a fragment or garbage).
fn is_plausible_url(url: &str) -> bool {
    // Must have a domain-like component after the scheme
    if let Some(rest) = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .or_else(|| url.strip_prefix("ftp://"))
    {
        // Must have at least one dot in the domain
        let domain_end = rest.find('/').unwrap_or(rest.len());
        let domain = &rest[..domain_end];
        domain.contains('.') && domain.len() >= 4
    } else if url.starts_with("file:///") {
        url.len() > 10
    } else {
        false
    }
}

/// Try to find a page title near a URL in the binary data.
/// Only returns text that looks like a real title (not another URL or path).
fn find_nearby_title(data: &[u8], url_start: usize, _url_end: usize) -> Option<String> {
    // Look backwards from URL start for a text string
    let search_range = 200.min(url_start);
    if search_range < 5 {
        return None;
    }

    let region = &data[url_start - search_range..url_start];

    // Find the last printable UTF-8 string in the region
    let mut i = region.len();

    while i > 0 {
        i -= 1;
        if region[i] < 0x20 || region[i] == 0x7f {
            continue;
        }

        let text_end = i + 1;
        while i > 0 && region[i - 1] >= 0x20 && region[i - 1] < 0x7f {
            i -= 1;
        }

        let text = &region[i..text_end];
        if let Ok(s) = std::str::from_utf8(text) {
            let s = s.trim();
            if is_plausible_title(s) {
                return Some(s.to_string());
            }
        }

        if i == 0 {
            break;
        }
    }

    None
}

/// Check if text looks like a plausible page title rather than a URL fragment.
fn is_plausible_title(s: &str) -> bool {
    if s.len() < 4 || s.len() > 500 {
        return false;
    }
    // Reject URL-like strings
    if s.starts_with("http")
        || s.starts_with("ftp:")
        || s.starts_with("file:")
        || s.starts_with("://")
        || s.starts_with("ttp://")
    {
        return false;
    }
    // Reject filesystem paths
    if s.starts_with('/') || s.starts_with('\\') || (s.len() > 2 && s.as_bytes()[1] == b':') {
        return false;
    }
    // Reject strings that are mostly non-alphabetic (likely binary garbage)
    let alpha_count = s
        .chars()
        .filter(|c| c.is_alphabetic() || c.is_whitespace())
        .count();
    let ratio = alpha_count as f64 / s.len() as f64;
    if ratio < 0.5 {
        return false;
    }
    // Must contain at least one space or be a recognizable word
    s.contains(' ') || s.len() >= 4
}

/// Try to find and decode a Chrome/WebKit timestamp near the URL.
/// Chrome timestamps: microseconds since 1601-01-01 (typically 13-digit decimal or 8 bytes LE).
fn find_nearby_timestamp(data: &[u8], url_start: usize, url_end: usize) -> Option<DateTime<Utc>> {
    // Search a window around the URL for 8-byte values that look like Chrome timestamps
    let search_start = url_start.saturating_sub(64);
    let search_end = (url_end + 64).min(data.len());

    if search_end - search_start < 8 {
        return None;
    }

    let region = &data[search_start..search_end];

    for i in 0..region.len().saturating_sub(8) {
        // Try little-endian 8-byte integer (Chrome stores as LE in SQLite)
        let val = i64::from_le_bytes([
            region[i],
            region[i + 1],
            region[i + 2],
            region[i + 3],
            region[i + 4],
            region[i + 5],
            region[i + 6],
            region[i + 7],
        ]);

        // Chrome WebKit timestamps for ~2000-2030 range:
        // 2000: ~12622780800000000
        // 2030: ~13569465600000000
        if (12_000_000_000_000_000..14_000_000_000_000_000).contains(&val) {
            return chrome_time_to_datetime(val);
        }

        // Firefox PRTime (microseconds since Unix epoch):
        // 2000: ~946684800000000
        // 2030: ~1893456000000000
        if (900_000_000_000_000..2_000_000_000_000_000).contains(&val) {
            return prtime_to_datetime(val);
        }

        // Safari Core Data (seconds since 2001-01-01, stored as f64):
        // Try interpreting as f64
        let fval = f64::from_le_bytes([
            region[i],
            region[i + 1],
            region[i + 2],
            region[i + 3],
            region[i + 4],
            region[i + 5],
            region[i + 6],
            region[i + 7],
        ]);
        // Safari timestamps for ~2000-2030 range:
        // 2010: ~283996800.0
        // 2030: ~915148800.0
        if (100_000_000.0..1_000_000_000.0).contains(&fval) && fval.is_finite() {
            return safari_time_to_datetime(fval);
        }
    }

    None
}

/// Chrome time: microseconds since 1601-01-01 UTC.
fn chrome_time_to_datetime(microseconds: i64) -> Option<DateTime<Utc>> {
    let epoch = NaiveDate::from_ymd_opt(1601, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::microseconds(microseconds);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Firefox PRTime: microseconds since 1970-01-01 UTC.
fn prtime_to_datetime(microseconds: i64) -> Option<DateTime<Utc>> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch + Duration::microseconds(microseconds);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Safari Core Data: seconds since 2001-01-01 UTC.
fn safari_time_to_datetime(seconds: f64) -> Option<DateTime<Utc>> {
    let epoch = NaiveDate::from_ymd_opt(2001, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let micros = (seconds * 1_000_000.0) as i64;
    let dt = epoch + Duration::microseconds(micros);
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Guess browser from the database filename/path.
fn guess_browser_from_url(path: &str) -> String {
    let lower = path.to_lowercase();
    if lower.contains("firefox") || lower.contains("places.sqlite") {
        "Firefox".to_string()
    } else if lower.contains("safari") || lower.contains("history.db") {
        "Safari".to_string()
    } else if lower.contains("brave") {
        "Brave".to_string()
    } else if lower.contains("edge") {
        "Edge".to_string()
    } else if lower.contains("opera") {
        "Opera".to_string()
    } else if lower.contains("vivaldi") {
        "Vivaldi".to_string()
    } else if lower.contains("arc") {
        "Arc".to_string()
    } else {
        "Chrome".to_string()
    }
}

/// Produce a natural-language description of a carved browser history entry for semantic indexing.
fn linearize_carved(entry: &CarvedEntry) -> String {
    let mut parts = Vec::new();

    // Timestamp
    if let Some(dt) = entry.visit_time {
        parts.push(format!("[{}]", dt.format("%Y-%m-%d %H:%M:%S")));
    } else {
        parts.push("[Unknown Time]".to_string());
    }

    // Activity — always "Recovered" since these are carved entries
    parts.push("Recovered Web Visit".to_string());

    // Browser
    if !entry.browser_hint.is_empty() {
        parts.push(format!("in {}", entry.browser_hint));
    }

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

    // Recovery source
    parts.push(format!("| Carved from {}", entry.source));

    parts.join(" ")
}

/// Write carved entries to CSV.
pub fn write_carved_csv(entries: &[CarvedEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = std::fs::File::create(output_path)
        .with_context(|| format!("Failed to create output: {}", output_path.display()))?;
    let mut wtr = csv::Writer::from_writer(file);

    wtr.write_record([
        "URL",
        "Title",
        "Visit Time",
        "Browser Hint",
        "Recovery Source",
        "Source File",
        "NaturalLanguage",
    ])?;

    for entry in entries {
        let nl = linearize_carved(entry);
        wtr.write_record([
            &entry.url,
            &entry.title,
            &entry
                .visit_time
                .map(|dt| dt.format("%m/%d/%Y %I:%M:%S %p").to_string())
                .unwrap_or_default(),
            &entry.browser_hint,
            &entry.source.to_string(),
            &entry.source_file,
            &nl,
        ])?;
    }

    wtr.flush()?;
    Ok(entries.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_plausible_url() {
        assert!(is_plausible_url("https://www.google.com/search?q=test"));
        assert!(is_plausible_url("http://example.com/path"));
        assert!(is_plausible_url("file:///Users/test/doc.pdf"));
        assert!(!is_plausible_url("https://x")); // too short, no dot
        assert!(!is_plausible_url("http://ab")); // too short
    }

    #[test]
    fn test_chrome_time() {
        let dt = chrome_time_to_datetime(13245010621000000);
        assert!(dt.is_some());
        assert_eq!(dt.unwrap().format("%Y-%m-%d").to_string(), "2020-09-19");
    }

    #[test]
    fn test_sqlite_magic() {
        let mut fake_header = vec![0u8; 100];
        fake_header[..16].copy_from_slice(b"SQLite format 3\0");
        // page_size = 4096
        fake_header[16] = 0x10;
        fake_header[17] = 0x00;
        let hdr = read_sqlite_header(&fake_header).unwrap();
        assert_eq!(hdr.page_size, 4096);
    }

    #[test]
    fn test_extract_urls_from_bytes() {
        let mut data = vec![0u8; 256];
        let url = b"https://www.example.com/test/page";
        data[50..50 + url.len()].copy_from_slice(url);

        let entries = extract_urls_from_page(&data, "test.db", CarveSource::RawScan);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].url, "https://www.example.com/test/page");
    }
}
