use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use libesedb::EseDb;
use std::collections::HashSet;
use std::path::Path;

use super::{BrowserType, HistoryEntry};

/// Parse a datetime string produced by libesedb Value::to_string().
/// The library formats FILETIME values as human-readable strings.
fn parse_ese_datetime(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    if s.is_empty() || s == "0" || s == "Not set" {
        return None;
    }

    // libesedb formats as: "Mon DD, YYYY HH:MM:SS.NNN" or similar
    // Try common patterns
    for fmt in &[
        "%b %d, %Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
    ] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(DateTime::from_naive_utc_and_offset(ndt, Utc));
        }
    }

    // Try parsing as FILETIME integer (100ns intervals since 1601-01-01)
    if let Ok(ft) = s.parse::<u64>() {
        if ft > 0 {
            let microseconds = ft / 10;
            let epoch = chrono::NaiveDate::from_ymd_opt(1601, 1, 1)?
                .and_hms_opt(0, 0, 0)?;
            let dt = epoch + chrono::Duration::microseconds(microseconds as i64);
            return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
    }

    None
}

/// Parse URL from ESE value string — handles multiple IE URL formats:
///   - "Visited: Username@url"  (History container)
///   - ":YYYYMMDDYYYYMMDD: Username@url"  (MSHist container)
///   - ":YYYYMMDDYYYYMMDD: Username@:Host: hostname"  (MSHist host entry — skip)
///   - Plain URL
fn parse_url(text: &str) -> (Option<String>, Option<String>) {
    let text = text.trim().trim_end_matches('\0');
    if text.is_empty() {
        return (None, None);
    }

    // IE History container: "Visited: Username@url"
    if let Some(rest) = text.strip_prefix("Visited:") {
        let rest = rest.trim();
        if let Some(at_pos) = rest.find('@') {
            let user = rest[..at_pos].trim().to_string();
            let url = rest[at_pos + 1..].trim().to_string();
            if url.starts_with(":Host:") || url.starts_with(":host:") {
                return (None, None);
            }
            return (Some(url), Some(user));
        }
        return (Some(rest.to_string()), None);
    }

    // MSHist container: ":20200918202009: Username@url" or ":20200918202009: Username@:Host: host"
    if text.starts_with(':') {
        // Find the second colon (end of date range)
        if let Some(second_colon) = text[1..].find(':') {
            let rest = text[second_colon + 2..].trim(); // skip ":daterange: "
            if let Some(at_pos) = rest.find('@') {
                let user = rest[..at_pos].trim().to_string();
                let url = rest[at_pos + 1..].trim().to_string();
                if url.starts_with(":Host:") || url.starts_with(":host:") {
                    return (None, None);
                }
                if url.is_empty() {
                    return (None, None);
                }
                return (Some(url), Some(user));
            }
        }
        // Unrecognized colon-prefixed entry
        return (None, None);
    }

    // Skip standalone :Host: entries
    if text.starts_with(":Host:") || text.starts_with(":host:") {
        return (None, None);
    }

    (Some(text.to_string()), None)
}

/// Extract browsing history from an IE/Edge WebCacheV01.dat ESE database.
pub fn extract(db_path: &Path, username: &str) -> Result<Vec<HistoryEntry>> {
    let db_str = db_path.to_string_lossy().to_string();

    let db = EseDb::open(db_path)
        .with_context(|| format!("Failed to open ESE database: {}", db_str))?;

    // Find history container IDs from the Containers table
    let containers = db
        .table_by_name("Containers")
        .context("Containers table not found")?;

    let mut history_container_ids = Vec::new();
    for rec_result in containers.iter_records()? {
        let rec = match rec_result {
            Ok(r) => r,
            Err(_) => continue,
        };

        let vals: Vec<String> = rec
            .iter_values()
            .ok()
            .into_iter()
            .flat_map(|iter| {
                iter.map(|v| v.map(|val| val.to_string()).unwrap_or_default())
            })
            .collect();

        // Column 0 = ContainerId, Column 8 = Name
        if vals.len() > 8 {
            let name = &vals[8];
            if name == "History" || name.starts_with("MSHist") {
                if let Ok(cid) = vals[0].parse::<u64>() {
                    history_container_ids.push(cid);
                }
            }
        }
    }

    if history_container_ids.is_empty() {
        anyhow::bail!("No history containers found in {}", db_str);
    }

    let mut entries = Vec::new();
    for cid in &history_container_ids {
        let table_name = format!("Container_{cid}");
        let table = match db.table_by_name(&table_name) {
            Ok(t) => t,
            Err(_) => continue,
        };

        // Build column name -> index map
        let col_count = table.count_columns().unwrap_or(0);
        let mut col_names: Vec<String> = Vec::new();
        for i in 0..col_count {
            let name = table
                .column(i)
                .ok()
                .and_then(|c| c.name().ok())
                .unwrap_or_default();
            col_names.push(name);
        }

        let url_idx = col_names.iter().position(|c| c == "Url");
        let accessed_idx = col_names.iter().position(|c| c == "AccessedTime");
        let modified_idx = col_names.iter().position(|c| c == "ModifiedTime");
        let access_count_idx = col_names.iter().position(|c| c == "AccessCount");
        let entry_id_idx = col_names.iter().position(|c| c == "EntryId");

        for rec_result in table.iter_records()? {
            let rec = match rec_result {
                Ok(r) => r,
                Err(_) => continue,
            };

            let vals: Vec<String> = rec
                .iter_values()
                .ok()
                .into_iter()
                .flat_map(|iter| {
                    iter.map(|v: std::io::Result<libesedb::Value>| {
                        v.map(|val| val.to_string()).unwrap_or_default()
                    })
                })
                .collect();

            // Get URL
            let url_raw = url_idx.and_then(|i| vals.get(i)).map(|s| s.as_str()).unwrap_or("");
            let (url_opt, user_opt) = parse_url(url_raw);

            let url = match url_opt {
                Some(u) if !u.is_empty() => u,
                _ => continue,
            };

            // Get timestamps
            let accessed = accessed_idx
                .and_then(|i| vals.get(i))
                .and_then(|s| parse_ese_datetime(s));
            let modified = modified_idx
                .and_then(|i| vals.get(i))
                .and_then(|s| parse_ese_datetime(s));

            let visit_time = match accessed.or(modified) {
                Some(dt) => dt,
                None => continue,
            };

            let access_count = access_count_idx
                .and_then(|i| vals.get(i))
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(0);

            let entry_id = entry_id_idx
                .and_then(|i| vals.get(i))
                .and_then(|s| s.trim().parse::<i64>().ok())
                .unwrap_or(0);

            // Prefer username from URL (embedded in triage data) over path-based username
            let effective_user = match &user_opt {
                Some(u) if !u.is_empty() => u.clone(),
                _ if !username.is_empty() => username.to_string(),
                _ => String::new(),
            };

            entries.push(HistoryEntry {
                url_length: url.len(),
                url,
                title: String::new(),
                visit_time,
                visit_count: access_count,
                visited_from: String::new(),
                visit_type: String::new(),
                visit_duration: String::new(),
                web_browser: BrowserType::InternetExplorer.display_name().to_string(),
                user_profile: effective_user,
                browser_profile: String::new(),
                typed_count: 0,
                history_file: db_str.clone(),
                record_id: entry_id,
            });
        }
    }

    // Deduplicate by (URL, Visit Time) — same entries appear in History and MSHist containers
    let mut seen = HashSet::new();
    entries.retain(|e| {
        let key = (
            e.url.clone(),
            e.visit_time.format("%m/%d/%Y %I:%M:%S %p").to_string(),
        );
        seen.insert(key)
    });

    // Sort by visit time
    entries.sort_by_key(|e| e.visit_time);

    Ok(entries)
}
