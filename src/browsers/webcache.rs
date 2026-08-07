use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use libesedb::EseDb;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::{BrowserType, HistoryEntry};

/// Normalize an ESE database path before handing it to libesedb.
///
/// The Windows backend of libesedb cannot handle paths that mix forward
/// and backward slashes (e.g. `"F:/triage/C\Users\bob\..."` — which is
/// exactly what `walkdir` produces when the caller supplies a
/// forward-slash `--dir` on Windows). The failure surfaces as a
/// `libesedb_file_open: unable to open file` error, but the underlying
/// cause is libesedb prepending the cwd to a path it interprets as
/// relative, producing a Windows-invalid mixed-slash string like
/// `\\?\F:\/triage/C\Users\bob\...`.
///
/// `std::fs::canonicalize` returns a clean extended-length `\\?\` path
/// with all backslashes, which libesedb handles correctly. If
/// canonicalization fails (file doesn't exist, permission denied,
/// etc.) we return the original path so the eventual error message
/// still points at what the caller asked for.
fn normalize_ese_path(db_path: &Path) -> PathBuf {
    std::fs::canonicalize(db_path).unwrap_or_else(|_| db_path.to_path_buf())
}

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
        return filetime_to_datetime(ft);
    }

    None
}

/// Convert a Windows FILETIME value (100ns intervals since 1601-01-01 UTC).
pub fn filetime_to_datetime(filetime: u64) -> Option<DateTime<Utc>> {
    if filetime == 0 {
        return None;
    }
    let microseconds = i64::try_from(filetime / 10).ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1601, 1, 1)?.and_hms_opt(0, 0, 0)?;
    let dt = epoch.checked_add_signed(chrono::Duration::microseconds(microseconds))?;
    Some(DateTime::from_naive_utc_and_offset(dt, Utc))
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
    if let Some(after_first_colon) = text.strip_prefix(':') {
        // Find the second colon (end of date range)
        if let Some(second_colon) = after_first_colon.find(':') {
            let rest = after_first_colon[second_colon + 1..].trim(); // skip "daterange: "
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

    // See `normalize_ese_path` for why this is required on Windows.
    let canonical = normalize_ese_path(db_path);

    let db = EseDb::open(&canonical)
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
            .flat_map(|iter| iter.map(|v| v.map(|val| val.to_string()).unwrap_or_default()))
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
            let url_raw = url_idx
                .and_then(|i| vals.get(i))
                .map(|s| s.as_str())
                .unwrap_or("");
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
            e.visit_time.format("%Y-%m-%d %H:%M:%S").to_string(),
        );
        seen.insert(key)
    });

    // Sort by visit time
    entries.sort_by_key(|e| e.visit_time);

    Ok(entries)
}

#[cfg(test)]
mod tests {
    //! Regression tests for the WebCacheV01.dat path-handling bug
    //! (acquiredsecurity/forensic-webhistory#4).
    //!
    //! These tests do NOT need a real WebCacheV01.dat fixture; they
    //! cover the path-normalization layer that sits in front of
    //! libesedb. The actual ESE-open behavior is exercised by manual
    //! triage runs documented in the issue.

    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    /// A pre-existing file at a pure-backslash absolute path
    /// canonicalizes to a path libesedb can open. The exact form is
    /// platform-specific (Windows uses `\\?\`-prefixed extended-length
    /// paths) so the test only asserts that canonicalization succeeds
    /// and the result still points at the same file.
    #[test]
    fn normalize_handles_existing_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("WebCacheV01.dat");
        File::create(&path).unwrap();

        let normalized = normalize_ese_path(&path);
        assert!(
            normalized.exists(),
            "normalized path should still point at the file"
        );
    }

    /// A path that mixes forward and backward slashes (the exact bug
    /// shape that caused issue #4 — `walkdir` joins forward-slash
    /// `--dir` input with backslash subdirs on Windows) must be
    /// normalized to a form that libesedb can open. Without
    /// canonicalization, this path triggers
    /// `libesedb_file_open: unable to open file` even though the file
    /// is fine.
    #[cfg(windows)]
    #[test]
    fn normalize_handles_mixed_slash_path() {
        let dir = TempDir::new().unwrap();
        let real_path = dir.path().join("WebCacheV01.dat");
        File::create(&real_path).unwrap();

        // Build a mixed-slash path that points at the same file. The
        // walkdir bug looked like "F:/triage/C\Users\bob\WebCacheV01.dat".
        // Reproduce that shape here by replacing all backslashes in the
        // tempdir prefix with forward slashes, then joining with a
        // backslash-style child component.
        let prefix_fwd = dir.path().to_string_lossy().replace('\\', "/");
        let mixed = format!("{}\\WebCacheV01.dat", prefix_fwd);
        let mixed_path = PathBuf::from(&mixed);

        // Sanity: the mixed path actually identifies the same file
        // (Windows resolves it under the hood; this just confirms the
        // test setup is right).
        assert!(
            std::fs::metadata(&mixed_path).is_ok(),
            "test setup invalid: mixed-slash path doesn't resolve"
        );

        let normalized = normalize_ese_path(&mixed_path);

        // The normalized path must:
        //  1. still exist (it's the same file)
        //  2. NOT contain any forward slashes — libesedb chokes on those
        assert!(normalized.exists());
        let normalized_str = normalized.to_string_lossy();
        assert!(
            !normalized_str.contains('/'),
            "normalized path still contains forward slashes: {}",
            normalized_str
        );
    }

    /// If the file does not exist, normalization must fall back to the
    /// original path so the eventual `EseDb::open()` error message
    /// still points at what the caller asked for.
    #[test]
    fn normalize_falls_back_for_missing_file() {
        // Build a path inside a real temp dir but with a leaf that
        // does not exist. canonicalize() will fail on the missing leaf
        // and the helper must return the input path unchanged. Using
        // a tempdir-scoped path keeps the test deterministic across
        // platforms instead of relying on a hardcoded `/nonexistent/...`.
        let dir = TempDir::new().unwrap();
        let nonexistent = dir.path().join("missing-WebCacheV01.dat");
        let normalized = normalize_ese_path(&nonexistent);
        assert_eq!(normalized, nonexistent);
    }
}
