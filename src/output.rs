use anyhow::{Context, Result};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::browsers::{
    linearize_autofill, linearize_bookmark, linearize_cookie, linearize_download, linearize_entry,
    linearize_extension, linearize_keyword_search, linearize_login, AutofillEntry, BookmarkEntry,
    CookieEntry, DownloadEntry, ExtensionEntry, HistoryEntry, KeywordSearchEntry, LoginEntry,
};

// ============================================================================
// Shared helpers
// ============================================================================

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }
    Ok(())
}

fn fmt_opt_dt(dt: &Option<chrono::DateTime<chrono::Utc>>) -> String {
    dt.map(|d| d.format("%m/%d/%Y %I:%M:%S %p").to_string())
        .unwrap_or_default()
}

fn write_parquet_batch(
    batch: &RecordBatch,
    schema: Arc<Schema>,
    output_path: &Path,
) -> Result<()> {
    ensure_parent(output_path)?;
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create Parquet file: {}", output_path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .context("Failed to create Parquet writer")?;
    writer
        .write(batch)
        .context("Failed to write Parquet batch")?;
    writer.close().context("Failed to close Parquet writer")?;
    Ok(())
}

// ============================================================================
// History
// ============================================================================

const HISTORY_HEADERS: &[&str] = &[
    "URL",
    "Title",
    "Visit Time",
    "Visit Count",
    "Visited From",
    "Visit Type",
    "Visit Duration",
    "Web Browser",
    "User Profile",
    "Browser Profile",
    "URL Length",
    "Typed Count",
    "History File",
    "Record ID",
    "NaturalLanguage",
];

pub fn write_csv(entries: &[HistoryEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }
    ensure_parent(output_path)?;
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(HISTORY_HEADERS)?;
    for entry in entries {
        let nl = linearize_entry(entry);
        wtr.write_record([
            &entry.url,
            &entry.title,
            &entry.visit_time.format("%m/%d/%Y %I:%M:%S %p").to_string(),
            &entry.visit_count.to_string(),
            &entry.visited_from,
            &entry.visit_type,
            &entry.visit_duration,
            &entry.web_browser,
            &entry.user_profile,
            &entry.browser_profile,
            &entry.url_length.to_string(),
            &entry.typed_count.to_string(),
            &entry.history_file,
            &entry.record_id.to_string(),
            &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

pub fn write_csv_stdout(entries: &[HistoryEntry]) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }
    let stdout = std::io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(HISTORY_HEADERS)?;
    for entry in entries {
        let nl = linearize_entry(entry);
        wtr.write_record([
            &entry.url,
            &entry.title,
            &entry.visit_time.format("%m/%d/%Y %I:%M:%S %p").to_string(),
            &entry.visit_count.to_string(),
            &entry.visited_from,
            &entry.visit_type,
            &entry.visit_duration,
            &entry.web_browser,
            &entry.user_profile,
            &entry.browser_profile,
            &entry.url_length.to_string(),
            &entry.typed_count.to_string(),
            &entry.history_file,
            &entry.record_id.to_string(),
            &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

pub fn write_parquet(entries: &[HistoryEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, true),
        Field::new("Title", DataType::Utf8, true),
        Field::new("VisitTime", DataType::Utf8, true),
        Field::new("VisitCount", DataType::UInt32, false),
        Field::new("VisitedFrom", DataType::Utf8, true),
        Field::new("VisitType", DataType::Utf8, true),
        Field::new("VisitDuration", DataType::Utf8, true),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("BrowserProfile", DataType::Utf8, true),
        Field::new("URLLength", DataType::UInt32, false),
        Field::new("TypedCount", DataType::UInt32, false),
        Field::new("HistoryFile", DataType::Utf8, true),
        Field::new("RecordID", DataType::Int64, false),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new();
    let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new();
    let mut b3 = UInt32Builder::new();
    let mut b4 = StringBuilder::new();
    let mut b5 = StringBuilder::new();
    let mut b6 = StringBuilder::new();
    let mut b7 = StringBuilder::new();
    let mut b8 = StringBuilder::new();
    let mut b9 = StringBuilder::new();
    let mut b10 = UInt32Builder::new();
    let mut b11 = UInt32Builder::new();
    let mut b12 = StringBuilder::new();
    let mut b13 = Int64Builder::new();
    let mut b14 = StringBuilder::new();
    for entry in entries {
        let nl = linearize_entry(entry);
        b0.append_value(&entry.url);
        b1.append_value(&entry.title);
        b2.append_value(entry.visit_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string());
        b3.append_value(entry.visit_count);
        b4.append_value(&entry.visited_from);
        b5.append_value(&entry.visit_type);
        b6.append_value(&entry.visit_duration);
        b7.append_value(&entry.web_browser);
        b8.append_value(&entry.user_profile);
        b9.append_value(&entry.browser_profile);
        b10.append_value(entry.url_length as u32);
        b11.append_value(entry.typed_count);
        b12.append_value(&entry.history_file);
        b13.append_value(entry.record_id);
        b14.append_value(&nl);
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(b0.finish()), Arc::new(b1.finish()), Arc::new(b2.finish()),
        Arc::new(b3.finish()), Arc::new(b4.finish()), Arc::new(b5.finish()),
        Arc::new(b6.finish()), Arc::new(b7.finish()), Arc::new(b8.finish()),
        Arc::new(b9.finish()), Arc::new(b10.finish()), Arc::new(b11.finish()),
        Arc::new(b12.finish()), Arc::new(b13.finish()), Arc::new(b14.finish()),
    ])?;
    write_parquet_batch(&batch, schema, output_path)?;
    Ok(entries.len())
}

// ============================================================================
// Downloads
// ============================================================================

const DOWNLOAD_HEADERS: &[&str] = &[
    "URL", "Target Path", "Current Path", "Start Time", "End Time",
    "Received Bytes", "Total Bytes", "State", "Danger Type", "MIME Type",
    "Referrer", "Tab URL", "Opened", "Web Browser", "User Profile",
    "Browser Profile", "Source File", "Record ID", "NaturalLanguage",
];

pub fn write_downloads_csv(entries: &[DownloadEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(DOWNLOAD_HEADERS)?;
    for e in entries {
        let nl = linearize_download(e);
        wtr.write_record([
            &e.url, &e.target_path, &e.current_path,
            &e.start_time.format("%m/%d/%Y %I:%M:%S %p").to_string(),
            &fmt_opt_dt(&e.end_time),
            &e.received_bytes.to_string(), &e.total_bytes.to_string(),
            &e.state, &e.danger_type, &e.mime_type, &e.referrer, &e.tab_url,
            &e.opened.to_string(), &e.web_browser, &e.user_profile,
            &e.browser_profile, &e.source_file, &e.record_id.to_string(), &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

pub fn write_downloads_parquet(entries: &[DownloadEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    let schema = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, true),
        Field::new("TargetPath", DataType::Utf8, true),
        Field::new("StartTime", DataType::Utf8, true),
        Field::new("TotalBytes", DataType::Int64, false),
        Field::new("State", DataType::Utf8, true),
        Field::new("DangerType", DataType::Utf8, true),
        Field::new("MIMEType", DataType::Utf8, true),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("RecordID", DataType::Int64, false),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new(); let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new(); let mut b3 = Int64Builder::new();
    let mut b4 = StringBuilder::new(); let mut b5 = StringBuilder::new();
    let mut b6 = StringBuilder::new(); let mut b7 = StringBuilder::new();
    let mut b8 = StringBuilder::new(); let mut b9 = Int64Builder::new();
    let mut b10 = StringBuilder::new();
    for e in entries {
        b0.append_value(&e.url); b1.append_value(&e.target_path);
        b2.append_value(e.start_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string());
        b3.append_value(e.total_bytes); b4.append_value(&e.state);
        b5.append_value(&e.danger_type); b6.append_value(&e.mime_type);
        b7.append_value(&e.web_browser); b8.append_value(&e.user_profile);
        b9.append_value(e.record_id); b10.append_value(linearize_download(e));
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(b0.finish()), Arc::new(b1.finish()), Arc::new(b2.finish()),
        Arc::new(b3.finish()), Arc::new(b4.finish()), Arc::new(b5.finish()),
        Arc::new(b6.finish()), Arc::new(b7.finish()), Arc::new(b8.finish()),
        Arc::new(b9.finish()), Arc::new(b10.finish()),
    ])?;
    write_parquet_batch(&batch, schema, output_path)?;
    Ok(entries.len())
}

// ============================================================================
// Keyword Searches
// ============================================================================

const KEYWORD_HEADERS: &[&str] = &[
    "Search Term", "Normalized Term", "URL", "Title", "Visit Time",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Keyword ID", "URL ID", "NaturalLanguage",
];

pub fn write_keywords_csv(entries: &[KeywordSearchEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(KEYWORD_HEADERS)?;
    for e in entries {
        let nl = linearize_keyword_search(e);
        wtr.write_record([
            &e.search_term, &e.normalized_term, &e.url, &e.title,
            &fmt_opt_dt(&e.visit_time), &e.web_browser, &e.user_profile,
            &e.browser_profile, &e.source_file, &e.keyword_id.to_string(),
            &e.url_id.to_string(), &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

// ============================================================================
// Cookies
// ============================================================================

const COOKIE_HEADERS: &[&str] = &[
    "Host", "Name", "Path", "Value", "Creation Time", "Expiry Time",
    "Last Access Time", "Secure", "HttpOnly", "Persistent", "SameSite",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Record ID", "NaturalLanguage",
];

pub fn write_cookies_csv(entries: &[CookieEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(COOKIE_HEADERS)?;
    for e in entries {
        let nl = linearize_cookie(e);
        wtr.write_record([
            &e.host, &e.name, &e.path, &e.value,
            &e.creation_time.format("%m/%d/%Y %I:%M:%S %p").to_string(),
            &fmt_opt_dt(&e.expiry_time), &fmt_opt_dt(&e.last_access_time),
            &e.is_secure.to_string(), &e.is_httponly.to_string(),
            &e.is_persistent.to_string(), &e.same_site,
            &e.web_browser, &e.user_profile, &e.browser_profile,
            &e.source_file, &e.record_id.to_string(), &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

// ============================================================================
// Autofill
// ============================================================================

const AUTOFILL_HEADERS: &[&str] = &[
    "Field Name", "Value", "Times Used", "First Used", "Last Used",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Record ID", "NaturalLanguage",
];

pub fn write_autofill_csv(entries: &[AutofillEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(AUTOFILL_HEADERS)?;
    for e in entries {
        let nl = linearize_autofill(e);
        wtr.write_record([
            &e.field_name, &e.value, &e.times_used.to_string(),
            &fmt_opt_dt(&e.first_used), &fmt_opt_dt(&e.last_used),
            &e.web_browser, &e.user_profile, &e.browser_profile,
            &e.source_file, &e.record_id.to_string(), &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

// ============================================================================
// Bookmarks
// ============================================================================

const BOOKMARK_HEADERS: &[&str] = &[
    "URL", "Title", "Date Added", "Date Last Used", "Folder Path",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Record ID", "NaturalLanguage",
];

pub fn write_bookmarks_csv(entries: &[BookmarkEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(BOOKMARK_HEADERS)?;
    for e in entries {
        let nl = linearize_bookmark(e);
        wtr.write_record([
            &e.url, &e.title, &fmt_opt_dt(&e.date_added),
            &fmt_opt_dt(&e.date_last_used), &e.folder_path,
            &e.web_browser, &e.user_profile, &e.browser_profile,
            &e.source_file, &e.record_id.to_string(), &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

// ============================================================================
// Login Data
// ============================================================================

const LOGIN_HEADERS: &[&str] = &[
    "Origin URL", "Action URL", "Username", "Date Created", "Date Last Used",
    "Date Password Modified", "Times Used", "Web Browser", "User Profile",
    "Browser Profile", "Source File", "Record ID", "NaturalLanguage",
];

pub fn write_logins_csv(entries: &[LoginEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(LOGIN_HEADERS)?;
    for e in entries {
        let nl = linearize_login(e);
        wtr.write_record([
            &e.origin_url, &e.action_url, &e.username_value,
            &fmt_opt_dt(&e.date_created), &fmt_opt_dt(&e.date_last_used),
            &fmt_opt_dt(&e.date_password_modified), &e.times_used.to_string(),
            &e.web_browser, &e.user_profile, &e.browser_profile,
            &e.source_file, &e.record_id.to_string(), &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

// ============================================================================
// Extensions
// ============================================================================

const EXTENSION_HEADERS: &[&str] = &[
    "Extension ID", "Name", "Version", "Description", "Enabled",
    "Install Time", "Update URL", "Permissions", "Web Browser",
    "User Profile", "Browser Profile", "Source File", "NaturalLanguage",
];

pub fn write_extensions_csv(entries: &[ExtensionEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(EXTENSION_HEADERS)?;
    for e in entries {
        let nl = linearize_extension(e);
        wtr.write_record([
            &e.extension_id, &e.name, &e.version, &e.description,
            &e.enabled.to_string(), &fmt_opt_dt(&e.install_time),
            &e.update_url, &e.permissions, &e.web_browser,
            &e.user_profile, &e.browser_profile, &e.source_file, &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

