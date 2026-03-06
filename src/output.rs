use anyhow::{Context, Result};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{BooleanBuilder, Int64Builder, StringBuilder, UInt32Builder};
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

fn fmt_dt(dt: &chrono::DateTime<chrono::Utc>, fmt: &str) -> String {
    dt.format(fmt).to_string()
}

fn fmt_opt_dt(dt: &Option<chrono::DateTime<chrono::Utc>>, fmt: &str) -> String {
    dt.map(|d| d.format(fmt).to_string())
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
    "Visit Time",
    "URL",
    "Title",
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

pub fn write_csv(entries: &[HistoryEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
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
            &fmt_dt(&entry.visit_time, date_fmt),
            &entry.url,
            &entry.title,
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

pub fn write_csv_stdout(entries: &[HistoryEntry], date_fmt: &str) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }
    let stdout = std::io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(HISTORY_HEADERS)?;
    for entry in entries {
        let nl = linearize_entry(entry);
        wtr.write_record([
            &fmt_dt(&entry.visit_time, date_fmt),
            &entry.url,
            &entry.title,
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
        Field::new("VisitTime", DataType::Utf8, true),
        Field::new("URL", DataType::Utf8, true),
        Field::new("Title", DataType::Utf8, true),
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
        b0.append_value(entry.visit_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string());
        b1.append_value(&entry.url);
        b2.append_value(&entry.title);
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
    "Start Time", "End Time", "URL", "Target Path", "Current Path",
    "Received Bytes", "Total Bytes", "State", "Danger Type", "MIME Type",
    "Referrer", "Tab URL", "Opened", "Web Browser", "User Profile",
    "Browser Profile", "Source File", "Record ID", "NaturalLanguage",
];

pub fn write_downloads_csv(entries: &[DownloadEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(DOWNLOAD_HEADERS)?;
    for e in entries {
        let nl = linearize_download(e);
        wtr.write_record([
            &fmt_dt(&e.start_time, date_fmt),
            &fmt_opt_dt(&e.end_time, date_fmt),
            &e.url, &e.target_path, &e.current_path,
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
        Field::new("StartTime", DataType::Utf8, true),
        Field::new("URL", DataType::Utf8, true),
        Field::new("TargetPath", DataType::Utf8, true),
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
        b0.append_value(e.start_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string());
        b1.append_value(&e.url); b2.append_value(&e.target_path);
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
    "Visit Time", "Search Term", "Normalized Term", "URL", "Title",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Keyword ID", "URL ID", "NaturalLanguage",
];

pub fn write_keywords_csv(entries: &[KeywordSearchEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(KEYWORD_HEADERS)?;
    for e in entries {
        let nl = linearize_keyword_search(e);
        wtr.write_record([
            &fmt_opt_dt(&e.visit_time, date_fmt),
            &e.search_term, &e.normalized_term, &e.url, &e.title,
            &e.web_browser, &e.user_profile,
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
    "Creation Time", "Expiry Time", "Last Access Time",
    "Host", "Name", "Path", "Value",
    "Secure", "HttpOnly", "Persistent", "SameSite",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Record ID", "NaturalLanguage",
];

pub fn write_cookies_csv(entries: &[CookieEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(COOKIE_HEADERS)?;
    for e in entries {
        let nl = linearize_cookie(e);
        wtr.write_record([
            &fmt_dt(&e.creation_time, date_fmt),
            &fmt_opt_dt(&e.expiry_time, date_fmt), &fmt_opt_dt(&e.last_access_time, date_fmt),
            &e.host, &e.name, &e.path, &e.value,
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
    "First Used", "Last Used", "Field Name", "Value", "Times Used",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Record ID", "NaturalLanguage",
];

pub fn write_autofill_csv(entries: &[AutofillEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(AUTOFILL_HEADERS)?;
    for e in entries {
        let nl = linearize_autofill(e);
        wtr.write_record([
            &fmt_opt_dt(&e.first_used, date_fmt), &fmt_opt_dt(&e.last_used, date_fmt),
            &e.field_name, &e.value, &e.times_used.to_string(),
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
    "Date Added", "Date Last Used", "URL", "Title", "Folder Path",
    "Web Browser", "User Profile", "Browser Profile", "Source File",
    "Record ID", "NaturalLanguage",
];

pub fn write_bookmarks_csv(entries: &[BookmarkEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(BOOKMARK_HEADERS)?;
    for e in entries {
        let nl = linearize_bookmark(e);
        wtr.write_record([
            &fmt_opt_dt(&e.date_added, date_fmt),
            &fmt_opt_dt(&e.date_last_used, date_fmt),
            &e.url, &e.title, &e.folder_path,
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
    "Date Created", "Date Last Used", "Date Password Modified",
    "Origin URL", "Action URL", "Username",
    "Times Used", "Web Browser", "User Profile",
    "Browser Profile", "Source File", "Record ID", "NaturalLanguage",
];

pub fn write_logins_csv(entries: &[LoginEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(LOGIN_HEADERS)?;
    for e in entries {
        let nl = linearize_login(e);
        wtr.write_record([
            &fmt_opt_dt(&e.date_created, date_fmt), &fmt_opt_dt(&e.date_last_used, date_fmt),
            &fmt_opt_dt(&e.date_password_modified, date_fmt),
            &e.origin_url, &e.action_url, &e.username_value,
            &e.times_used.to_string(),
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
    "Install Time", "Extension ID", "Name", "Version", "Description", "Enabled",
    "Update URL", "Permissions", "Web Browser",
    "User Profile", "Browser Profile", "Source File", "NaturalLanguage",
];

pub fn write_extensions_csv(entries: &[ExtensionEntry], output_path: &Path, date_fmt: &str) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    ensure_parent(output_path)?;
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(EXTENSION_HEADERS)?;
    for e in entries {
        let nl = linearize_extension(e);
        wtr.write_record([
            &fmt_opt_dt(&e.install_time, date_fmt),
            &e.extension_id, &e.name, &e.version, &e.description,
            &e.enabled.to_string(),
            &e.update_url, &e.permissions, &e.web_browser,
            &e.user_profile, &e.browser_profile, &e.source_file, &nl,
        ])?;
    }
    wtr.flush()?;
    Ok(entries.len())
}

// ============================================================================
// Parquet writers for remaining artifact types
// ============================================================================

pub fn write_keywords_parquet(entries: &[KeywordSearchEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    let schema = Arc::new(Schema::new(vec![
        Field::new("VisitTime", DataType::Utf8, true),
        Field::new("SearchTerm", DataType::Utf8, true),
        Field::new("NormalizedTerm", DataType::Utf8, true),
        Field::new("URL", DataType::Utf8, true),
        Field::new("Title", DataType::Utf8, true),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("BrowserProfile", DataType::Utf8, true),
        Field::new("KeywordID", DataType::Int64, false),
        Field::new("URLID", DataType::Int64, false),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new(); let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new(); let mut b3 = StringBuilder::new();
    let mut b4 = StringBuilder::new(); let mut b5 = StringBuilder::new();
    let mut b6 = StringBuilder::new(); let mut b7 = StringBuilder::new();
    let mut b8 = Int64Builder::new(); let mut b9 = Int64Builder::new();
    let mut b10 = StringBuilder::new();
    for e in entries {
        b0.append_value(e.visit_time.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b1.append_value(&e.search_term); b2.append_value(&e.normalized_term);
        b3.append_value(&e.url); b4.append_value(&e.title);
        b5.append_value(&e.web_browser); b6.append_value(&e.user_profile);
        b7.append_value(&e.browser_profile);
        b8.append_value(e.keyword_id); b9.append_value(e.url_id);
        b10.append_value(linearize_keyword_search(e));
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

pub fn write_cookies_parquet(entries: &[CookieEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    let schema = Arc::new(Schema::new(vec![
        Field::new("CreationTime", DataType::Utf8, true),
        Field::new("ExpiryTime", DataType::Utf8, true),
        Field::new("LastAccessTime", DataType::Utf8, true),
        Field::new("Host", DataType::Utf8, true),
        Field::new("Name", DataType::Utf8, true),
        Field::new("Path", DataType::Utf8, true),
        Field::new("Secure", DataType::Boolean, false),
        Field::new("HttpOnly", DataType::Boolean, false),
        Field::new("SameSite", DataType::Utf8, true),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("RecordID", DataType::Int64, false),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new(); let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new(); let mut b3 = StringBuilder::new();
    let mut b4 = StringBuilder::new(); let mut b5 = StringBuilder::new();
    let mut b6 = BooleanBuilder::new(); let mut b7 = BooleanBuilder::new();
    let mut b8 = StringBuilder::new(); let mut b9 = StringBuilder::new();
    let mut b10 = StringBuilder::new(); let mut b11 = Int64Builder::new();
    let mut b12 = StringBuilder::new();
    for e in entries {
        b0.append_value(e.creation_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string());
        b1.append_value(e.expiry_time.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b2.append_value(e.last_access_time.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b3.append_value(&e.host); b4.append_value(&e.name);
        b5.append_value(&e.path);
        b6.append_value(e.is_secure); b7.append_value(e.is_httponly);
        b8.append_value(&e.same_site); b9.append_value(&e.web_browser);
        b10.append_value(&e.user_profile); b11.append_value(e.record_id);
        b12.append_value(linearize_cookie(e));
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(b0.finish()), Arc::new(b1.finish()), Arc::new(b2.finish()),
        Arc::new(b3.finish()), Arc::new(b4.finish()), Arc::new(b5.finish()),
        Arc::new(b6.finish()), Arc::new(b7.finish()), Arc::new(b8.finish()),
        Arc::new(b9.finish()), Arc::new(b10.finish()), Arc::new(b11.finish()),
        Arc::new(b12.finish()),
    ])?;
    write_parquet_batch(&batch, schema, output_path)?;
    Ok(entries.len())
}

pub fn write_autofill_parquet(entries: &[AutofillEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    let schema = Arc::new(Schema::new(vec![
        Field::new("FirstUsed", DataType::Utf8, true),
        Field::new("LastUsed", DataType::Utf8, true),
        Field::new("FieldName", DataType::Utf8, true),
        Field::new("Value", DataType::Utf8, true),
        Field::new("TimesUsed", DataType::UInt32, false),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("RecordID", DataType::Int64, false),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new(); let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new(); let mut b3 = StringBuilder::new();
    let mut b4 = UInt32Builder::new(); let mut b5 = StringBuilder::new();
    let mut b6 = StringBuilder::new(); let mut b7 = Int64Builder::new();
    let mut b8 = StringBuilder::new();
    for e in entries {
        b0.append_value(e.first_used.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b1.append_value(e.last_used.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b2.append_value(&e.field_name); b3.append_value(&e.value);
        b4.append_value(e.times_used);
        b5.append_value(&e.web_browser); b6.append_value(&e.user_profile);
        b7.append_value(e.record_id); b8.append_value(linearize_autofill(e));
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(b0.finish()), Arc::new(b1.finish()), Arc::new(b2.finish()),
        Arc::new(b3.finish()), Arc::new(b4.finish()), Arc::new(b5.finish()),
        Arc::new(b6.finish()), Arc::new(b7.finish()), Arc::new(b8.finish()),
    ])?;
    write_parquet_batch(&batch, schema, output_path)?;
    Ok(entries.len())
}

pub fn write_bookmarks_parquet(entries: &[BookmarkEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    let schema = Arc::new(Schema::new(vec![
        Field::new("DateAdded", DataType::Utf8, true),
        Field::new("DateLastUsed", DataType::Utf8, true),
        Field::new("URL", DataType::Utf8, true),
        Field::new("Title", DataType::Utf8, true),
        Field::new("FolderPath", DataType::Utf8, true),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("RecordID", DataType::Int64, false),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new(); let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new(); let mut b3 = StringBuilder::new();
    let mut b4 = StringBuilder::new(); let mut b5 = StringBuilder::new();
    let mut b6 = StringBuilder::new(); let mut b7 = Int64Builder::new();
    let mut b8 = StringBuilder::new();
    for e in entries {
        b0.append_value(e.date_added.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b1.append_value(e.date_last_used.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b2.append_value(&e.url); b3.append_value(&e.title);
        b4.append_value(&e.folder_path); b5.append_value(&e.web_browser);
        b6.append_value(&e.user_profile); b7.append_value(e.record_id);
        b8.append_value(linearize_bookmark(e));
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(b0.finish()), Arc::new(b1.finish()), Arc::new(b2.finish()),
        Arc::new(b3.finish()), Arc::new(b4.finish()), Arc::new(b5.finish()),
        Arc::new(b6.finish()), Arc::new(b7.finish()), Arc::new(b8.finish()),
    ])?;
    write_parquet_batch(&batch, schema, output_path)?;
    Ok(entries.len())
}

pub fn write_logins_parquet(entries: &[LoginEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    let schema = Arc::new(Schema::new(vec![
        Field::new("DateCreated", DataType::Utf8, true),
        Field::new("DateLastUsed", DataType::Utf8, true),
        Field::new("OriginURL", DataType::Utf8, true),
        Field::new("ActionURL", DataType::Utf8, true),
        Field::new("Username", DataType::Utf8, true),
        Field::new("TimesUsed", DataType::UInt32, false),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("RecordID", DataType::Int64, false),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new(); let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new(); let mut b3 = StringBuilder::new();
    let mut b4 = StringBuilder::new(); let mut b5 = UInt32Builder::new();
    let mut b6 = StringBuilder::new(); let mut b7 = StringBuilder::new();
    let mut b8 = Int64Builder::new(); let mut b9 = StringBuilder::new();
    for e in entries {
        b0.append_value(e.date_created.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b1.append_value(e.date_last_used.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b2.append_value(&e.origin_url); b3.append_value(&e.action_url);
        b4.append_value(&e.username_value);
        b5.append_value(e.times_used); b6.append_value(&e.web_browser);
        b7.append_value(&e.user_profile); b8.append_value(e.record_id);
        b9.append_value(linearize_login(e));
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(b0.finish()), Arc::new(b1.finish()), Arc::new(b2.finish()),
        Arc::new(b3.finish()), Arc::new(b4.finish()), Arc::new(b5.finish()),
        Arc::new(b6.finish()), Arc::new(b7.finish()), Arc::new(b8.finish()),
        Arc::new(b9.finish()),
    ])?;
    write_parquet_batch(&batch, schema, output_path)?;
    Ok(entries.len())
}

pub fn write_extensions_parquet(entries: &[ExtensionEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() { return Ok(0); }
    let schema = Arc::new(Schema::new(vec![
        Field::new("InstallTime", DataType::Utf8, true),
        Field::new("ExtensionID", DataType::Utf8, true),
        Field::new("Name", DataType::Utf8, true),
        Field::new("Version", DataType::Utf8, true),
        Field::new("Description", DataType::Utf8, true),
        Field::new("Enabled", DataType::Boolean, false),
        Field::new("Permissions", DataType::Utf8, true),
        Field::new("WebBrowser", DataType::Utf8, true),
        Field::new("UserProfile", DataType::Utf8, true),
        Field::new("NaturalLanguage", DataType::Utf8, true),
    ]));
    let mut b0 = StringBuilder::new(); let mut b1 = StringBuilder::new();
    let mut b2 = StringBuilder::new(); let mut b3 = StringBuilder::new();
    let mut b4 = StringBuilder::new(); let mut b5 = BooleanBuilder::new();
    let mut b6 = StringBuilder::new(); let mut b7 = StringBuilder::new();
    let mut b8 = StringBuilder::new(); let mut b9 = StringBuilder::new();
    for e in entries {
        b0.append_value(e.install_time.map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string()).unwrap_or_default());
        b1.append_value(&e.extension_id); b2.append_value(&e.name);
        b3.append_value(&e.version); b4.append_value(&e.description);
        b5.append_value(e.enabled);
        b6.append_value(&e.permissions); b7.append_value(&e.web_browser);
        b8.append_value(&e.user_profile); b9.append_value(linearize_extension(e));
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(b0.finish()), Arc::new(b1.finish()), Arc::new(b2.finish()),
        Arc::new(b3.finish()), Arc::new(b4.finish()), Arc::new(b5.finish()),
        Arc::new(b6.finish()), Arc::new(b7.finish()), Arc::new(b8.finish()),
        Arc::new(b9.finish()),
    ])?;
    write_parquet_batch(&batch, schema, output_path)?;
    Ok(entries.len())
}

