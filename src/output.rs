use anyhow::{Context, Result};
use std::path::Path;

use crate::browsers::{linearize_entry, HistoryEntry};

/// NirSoft BrowsingHistoryView CSV column headers (extended with NaturalLanguage).
const HEADERS: &[&str] = &[
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

/// Write history entries to a CSV file in NirSoft BrowsingHistoryView format.
pub fn write_csv(entries: &[HistoryEntry], output_path: &Path) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    // Ensure parent directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create output directory: {}", parent.display()))?;
    }

    let file = std::fs::File::create(output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    let mut wtr = csv::Writer::from_writer(file);

    // Write header
    wtr.write_record(HEADERS)?;

    // Write entries
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

/// Write history entries to stdout as CSV.
pub fn write_csv_stdout(entries: &[HistoryEntry]) -> Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    let stdout = std::io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());

    wtr.write_record(HEADERS)?;

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
