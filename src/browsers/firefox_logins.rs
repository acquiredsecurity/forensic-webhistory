use anyhow::{Context, Result};
use std::path::Path;

use super::{unix_millis_to_datetime, LoginEntry};

/// Extract login metadata from a Firefox `logins.json` file.
///
/// IMPORTANT: Only extracts metadata (URLs, usernames, timestamps, usage counts).
/// Encrypted password data is NEVER extracted.
pub fn extract(file_path: &Path, username: &str) -> Result<Vec<LoginEntry>> {
    let file_str = file_path.to_string_lossy().to_string();

    let data = std::fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read logins.json: {}", file_str))?;

    let root: serde_json::Value = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse logins.json: {}", file_str))?;

    let logins = match root.get("logins").and_then(|l| l.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };

    let mut entries = Vec::new();
    for (idx, login) in logins.iter().enumerate() {
        let hostname = login
            .get("hostname")
            .or_else(|| login.get("origin"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if hostname.is_empty() {
            continue;
        }

        let form_submit_url = login
            .get("formSubmitURL")
            .or_else(|| login.get("formActionOrigin"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let username_field = login
            .get("usernameField")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let time_created = login
            .get("timeCreated")
            .and_then(|v| v.as_i64())
            .and_then(unix_millis_to_datetime);

        let time_last_used = login
            .get("timeLastUsed")
            .and_then(|v| v.as_i64())
            .and_then(unix_millis_to_datetime);

        let time_password_changed = login
            .get("timePasswordChanged")
            .and_then(|v| v.as_i64())
            .and_then(unix_millis_to_datetime);

        let times_used = login
            .get("timesUsed")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as u32;

        entries.push(LoginEntry {
            origin_url: hostname.to_string(),
            action_url: form_submit_url.to_string(),
            username_value: username_field.to_string(),
            date_created: time_created,
            date_last_used: time_last_used,
            date_password_modified: time_password_changed,
            times_used,
            web_browser: "Firefox".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: file_str.clone(),
            record_id: idx as i64,
        });
    }

    Ok(entries)
}
