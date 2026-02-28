use anyhow::{Context, Result};
use std::path::Path;

use super::{chrome_time_to_datetime, detect_chromium_browser, BrowserType, ExtensionEntry};

/// Extract extension metadata from a Chrome/Chromium `Preferences` JSON file.
pub fn extract(
    file_path: &Path,
    username: &str,
    browser_override: Option<BrowserType>,
) -> Result<Vec<ExtensionEntry>> {
    let file_str = file_path.to_string_lossy().to_string();
    let browser = browser_override.unwrap_or_else(|| detect_chromium_browser(&file_str));

    let data = std::fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read Preferences file: {}", file_str))?;

    let root: serde_json::Value = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse Preferences JSON: {}", file_str))?;

    let settings = match root
        .get("extensions")
        .and_then(|e| e.get("settings"))
        .and_then(|s| s.as_object())
    {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };

    let mut entries = Vec::new();
    for (ext_id, ext_data) in settings {
        // Skip component extensions (built-in Chrome features)
        let location = ext_data.get("location").and_then(|v| v.as_i64()).unwrap_or(0);
        // location 5 = COMPONENT, 10 = EXTERNAL_COMPONENT
        if location == 5 || location == 10 {
            continue;
        }

        let manifest = ext_data.get("manifest");

        let name = manifest
            .and_then(|m| m.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        // Skip entries with MSG-prefixed names (internal Chrome extensions)
        if name.starts_with("__MSG_") {
            // Try to get a better name from other fields
            let alt_name = ext_data
                .get("manifest")
                .and_then(|m| m.get("short_name"))
                .and_then(|v| v.as_str())
                .unwrap_or(name);
            if alt_name.starts_with("__MSG_") && name.starts_with("__MSG_") {
                // Still no good name — use ext_id prefix
            }
        }

        let version = manifest
            .and_then(|m| m.get("version"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let description = manifest
            .and_then(|m| m.get("description"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let permissions = manifest
            .and_then(|m| m.get("permissions"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();

        // state: 0=DISABLED, 1=ENABLED
        let state = ext_data.get("state").and_then(|v| v.as_i64()).unwrap_or(0);

        let install_time = ext_data
            .get("install_time")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(chrome_time_to_datetime);

        let update_url = manifest
            .and_then(|m| m.get("update_url"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        entries.push(ExtensionEntry {
            extension_id: ext_id.clone(),
            name: name.to_string(),
            version: version.to_string(),
            description: description.to_string(),
            enabled: state == 1,
            install_time,
            update_url: update_url.to_string(),
            permissions,
            web_browser: browser.display_name().to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: file_str.clone(),
        });
    }

    Ok(entries)
}
