use anyhow::{Context, Result};
use std::path::Path;

use super::{unix_millis_to_datetime, ExtensionEntry};

/// Extract extension/add-on metadata from a Firefox `extensions.json` file.
pub fn extract(file_path: &Path, username: &str) -> Result<Vec<ExtensionEntry>> {
    let file_str = file_path.to_string_lossy().to_string();

    let data = std::fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read extensions.json: {}", file_str))?;

    let root: serde_json::Value = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse extensions.json: {}", file_str))?;

    let addons = match root.get("addons").and_then(|a| a.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };

    let mut entries = Vec::new();
    for addon in addons {
        let id = addon.get("id").and_then(|v| v.as_str()).unwrap_or_default();
        if id.is_empty() {
            continue;
        }

        // Skip system add-ons (location = "app-system-defaults" or "app-builtin")
        let location = addon
            .get("location")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if location.contains("system") || location.contains("builtin") {
            continue;
        }

        let name = addon
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let version = addon
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let description = addon
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let active = addon
            .get("active")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let install_date = addon
            .get("installDate")
            .and_then(|v| v.as_i64())
            .and_then(unix_millis_to_datetime);

        let update_url = addon
            .get("updateURL")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let permissions = addon
            .get("userPermissions")
            .and_then(|up| up.get("permissions"))
            .and_then(|p| p.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();

        entries.push(ExtensionEntry {
            extension_id: id.to_string(),
            name: name.to_string(),
            version: version.to_string(),
            description: description.to_string(),
            enabled: active,
            install_time: install_date,
            update_url: update_url.to_string(),
            permissions,
            web_browser: "Firefox".to_string(),
            user_profile: username.to_string(),
            browser_profile: String::new(),
            source_file: file_str.clone(),
        });
    }

    Ok(entries)
}
