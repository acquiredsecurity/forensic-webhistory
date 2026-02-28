use std::path::Path;
use walkdir::WalkDir;

use crate::browsers::{ArtifactType, BrowserArtifact, BrowserType};

/// Extract username from a file path by finding the segment after the LAST "Users/".
/// Uses rfind to handle cases where triage data is stored under a local user's home dir
/// (e.g., /Users/analyst/Desktop/triage/C/Users/suspect/AppData/... → "suspect").
fn extract_username(path: &Path) -> String {
    let path_str = path.to_string_lossy();
    let lower = path_str.to_lowercase();
    if let Some(idx) = lower.rfind("users") {
        let after = &path_str[idx + 6..]; // skip "Users/"
        if let Some(sep) = after.find(['/', '\\']) {
            return after[..sep].to_string();
        }
    }
    String::new()
}

/// Extract profile name from path (parent directory name).
fn extract_profile_name(path: &Path) -> String {
    path.parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("Default")
        .to_string()
}

/// Scan a triage directory for all browser artifacts.
pub fn scan(triage_path: &Path) -> Vec<BrowserArtifact> {
    let mut artifacts = Vec::new();

    for entry in WalkDir::new(triage_path)
        .follow_links(true)
        .max_depth(15)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        let path_str = path.to_string_lossy().to_string();
        let path_lower = path_str.to_lowercase();

        match file_name {
            // ---- History ----
            "History" if is_chromium_profile(&path_lower) => {
                let browser = detect_chromium_browser(&path_lower);
                artifacts.push(BrowserArtifact {
                    browser,
                    artifact_type: ArtifactType::History,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            "places.sqlite"
                if path_lower.contains("firefox") || path_lower.contains("mozilla") =>
            {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::Firefox,
                    artifact_type: ArtifactType::History,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            "History.db" if path_lower.contains("safari") => {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::Safari,
                    artifact_type: ArtifactType::History,
                    db_path: path_str,
                    profile_name: String::new(),
                    username: extract_username(path),
                });
            }

            "WebCacheV01.dat" => {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::InternetExplorer,
                    artifact_type: ArtifactType::History,
                    db_path: path_str,
                    profile_name: String::new(),
                    username: extract_username(path),
                });
            }

            // ---- Cookies ----
            "Cookies" if is_chromium_profile(&path_lower) => {
                let browser = detect_chromium_browser(&path_lower);
                artifacts.push(BrowserArtifact {
                    browser,
                    artifact_type: ArtifactType::Cookies,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            "cookies.sqlite"
                if path_lower.contains("firefox") || path_lower.contains("mozilla") =>
            {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::Firefox,
                    artifact_type: ArtifactType::Cookies,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            // ---- Autofill ----
            "Web Data" if is_chromium_profile(&path_lower) => {
                let browser = detect_chromium_browser(&path_lower);
                artifacts.push(BrowserArtifact {
                    browser,
                    artifact_type: ArtifactType::Autofill,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            "formhistory.sqlite"
                if path_lower.contains("firefox") || path_lower.contains("mozilla") =>
            {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::Firefox,
                    artifact_type: ArtifactType::Autofill,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            // ---- Login Data ----
            "Login Data" if is_chromium_profile(&path_lower) => {
                let browser = detect_chromium_browser(&path_lower);
                artifacts.push(BrowserArtifact {
                    browser,
                    artifact_type: ArtifactType::LoginData,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            "logins.json"
                if path_lower.contains("firefox") || path_lower.contains("mozilla") =>
            {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::Firefox,
                    artifact_type: ArtifactType::LoginData,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            // ---- Bookmarks (Chrome JSON) ----
            "Bookmarks" if is_chromium_profile(&path_lower) => {
                let browser = detect_chromium_browser(&path_lower);
                artifacts.push(BrowserArtifact {
                    browser,
                    artifact_type: ArtifactType::Bookmarks,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            // ---- Extensions ----
            "Preferences" if is_chromium_profile(&path_lower) => {
                let browser = detect_chromium_browser(&path_lower);
                artifacts.push(BrowserArtifact {
                    browser,
                    artifact_type: ArtifactType::Extensions,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            "extensions.json"
                if path_lower.contains("firefox") || path_lower.contains("mozilla") =>
            {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::Firefox,
                    artifact_type: ArtifactType::Extensions,
                    db_path: path_str,
                    profile_name: extract_profile_name(path),
                    username: extract_username(path),
                });
            }

            _ => {}
        }
    }

    // Synthesize additional artifacts from multi-artifact database files
    let synthesized = synthesize_additional_artifacts(&artifacts);
    artifacts.extend(synthesized);

    artifacts
}

/// When we find a History DB, it also contains downloads and keyword searches.
/// When we find Firefox places.sqlite, it also has bookmarks and downloads.
fn synthesize_additional_artifacts(artifacts: &[BrowserArtifact]) -> Vec<BrowserArtifact> {
    let mut additional = Vec::new();
    for a in artifacts {
        match (&a.browser, &a.artifact_type) {
            // Chrome History DB also has downloads + keyword searches
            (b, ArtifactType::History) if b.is_chromium() => {
                additional.push(BrowserArtifact {
                    artifact_type: ArtifactType::Downloads,
                    ..a.clone()
                });
                additional.push(BrowserArtifact {
                    artifact_type: ArtifactType::KeywordSearches,
                    ..a.clone()
                });
            }
            // Firefox places.sqlite also has downloads + bookmarks
            (BrowserType::Firefox, ArtifactType::History) => {
                additional.push(BrowserArtifact {
                    artifact_type: ArtifactType::Downloads,
                    ..a.clone()
                });
                additional.push(BrowserArtifact {
                    artifact_type: ArtifactType::Bookmarks,
                    ..a.clone()
                });
            }
            _ => {}
        }
    }
    additional
}

/// Check if a path is inside a Chromium browser profile directory.
fn is_chromium_profile(path_lower: &str) -> bool {
    path_lower.contains("chrome")
        || path_lower.contains("chromium")
        || path_lower.contains("edge")
        || path_lower.contains("brave")
        || path_lower.contains("opera")
        || path_lower.contains("vivaldi")
        || path_lower.contains("/arc/")
        || path_lower.contains("user data")
}

/// Detect which Chromium browser variant from the path.
fn detect_chromium_browser(path_lower: &str) -> BrowserType {
    if path_lower.contains("brave") {
        BrowserType::Brave
    } else if path_lower.contains("opera") {
        BrowserType::Opera
    } else if path_lower.contains("vivaldi") {
        BrowserType::Vivaldi
    } else if path_lower.contains("edge") || path_lower.contains("msedge") {
        BrowserType::EdgeChromium
    } else if path_lower.contains("/arc/") {
        BrowserType::Arc
    } else if path_lower.contains("chromium") {
        BrowserType::Chromium
    } else {
        BrowserType::Chrome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_username() {
        let path = Path::new(
            "/triage/F/Users/Administrator/AppData/Local/Google/Chrome/User Data/Default/History",
        );
        assert_eq!(extract_username(path), "Administrator");

        let path = Path::new(
            "/triage/C/Users/john.doe/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
        );
        assert_eq!(extract_username(path), "john.doe");
    }

    #[test]
    fn test_detect_chromium_browser() {
        assert_eq!(
            detect_chromium_browser("/appdata/local/google/chrome/user data/default/history"),
            BrowserType::Chrome
        );
        assert_eq!(
            detect_chromium_browser(
                "/appdata/local/bravesoftware/brave-browser/user data/default/history"
            ),
            BrowserType::Brave
        );
    }
}
