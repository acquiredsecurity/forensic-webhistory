use std::path::Path;
use walkdir::WalkDir;

use crate::browsers::{BrowserArtifact, BrowserType};

/// Extract username from a file path by finding the segment after "Users/".
fn extract_username(path: &Path) -> String {
    let path_str = path.to_string_lossy();
    let lower = path_str.to_lowercase();
    if let Some(idx) = lower.find("users") {
        let after = &path_str[idx + 6..]; // skip "Users/"
        if let Some(sep) = after.find(|c| c == '/' || c == '\\') {
            return after[..sep].to_string();
        }
    }
    String::new()
}

/// Scan a triage directory for all browser history artifacts.
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
            // Chrome/Chromium — file is literally named "History" (no extension)
            "History" if is_chromium_history(path) => {
                let browser = detect_chromium_browser(&path_lower);
                let profile_name = path
                    .parent()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("Default")
                    .to_string();

                artifacts.push(BrowserArtifact {
                    browser,
                    db_path: path_str,
                    profile_name,
                    username: extract_username(path),
                });
            }

            // Firefox — places.sqlite
            "places.sqlite" if path_lower.contains("firefox") || path_lower.contains("mozilla") => {
                let profile_name = path
                    .parent()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("default")
                    .to_string();

                artifacts.push(BrowserArtifact {
                    browser: BrowserType::Firefox,
                    db_path: path_str,
                    profile_name,
                    username: extract_username(path),
                });
            }

            // IE/Edge Legacy — WebCacheV01.dat
            "WebCacheV01.dat" => {
                artifacts.push(BrowserArtifact {
                    browser: BrowserType::InternetExplorer,
                    db_path: path_str,
                    profile_name: String::new(),
                    username: extract_username(path),
                });
            }

            _ => {}
        }
    }

    artifacts
}

/// Check if a "History" file is actually a Chromium history database
/// (by verifying it's inside a known Chromium profile directory structure).
fn is_chromium_history(path: &Path) -> bool {
    let path_lower = path.to_string_lossy().to_lowercase();

    // Must be inside a known Chromium browser directory
    path_lower.contains("chrome")
        || path_lower.contains("chromium")
        || path_lower.contains("edge")
        || path_lower.contains("brave")
        || path_lower.contains("opera")
        || path_lower.contains("vivaldi")
        // Or inside a "User Data" directory (generic Chromium pattern)
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
        let path = Path::new("/triage/F/Users/Administrator/AppData/Local/Google/Chrome/User Data/Default/History");
        assert_eq!(extract_username(path), "Administrator");

        let path = Path::new("/triage/C/Users/john.doe/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat");
        assert_eq!(extract_username(path), "john.doe");
    }

    #[test]
    fn test_detect_chromium_browser() {
        assert_eq!(
            detect_chromium_browser("/appdata/local/google/chrome/user data/default/history"),
            BrowserType::Chrome
        );
        assert_eq!(
            detect_chromium_browser("/appdata/local/bravesoftware/brave-browser/user data/default/history"),
            BrowserType::Brave
        );
    }
}
