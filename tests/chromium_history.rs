use forensic_webhistory::browsers::{chrome, BrowserType};
use std::path::Path;

#[test]
fn chromium_history_preserves_known_visit_fields() {
    let history = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/Users/synthetic/AppData/Local/Google/Chrome/User Data/Default/History",
    );
    let rows = chrome::extract(&history, "synthetic", Some(BrowserType::Chrome)).unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.url, "https://synthetic.invalid/example");
    assert_eq!(row.title, "Synthetic history");
    assert_eq!(row.visit_time.to_rfc3339(), "2025-01-01T00:00:00+00:00");
    assert_eq!(row.visit_count, 1);
    assert_eq!(row.visit_type, "Typed");
}
