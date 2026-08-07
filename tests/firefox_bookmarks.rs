use forensic_webhistory::browsers::firefox_bookmarks;
use std::path::Path;

#[test]
fn firefox_emits_bookmarks_but_not_folders() {
    let places = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/Users/synthetic/AppData/Roaming/Mozilla/Firefox/Profiles/synthetic.default-release/places.sqlite",
    );
    let rows = firefox_bookmarks::extract(&places, "synthetic").unwrap();

    // Confirmed against the fixture source database: 4 type=1 rows and 7 type=2 folders.
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| row.url.starts_with("https://")));
}
