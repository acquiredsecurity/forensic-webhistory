use forensic_webhistory::browsers::firefox;
use std::path::Path;

#[test]
fn firefox_excludes_places_without_visits() {
    let places = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/Users/synthetic/AppData/Roaming/Mozilla/Firefox/Profiles/synthetic.default-release/places.sqlite",
    );
    let rows = firefox::extract(&places, "synthetic").unwrap();

    // Confirmed against the fixture source database: visit_count=0 has no history-visit row.
    assert_eq!(rows.len(), 4);
    assert!(!rows.iter().any(|row| row.url.ends_with("/unvisited")));
}
