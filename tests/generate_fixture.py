"""Generate deterministic Chromium History (no downloads) and Firefox places.sqlite fixtures."""
from pathlib import Path
import sqlite3

path = Path(__file__).parent / "fixtures" / "Users" / "synthetic" / "AppData" / "Local" / "Google" / "Chrome" / "User Data" / "Default" / "History"
path.parent.mkdir(parents=True, exist_ok=True)
if path.exists():
    path.unlink()
db = sqlite3.connect(path)
db.executescript("""
CREATE TABLE urls (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INTEGER, typed_count INTEGER);
CREATE TABLE visits (id INTEGER PRIMARY KEY, url INTEGER, visit_time INTEGER, from_visit INTEGER, transition INTEGER);
CREATE TABLE downloads (id INTEGER PRIMARY KEY, current_path TEXT, target_path TEXT, start_time INTEGER, end_time INTEGER, received_bytes INTEGER, total_bytes INTEGER, state INTEGER, danger_type INTEGER, opened INTEGER, referrer TEXT, tab_url TEXT, mime_type TEXT, original_mime_type TEXT);
INSERT INTO urls VALUES (1, 'https://synthetic.invalid/example', 'Synthetic history', 1, 1);
INSERT INTO visits VALUES (1, 1, 13380163200000000, 0, 1);
""")
db.commit()
db.close()

firefox_path = Path(__file__).parent / "fixtures" / "Users" / "synthetic" / "AppData" / "Roaming" / "Mozilla" / "Firefox" / "Profiles" / "synthetic.default-release" / "places.sqlite"
firefox_path.parent.mkdir(parents=True, exist_ok=True)
if firefox_path.exists():
    firefox_path.unlink()
db = sqlite3.connect(firefox_path)
db.executescript("""
CREATE TABLE moz_places (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INTEGER);
CREATE TABLE moz_historyvisits (id INTEGER PRIMARY KEY, place_id INTEGER, visit_date INTEGER, from_visit INTEGER, visit_type INTEGER);
CREATE TABLE moz_bookmarks (id INTEGER PRIMARY KEY, type INTEGER, fk INTEGER, parent INTEGER, title TEXT, dateAdded INTEGER, lastModified INTEGER);
INSERT INTO moz_places VALUES (1, 'https://synthetic.invalid/firefox/one', 'Firefox one', 1);
INSERT INTO moz_places VALUES (2, 'https://synthetic.invalid/firefox/two', 'Firefox two', 1);
INSERT INTO moz_places VALUES (3, 'https://synthetic.invalid/firefox/three', 'Firefox three', 1);
INSERT INTO moz_places VALUES (4, 'https://synthetic.invalid/firefox/four', 'Firefox four', 1);
INSERT INTO moz_places VALUES (5, 'https://synthetic.invalid/firefox/unvisited', 'Never visited', 0);
INSERT INTO moz_historyvisits VALUES (1, 1, 1609459200000000, 0, 1);
INSERT INTO moz_historyvisits VALUES (2, 2, 1609459201000000, 0, 2);
INSERT INTO moz_historyvisits VALUES (3, 3, 1609459202000000, 0, 3);
INSERT INTO moz_historyvisits VALUES (4, 4, 1609459203000000, 0, 1);
INSERT INTO moz_bookmarks VALUES (1, 2, NULL, 0, 'root', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (2, 2, NULL, 1, 'toolbar', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (3, 2, NULL, 1, 'menu', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (4, 2, NULL, 1, 'unfiled', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (5, 2, NULL, 2, 'folder-a', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (6, 2, NULL, 2, 'folder-b', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (7, 2, NULL, 3, 'folder-c', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (8, 1, 1, 5, 'Bookmark one', 1609459200000000, 1609459200000000);
INSERT INTO moz_bookmarks VALUES (9, 1, 2, 5, 'Bookmark two', 1609459201000000, 1609459201000000);
INSERT INTO moz_bookmarks VALUES (10, 1, 3, 6, 'Bookmark three', 1609459202000000, 1609459202000000);
INSERT INTO moz_bookmarks VALUES (11, 1, 4, 7, 'Bookmark four', 1609459203000000, 1609459203000000);
""")
db.commit()
db.close()
