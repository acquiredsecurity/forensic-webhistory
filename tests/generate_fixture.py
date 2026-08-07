"""Generate a deterministic Chromium History fixture with no downloads."""
from pathlib import Path
import sqlite3

path = Path(__file__).parent / "fixtures" / "Users" / "synthetic" / "AppData" / "Local" / "Google" / "Chrome" / "User Data" / "Default" / "History"
path.parent.mkdir(parents=True, exist_ok=True)
if path.exists(): path.unlink()
db = sqlite3.connect(path)
db.executescript("""
CREATE TABLE urls (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INTEGER, typed_count INTEGER);
CREATE TABLE visits (id INTEGER PRIMARY KEY, url INTEGER, visit_time INTEGER, from_visit INTEGER, transition INTEGER);
CREATE TABLE downloads (id INTEGER PRIMARY KEY, current_path TEXT, target_path TEXT, start_time INTEGER, end_time INTEGER, received_bytes INTEGER, total_bytes INTEGER, state INTEGER, danger_type INTEGER, opened INTEGER, referrer TEXT, tab_url TEXT, mime_type TEXT, original_mime_type TEXT);
INSERT INTO urls VALUES (1, 'https://synthetic.invalid/example', 'Synthetic history', 1, 1);
INSERT INTO visits VALUES (1, 1, 13380163200000000, 0, 1);
""")
db.commit(); db.close()
