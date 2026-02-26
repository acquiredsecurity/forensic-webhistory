```
FORENSIC BROWSER HISTORY ANALYZER
```

# Forensic Browser History Analyzer

Cross-platform browser history forensic extractor written in Rust. Point it at a triage folder (KAPE output, mounted disk image, or raw filesystem) and it will automatically detect and extract browsing history from every supported browser database format.

## Supported Browsers

| Browser | Database Format | Engine |
|---------|----------------|--------|
| Google Chrome | SQLite (`History`) | Chromium |
| Microsoft Edge (Chromium) | SQLite (`History`) | Chromium |
| Brave | SQLite (`History`) | Chromium |
| Opera | SQLite (`History`) | Chromium |
| Vivaldi | SQLite (`History`) | Chromium |
| Arc | SQLite (`History`) | Chromium |
| Mozilla Firefox | SQLite (`places.sqlite`) | Gecko |
| Apple Safari | SQLite (`History.db`) | WebKit |
| Internet Explorer 10/11 | ESE (`WebCacheV01.dat`) | Trident |
| Microsoft Edge (Legacy) | ESE (`WebCacheV01.dat`) | EdgeHTML |

## Installation

### From Source

```bash
git clone https://github.com/acquiredsecurity/forensic-webhistory.git
cd forensic-webhistory
cargo build --release
```

The binary will be at `target/release/forensic-webhistory`.

### Pre-built Binaries

Check the [Releases](https://github.com/acquiredsecurity/forensic-webhistory/releases) page for pre-compiled binaries for Windows, macOS (x86 + ARM), and Linux.

## Usage

### Interactive Mode

Run without arguments or with `-i` for the interactive menu:

```bash
forensic-webhistory
forensic-webhistory -i
```

### Scan a Triage Directory

Recursively scans a directory for all browser artifacts and extracts history from everything it finds:

```bash
forensic-webhistory scan -d /path/to/triage/folder -o /path/to/output/
```

Example with KAPE triage output:

```bash
forensic-webhistory scan -d /cases/CASE001/Triage/ -o /cases/CASE001/output/webhistory/
```

### Carve Deleted Browser History

Recover deleted browsing history from SQLite freelist pages, WAL files, and raw byte scanning:

```bash
forensic-webhistory carve -d /path/to/triage/folder -o /path/to/output/
```

### Extract from a Specific File

Extract history from a single browser database:

```bash
# Chrome / Chromium-based (auto-detected)
forensic-webhistory extract -i /path/to/History -o chrome_history.csv

# Firefox (auto-detected)
forensic-webhistory extract -i /path/to/places.sqlite -o firefox_history.csv

# Safari (auto-detected)
forensic-webhistory extract -i /path/to/History.db -o safari_history.csv

# IE/Edge Legacy (auto-detected)
forensic-webhistory extract -i /path/to/WebCacheV01.dat -o ie_history.csv

# Specify browser explicitly
forensic-webhistory extract -i /path/to/History -o output.csv --browser brave
```

### Verbose Logging

```bash
RUST_LOG=debug forensic-webhistory scan -d /path/to/triage/ -o /output/
```

## Output Format

### Live Browser History CSV

Outputs CSV files compatible with NirSoft BrowsingHistoryView format:

| Column | Description |
|--------|-------------|
| URL | Full URL visited |
| Title | Page title (if available) |
| Visit Time | Timestamp in UTC (`MM/DD/YYYY HH:MM:SS AM/PM`) |
| Visit Count | Number of times the URL was visited |
| Visited From | Referring URL (if available) |
| Visit Type | Link, Typed, Bookmark, Reload, etc. |
| Visit Duration | Duration on page (if available) |
| Web Browser | Browser name |
| User Profile | Windows username (extracted from path or URL) |
| Browser Profile | Profile directory name |
| URL Length | Character length of URL |
| Typed Count | Times URL was typed into address bar |
| History File | Full path to source database |
| Record ID | Internal database record ID |
| NaturalLanguage | Human-readable event narrative for semantic indexing |

### Carved (Recovered) History CSV

| Column | Description |
|--------|-------------|
| URL | Full URL recovered |
| Title | Page title (if found nearby in binary data) |
| Visit Time | Timestamp (if a valid timestamp was found near the URL) |
| Browser Hint | Likely browser based on file path |
| Recovery Source | Where the entry was found: Freelist Page, WAL File, or Raw Scan |
| Source File | Path to the database file that was carved |
| NaturalLanguage | Human-readable event narrative for semantic indexing |

## Deleted History Carving

The `carve` command recovers deleted browsing history entries that still exist in database files. SQLite databases lazily reuse freed pages, so deleted records often persist until overwritten.

Recovery sources:
- **Freelist Pages** — SQLite pages marked as free but still containing URL data
- **WAL (Write-Ahead Log)** — Uncommitted or rolled-back entries in the WAL file
- **Raw Scan** — Byte-level URL pattern matching in unallocated database space

## How It Works

1. **Scanner** recursively walks the triage directory looking for known browser database files
2. **Browser detection** identifies the browser type from file paths and names
3. **Extractors** read the database (copying to a temp file first to avoid lock conflicts):
   - **Chromium**: Reads `urls` + `visits` tables from SQLite, converts WebKit timestamps (microseconds since 1601-01-01 UTC)
   - **Firefox**: Reads `moz_places` + `moz_historyvisits` tables from SQLite, converts PRTime (microseconds since 1970-01-01 UTC)
   - **Safari**: Reads `history_items` + `history_visits` tables from SQLite, converts Core Data timestamps (seconds since 2001-01-01 UTC)
   - **IE/Edge**: Reads `Containers` and `Container_N` tables from ESE database, parses `Visited: User@URL` format, converts FILETIME (100ns since 1601-01-01 UTC), deduplicates History/MSHist entries
4. **Carver** scans database files for deleted records in freelist pages, WAL files, and raw byte patterns
5. **Output** writes all entries in NirSoft-compatible CSV format with all timestamps in UTC

## Building

Requires Rust 1.70+.

```bash
cargo build              # Debug build
cargo build --release    # Optimized release build
cargo test               # Run unit tests
```

No external dependencies required at runtime. SQLite and libesedb are compiled from source and statically linked.

## License

MIT
