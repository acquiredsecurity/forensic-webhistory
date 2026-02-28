<p align="center">
  <img src="logo.webp" alt="WebX Logo" width="100%">
</p>

# WebX — Forensic Browser Artifact Analyzer

Cross-platform browser artifact forensic extractor written in Rust. Point it at a triage folder (KAPE output, mounted disk image, or raw filesystem) and it will automatically detect and extract **all browser artifacts** from every supported browser — history, downloads, cookies, autofill, bookmarks, login metadata, keyword searches, and extensions.

## Supported Browsers

| Browser | Database Format | Engine |
|---------|----------------|--------|
| Google Chrome | SQLite (`History`, `Cookies`, `Web Data`, `Login Data`, `Bookmarks`, `Preferences`) | Chromium |
| Microsoft Edge (Chromium) | SQLite + JSON | Chromium |
| Brave | SQLite + JSON | Chromium |
| Opera | SQLite + JSON | Chromium |
| Vivaldi | SQLite + JSON | Chromium |
| Arc | SQLite + JSON | Chromium |
| Mozilla Firefox | SQLite + JSON (`places.sqlite`, `cookies.sqlite`, `formhistory.sqlite`, `logins.json`, `extensions.json`) | Gecko |
| Apple Safari | SQLite (`History.db`) | WebKit |
| Internet Explorer 10/11 | ESE (`WebCacheV01.dat`) | Trident |
| Microsoft Edge (Legacy) | ESE (`WebCacheV01.dat`) | EdgeHTML |

## Artifact Coverage

All artifact types are extracted by default. Use `--artifacts` to limit extraction to specific types.

| Artifact | Chrome/Edge/Brave/Opera/Vivaldi/Arc | Firefox | Safari | IE/Edge Legacy |
|---|---|---|---|---|
| **History** | SQLite `urls` + `visits` tables | SQLite `moz_places` + `moz_historyvisits` | SQLite `history_items` + `history_visits` | ESE `Containers` |
| **Downloads** | SQLite `downloads` + `downloads_url_chains` | SQLite `moz_annos` (modern) / `moz_downloads` (legacy) | — | — |
| **Keyword Searches** | SQLite `keyword_search_terms` + `urls` | — | — | — |
| **Cookies** | SQLite `cookies` table (separate `Cookies` DB) | SQLite `moz_cookies` | — | — |
| **Autofill / Form History** | SQLite `autofill` table (`Web Data` DB) | SQLite `moz_formhistory` | — | — |
| **Bookmarks** | JSON `Bookmarks` file (recursive tree walk) | SQLite `moz_bookmarks` + `moz_places` | — | — |
| **Login Data** | SQLite `logins` table (metadata only) | JSON `logins.json` (metadata only) | — | — |
| **Extensions** | JSON `Preferences` → `extensions.settings` | JSON `extensions.json` → `addons[]` | — | — |
| **Deleted History Carving** | Freelist + WAL + raw scan | Freelist + WAL + raw scan | Freelist + WAL + raw scan | — |

> **Security note:** Login Data extraction captures only metadata (URLs, usernames, timestamps, usage counts). **Passwords are NEVER extracted.**

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
webx
webx -i
```

### Scan a Triage Directory

Recursively scans a directory for all browser artifacts and extracts everything it finds:

```bash
# Extract all artifact types (default)
webx scan -d /path/to/triage/folder -o /path/to/output/

# Extract only specific artifact types
webx scan -d /path/to/triage/folder -o /path/to/output/ --artifacts history,downloads,cookies

# KAPE triage example
webx scan -d /cases/CASE001/Triage/ -o /cases/CASE001/output/
```

Available artifact type names for `--artifacts`:
`history`, `downloads`, `keywords`, `cookies`, `autofill`, `bookmarks`, `logins`, `extensions`

### Carve Deleted Browser History

Recover deleted browsing history from SQLite freelist pages, WAL files, and raw byte scanning:

```bash
webx carve -i /path/to/History -o /path/to/carved_output.csv
webx carve -i /path/to/triage/folder -o /path/to/carved_output.csv
```

### Extract from a Specific File

Extract from a single browser database:

```bash
# Chrome / Chromium-based (auto-detected)
webx extract -i /path/to/History -o chrome_history.csv

# Firefox (auto-detected)
webx extract -i /path/to/places.sqlite -o firefox_history.csv

# Safari (auto-detected)
webx extract -i /path/to/History.db -o safari_history.csv

# IE/Edge Legacy (auto-detected)
webx extract -i /path/to/WebCacheV01.dat -o ie_history.csv

# Specify browser explicitly
webx extract -i /path/to/History -o output.csv --browser brave
```

### Verbose Logging

```bash
RUST_LOG=debug webx scan -d /path/to/triage/ -o /output/
```

## Output Format

Each artifact type generates its own CSV file with the naming pattern:
`{Browser}_{artifact_type}_{username}_{profile}.csv`

### History CSV

| Column | Description |
|--------|-------------|
| URL | Full URL visited |
| Title | Page title |
| Visit Time | Timestamp in UTC |
| Visit Count | Number of visits to this URL |
| Visited From | Referring URL |
| Visit Type | Link, Typed, Bookmark, Reload, etc. |
| Visit Duration | Duration on page |
| Web Browser | Browser name |
| User Profile | OS username (extracted from path) |
| Browser Profile | Profile directory name |
| URL Length | Character length of URL |
| Typed Count | Times URL was typed into address bar |
| History File | Full path to source database |
| Record ID | Internal database record ID |
| NaturalLanguage | Human-readable event narrative for semantic indexing |

### Downloads CSV

| Column | Description |
|--------|-------------|
| URL | Download source URL |
| Target Path | Intended save location |
| Current Path | Actual file location |
| Start Time / End Time | Download timestamps |
| Received Bytes / Total Bytes | Download progress |
| State | In Progress, Complete, Cancelled, Interrupted |
| Danger Type | Not Dangerous, Dangerous File, Uncommon Content, etc. |
| MIME Type | File content type |
| Referrer / Tab URL | Origin of the download |

### Cookies CSV

| Column | Description |
|--------|-------------|
| Host | Cookie domain |
| Name / Path / Value | Cookie details |
| Creation Time / Expiry Time / Last Access Time | Timestamps |
| Secure / HttpOnly / Persistent / SameSite | Cookie flags |

### Autofill CSV

| Column | Description |
|--------|-------------|
| Field Name | Form field name |
| Value | Submitted value |
| Times Used | Usage count |
| First Used / Last Used | Timestamps |

### Bookmarks CSV

| Column | Description |
|--------|-------------|
| URL | Bookmarked URL |
| Title | Bookmark title |
| Date Added / Date Last Used | Timestamps |
| Folder Path | e.g., "Bookmarks Bar > Work > Research" |

### Login Data CSV

| Column | Description |
|--------|-------------|
| Origin URL / Action URL | Login page URLs |
| Username | Username field value |
| Date Created / Date Last Used / Date Password Modified | Timestamps |
| Times Used | Usage count |

### Keyword Searches CSV

| Column | Description |
|--------|-------------|
| Search Term / Normalized Term | Search queries |
| URL / Title | Search result page |
| Visit Time | When the search was performed |

### Extensions CSV

| Column | Description |
|--------|-------------|
| Extension ID | Unique identifier |
| Name / Version / Description | Extension metadata |
| Enabled | Active state |
| Install Time | When installed |
| Permissions | Granted permissions list |

### Carved (Recovered) History CSV

| Column | Description |
|--------|-------------|
| URL | Full URL recovered |
| Title | Page title (if found nearby in binary data) |
| Visit Time | Timestamp (if a valid timestamp was found near the URL) |
| Browser Hint | Likely browser based on file path |
| Recovery Source | Freelist Page, WAL File, or Raw Scan |
| Source File | Path to the database file that was carved |
| NaturalLanguage | Human-readable event narrative |

> All CSV columns include `Web Browser`, `User Profile`, `Browser Profile`, `Source File`, and `NaturalLanguage` fields.

## How It Works

1. **Scanner** recursively walks the triage directory looking for known browser database and JSON files
2. **Browser detection** identifies the browser type from file paths and names
3. **Artifact synthesis** — when a multi-artifact database is found (e.g., Chrome `History` contains both history and downloads), additional artifact entries are automatically created
4. **Extractors** read databases (copying to a temp file first to avoid lock conflicts, or opening read-only when possible):
   - **Chromium**: WebKit timestamps (microseconds since 1601-01-01 UTC)
   - **Firefox**: PRTime (microseconds since 1970-01-01 UTC), Unix milliseconds for logins
   - **Safari**: Core Data timestamps (seconds since 2001-01-01 UTC)
   - **IE/Edge**: FILETIME (100ns since 1601-01-01 UTC)
5. **Carver** scans database files for deleted records in freelist pages, WAL files, and raw byte patterns
6. **Output** writes per-artifact CSV files with all timestamps in UTC

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
