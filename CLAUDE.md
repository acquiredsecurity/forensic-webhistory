# Forensic Browser History Analyzer

## Project Overview
Cross-platform browser history forensic extractor written in Rust. Extracts browsing history from Chrome, Firefox, IE/Edge (and variants like Brave, Opera, Vivaldi) into CSV format.

## Architecture
- `src/main.rs` — CLI entry point (clap subcommands: `scan` and `extract`)
- `src/browsers/mod.rs` — Shared types: `HistoryEntry`, `BrowserArtifact`, `BrowserType`
- `src/browsers/chrome.rs` — Chrome/Chromium SQLite History extractor (also Edge Chromium, Brave, Opera, Vivaldi)
- `src/browsers/firefox.rs` — Firefox places.sqlite extractor
- `src/browsers/webcache.rs` — IE/Edge Legacy WebCacheV01.dat ESE database extractor
- `src/scanner.rs` — Auto-detect browser artifacts in triage directories (KAPE output, mounted images)
- `src/output.rs` — NirSoft-compatible CSV writer

## Key Decisions
- All timestamps stored as `DateTime<Utc>` — output always in UTC
- Chrome timestamps: microseconds since 1601-01-01 (WebKit epoch)
- Firefox timestamps: microseconds since 1970-01-01 (PRTime/Unix epoch)
- IE/Edge timestamps: FILETIME (100ns intervals since 1601-01-01)
- ESE database parsing via `libesedb` crate (C library compiled from source via `libesedb-sys`)
- SQLite via `rusqlite` with `bundled` feature (no system SQLite dependency)
- Databases copied to temp directory before reading (browsers lock their DBs)
- IE WebCache deduplication by (URL, Visit Time) — same entries in History + MSHist containers

## Build & Test
```bash
cargo build              # Debug build
cargo build --release    # Release build (optimized)
cargo test               # Run unit tests
```

## Usage
```bash
# Scan a triage directory for all browser artifacts
forensic-webhistory scan -d /path/to/triage/ -o /path/to/output/

# Extract from a specific database file
forensic-webhistory extract -i /path/to/History -o output.csv
forensic-webhistory extract -i /path/to/places.sqlite -o output.csv
forensic-webhistory extract -i /path/to/WebCacheV01.dat -o output.csv
```

## Output Format
CSV with columns:
URL, Title, Visit Time, Visit Count, Visited From, Visit Type, Visit Duration,
Web Browser, User Profile, Browser Profile, URL Length, Typed Count, History File, Record ID
