use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use log::{error, info, warn};
use std::collections::HashSet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use forensic_webhistory::browsers::{self, ArtifactType, BrowserType, HistoryEntry};
use forensic_webhistory::carver;
use forensic_webhistory::output;
use forensic_webhistory::scanner;

#[derive(Parser)]
#[command(
    name = "webx",
    about = "WebX — Forensic Browser Artifact Analyzer",
    long_about = "Extract browsing history, downloads, cookies, autofill, bookmarks, login metadata,\n\
                  keyword searches, and extensions from Chrome, Firefox, IE/Edge, Brave, Opera, Vivaldi, Arc, and Safari.\n\n\
                  Set RUST_LOG=debug for verbose logging.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Launch interactive menu
    #[arg(short = 'i', long)]
    interactive: bool,

    /// Verbose output
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan a triage directory for all browser artifacts and extract everything
    Scan {
        /// Path to triage directory (KAPE output, mounted image, etc.)
        #[arg(short, long)]
        dir: PathBuf,

        /// Output directory for CSV files
        #[arg(short, long)]
        output: PathBuf,

        /// Override username (auto-detected from path if omitted)
        #[arg(short, long)]
        user: Option<String>,

        /// Also write Parquet output alongside CSV
        #[arg(long = "out")]
        parquet_dir: Option<PathBuf>,

        /// Artifact types to extract (comma-separated). Default: all.
        /// Options: history,downloads,keywords,cookies,autofill,bookmarks,logins,extensions
        #[arg(long, value_delimiter = ',')]
        artifacts: Option<Vec<String>>,
    },

    /// Carve deleted/residual browser history from database files
    Carve {
        /// Path to browser database file (or directory to scan)
        #[arg(short, long)]
        input: PathBuf,

        /// Output CSV file for recovered entries
        #[arg(short, long)]
        output: PathBuf,
    },

    /// Extract from a specific browser database file
    Extract {
        /// Path to browser database file (History, places.sqlite, WebCacheV01.dat, Cookies, etc.)
        #[arg(short, long)]
        input: PathBuf,

        /// Output CSV file path (omit to write to stdout for history)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Browser type: chrome, firefox, ie, safari (auto-detected if omitted)
        #[arg(short, long)]
        browser: Option<String>,

        /// Username to include in output
        #[arg(short, long)]
        user: Option<String>,

        /// Also write Parquet output alongside CSV
        #[arg(long = "out")]
        parquet_dir: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();

    let cli = Cli::parse();

    if cli.interactive || cli.command.is_none() {
        return interactive_menu();
    }

    match cli.command.unwrap() {
        Commands::Scan {
            dir,
            output,
            user,
            parquet_dir,
            artifacts,
        } => cmd_scan(
            &dir,
            &output,
            user.as_deref(),
            parquet_dir.as_deref(),
            &parse_artifact_filter(&artifacts),
        ),
        Commands::Carve { input, output } => cmd_carve(&input, &output),
        Commands::Extract {
            input,
            output,
            browser,
            user,
            parquet_dir,
        } => cmd_extract(
            &input,
            output.as_deref(),
            browser.as_deref(),
            user.as_deref(),
            parquet_dir.as_deref(),
        ),
    }
}

fn parse_artifact_filter(artifacts: &Option<Vec<String>>) -> HashSet<ArtifactType> {
    match artifacts {
        None => vec![
            ArtifactType::History,
            ArtifactType::Downloads,
            ArtifactType::KeywordSearches,
            ArtifactType::Cookies,
            ArtifactType::Autofill,
            ArtifactType::Bookmarks,
            ArtifactType::LoginData,
            ArtifactType::Extensions,
        ]
        .into_iter()
        .collect(),
        Some(list) => list
            .iter()
            .filter_map(|s| match s.to_lowercase().as_str() {
                "history" => Some(ArtifactType::History),
                "downloads" => Some(ArtifactType::Downloads),
                "keywords" | "searches" => Some(ArtifactType::KeywordSearches),
                "cookies" => Some(ArtifactType::Cookies),
                "autofill" | "forms" => Some(ArtifactType::Autofill),
                "bookmarks" => Some(ArtifactType::Bookmarks),
                "logins" | "passwords" | "login_data" => Some(ArtifactType::LoginData),
                "extensions" | "addons" => Some(ArtifactType::Extensions),
                _ => {
                    warn!("Unknown artifact type: {}", s);
                    None
                }
            })
            .collect(),
    }
}

fn interactive_menu() -> Result<()> {
    println!();
    println!(
        "  WebX — Forensic Browser Artifact Analyzer v{}",
        env!("CARGO_PKG_VERSION")
    );
    println!();
    println!("  Supported Browsers:");
    println!("    Chrome, Edge Chromium, Brave, Opera, Vivaldi, Arc (SQLite)");
    println!("    Firefox (places.sqlite)");
    println!("    Safari (History.db — macOS)");
    println!("    Internet Explorer / Edge Legacy (WebCacheV01.dat ESE)");
    println!();
    println!("  Artifact Types (all extracted by default):");
    println!("    History, Downloads, Keywords, Cookies, Autofill, Bookmarks, Logins, Extensions");
    println!();

    loop {
        println!("  [1] Scan triage directory (auto-detect all artifacts)");
        println!("  [2] Extract from specific database file");
        println!("  [3] Show help");
        println!("  [0] Exit");
        print!("\n  Select option: ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let choice = input.trim();

        match choice {
            "1" => {
                let dir = prompt("  Triage directory path: ")?;
                let output = prompt("  Output directory path: ")?;
                let user = prompt_optional("  Username override (Enter to auto-detect): ")?;

                let dir = PathBuf::from(dir.trim());
                let output = PathBuf::from(output.trim());
                let all = parse_artifact_filter(&None);
                match cmd_scan(&dir, &output, user.as_deref(), None, &all) {
                    Ok(()) => println!("\n  Done!\n"),
                    Err(e) => println!("\n  Error: {e}\n"),
                }
            }
            "2" => {
                let file = prompt("  Database file path: ")?;
                let output = prompt_optional("  Output CSV path (Enter for stdout): ")?;
                let browser =
                    prompt_optional("  Browser type [chrome/firefox/ie] (Enter to auto-detect): ")?;
                let user = prompt_optional("  Username (Enter to skip): ")?;

                let file = PathBuf::from(file.trim());
                let output_path = output.as_ref().map(PathBuf::from);
                match cmd_extract(
                    &file,
                    output_path.as_deref(),
                    browser.as_deref(),
                    user.as_deref(),
                    None,
                ) {
                    Ok(()) => println!("\n  Done!\n"),
                    Err(e) => println!("\n  Error: {e}\n"),
                }
            }
            "3" => {
                println!();
                println!("  USAGE:");
                println!("    webx scan -d <triage_dir> -o <output_dir>");
                println!("    webx scan -d <triage_dir> -o <output_dir> --artifacts history,downloads,cookies");
                println!("    webx extract -i <db_file> -o <output.csv>");
                println!("    webx carve -i <db_file> -o <output.csv>");
                println!();
    println!("  ARTIFACT TYPES (all extracted by default):");
                println!("    history, downloads, keywords, cookies, autofill, bookmarks, logins, extensions");
                println!("    Use --artifacts to limit, e.g. --artifacts history,downloads");
                println!();
            }
            "0" | "q" | "quit" | "exit" => {
                println!("  Goodbye.");
                return Ok(());
            }
            _ => {
                println!("  Invalid option. Please select 0-3.\n");
            }
        }
    }
}

fn prompt(message: &str) -> Result<String> {
    print!("{message}");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let trimmed = input.trim().to_string();
    if trimmed.is_empty() {
        anyhow::bail!("Input required");
    }
    Ok(trimmed)
}

fn prompt_optional(message: &str) -> Result<Option<String>> {
    print!("{message}");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let trimmed = input.trim().to_string();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed))
    }
}

fn cmd_scan(
    dir: &Path,
    output_dir: &Path,
    user: Option<&str>,
    parquet_dir: Option<&Path>,
    artifact_filter: &HashSet<ArtifactType>,
) -> Result<()> {
    if !dir.exists() {
        anyhow::bail!("Directory not found: {}", dir.display());
    }

    info!("Scanning for browser artifacts in {}", dir.display());

    let artifacts = scanner::scan(dir);

    if artifacts.is_empty() {
        warn!("No browser artifacts found in {}", dir.display());
        return Ok(());
    }

    // Count by type
    let mut type_counts = std::collections::HashMap::new();
    for a in &artifacts {
        *type_counts
            .entry(a.artifact_type.display_name())
            .or_insert(0usize) += 1;
    }
    info!("Found {} artifact(s):", artifacts.len());
    for (atype, count) in &type_counts {
        info!("  {} x {}", count, atype);
    }

    std::fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "Failed to create output directory: {}",
            output_dir.display()
        )
    })?;

    let mut total = 0usize;
    let mut errors = 0usize;

    for artifact in &artifacts {
        if !artifact_filter.contains(&artifact.artifact_type) {
            continue;
        }

        let username = user.unwrap_or(&artifact.username);
        let db_path = PathBuf::from(&artifact.db_path);
        let label = format!(
            "{}_{}_{}{}",
            artifact.browser.display_name().replace([' ', '/'], "_"),
            artifact.artifact_type.file_suffix(),
            username.replace([' ', '/', '\\'], "_"),
            if artifact.profile_name.is_empty() {
                String::new()
            } else {
                format!("_{}", artifact.profile_name)
            }
        );

        match artifact.artifact_type {
            ArtifactType::History => {
                let entries = match artifact.browser {
                    BrowserType::InternetExplorer => {
                        browsers::webcache::extract(&db_path, username)
                    }
                    BrowserType::Firefox => browsers::firefox::extract(&db_path, username),
                    BrowserType::Safari => browsers::safari::extract(&db_path, username),
                    _ => browsers::chrome::extract(&db_path, username, Some(artifact.browser)),
                };
                match entries {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        if let Some(pq_dir) = parquet_dir {
                            let pq_file = pq_dir.join(format!("{label}.parquet"));
                            output::write_parquet(&entries, &pq_file)?;
                        }
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
            ArtifactType::Downloads => {
                let entries = if artifact.browser.is_chromium() {
                    browsers::chrome_downloads::extract(&db_path, username, Some(artifact.browser))
                } else if artifact.browser == BrowserType::Firefox {
                    browsers::firefox_downloads::extract(&db_path, username)
                } else {
                    continue;
                };
                match entries {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_downloads_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        if let Some(pq_dir) = parquet_dir {
                            let pq_file = pq_dir.join(format!("{label}.parquet"));
                            output::write_downloads_parquet(&entries, &pq_file)?;
                        }
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
            ArtifactType::KeywordSearches => {
                if !artifact.browser.is_chromium() {
                    continue;
                }
                match browsers::chrome_keywords::extract(
                    &db_path,
                    username,
                    Some(artifact.browser),
                ) {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_keywords_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
            ArtifactType::Cookies => {
                let entries = if artifact.browser.is_chromium() {
                    browsers::chrome_cookies::extract(&db_path, username, Some(artifact.browser))
                } else if artifact.browser == BrowserType::Firefox {
                    browsers::firefox_cookies::extract(&db_path, username)
                } else {
                    continue;
                };
                match entries {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_cookies_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
            ArtifactType::Autofill => {
                let entries = if artifact.browser.is_chromium() {
                    browsers::chrome_autofill::extract(&db_path, username, Some(artifact.browser))
                } else if artifact.browser == BrowserType::Firefox {
                    browsers::firefox_autofill::extract(&db_path, username)
                } else {
                    continue;
                };
                match entries {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_autofill_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
            ArtifactType::Bookmarks => {
                let entries = if artifact.browser.is_chromium() {
                    browsers::chrome_bookmarks::extract(&db_path, username, Some(artifact.browser))
                } else if artifact.browser == BrowserType::Firefox {
                    browsers::firefox_bookmarks::extract(&db_path, username)
                } else {
                    continue;
                };
                match entries {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_bookmarks_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
            ArtifactType::LoginData => {
                let entries = if artifact.browser.is_chromium() {
                    browsers::chrome_logins::extract(&db_path, username, Some(artifact.browser))
                } else if artifact.browser == BrowserType::Firefox {
                    browsers::firefox_logins::extract(&db_path, username)
                } else {
                    continue;
                };
                match entries {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_logins_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
            ArtifactType::Extensions => {
                let entries = if artifact.browser.is_chromium() {
                    browsers::chrome_extensions::extract(&db_path, username, Some(artifact.browser))
                } else if artifact.browser == BrowserType::Firefox {
                    browsers::firefox_extensions::extract(&db_path, username)
                } else {
                    continue;
                };
                match entries {
                    Ok(entries) => {
                        let out_file = output_dir.join(format!("{label}.csv"));
                        let count = output::write_extensions_csv(&entries, &out_file)?;
                        info!("  {} — {} entries -> {}", label, count, out_file.display());
                        total += count;
                    }
                    Err(e) => {
                        error!("  {} — FAILED: {}", label, e);
                        errors += 1;
                    }
                }
            }
        }
    }

    info!("");
    info!(
        "Complete: {} total entries extracted from {} artifact(s) ({} errors)",
        total,
        artifacts.len(),
        errors
    );
    Ok(())
}

fn cmd_extract(
    input: &Path,
    output: Option<&Path>,
    browser: Option<&str>,
    user: Option<&str>,
    parquet_dir: Option<&Path>,
) -> Result<()> {
    if !input.exists() {
        anyhow::bail!("File not found: {}", input.display());
    }

    let username = user.unwrap_or("");
    let file_name = input.file_name().and_then(|n| n.to_str()).unwrap_or("");

    info!("Extracting from: {}", input.display());

    let entries: Vec<HistoryEntry> = match browser.map(|b| b.to_lowercase()).as_deref() {
        Some("chrome") | Some("chromium") | Some("edge") | Some("brave") | Some("opera")
        | Some("vivaldi") | Some("arc") => {
            let bt = match browser.unwrap().to_lowercase().as_str() {
                "edge" => BrowserType::EdgeChromium,
                "brave" => BrowserType::Brave,
                "opera" => BrowserType::Opera,
                "vivaldi" => BrowserType::Vivaldi,
                "chromium" => BrowserType::Chromium,
                "arc" => BrowserType::Arc,
                _ => BrowserType::Chrome,
            };
            info!("Browser: {} (specified)", bt.display_name());
            browsers::chrome::extract(input, username, Some(bt))?
        }
        Some("firefox") => {
            info!("Browser: Firefox (specified)");
            browsers::firefox::extract(input, username)?
        }
        Some("safari") => {
            info!("Browser: Safari (specified)");
            browsers::safari::extract(input, username)?
        }
        Some("ie") | Some("edge-legacy") | Some("webcache") => {
            info!("Browser: IE/Edge Legacy (specified)");
            browsers::webcache::extract(input, username)?
        }
        None => match file_name {
            "History" => {
                info!("Browser: Chrome/Chromium (auto-detected from filename)");
                browsers::chrome::extract(input, username, None)?
            }
            "places.sqlite" => {
                info!("Browser: Firefox (auto-detected from filename)");
                browsers::firefox::extract(input, username)?
            }
            "History.db" => {
                info!("Browser: Safari (auto-detected from filename)");
                browsers::safari::extract(input, username)?
            }
            "WebCacheV01.dat" => {
                info!("Browser: IE/Edge Legacy (auto-detected from filename)");
                browsers::webcache::extract(input, username)?
            }
            _ => anyhow::bail!(
                "Cannot auto-detect browser from filename '{}'. Use --browser to specify.",
                file_name
            ),
        },
        Some(other) => anyhow::bail!(
            "Unknown browser '{}'. Valid: chrome, firefox, safari, ie, edge, brave, opera, vivaldi, arc",
            other
        ),
    };

    info!("Extracted {} history entries", entries.len());

    let _count = if let Some(out_path) = output {
        let c = output::write_csv(&entries, out_path)?;
        info!("Wrote {} entries to {}", c, out_path.display());
        c
    } else {
        output::write_csv_stdout(&entries)?
    };

    if let Some(pq_dir) = parquet_dir {
        let stem = input
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("webhistory");
        let pq_file = pq_dir.join(format!("{stem}.parquet"));
        output::write_parquet(&entries, &pq_file)?;
        info!("Parquet: {}", pq_file.display());
    }

    Ok(())
}

fn cmd_carve(input: &Path, output: &Path) -> Result<()> {
    if !input.exists() {
        anyhow::bail!("Path not found: {}", input.display());
    }

    let mut all_entries = Vec::new();

    if input.is_dir() {
        info!("Scanning for browser databases in {}", input.display());
        let db_names = ["History", "places.sqlite", "History.db"];

        for entry in walkdir::WalkDir::new(input)
            .follow_links(true)
            .max_depth(10)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            if db_names.contains(&name) {
                info!("  Carving: {}", entry.path().display());
                match carver::carve(entry.path()) {
                    Ok(entries) => {
                        info!("    Recovered {} entries", entries.len());
                        all_entries.extend(entries);
                    }
                    Err(e) => {
                        warn!("    Failed: {}", e);
                    }
                }
            }
        }
    } else {
        info!("Carving deleted entries from: {}", input.display());
        all_entries = carver::carve(input)?;
    }

    info!(
        "Total recovered: {} unique deleted entries",
        all_entries.len()
    );

    let count = carver::write_carved_csv(&all_entries, output)?;
    info!("Wrote {} entries to {}", count, output.display());

    Ok(())
}
