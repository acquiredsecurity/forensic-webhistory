use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use log::{debug, error, info, warn};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use forensic_webhistory::browsers::{self, BrowserType, HistoryEntry};
use forensic_webhistory::output;
use forensic_webhistory::scanner;

#[derive(Parser)]
#[command(
    name = "forensic-webhistory",
    about = "Forensic Browser History Analyzer",
    long_about = "Extract browsing history from Chrome, Firefox, IE/Edge, Brave, Opera, and Vivaldi.\n\
                  Outputs NirSoft BrowsingHistoryView-compatible CSV format.\n\n\
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
    /// Scan a triage directory for all browser artifacts and extract history
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
    },

    /// Extract history from a specific browser database file
    Extract {
        /// Path to browser database file (History, places.sqlite, WebCacheV01.dat)
        #[arg(short, long)]
        input: PathBuf,

        /// Output CSV file path (omit to write to stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Browser type: chrome, firefox, ie (auto-detected if omitted)
        #[arg(short, long)]
        browser: Option<String>,

        /// Username to include in output
        #[arg(short, long)]
        user: Option<String>,
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
        Commands::Scan { dir, output, user } => {
            cmd_scan(&dir, &output, user.as_deref(), cli.verbose)
        }
        Commands::Extract {
            input,
            output,
            browser,
            user,
        } => cmd_extract(
            &input,
            output.as_deref(),
            browser.as_deref(),
            user.as_deref(),
            cli.verbose,
        ),
    }
}

fn interactive_menu() -> Result<()> {
    println!();
    println!(
        "  Forensic Browser History Analyzer v{}",
        env!("CARGO_PKG_VERSION")
    );
    println!();
    println!("  Supported Browsers:");
    println!("    Chrome, Edge Chromium, Brave, Opera, Vivaldi (SQLite)");
    println!("    Firefox (places.sqlite)");
    println!("    Internet Explorer / Edge Legacy (WebCacheV01.dat ESE)");
    println!();

    loop {
        println!("  [1] Scan triage directory (auto-detect)");
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
                match cmd_scan(&dir, &output, user.as_deref(), true) {
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
                    true,
                ) {
                    Ok(()) => println!("\n  Done!\n"),
                    Err(e) => println!("\n  Error: {e}\n"),
                }
            }
            "3" => {
                println!();
                println!("  USAGE:");
                println!("    forensic-webhistory scan -d <triage_dir> -o <output_dir>");
                println!("    forensic-webhistory extract -i <db_file> -o <output.csv>");
                println!();
                println!("  SCAN MODE:");
                println!("    Recursively scans a triage directory (KAPE output, mounted image)");
                println!(
                    "    for browser databases and extracts history from all found artifacts."
                );
                println!();
                println!("  EXTRACT MODE:");
                println!("    Extracts history from a single browser database file.");
                println!(
                    "    Auto-detects browser from filename (History, places.sqlite, WebCacheV01.dat)."
                );
                println!();
                println!("  OUTPUT FORMAT:");
                println!("    NirSoft BrowsingHistoryView-compatible CSV.");
                println!("    All timestamps in UTC.");
                println!();
                println!("  LOGGING:");
                println!("    RUST_LOG=debug forensic-webhistory scan ...");
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

fn cmd_scan(dir: &Path, output_dir: &Path, user: Option<&str>, _verbose: bool) -> Result<()> {
    if !dir.exists() {
        anyhow::bail!("Directory not found: {}", dir.display());
    }

    info!("Scanning for browser artifacts in {}", dir.display());

    let artifacts = scanner::scan(dir);

    if artifacts.is_empty() {
        warn!("No browser artifacts found in {}", dir.display());
        return Ok(());
    }

    info!("Found {} browser artifact(s):", artifacts.len());
    for a in &artifacts {
        info!(
            "  {} — {} [{}]",
            a.browser.display_name(),
            a.profile_name,
            if a.username.is_empty() {
                "unknown user"
            } else {
                &a.username
            }
        );
    }

    std::fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "Failed to create output directory: {}",
            output_dir.display()
        )
    })?;

    let mut total = 0usize;
    let mut errors = 0usize;

    for (idx, artifact) in artifacts.iter().enumerate() {
        let username = user.unwrap_or(&artifact.username);
        let db_path = PathBuf::from(&artifact.db_path);

        debug!(
            "Processing artifact {}/{}: {} at {}",
            idx + 1,
            artifacts.len(),
            artifact.browser.display_name(),
            artifact.db_path
        );

        let entries = match artifact.browser {
            BrowserType::InternetExplorer => browsers::webcache::extract(&db_path, username),
            BrowserType::Firefox => browsers::firefox::extract(&db_path, username),
            _ => browsers::chrome::extract(&db_path, username, Some(artifact.browser)),
        };

        match entries {
            Ok(entries) => {
                let label = format!(
                    "{}{}",
                    artifact.browser.display_name().replace([' ', '/'], "_"),
                    if artifact.profile_name.is_empty() {
                        String::new()
                    } else {
                        format!("_{}", artifact.profile_name)
                    }
                );
                let out_file = output_dir.join(format!("{label}.csv"));
                let count = output::write_csv(&entries, &out_file)?;
                info!(
                    "  [{}/{}] {} — {} entries -> {}",
                    idx + 1,
                    artifacts.len(),
                    artifact.browser.display_name(),
                    count,
                    out_file.display()
                );
                total += count;
            }
            Err(e) => {
                error!(
                    "  [{}/{}] {} — FAILED: {}",
                    idx + 1,
                    artifacts.len(),
                    artifact.browser.display_name(),
                    e
                );
                errors += 1;
            }
        }
    }

    info!("");
    info!(
        "Complete: {} entries extracted from {} artifact(s) ({} errors)",
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
    _verbose: bool,
) -> Result<()> {
    if !input.exists() {
        anyhow::bail!("File not found: {}", input.display());
    }

    let username = user.unwrap_or("");
    let file_name = input.file_name().and_then(|n| n.to_str()).unwrap_or("");

    info!("Extracting from: {}", input.display());

    let entries: Vec<HistoryEntry> = match browser.map(|b| b.to_lowercase()).as_deref() {
        Some("chrome") | Some("chromium") | Some("edge") | Some("brave") | Some("opera")
        | Some("vivaldi") => {
            let bt = match browser.unwrap().to_lowercase().as_str() {
                "edge" => BrowserType::EdgeChromium,
                "brave" => BrowserType::Brave,
                "opera" => BrowserType::Opera,
                "vivaldi" => BrowserType::Vivaldi,
                "chromium" => BrowserType::Chromium,
                _ => BrowserType::Chrome,
            };
            info!("Browser: {} (specified)", bt.display_name());
            browsers::chrome::extract(input, username, Some(bt))?
        }
        Some("firefox") => {
            info!("Browser: Firefox (specified)");
            browsers::firefox::extract(input, username)?
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
            "Unknown browser '{}'. Valid: chrome, firefox, ie, edge, brave, opera, vivaldi",
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

    Ok(())
}
