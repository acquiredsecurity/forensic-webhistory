use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "webx",
    about = "WebX — Forensic Browser Artifact Analyzer",
    long_about = "Extract browsing history, downloads, cookies, autofill, bookmarks, login metadata,\n\
                  keyword searches, and extensions from Chrome, Firefox, IE/Edge, Brave, Opera, Vivaldi, Arc, and Safari.\n\n\
                  Set RUST_LOG=debug for verbose logging.",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
    /// Launch interactive menu
    #[arg(short = 'i', long)]
    pub interactive: bool,
    /// Verbose output
    #[arg(short, long, global = true)]
    pub verbose: bool,
    /// Date format for CSV output. Use "iso" for "%Y-%m-%d %H:%M:%S",
    /// or provide a custom strftime format string. Default: "%m/%d/%Y %I:%M:%S %p"
    #[arg(long, global = true, default_value = "%m/%d/%Y %I:%M:%S %p")]
    pub date_format: String,
}

#[derive(Subcommand)]
pub enum Commands {
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

pub fn resolve_date_format(fmt: &str) -> &str {
    match fmt.to_lowercase().as_str() {
        "iso" | "iso8601" => "%Y-%m-%d %H:%M:%S",
        _ => fmt,
    }
}
