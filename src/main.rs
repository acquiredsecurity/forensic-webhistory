use anyhow::Result;
use clap::Parser;
use forensic_webhistory::cli::Cli;

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();
    forensic_webhistory::app::run(Cli::parse())
}
