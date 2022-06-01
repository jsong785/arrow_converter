pub mod cli;
use clap::Parser;
use anyhow::Result;

fn main() -> Result<()> {
    cli::Cli::try_parse()?.execute()
}
