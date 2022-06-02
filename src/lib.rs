pub mod convert_to_arrow;
pub mod convert_from_arrow;
pub mod convert_pipe;
pub mod types;

use anyhow::Result;
use clap::Parser;
use crate::types::File;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(flatten)]
    input: File,
    //#[clap(short, long)]
    //outout: File,
}

impl Cli {
    fn execute(&self) -> Result<()> {
        Ok(())
    }
}

pub fn run_cli() -> Result<()> {
    Cli::try_parse()?.execute()
}
