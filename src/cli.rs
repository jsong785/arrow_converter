use anyhow::Result;
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    #[clap(flatten)]
    input: crate::convert_to_arrow::File,
    //#[clap(flatten)]
    //outout: convert_to_arrow::File,
}

impl Cli {
    pub fn execute(&self) -> Result<()> {
        Ok(())
    }
}
