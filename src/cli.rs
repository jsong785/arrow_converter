use anyhow::Result;
use clap::{ Parser, Args, ArgEnum };

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    #[clap(flatten)]
    input: File,
    //#[clap(flatten)]
    //outout: File,
}

impl Cli {
    pub fn execute(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(ArgEnum, Clone, Debug)]
pub enum FileType {
    Csv,
    Json,
    Xlsx,
    Parquet,
}

#[derive(Args, Debug)]
pub struct File {
    name: String,
    #[clap(arg_enum)]
    method: FileType,
}
