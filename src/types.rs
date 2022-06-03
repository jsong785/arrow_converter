use clap::{ArgEnum, Args};

#[derive(ArgEnum, Clone, Debug)]
pub enum Type {
    Csv,
    Json,
    Xlsx,
    Parquet,
}

#[derive(Args, Debug)]
pub struct File {
    pub name: String,
    #[clap(arg_enum)]
    pub method: Type,
}
