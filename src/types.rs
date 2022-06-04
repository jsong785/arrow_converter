//use clap::{ArgEnum, Args};

//#[derive(ArgEnum, Clone, Debug)]
pub enum Type {
    Csv(
        crate::convert_to_arrow::csv::FileInfo,
        crate::convert_from_arrow::csv::FileInfo,
    ),
    Json(
        crate::convert_to_arrow::json::FileInfo,
        crate::convert_from_arrow::json::FileInfo,
    ),
    Xlsx,
    Parquet,
}

//#[derive(Args, Debug)]
pub struct File {
    pub name: String,
    //#[clap(arg_enum)]
    pub method: Type,
}
