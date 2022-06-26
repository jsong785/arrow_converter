use super::ReadBuffer;
use anyhow::Result;
use arrow::datatypes::Schema;
use arrow::json::reader::ReaderBuilder;
use serde::{Deserialize, Serialize};

pub use arrow::json::reader::Reader;

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Options {
    pub schema: Option<Schema>,
}

pub(crate) fn create_reader<R: ReadBuffer>(
    reader: R,
    options: &Options,
) -> Result<super::Readers<R>> {
    let build = ReaderBuilder::new();
    let res = match &options.schema {
        Some(schema) => build.with_schema(std::sync::Arc::new(schema.clone())),
        None => build.infer_schema(Some(1_usize)),
    }
    .build(reader)?;
    Ok(super::Readers::Json(super::ArrowAdapter { inner: res }))
}
