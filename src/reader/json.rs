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
) -> Result<super::ReaderWrap<R>> {
    let mut build = ReaderBuilder::new();
    build = match &options.schema {
        Some(schema) => build.with_schema(std::sync::Arc::new(schema.clone())),
        None => build.infer_schema(Some(1_usize)),
    };
    Ok(super::ReaderWrap {
        reader: super::Types::Json(super::ArrowAdapter {
            inner: build.build(reader)?,
        }),
    })
}
