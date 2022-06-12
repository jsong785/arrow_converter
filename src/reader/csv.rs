use super::ReadBuffer;
use anyhow::Result;
use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

pub use arrow::csv::reader::Reader;

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Options {
    pub schema: Option<Schema>,
    pub has_header: bool,
}

pub(crate) fn create_reader<R: ReadBuffer>(
    reader: R,
    options: &Options,
) -> Result<super::Readers<R>> {
    use arrow::csv::reader::ReaderBuilder;
    let mut build = ReaderBuilder::new().has_header(options.has_header);
    build = match &options.schema {
        Some(schema) => build.with_schema(std::sync::Arc::new(schema.clone())),
        None => build.infer_schema(Some(1_usize)),
    };
    Ok(super::Readers::Csv(super::ArrowAdapter {
        inner: build.build(reader)?,
    }))
}
