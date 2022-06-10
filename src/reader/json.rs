use super::ReadBuffer;
use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::json::reader::ReaderBuilder;

#[derive(Default, Debug)]
pub struct Options {
    pub file_name: String,
    pub schema: Option<SchemaRef>,
}

pub(crate) fn create_reader(
    reader: impl ReadBuffer,
    options: &Options,
) -> Result<impl super::Reader> {
    let mut build = ReaderBuilder::new();
    build = match &options.schema {
        Some(schema) => build.with_schema(schema.clone()),
        None => build.infer_schema(Some(1_usize)),
    };
    Ok(super::ArrowAdapter {
        inner: build.build(reader)?,
    })
}