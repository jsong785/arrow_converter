use super::ReadBuffer;
use anyhow::Result;
use arrow::datatypes::SchemaRef;

#[derive(Default, Debug)]
pub struct Options {
    pub schema: Option<SchemaRef>,
    pub has_header: bool,
}

pub(crate) fn create_reader(
    reader: impl ReadBuffer,
    options: &Options,
) -> Result<impl super::Reader> {
    use arrow::csv::reader::ReaderBuilder;
    let mut build = ReaderBuilder::new().has_header(options.has_header);
    build = match &options.schema {
        Some(schema) => build.with_schema(schema.clone()),
        None => build.infer_schema(Some(1_usize)),
    };
    Ok(super::ArrowAdapter {
        inner: build.build(reader)?,
    })
}
