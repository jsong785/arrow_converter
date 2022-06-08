use anyhow::Result;
use arrow::record_batch::RecordBatch;

pub trait Reader: std::iter::Iterator<Item = Result<RecordBatch>> {}

struct ArrowAdapter<T: std::iter::Iterator<Item = arrow::error::Result<RecordBatch>>> {
    inner: T,
}

pub trait ReadBuffer: std::io::Read + std::io::Seek {}
impl<T: std::io::Read + std::io::Seek> ReadBuffer for T {}

impl<T: std::iter::Iterator<Item = arrow::error::Result<RecordBatch>>> std::iter::Iterator
    for ArrowAdapter<T>
{
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(inner) => {
                if inner.is_ok() {
                    unsafe { Some(Ok(inner.unwrap_unchecked())) }
                } else {
                    unsafe { Some(Err(inner.unwrap_err_unchecked().into())) }
                }
            }
            None => None,
        }
    }
}
impl<T: std::iter::Iterator<Item = arrow::error::Result<RecordBatch>>> Reader for ArrowAdapter<T> {}

pub mod csv {
    use super::ReadBuffer;
    use anyhow::Result;
    use arrow::datatypes::SchemaRef;

    #[derive(Default, Debug)]
    pub struct Options {
        pub file_name: String,
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
}

pub mod json {
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
}
