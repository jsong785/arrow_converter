use anyhow::Result;
use arrow::record_batch::RecordBatch;

pub struct RecordBatchIteratorArrowAdapter<
    T: std::iter::Iterator<Item = arrow::error::Result<RecordBatch>>,
> {
    pub inner: T,
}

impl<T: std::iter::Iterator<Item = arrow::error::Result<RecordBatch>>> Iterator
    for RecordBatchIteratorArrowAdapter<T>
{
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(inner) => unsafe {
                if inner.is_ok() {
                    Some(Ok(inner.unwrap_unchecked()))
                } else {
                    Some(Err(inner.unwrap_err_unchecked().into()))
                }
            },
            None => None,
        }
    }
}

pub mod csv {
    use anyhow::Result;
    use arrow::csv::reader::Reader;
    use arrow::datatypes::SchemaRef;
    use std::io::{Read, Seek};

    #[derive(Default)]
    pub struct FileInfo {
        pub file_name: String,
        pub schema: Option<SchemaRef>,
        pub has_header: bool,
    }

    pub fn create_reader(info: &FileInfo) -> Result<Reader<std::fs::File>> {
        use std::fs::File;
        create_reader_with_buffer(File::open(&info.file_name)?, info)
    }

    pub(crate) fn create_reader_with_buffer<R: Read + Seek>(
        reader: R,
        info: &FileInfo,
    ) -> Result<Reader<R>> {
        use arrow::csv::reader::ReaderBuilder;
        let mut build = ReaderBuilder::new().has_header(info.has_header);
        build = match &info.schema {
            Some(schema) => build.with_schema(schema.clone()),
            None => build.infer_schema(Some(1_usize)),
        };
        Ok(build.build(reader)?)
    }
}

pub mod json {
    use anyhow::Result;
    use arrow::datatypes::SchemaRef;
    use arrow::json::reader::{Reader, ReaderBuilder};
    use std::io::{Read, Seek};

    #[derive(Default)]
    pub struct FileInfo {
        pub file_name: String,
        pub schema: Option<SchemaRef>,
    }

    pub fn create_reader(info: &FileInfo) -> Result<Reader<std::fs::File>> {
        use std::fs::File;
        create_reader_with_buffer(File::open(&info.file_name)?, info)
    }

    pub(crate) fn create_reader_with_buffer<R: Read + Seek>(
        reader: R,
        info: &FileInfo,
    ) -> Result<Reader<R>> {
        let mut build = ReaderBuilder::new();
        build = match &info.schema {
            Some(schema) => build.with_schema(schema.clone()),
            None => build.infer_schema(Some(1_usize)),
        };
        Ok(build.build(reader)?)
    }
}
