use anyhow::Result;
use arrow::record_batch::RecordBatch;

use crate::types::Type;

pub fn create_reader<'a>(
    t: Type,
    r: impl std::io::Read + std::io::Seek + 'a,
) -> Result<Option<Box<dyn 'a + std::iter::Iterator<Item = Result<RecordBatch>>>>> {
    match t {
        Type::Csv => Ok(Some(Box::new(RecordBatchIteratorArrowAdapter {
            inner: csv::create_reader(r, None)?,
        }))),
        Type::Json => Ok(Some(Box::new(RecordBatchIteratorArrowAdapter {
            inner: json::create_reader(r, None)?,
        }))),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}

struct RecordBatchIteratorArrowAdapter<
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

pub(super) mod csv {
    use anyhow::Result;
    use arrow::csv::reader::Reader;
    use arrow::datatypes::SchemaRef;
    use std::io::{Read, Seek};

    pub(super) fn create_reader<R: Read + Seek>(
        reader: R,
        schema: Option<SchemaRef>,
    ) -> Result<Reader<R>> {
        use arrow::csv::reader::ReaderBuilder;
        let mut build = ReaderBuilder::new().has_header(true);
        build = match schema {
            Some(schema) => build.with_schema(schema),
            None => build.infer_schema(Some(1)),
        };
        Ok(build.build(reader)?)
    }
}

pub(super) mod json {
    use anyhow::Result;
    use arrow::datatypes::SchemaRef;
    use arrow::json::reader::{Reader, ReaderBuilder};
    use std::io::{Read, Seek};

    pub(super) fn create_reader<R: Read + Seek>(
        reader: R,
        schema: Option<SchemaRef>,
    ) -> Result<Reader<R>> {
        let mut build = ReaderBuilder::new();
        build = match schema {
            Some(schema) => build.with_schema(schema),
            None => build.infer_schema(Some(1)),
        };
        Ok(build.build(reader)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_reader() -> Result<()> {
        use std::io::Cursor;

        assert!(create_reader(Type::Csv, Cursor::new(Vec::new()))?.is_some());
        assert!(create_reader(Type::Json, Cursor::new(Vec::new()))?.is_some());
        assert!(create_reader(Type::Xlsx, Cursor::new(Vec::new()))?.is_none());
        assert!(create_reader(Type::Parquet, Cursor::new(Vec::new()))?.is_none());
        Ok(())
    }
}
