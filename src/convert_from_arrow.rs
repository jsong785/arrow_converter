use anyhow::Result;
use arrow::record_batch::RecordBatch;

use crate::types::Type;

pub trait Writer {
    type InnerType;

    fn write(&mut self, batches: RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub fn create_writer<'a, W: std::io::Write + 'a>(
    t: Type,
    w: W,
) -> Result<Option<Box<dyn 'a + Writer<InnerType = W>>>> {
    match t {
        Type::Csv => Ok(Some(Box::new(csv::create_writer(w, None)?))),
        Type::Json => Ok(Some(Box::new(json::create_writer(w, None)?))),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}

pub(super) mod csv {
    use anyhow::Result;
    use arrow::datatypes::SchemaRef;

    use arrow::csv::writer::Writer;
    use std::io::Write;
    pub(super) struct CsvWriter<W: Write> {
        pub schema: Option<SchemaRef>,
        pub writer: Writer<W>,
    }
    use arrow::record_batch::RecordBatch;
    impl<W: Write> super::Writer for CsvWriter<W> {
        type InnerType = W;

        fn write(&mut self, batches: RecordBatch) -> Result<()> {
            self.writer.write(&batches)?;
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    pub(super) fn create_writer<W: Write>(
        writer: W,
        schema_ref: Option<SchemaRef>,
    ) -> Result<CsvWriter<W>> {
        use arrow::csv::writer::WriterBuilder;
        let builder = WriterBuilder::new().has_headers(true);
        Ok(CsvWriter {
            schema: schema_ref,
            writer: builder.build(writer),
        })
    }
}

pub(super) mod json {

    use anyhow::Result;
    use arrow::datatypes::SchemaRef;
    use arrow::json::writer::LineDelimitedWriter;
    use arrow::record_batch::RecordBatch;
    use std::io::Write;
    pub(super) struct JsonWriter<W: Write> {
        pub(super) schema: Option<SchemaRef>,
        pub(super) writer: LineDelimitedWriter<W>,
    }
    impl<W: Write> super::Writer for JsonWriter<W> {
        type InnerType = W;

        fn write(&mut self, batches: RecordBatch) -> Result<()> {
            self.writer.write(batches)?;
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            self.writer.finish()?;
            Ok(())
        }
    }

    pub(super) fn create_writer<W: Write>(
        writer: W,
        schema_ref: Option<SchemaRef>,
    ) -> Result<JsonWriter<W>> {
        Ok(JsonWriter {
            schema: schema_ref,
            writer: LineDelimitedWriter::new(writer),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_writer() -> Result<()> {
        assert!(create_writer(Type::Csv, Vec::new())?.is_some());
        assert!(create_writer(Type::Json, Vec::new())?.is_some());
        assert!(create_writer(Type::Xlsx, Vec::new())?.is_none());
        assert!(create_writer(Type::Parquet, Vec::new())?.is_none());
        Ok(())
    }
}
