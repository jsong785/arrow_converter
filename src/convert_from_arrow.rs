use anyhow::Result;
use arrow::record_batch::RecordBatch;

pub trait Writer {
    fn write(&mut self, batches: RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub trait WriteBuffer: std::io::Write {}
impl<T: std::io::Write> WriteBuffer for T {}

pub mod csv {
    use super::{WriteBuffer, Writer};
    use anyhow::Result;

    pub(crate) struct CsvWriter<W: WriteBuffer> {
        writer: arrow::csv::writer::Writer<W>,
    }
    use arrow::record_batch::RecordBatch;
    impl<W: WriteBuffer> Writer for CsvWriter<W> {
        fn write(&mut self, batches: RecordBatch) -> Result<()> {
            self.writer.write(&batches)?;
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Default, Debug)]
    pub struct Options {
        pub has_headers: bool,
    }

    pub(crate) fn create_writer(
        writer: impl WriteBuffer,
        options: &Options,
    ) -> Result<impl Writer> {
        use arrow::csv::writer::WriterBuilder;
        let builder = WriterBuilder::new().has_headers(options.has_headers);
        Ok(CsvWriter {
            writer: builder.build(writer),
        })
    }
}

pub mod json {

    use super::{WriteBuffer, Writer};
    use anyhow::Result;
    use arrow::json::writer::LineDelimitedWriter;
    use arrow::record_batch::RecordBatch;

    pub(crate) struct JsonWriter<W: WriteBuffer> {
        writer: LineDelimitedWriter<W>,
    }
    impl<W: WriteBuffer> Writer for JsonWriter<W> {
        fn write(&mut self, batches: RecordBatch) -> Result<()> {
            self.writer.write(batches)?;
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            self.writer.finish()?;
            Ok(())
        }
    }

    pub(crate) fn create_writer(writer: impl WriteBuffer) -> Result<impl Writer> {
        Ok(JsonWriter {
            writer: LineDelimitedWriter::new(writer),
        })
    }
}
