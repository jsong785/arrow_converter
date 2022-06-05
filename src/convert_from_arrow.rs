use anyhow::Result;
use arrow::record_batch::RecordBatch;

pub trait Writer {
    fn write(&mut self, batches: RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub mod csv {
    use anyhow::Result;

    use arrow::csv::writer::Writer;
    use std::io::Write;

    pub(crate) struct CsvWriter<W: Write> {
        writer: Writer<W>,
    }
    use arrow::record_batch::RecordBatch;
    impl<W: Write> super::Writer for CsvWriter<W> {
        fn write(&mut self, batches: RecordBatch) -> Result<()> {
            self.writer.write(&batches)?;
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    pub struct FileInfo {
        pub file_name: String,
        pub has_headers: bool,
    }

    pub(crate) fn create_writer(info: &FileInfo) -> Result<CsvWriter<std::fs::File>> {
        use std::fs::File;
        create_writer_with_buffer(File::create(&info.file_name)?, info)
    }

    pub(crate) fn create_writer_with_buffer<W: std::io::Write>(
        writer: W,
        info: &FileInfo,
    ) -> Result<CsvWriter<W>> {
        use arrow::csv::writer::WriterBuilder;
        let builder = WriterBuilder::new().has_headers(info.has_headers);
        Ok(CsvWriter {
            writer: builder.build(writer),
        })
    }
}

pub mod json {

    use anyhow::Result;
    use arrow::json::writer::LineDelimitedWriter;
    use arrow::record_batch::RecordBatch;
    use std::io::Write;
    pub(crate) struct JsonWriter<W: Write> {
        writer: LineDelimitedWriter<W>,
    }
    impl<W: Write> super::Writer for JsonWriter<W> {
        fn write(&mut self, batches: RecordBatch) -> Result<()> {
            self.writer.write(batches)?;
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            self.writer.finish()?;
            Ok(())
        }
    }

    #[derive(Default)]
    pub struct FileInfo {
        pub file_name: String,
    }

    pub(crate) fn create_writer(info: &FileInfo) -> Result<JsonWriter<std::fs::File>> {
        use std::fs::File;
        create_writer_with_buffer(File::create(&info.file_name)?)
    }

    pub(crate) fn create_writer_with_buffer<W: Write>(writer: W) -> Result<JsonWriter<W>> {
        Ok(JsonWriter {
            writer: LineDelimitedWriter::new(writer),
        })
    }
}

pub mod parquet {

}