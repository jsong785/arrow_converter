use super::{WriteBuffer, Writer};
use anyhow::Result;
use arrow::json::writer::LineDelimitedWriter;

pub use arrow::json::writer::LineDelimitedWriter as DWriter;

pub(crate) struct JsonWriter<W: WriteBuffer> {
    writer: LineDelimitedWriter<W>,
}
impl<W: WriteBuffer> Writer for JsonWriter<W> {
    fn write(&mut self, batches: arrow::record_batch::RecordBatch) -> Result<()> {
        self.writer.write(batches)?;
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.writer.finish()?;
        Ok(())
    }
}

pub(crate) fn create_writer<W: WriteBuffer>(writer: W) -> Result<super::WriterWrap<W>> {
    Ok(super::WriterWrap {
        writer: super::Writers::Json(LineDelimitedWriter::new(writer)),
    })
}
