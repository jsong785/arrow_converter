use super::{WriteBuffer, Writer};
use anyhow::Result;
use arrow::json::writer::LineDelimitedWriter;

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

pub(crate) fn create_writer(writer: impl WriteBuffer) -> Result<impl Writer> {
    Ok(JsonWriter {
        writer: LineDelimitedWriter::new(writer),
    })
}