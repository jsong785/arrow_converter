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

pub(crate) fn create_writer(writer: impl WriteBuffer, options: &Options) -> Result<impl Writer> {
    use arrow::csv::writer::WriterBuilder;
    let builder = WriterBuilder::new().has_headers(options.has_headers);
    Ok(CsvWriter {
        writer: builder.build(writer),
    })
}
