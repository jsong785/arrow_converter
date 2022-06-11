use super::{WriteBuffer, Writer};
use anyhow::Result;
use serde::{Deserialize, Serialize};

pub use arrow::csv::writer::Writer as CWriter;

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

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Options {
    pub has_headers: bool,
}

pub(crate) fn create_writer<W: WriteBuffer>(
    writer: W,
    options: &Options,
) -> Result<super::WriterWrap<W>> {
    use arrow::csv::writer::WriterBuilder;
    let builder = WriterBuilder::new().has_headers(options.has_headers);
    Ok(super::WriterWrap {
        writer: super::Writers::Csv(builder.build(writer)),
    })
}
