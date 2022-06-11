pub mod csv;
pub mod json;
use anyhow::Result;

pub trait Writer {
    fn write(&mut self, batches: arrow::record_batch::RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub trait WriteBuffer: std::io::Write {}
impl<T: std::io::Write> WriteBuffer for T {}

pub struct WriterWrap<B: WriteBuffer> {
    writer: Writers<B>,
}
impl<B: WriteBuffer> Writer for WriterWrap<B> {
    fn write(&mut self, batches: arrow::record_batch::RecordBatch) -> Result<()> {
        match &mut self.writer {
            Writers::Csv(c) => {
                c.write(&batches)?;
                Ok(())
            }
            Writers::Json(j) => {
                j.write(batches)?;
                Ok(())
            }
        }
    }
    fn finish(&mut self) -> Result<()> {
        match &mut self.writer {
            Writers::Csv(_) => Ok(()),
            Writers::Json(j) => {
                j.finish()?;
                Ok(())
            }
        }
    }
}

enum Writers<W: WriteBuffer> {
    Csv(csv::CWriter<W>),
    Json(json::DWriter<W>),
}
