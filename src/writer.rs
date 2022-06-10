pub mod csv;
pub mod json;
use anyhow::Result;

pub trait Writer {
    fn write(&mut self, batches: arrow::record_batch::RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub trait WriteBuffer: std::io::Write {}
impl<T: std::io::Write> WriteBuffer for T {}
