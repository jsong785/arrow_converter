use crate::convert_from_arrow::Writer;
use anyhow::Result;
use arrow::record_batch::RecordBatch;

pub fn pipe<T: std::io::Write>(
    reader: &mut Box<dyn std::iter::Iterator<Item = Result<RecordBatch>>>,
    writer: &mut Box<dyn Writer<InnerType = T>>,
) -> Result<()> {
    reader.try_for_each(|batch| -> Result<()> { writer.write(batch?) })
}
