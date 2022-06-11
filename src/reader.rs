pub mod csv;
pub mod json;

use anyhow::Result;

pub trait ReadBufferNoSeek: std::io::Read {}
impl<T: std::io::Read + std::io::Seek> ReadBuffer for T {}
pub trait ReadBuffer: ReadBufferNoSeek + std::io::Seek {}
impl<T: std::io::Read> ReadBufferNoSeek for T {}

pub struct ReaderWrap<B: ReadBufferNoSeek> {
    reader: Types<B>,
}

impl<B: ReadBufferNoSeek> Iterator for ReaderWrap<B> {
    type Item = Result<arrow::record_batch::RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.reader {
            Types::Csv(c) => c.next(),
            Types::Json(j) => j.next(),
        }
    }
}

trait Reader: std::iter::Iterator<Item = Result<arrow::record_batch::RecordBatch>> {}

struct ArrowAdapter<
    T: std::iter::Iterator<Item = arrow::error::Result<arrow::record_batch::RecordBatch>>,
> {
    inner: T,
}

impl<T: std::iter::Iterator<Item = arrow::error::Result<arrow::record_batch::RecordBatch>>>
    std::iter::Iterator for ArrowAdapter<T>
{
    type Item = Result<arrow::record_batch::RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(inner) => {
                if inner.is_ok() {
                    unsafe { Some(Ok(inner.unwrap_unchecked())) }
                } else {
                    unsafe { Some(Err(inner.unwrap_err_unchecked().into())) }
                }
            }
            None => None,
        }
    }
}
impl<T: std::iter::Iterator<Item = arrow::error::Result<arrow::record_batch::RecordBatch>>> Reader
    for ArrowAdapter<T>
{
}

enum Types<B: ReadBufferNoSeek> {
    Csv(ArrowAdapter<csv::Reader<B>>),
    Json(ArrowAdapter<json::Reader<B>>),
}
