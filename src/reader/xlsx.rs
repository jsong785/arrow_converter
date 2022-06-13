use super::ReadBuffer;
use umya_spreadsheet::reader::xlsx::read_reader;

pub(crate) fn create_reader<R: ReadBuffer>(
    reader: R,
    options: &Options,
) -> Result<super::Readers<R>> {
    let r = read_reader(reader, false);
}
