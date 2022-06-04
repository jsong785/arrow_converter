//use clap::{ArgEnum, Args};
use anyhow::Result;
use arrow::record_batch::RecordBatch;

//#[derive(ArgEnum, Clone, Debug)]
pub enum Type {
    Csv(
        crate::convert_to_arrow::csv::FileInfo,
        crate::convert_from_arrow::csv::FileInfo,
    ),
    Json(
        crate::convert_to_arrow::json::FileInfo,
        crate::convert_from_arrow::json::FileInfo,
    ),
    Xlsx,
    Parquet,
}

//#[derive(Args, Debug)]
pub struct File {
    pub name: String,
    //#[clap(arg_enum)]
    pub method: Type,
}

use crate::convert_from_arrow::Writer;
pub fn create_writer(t: Type) -> Result<Option<Box<dyn Writer>>> {
    use crate::convert_from_arrow::{csv, json};
    match t {
        Type::Csv(_, w) => Ok(Some(Box::new(csv::create_writer(&w)?))),
        Type::Json(_, w) => Ok(Some(Box::new(json::create_writer(&w)?))),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}
pub fn create_writer_with_buffer<'a, W: 'a + std::io::Write>(
    t: Type,
    writer: W,
) -> Result<Option<Box<dyn 'a + Writer>>> {
    use crate::convert_from_arrow::{csv, json};
    match t {
        Type::Csv(_, w) => Ok(Some(Box::new(csv::create_writer_with_buffer(writer, &w)?))),
        Type::Json(..) => Ok(Some(Box::new(json::create_writer_with_buffer(writer)?))),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}

pub fn create_reader(
    t: Type,
) -> Result<Option<Box<dyn std::iter::Iterator<Item = Result<RecordBatch>>>>> {
    use crate::convert_to_arrow::RecordBatchIteratorArrowAdapter;
    use crate::convert_to_arrow::{csv, json};
    match t {
        Type::Csv(r, _) => Ok(Some(Box::new(RecordBatchIteratorArrowAdapter {
            inner: csv::create_reader(&r)?,
        }))),
        Type::Json(r, _) => Ok(Some(Box::new(RecordBatchIteratorArrowAdapter {
            inner: json::create_reader(&r)?,
        }))),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}
pub fn create_reader_with_buffer<'a, R: 'a + std::io::Read + std::io::Seek>(
    t: Type,
    reader: R,
) -> Result<Option<Box<dyn 'a + std::iter::Iterator<Item = Result<RecordBatch>>>>> {
    use crate::convert_to_arrow::RecordBatchIteratorArrowAdapter;
    use crate::convert_to_arrow::{csv, json};
    match t {
        Type::Csv(r, _) => Ok(Some(Box::new(RecordBatchIteratorArrowAdapter {
            inner: csv::create_reader_with_buffer(reader, &r)?,
        }))),
        Type::Json(r, _) => Ok(Some(Box::new(RecordBatchIteratorArrowAdapter {
            inner: json::create_reader_with_buffer(reader, &r)?,
        }))),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeFile {
        file_name: String,
    }

    impl std::ops::Drop for FakeFile {
        fn drop(&mut self) {
            _ = std::fs::remove_file(&self.file_name);
        }
    }

    #[test]
    fn test_create_writer() -> Result<()> {
        let fake_file_name = "some_non_existent_file_1";
        let _fake = FakeFile {
            file_name: fake_file_name.to_string(),
        };

        use crate::convert_from_arrow;
        use crate::convert_to_arrow;
        assert!(create_writer(Type::Csv(
            convert_to_arrow::csv::FileInfo::default(),
            convert_from_arrow::csv::FileInfo {
                file_name: fake_file_name.to_string(),
                ..Default::default()
            }
        ))?
        .is_some());
        assert!(create_writer(Type::Json(
            convert_to_arrow::json::FileInfo::default(),
            convert_from_arrow::json::FileInfo {
                file_name: fake_file_name.to_string(),
                ..Default::default()
            }
        ))?
        .is_some());
        assert!(create_writer(Type::Xlsx)?.is_none());
        assert!(create_writer(Type::Parquet)?.is_none());
        Ok(())
    }

    #[test]
    fn test_create_reader() -> Result<()> {
        let fake_file_name = "some_non_existent_file_2";
        let _res = std::fs::File::create(fake_file_name)?;

        let _fake = FakeFile {
            file_name: fake_file_name.to_string(),
        };
        use crate::convert_from_arrow;
        use crate::convert_to_arrow;
        assert!(create_reader(Type::Csv(
            convert_to_arrow::csv::FileInfo {
                file_name: fake_file_name.to_string(),
                ..Default::default()
            },
            convert_from_arrow::csv::FileInfo::default()
        ))?
        .is_some());
        assert!(create_reader(Type::Json(
            convert_to_arrow::json::FileInfo {
                file_name: fake_file_name.to_string(),
                ..Default::default()
            },
            convert_from_arrow::json::FileInfo::default()
        ))?
        .is_some());
        assert!(create_reader(Type::Xlsx)?.is_none());
        assert!(create_reader(Type::Parquet)?.is_none());
        Ok(())
    }
}
