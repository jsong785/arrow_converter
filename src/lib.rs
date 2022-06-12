pub mod input;
pub mod output;
pub mod reader;
pub mod types;
pub mod writer;

use crate::reader::{ReadBuffer, Readers};
use crate::writer::{WriteBuffer, Writers};
use anyhow::Result;
use clap::Parser;
use types::Type;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long)]
    input: Option<String>,
    #[clap(long)]
    input_as_stream: bool,
    #[clap(long, arg_enum)]
    input_type: Option<Type>,
    #[clap(long)]
    input_options: Option<String>,

    #[clap(short, long)]
    output: Option<String>,
    #[clap(long)]
    output_as_stream: bool,
    #[clap(long, arg_enum)]
    output_type: Option<Type>,
    #[clap(long)]
    output_options: Option<String>,
}

impl Cli {
    fn execute(&self) -> Result<()> {
        let itype = infer_type(&self.input, &self.input_type)?;
        let otype = infer_type(&self.output, &self.output_type)?;

        let inputsource = get_input(&self.input, self.input_as_stream)?;
        let outputsource = get_output(&self.output, self.output_as_stream)?;

        let mut reader = create_reader(itype, inputsource, self.input_options.clone())?.unwrap();
        let mut writer = create_writer(otype, outputsource, self.output_options.clone())?.unwrap();
        pipe(&mut reader, &mut writer)
    }
}

pub fn run_cli() -> Result<()> {
    use clap::CommandFactory;
    use clap::FromArgMatches;
    let command = build_arg_group(Cli::command());
    Cli::from_arg_matches(&command.try_get_matches()?)?.execute()
}

fn pipe<R: ReadBuffer, W: WriteBuffer>(r: &mut Readers<R>, w: &mut Writers<W>) -> Result<()> {
    use writer::Writer;
    r.try_for_each(|batch| w.write(batch?))
}

fn build_arg_group(command: clap::Command) -> clap::Command {
    use clap::ArgGroup;
    command
        .group(
            ArgGroup::new("input_group")
                .args(&["input", "input-as-stream"])
                .required(true),
        )
        .group(
            ArgGroup::new("output_group")
                .args(&["output", "output-as-stream"])
                .required(true),
        )
        .group(
            ArgGroup::new("input_group_stream")
                .arg("input-as-stream")
                .requires("input-type"),
        )
        .group(
            ArgGroup::new("output_group_stream")
                .arg("output-as-stream")
                .requires("output-type"),
        )
        .group(
            ArgGroup::new("input-as-stream-gotcha")
                .arg("input-as-stream")
                .requires("input-options"),
        )
}

fn infer_type(f: &Option<String>, t: &Option<Type>) -> Result<Type> {
    use anyhow::anyhow;
    if let Some(t) = &t {
        Ok(t.clone())
    } else if let Some(insider) = &f {
        use std::path::Path;
        match Path::new(&insider)
            .extension()
            .and_then(|insider| insider.to_str())
        {
            Some(p) => Type::try_from(p).map_err(|_| -> anyhow::Error {
                anyhow!("Could not infer type from given filename")
            }),
            _ => Err(anyhow!("Could not infer type from given filename")),
        }
    } else {
        Err(anyhow!("test error"))
    }
}

fn get_input(f: &Option<String>, as_stream: bool) -> Result<input::InputSource> {
    if as_stream {
        Ok(input::InputSource::Stream(std::io::stdin()))
    } else if let Some(inner) = &f {
        Ok(input::InputSource::File(std::fs::File::open(&inner)?))
    } else {
        Err(anyhow::anyhow!("Unexpected error"))
    }
}

fn get_output(f: &Option<String>, as_stream: bool) -> Result<output::OutputSource> {
    if as_stream {
        Ok(output::OutputSource::Stream(std::io::stdout()))
    } else if let Some(inner) = &f {
        Ok(output::OutputSource::File(std::fs::File::create(&inner)?))
    } else {
        Err(anyhow::anyhow!("Unexpected error"))
    }
}

pub fn create_reader<'a, R: 'a + ReadBuffer>(
    t: Type,
    reader: R,
    option: Option<String>,
) -> Result<Option<Readers<R>>> {
    use crate::reader::{csv, json};
    match t {
        Type::Csv => Ok(Some(csv::create_reader(
            reader,
            &option.map_or_else(
                || -> Result<csv::Options> { Ok(csv::Options::default()) },
                |o| -> Result<csv::Options> { Ok(serde_json::from_str::<csv::Options>(&o)?) },
            )?,
        )?)),
        Type::Json => Ok(Some(json::create_reader(
            reader,
            &option.map_or_else(
                || -> Result<json::Options> { Ok(json::Options::default()) },
                |o| -> Result<json::Options> { Ok(serde_json::from_str::<json::Options>(&o)?) },
            )?,
        )?)),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}

pub fn create_writer<'a, W: 'a + WriteBuffer>(
    t: Type,
    writer: W,
    option: Option<String>,
) -> Result<Option<Writers<W>>> {
    use crate::writer::{csv, json};
    match t {
        Type::Csv => Ok(Some(csv::create_writer(
            writer,
            &option.map_or_else(
                || -> Result<csv::Options> { Ok(csv::Options::default()) },
                |o| -> Result<csv::Options> { Ok(serde_json::from_str::<csv::Options>(&o)?) },
            )?,
        )?)),
        Type::Json => Ok(Some(json::create_writer(writer)?)),
        Type::Xlsx => Ok(None),
        Type::Parquet => Ok(None),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::cell::RefCell;
    use std::io::Cursor;
    use std::rc::Rc;

    struct TestBuffer<W: std::io::Write> {
        writer: Rc<RefCell<W>>,
    }

    impl<W: std::io::Write> TestBuffer<W> {
        fn new(w: W) -> TestBuffer<W> {
            TestBuffer {
                writer: Rc::new(RefCell::new(w)),
            }
        }
    }

    impl<W: std::io::Write> std::io::Write for TestBuffer<W> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            (*self.writer).borrow_mut().write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            (*self.writer).borrow_mut().flush()
        }
    }

    impl<W: std::io::Write> std::clone::Clone for TestBuffer<W> {
        fn clone(&self) -> TestBuffer<W> {
            TestBuffer {
                writer: Rc::clone(&self.writer),
            }
        }
    }

    #[test]
    fn json_to_json() -> Result<()> {
        let js = concat!(
            r#"{"a":1,"b":2.0,"c":"foo","d":false}"#,
            "\n",
            r#"{"a":4,"b":-5.5,"c":null,"d":true}"#,
            "\n"
        );
        let expected = concat!(
            r#"{"a":1,"b":2.0,"c":"foo","d":false}"#,
            "\n",
            r#"{"a":4,"b":-5.5,"d":true}"#,
            "\n"
        );

        let mut reader = create_reader(Type::Json, Cursor::new(js), None)?.unwrap();

        let buffer = TestBuffer::new(Vec::new());
        let mut writer = create_writer(Type::Json, buffer.clone(), None)?.unwrap();

        pipe(&mut reader, &mut writer)?;
        assert_eq!(
            expected,
            std::str::from_utf8(&buffer.writer.borrow()).unwrap()
        );
        Ok(())
    }

    #[test]
    fn json_to_csv() -> Result<()> {
        let js = concat!(
            r#"{"a":1,"b":2.0,"c":"foo","d":false}"#,
            "\n",
            r#"{"a":4,"b":-5.5,"d":true}"#,
            "\n"
        );
        let expected = "1,2.0,foo,false\n4,-5.5,,true\n";

        let mut reader = create_reader(Type::Json, Cursor::new(js), None)?.unwrap();

        let buffer = TestBuffer::new(Vec::new());
        let mut writer = create_writer(Type::Csv, buffer.clone(), None)?.unwrap();

        pipe(&mut reader, &mut writer)?;
        assert_eq!(
            expected,
            std::str::from_utf8(&buffer.writer.borrow()).unwrap()
        );
        Ok(())
    }

    #[test]
    fn csv_to_csv() -> Result<()> {
        let csv = "1,2.0,foo,false\n4,-5.5,,true\n";

        let mut reader = create_reader(Type::Csv, Cursor::new(csv), None)?.unwrap();

        let buffer = TestBuffer::new(Vec::new());
        let mut writer = create_writer(Type::Csv, buffer.clone(), None)?.unwrap();

        pipe(&mut reader, &mut writer)?;
        assert_eq!(csv, std::str::from_utf8(&buffer.writer.borrow()).unwrap());
        Ok(())
    }

    #[test]
    fn csv_to_json() -> Result<()> {
        let csv = "1,2.0,foo,false\n4,-5.5,,true\n";
        let expected = concat!(
            r#"{"column_1":1,"column_2":2.0,"column_3":"foo","column_4":false}"#,
            "\n",
            r#"{"column_1":4,"column_2":-5.5,"column_3":"","column_4":true}"#,
            "\n"
        );

        let mut reader = create_reader(Type::Csv, Cursor::new(csv), None)?.unwrap();

        let buffer = TestBuffer::new(Vec::new());
        let mut writer = create_writer(Type::Json, buffer.clone(), None)?.unwrap();

        pipe(&mut reader, &mut writer)?;
        assert_eq!(
            expected,
            std::str::from_utf8(&buffer.writer.borrow()).unwrap()
        );
        Ok(())
    }

    #[test]
    fn infer_no_type_given() -> Result<()> {
        assert!(infer_type(&None, &None).is_err());
        assert_eq!(
            infer_type(&Some("whatever".to_string()), &None)
                .unwrap_err()
                .to_string(),
            "Could not infer type from given filename".to_string()
        );
        assert_eq!(
            infer_type(&Some("file.exe".to_string()), &None)
                .unwrap_err()
                .to_string(),
            "Could not infer type from given filename".to_string()
        );

        assert_eq!(infer_type(&Some("file.csv".to_string()), &None)?, Type::Csv);
        assert_eq!(
            infer_type(&Some("file.json".to_string()), &None)?,
            Type::Json
        );
        assert_eq!(
            infer_type(&Some("file.xlsx".to_string()), &None)?,
            Type::Xlsx
        );
        assert_eq!(
            infer_type(&Some("file.parquet".to_string()), &None)?,
            Type::Parquet
        );
        Ok(())
    }

    #[test]
    fn infer_type_given() -> Result<()> {
        assert_eq!(
            infer_type(&Some("whatever".to_string()), &Some(Type::Csv))?,
            Type::Csv
        );
        assert_eq!(
            infer_type(&Some("whatever".to_string()), &Some(Type::Json))?,
            Type::Json
        );
        assert_eq!(
            infer_type(&Some("file.exe".to_string()), &Some(Type::Json))?,
            Type::Json
        );

        assert_eq!(
            infer_type(&Some("file.csv".to_string()), &Some(Type::Json))?,
            Type::Json
        );
        assert_eq!(
            infer_type(&Some("file.json".to_string()), &Some(Type::Csv))?,
            Type::Csv
        );
        assert_eq!(
            infer_type(&Some("file.xlsx".to_string()), &Some(Type::Parquet))?,
            Type::Parquet
        );
        assert_eq!(
            infer_type(&Some("file.parquet".to_string()), &Some(Type::Xlsx))?,
            Type::Xlsx
        );
        Ok(())
    }

    struct TestFile<P: AsRef<std::path::Path>> {
        path: P,
    }

    impl<P: AsRef<std::path::Path>> Drop for TestFile<P> {
        fn drop(&mut self) {
            _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn get_input_func() -> Result<()> {
        assert!(get_input(&None, false).is_err());
        _ = get_input(&None, true)?;

        {
            const FAKE_FILE_NAME: &str = "fake_file";
            _ = std::fs::File::create(FAKE_FILE_NAME)?;
            let _fake = TestFile {
                path: FAKE_FILE_NAME,
            };

            _ = get_input(&Some(FAKE_FILE_NAME.to_string()), true)?;
            _ = get_input(&Some(FAKE_FILE_NAME.to_string()), false)?;
        }
        Ok(())
    }

    #[test]
    fn get_output_func() -> Result<()> {
        assert!(get_output(&None, false).is_err());
        _ = get_output(&None, true)?;

        let run_test = |run: &dyn Fn(String) -> Result<()>| -> Result<()> {
            const FAKE_FILE_NAME: &str = "fake_file";
            let _fake = TestFile {
                path: FAKE_FILE_NAME,
            };
            run(FAKE_FILE_NAME.to_string())
        };

        run_test(&|f: String| {
            get_output(&Some(f.to_string()), true)?;
            Ok(())
        })?;
        run_test(&|f: String| {
            get_output(&Some(f.to_string()), false)?;
            Ok(())
        })?;
        Ok(())
    }
}
