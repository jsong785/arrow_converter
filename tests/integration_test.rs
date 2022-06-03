use anyhow::Result;
use arrow_converter::convert_from_arrow::create_writer;
use arrow_converter::convert_pipe::pipe;
use arrow_converter::convert_to_arrow::create_reader;
use arrow_converter::types::Type;
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

    let mut reader = create_reader(Type::Json, Cursor::new(js))?.unwrap();

    let buffer = TestBuffer::new(Vec::new());
    let mut writer = create_writer(Type::Json, buffer.clone())?.unwrap();

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
    let expected = "a,b,c,d\n1,2.0,foo,false\n4,-5.5,,true\n";

    let mut reader = create_reader(Type::Json, Cursor::new(js))?.unwrap();

    let buffer = TestBuffer::new(Vec::new());
    let mut writer = create_writer(Type::Csv, buffer.clone())?.unwrap();

    pipe(&mut reader, &mut writer)?;
    assert_eq!(
        expected,
        std::str::from_utf8(&buffer.writer.borrow()).unwrap()
    );
    Ok(())
}

#[test]
fn csv_to_csv() -> Result<()> {
    let csv = "a,b,c,d\n1,2.0,foo,false\n4,-5.5,,true\n";

    let mut reader = create_reader(Type::Csv, Cursor::new(csv))?.unwrap();

    let buffer = TestBuffer::new(Vec::new());
    let mut writer = create_writer(Type::Csv, buffer.clone())?.unwrap();

    pipe(&mut reader, &mut writer)?;
    assert_eq!(csv, std::str::from_utf8(&buffer.writer.borrow()).unwrap());
    Ok(())
}

#[test]
fn csv_to_json() -> Result<()> {
    let csv = "a,b,c,d\n1,2.0,foo,false\n4,-5.5,,true\n";
    let expected = concat!(
        r#"{"a":1,"b":2.0,"c":"foo","d":false}"#,
        "\n",
        r#"{"a":4,"b":-5.5,"c":"","d":true}"#,
        "\n"
    );

    let mut reader = create_reader(Type::Csv, Cursor::new(csv))?.unwrap();

    let buffer = TestBuffer::new(Vec::new());
    let mut writer = create_writer(Type::Json, buffer.clone())?.unwrap();

    pipe(&mut reader, &mut writer)?;
    assert_eq!(
        expected,
        std::str::from_utf8(&buffer.writer.borrow()).unwrap()
    );
    Ok(())
}
