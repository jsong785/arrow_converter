pub enum OutputSource {
    Stream(std::io::Stdout),
    File(std::fs::File),
}

impl std::io::Write for OutputSource {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Stream(s) => s.write(buf),
            Self::File(f) => f.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Stream(s) => s.flush(),
            Self::File(f) => f.flush(),
        }
    }
}
