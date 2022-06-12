pub enum InputSource {
    Stream(std::io::Stdin),
    File(std::fs::File),
}

impl std::io::Read for InputSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Stream(s) => s.read(buf),
            Self::File(f) => f.read(buf),
        }
    }
}

impl std::io::Seek for InputSource {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::Stream(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Seek is not supported for stdin",
            )),
            Self::File(f) => f.seek(pos),
        }
    }
}
