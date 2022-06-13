pub enum InputSource {
    File(std::fs::File),
}

impl std::io::Read for InputSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
        }
    }
}

impl std::io::Seek for InputSource {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(f) => f.seek(pos),
        }
    }
}
