use std::{
    fs::File,
    io::{BufReader, BufWriter, Cursor, Read, Result, Seek, Write},
    path::PathBuf,
    sync::Arc,
};

pub trait Reader: Read + Seek + Send + Sized {
    fn try_clone_independent(&self) -> Result<Self>;
}

pub trait Writer: Write + Seek + Send + Sized {
    type PairedReader: Reader;
    fn try_into_reader(self) -> Result<Self::PairedReader>;
}

// MemReader

pub struct MemReader {
    mem: Cursor<Arc<[u8]>>,
}

impl MemReader {
    fn new(mem: Arc<[u8]>) -> Self {
        Self {
            mem: Cursor::new(mem),
        }
    }
}

impl From<Vec<u8>> for MemReader {
    fn from(vec: Vec<u8>) -> Self {
        let rc: Arc<[u8]> = Arc::from(vec);
        Self::new(rc)
    }
}

impl Read for MemReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.mem.read(buf)
    }
}

impl Seek for MemReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        self.mem.seek(pos)
    }
}

impl Reader for MemReader {
    fn try_clone_independent(&self) -> Result<Self> {
        let rc = self.mem.get_ref().clone();
        Ok(Self::new(rc))
    }
}

// MemWriter

pub struct MemWriter {
    mem: Cursor<Vec<u8>>,
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.mem.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.mem.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        self.mem.seek(pos)
    }
}

impl Writer for MemWriter {
    type PairedReader = MemReader;
    fn try_into_reader(self) -> Result<Self::PairedReader> {
        let mem = self.mem.into_inner();
        let rc: Arc<[u8]> = Arc::from(mem);
        Ok(MemReader {
            mem: Cursor::new(rc),
        })
    }
}

// FileReader

pub struct FileReader {
    file: BufReader<File>,
    path: PathBuf,
}

impl FileReader {
    fn try_open_existing(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        let file = BufReader::new(file);
        Ok(Self { file, path })
    }
}
impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for FileReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        self.file.seek(pos)
    }
}

impl Reader for FileReader {
    fn try_clone_independent(&self) -> Result<Self> {
        FileReader::try_open_existing(self.path.clone())
    }
}

// FileWriter

pub struct FileWriter {
    file: BufWriter<File>,
    path: PathBuf,
}

impl FileWriter {
    fn try_create_non_existing(path: PathBuf) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let file = BufWriter::new(file);
        let path = path.to_owned();
        Ok(Self { file, path })
    }
}

impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.file.flush()
    }
}
impl Seek for FileWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        self.file.seek(pos)
    }
}

impl Writer for FileWriter {
    type PairedReader = FileReader;
    fn try_into_reader(self) -> Result<Self::PairedReader> {
        let Self { mut file, path } = self;
        // Make extra sure we've flushed-and-closed before
        // opening to read.
        file.flush()?;
        let file = file.into_inner()?;
        file.sync_all()?;
        drop(file);
        Ok(FileReader::try_open_existing(path)?)
    }
}
