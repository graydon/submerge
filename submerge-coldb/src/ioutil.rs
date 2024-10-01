use std::{
    fs::File,
    io::{BufReader, BufWriter, Cursor, Read, Seek, Write},
    path::{Display, PathBuf},
    sync::Arc,
};
use submerge_base::Result;

#[cfg(test)]
use crate::test::annotations::Annotations;
#[cfg(not(test))]
struct Annotations;
#[cfg(not(test))]
impl Annotations {
    fn new() -> Self {
        Self
    }
    fn push(&mut self, _range: std::ops::Range<i64>, _name: &str) {}
}

pub(crate) trait RangeExt {
    fn len(&self) -> i64;
}
impl RangeExt for std::ops::Range<i64> {
    fn len(&self) -> i64 {
        self.end - self.start
    }
}

// Reader and Writer

pub trait Reader: Read + Seek + Send + Sized {
    fn try_clone_independent(&self) -> Result<Self>;
}

pub trait Writer: Write + Seek + Send + Sized {
    type PairedReader: Reader;
    fn try_into_reader(self) -> Result<Self::PairedReader>;
    fn pos(&mut self) -> Result<i64> {
        Ok(self.stream_position()?.try_into()?)
    }
    fn get_annotations(&mut self) -> &mut Annotations;
    #[cfg(test)]
    fn annotate_pos(&mut self) -> Result<i64> {
        self.pos()
    }
    #[cfg(test)]
    fn annotate_to_pos_from(&mut self, name: &str, start: i64) -> Result<()> {
        let pos = self.annotate_pos()?;
        self.get_annotations().push((start..pos).into(), name);
        Ok(())
    }
    #[cfg(test)]
    fn annotate<T>(&mut self, name: &str, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let start = self.annotate_pos()?;
        let ok = f(self)?;
        self.annotate_to_pos_from(name, start)?;
        Ok(ok)
    }
    #[cfg(not(test))]
    fn annotate_pos(&mut self) -> Result<i64> {
        Ok(0)
    }
    #[cfg(not(test))]
    fn annotate_to_pos_from(&mut self, name: &str, start: i64) -> Result<()> {
        Ok(())
    }
    #[cfg(not(test))]
    fn annotate<T>(&mut self, name: &str, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        f(self)
    }
    fn write_annotated_byte_slice(&mut self, name: &str, val: &[u8]) -> Result<()> {
        self.annotate(name, |w| Ok(w.write_all(val)?))
    }
    fn write_annotated_num<const N: usize, T: funty::Numeric<Bytes = [u8; N]>>(
        &mut self,
        name: &str,
        val: T,
    ) -> Result<()> {
        self.write_annotated_byte_slice(name, &val.to_le_bytes())
    }
    fn write_annotated_num_slice<const N: usize, T: funty::Numeric<Bytes = [u8; N]>>(
        &mut self,
        name: &str,
        val: &[T],
    ) -> Result<()> {
        self.annotate(name, |w| {
            for &v in val {
                w.write_all(&v.to_le_bytes())?;
            }
            Ok(())
        })
    }
    fn write_lane_of_annotated_num_slice<const N: usize, T: funty::Numeric<Bytes = [u8; N]>>(
        &mut self,
        name: &str,
        lane: u8,
        val: &[T],
    ) -> Result<()> {
        self.annotate(name, |w| {
            for &v in val {
                let tmp = v.to_le_bytes();
                let byte = tmp[lane as usize];
                w.write(&[byte])?;
            }
            Ok(())
        })
    }
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
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.mem.read(buf)
    }
}

impl Seek for MemReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
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
    annotations: Annotations,
    mem: Cursor<Vec<u8>>,
}

impl MemWriter {
    pub fn new() -> Self {
        Self {
            annotations: Annotations::new(),
            mem: Cursor::new(Vec::new()),
        }
    }
    #[cfg(test)]
    pub(crate) fn render_annotations(&self) -> Result<String> {
        self.annotations
            .render_hexdump(self.mem.get_ref().as_slice())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.mem.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.mem.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
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
    fn get_annotations(&mut self) -> &mut Annotations {
        &mut self.annotations
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
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for FileReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
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
    annotations: Annotations,
}

impl FileWriter {
    fn try_create_non_existing(path: PathBuf) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let file = BufWriter::new(file);
        let path = path.to_owned();
        let annotations = Annotations::new();
        Ok(Self {
            file,
            path,
            annotations,
        })
    }
}

impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
impl Seek for FileWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Writer for FileWriter {
    type PairedReader = FileReader;
    fn try_into_reader(self) -> Result<Self::PairedReader> {
        let Self { mut file, path, .. } = self;
        // Make extra sure we've flushed-and-closed before
        // opening to read.
        file.flush()?;
        let file = file.into_inner()?;
        file.sync_all()?;
        drop(file);
        Ok(FileReader::try_open_existing(path)?)
    }
    fn get_annotations(&mut self) -> &mut Annotations {
        &mut self.annotations
    }
}
