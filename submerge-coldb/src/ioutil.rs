use std::{fs::File, path::PathBuf, io::{Cursor, Read, Seek, Write, BufWriter, BufReader, Result}, sync::Arc, convert::TryFrom};

mod private {
    // This is private because it can cause a reader to be made from a
    // still-changing writer. We eliminate that possibility in the
    // public API we expose below, which _consumes_ writers (using
    // Into) when making readers. Readers can be freely duplicated
    // though, and unlike File::clones such duplicates are
    // _independent_ in terms of seeks and errors and keeping the file
    // alive. They're the result of separate opens.
    pub trait TryGetReader {
	fn try_get_reader(&self) -> std::io::Result<Box<dyn super::Reader>>;
    }
}

impl private::TryGetReader for Cursor<Arc<[u8]>> {
    fn try_get_reader(&self) -> Result<Box<dyn Reader>> {
	Ok(Box::new(self.clone()))
    }
}

impl private::TryGetReader for Cursor<Vec<u8>> {
    fn try_get_reader(&self) -> Result<Box<dyn Reader>> {
	let vec = self.clone().into_inner();
	let rc: Arc<[u8]> = Arc::from(vec);
	Ok(Box::new(Cursor::new(rc)))
    }
}

impl TryFrom<Box<dyn Writer>> for Box<dyn Reader> {
    type Error = std::io::Error;
    fn try_from(writer: Box<dyn Writer>) -> Result<Self> {
	writer.try_get_reader()
    }
}

pub trait Reader : Read + Seek + private::TryGetReader + Send {
    fn try_clone_independent(&self) -> Result<Box<dyn Reader>> {
	self.try_get_reader()
    }
}

pub trait Writer : Write + Seek + private::TryGetReader + Send {}

pub fn new_writer_for_non_existing_file(path: PathBuf) -> Result<Box<dyn Writer>> {
    Ok(Box::new(FileWriter::try_create_non_existing(path)?))
}

pub fn new_writer_for_in_memory_buffer() -> Box<dyn Writer> {
    Box::new(Cursor::new(Vec::new()))
}


impl<T:Read+Seek+private::TryGetReader + Send> Reader for T {}
impl<T:Write+Seek+private::TryGetReader + Send> Writer for T {}

struct FileWriter {
    file: BufWriter<File>,
    path: PathBuf
}

struct FileReader {
    file: BufReader<File>,
    path: PathBuf
}

impl FileReader {
    fn try_open_existing(path: PathBuf) -> Result<Self> {
	let file = File::open(&path)?;
	let file = BufReader::new(file);
	Ok(Self{file,path})
    }
}

impl FileWriter {
    fn try_create_non_existing(path: PathBuf) -> Result<Self> {
	let file = std::fs::OpenOptions::new().write(true).create_new(true).open(&path)?;
	let file = BufWriter::new(file);
	let path = path.to_owned();
	Ok(Self{file,path})
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
impl private::TryGetReader for FileReader {
    fn try_get_reader(&self) -> Result<Box<dyn Reader>> {
	Ok(Box::new(FileReader::try_open_existing(self.path.clone())?))
    }
}
impl private::TryGetReader for FileWriter {
    fn try_get_reader(&self) -> Result<Box<dyn Reader>> {
	Ok(Box::new(FileReader::try_open_existing(self.path.clone())?))
    }
}
