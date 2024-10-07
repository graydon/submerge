#[derive(Debug, Default)]
pub(crate) struct Heap {
    pub(crate) data: Vec<u8>,
}

impl Heap {
    pub(crate) fn add(&mut self, new_data: &[u8]) -> usize {
        // This is quadratic as the heap grows, so it is probably worth placing some
        // limits or switching to a different data structure.
        if let Some(pos) = memchr::memmem::find(&self.data, new_data) {
            pos
        } else {
            let pos = self.data.len();
            self.data.extend_from_slice(new_data);
            pos
        }
    }
}
