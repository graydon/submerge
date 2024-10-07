/// A simple 32-byte / 256-bit bitmap that counts bits in order from
/// least-to-most significant bits and ascending words.
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub struct Bitmap256 {
    pub bits: [u64; 4],
}
impl Bitmap256 {
    pub fn new() -> Self {
        Bitmap256 { bits: [0; 4] }
    }
    pub fn set(&mut self, i: usize, val: bool) {
        if val {
            self.bits[i / 64] |= 1 << (i % 64);
        } else {
            self.bits[i / 64] &= !(1 << (i % 64));
        }
    }
    pub fn get(&self, i: usize) -> bool {
        (self.bits[i / 64] & (1 << (i % 64))) != 0
    }
    pub fn set_all(&mut self) {
        self.bits = [u64::MAX; 4];
    }
    pub fn clear_all(&mut self) {
        self.bits = [0; 4];
    }
    pub fn count(&self) -> u32 {
        self.bits.iter().map(|x| x.count_ones()).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|x| *x == 0)
    }
    pub fn any(&self) -> bool {
        self.bits.iter().any(|x| *x != 0)
    }
    pub fn is_full(&self) -> bool {
        self.bits.iter().all(|x| *x == u64::MAX)
    }
    pub fn union(&mut self, other: &Self) {
        for i in 0..4 {
            self.bits[i] |= other.bits[i];
        }
    }
    pub fn intersect(&mut self, other: &Self) {
        for i in 0..4 {
            self.bits[i] &= other.bits[i];
        }
    }
    pub fn subtract(&mut self, other: &Self) {
        for i in 0..4 {
            self.bits[i] &= !other.bits[i];
        }
    }
}
