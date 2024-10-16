use std::u64;

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
    pub fn set(&mut self, i: u8, val: bool) {
        let i = i as usize;
        if val {
            self.bits[i / 64] |= 1 << (i % 64);
        } else {
            self.bits[i / 64] &= !(1 << (i % 64));
        }
    }
    pub fn get(&self, i: u8) -> bool {
        let i = i as usize;
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
    // Return the number of bits set up to and including i.
    // NB: if all bits are set, this returns 256, not 255,
    // and so does not fit in a u8.
    pub fn rank(&self, mut i: u8) -> usize {
        let mut bits = 0;
        for word in &self.bits {
            if i <= 63 {
                let mask = u64::MAX >> (63 - i);
                bits += (word & mask).count_ones();
                break;
            }
            bits += word.count_ones();
            i -= 64;
        }
        bits as usize
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

// A convenience type for storing a set of 256 2-bit values
// representing numbers in the range 0..3. This comes up fairly
// often in the coldb codebase.
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub struct DoubleBitmap256 {
    pub double_bits: [u64; 8],
}

impl DoubleBitmap256 {
    pub fn new() -> Self {
        DoubleBitmap256 {
            double_bits: [0; 8],
        }
    }
    pub fn set(&mut self, i: u8, val: u8) {
        let lo = 2 * (i as usize);
        let hi = lo + 1;
        let lo_val = val & 1 != 0;
        let hi_val = val & 2 != 0;
        if lo_val {
            self.double_bits[lo / 64] |= 1 << (lo % 64);
        } else {
            self.double_bits[lo / 64] &= !(1 << (lo % 64));
        }
        if hi_val {
            self.double_bits[hi / 64] |= 1 << (hi % 64);
        } else {
            self.double_bits[hi / 64] &= !(1 << (hi % 64));
        }
    }
    pub fn get(&self, i: u8) -> u8 {
        let lo = 2 * (i as usize);
        let hi = lo + 1;
        let lo_val = (self.double_bits[lo / 64] & (1 << (lo % 64))) != 0;
        let hi_val = (self.double_bits[hi / 64] & (1 << (hi % 64))) != 0;
        (lo_val as u8) | (hi_val as u8) << 1
    }
}
