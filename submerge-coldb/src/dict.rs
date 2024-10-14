use ordered_float::OrderedFloat;
use super::heap::Heap;

pub(crate) trait DictEncodable: Eq + Ord {

    fn get_value_as_int(&self) -> i64;

    // The number of components in the encoding of this value.
    // Bin values have either 2 or 4 components, depending on
    // length. Int and flo have only 1 component.
    fn get_component_count(&self) -> usize { 1 }
    fn get_component_name(i: usize) -> &'static str { "val" }
    fn get_component_as_int(&self, component: usize, _heap: &mut Heap) -> i64 {
        if component == 0 {
            self.get_value_as_int()
        } else {
            panic!("unexpected component index")
        }
    }
}

impl DictEncodable for i64 {
    fn get_value_as_int(&self) -> i64 {
        *self
    }
}

impl DictEncodable for OrderedFloat<f64> {
    fn get_value_as_int(&self) -> i64 {
        let bytes = self.0.to_le_bytes();
        i64::from_le_bytes(bytes)
    }
}

pub(crate) const LARGE_BIN_COMPONENT_COUNT: usize = 4;

impl DictEncodable for &[u8] {
    fn get_value_as_int(&self) -> i64 {
        // We treat the first 8 byte prefix of the string as a
        // big-endian i64, which should I think sort strings
        // byte-lexicographically. Eventually we should use
        // a collator here, like the UCA DUCET sequence.
        let mut buf = [0_u8; 8];
        let n = self.len().min(8);
        buf[..n].copy_from_slice(&self[..n]);
        i64::from_be_bytes(buf)
    }
    fn get_component_count(&self) -> usize {
        if self.len() > 8 {
            // prefix, len, hash, offset
            4
        } else {
            // prefix, len
            2
        }
    }
    fn get_component_name(i: usize) -> &'static str {
        match i {
            0 => "prefix",
            1 => "len",
            2 => "hash",
            3 => "offset",
            _ => unreachable!(),
        }
    }
    fn get_component_as_int(&self, component: usize, heap: &mut Heap) -> i64 {
        match component {
            0 => {
                self.get_value_as_int()
            }
            1 => self.len() as i64,
            // We emit a small 16-bit hash of the bin; we don't want
            // to use a full 64-bit hash because that would use too
            // much space for too little benefit. By the time you've
            // filtered by length and prefix you're down to a small
            // collision probability already. 1/65536 more is plenty.
            2 => (rapidhash::rapidhash(self) & 0xffff) as i64,
            3 => heap.add(self) as i64,
            _ => unreachable!(),
        }
    }
}
