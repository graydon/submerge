use ordered_float::OrderedFloat;
use std::{any::type_name, io::Write, ops::Range};
use submerge_base::{err, Bitmap256, Error, Result};
use super::heap::Heap;
use super::ioutil::{Bitmap256IoExt, RangeExt, Reader, Writer};
use super::wordty::{WordTy, WordTy256};


pub(crate) trait DictEncodable: Eq + Ord {
    fn get_component_count(&self) -> usize;
    fn get_component_name(i: usize) -> &'static str;
    fn get_component_as_int(&self, component: usize, heap: &mut Heap) -> i64;

    fn write_dict_entries(vals: &[&Self], wr: &mut impl Writer) -> Result<()> {
        let mut heap = Heap::default();
        wr.push_context("entries");
        for (c, chunk) in vals.chunks(256).enumerate() {
            wr.push_context(c);
            let n_components = chunk
                .iter()
                .map(|x| x.get_component_count())
                .max()
                .unwrap_or(1);
            for component in 0..n_components {
                if n_components > 1 {
                    wr.push_context(Self::get_component_name(component));
                }
                let vals = chunk
                    .iter()
                    .map(|x| x.get_component_as_int(component, &mut heap))
                    .collect::<Vec<i64>>();
                let (min, wordty) = WordTy::select_min_and_ty(&vals);
                wr.write_annotated_le_wordty_slice("words", &vals, wordty)?;
                if n_components > 1 {
                    wr.pop_context();
                }
            }
            wr.pop_context();
        }
        if heap.data.len() > 0 {
            wr.push_context("heap");
            wr.write_annotated_le_num("len", heap.data.len())?;
            wr.write_annotated_byte_slice("data", &heap.data)?;
            wr.pop_context();
        }
        wr.pop_context();
        Ok(())
    }
}

impl DictEncodable for i64 {
    fn get_component_count(&self) -> usize {
        1
    }
    fn get_component_name(i: usize) -> &'static str {
        "int"
    }
    fn get_component_as_int(&self, _component: usize, _heap: &mut Heap) -> i64 {
        *self
    }
}

impl DictEncodable for OrderedFloat<f64> {
    fn get_component_count(&self) -> usize {
        1
    }
    fn get_component_name(i: usize) -> &'static str {
        "flo"
    }
    fn get_component_as_int(&self, _component: usize, _heap: &mut Heap) -> i64 {
        let bytes = self.0.to_le_bytes();
        i64::from_le_bytes(bytes)
    }
}

impl DictEncodable for &[u8] {
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
                // We treat the first 8 byte prefix of the string as a
                // big-endian i64, which should I think sort strings
                // byte-lexicographically. Eventually we should use
                // a collator here, like the UCA DUCET sequence.
                let mut buf = [0_u8; 8];
                let n = self.len().min(8);
                buf[..n].copy_from_slice(&self[..n]);
                i64::from_be_bytes(buf)
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
