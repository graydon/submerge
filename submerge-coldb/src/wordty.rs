use crate::{ioutil::Writer};
use submerge_base::{Bitmap256, Result};

#[derive(Clone, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) enum WordTy {
    Word1,
    Word2,
    Word4,
    Word8,
}
impl WordTy {
    pub(crate) fn len(&self) -> usize {
        match self {
            WordTy::Word1 => 1,
            WordTy::Word2 => 2,
            WordTy::Word4 => 4,
            WordTy::Word8 => 8,
        }
    }

    pub(crate) fn select_min_and_ty(vals: &[i64]) -> (u64, WordTy) {
        let min = vals.iter().map(|x| *x as u64).min().unwrap_or(0);
        let accum = vals.iter().map(|x| *x as u64 - min).fold(0, |a, x| a | x);
        let ty = if accum <= 0xff {
            WordTy::Word1
        } else if accum <= 0xffff {
            WordTy::Word2
        } else if accum <= 0xffff_ffff {
            WordTy::Word4
        } else {
            WordTy::Word8
        };
        (min, ty)
    }
}

// Wrapper for storing two bitmaps to encode
// an array of 4-case WordTy values.
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct WordTy256 {
    hi: Bitmap256,
    lo: Bitmap256,
}

impl WordTy256 {
    pub(crate) fn get_word_ty(&self, i: usize) -> WordTy {
        match (self.hi.get(i), self.lo.get(i)) {
            (false, false) => WordTy::Word1,
            (false, true) => WordTy::Word2,
            (true, false) => WordTy::Word4,
            (true, true) => WordTy::Word8,
        }
    }
    pub(crate) fn set_word_ty(&mut self, i: usize, ty: WordTy) {
        match ty {
            WordTy::Word1 => {
                self.hi.set(i, false);
                self.lo.set(i, false);
            }
            WordTy::Word2 => {
                self.hi.set(i, false);
                self.lo.set(i, true);
            }
            WordTy::Word4 => {
                self.hi.set(i, true);
                self.lo.set(i, false);
            }
            WordTy::Word8 => {
                self.hi.set(i, true);
                self.lo.set(i, true);
            }
        }
    }
    pub(crate) fn write_annotated(&self, name: &str, wr: &mut impl Writer) -> Result<()> {
        wr.annotate(name, |w| {
            for &v in self.lo.bits.iter().chain(self.hi.bits.iter()) {
                w.write_all(&v.to_le_bytes())?;
            }
            Ok(())
        })
    }
}
