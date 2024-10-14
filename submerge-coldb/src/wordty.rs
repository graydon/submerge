use crate::ioutil::{DoubleBitmap256IoExt, Writer};
use submerge_base::{DoubleBitmap256, Result};

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

    pub(crate) fn slice_name(&self) -> &'static str {
        match self {
            WordTy::Word1 => "word1_slice",
            WordTy::Word2 => "word2_slice",
            WordTy::Word4 => "word4_slice",
            WordTy::Word8 => "word8_slice",
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


#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct WordTy256 {
    bitmaps: DoubleBitmap256
}

impl WordTy256 {
    pub(crate) fn get_word_ty(&self, i: u8) -> WordTy {
        match self.bitmaps.get(i) {
            0b00 => WordTy::Word1,
            0b01 => WordTy::Word2,
            0b10 => WordTy::Word4,
            0b11 => WordTy::Word8,
            _ => panic!("unexpected word_ty code"),
        }
    }
    pub(crate) fn set_word_ty(&mut self, i: u8, ty: WordTy) {
        let val = match ty {
            WordTy::Word1 => 0b00,
            WordTy::Word2 => 0b01,
            WordTy::Word4 => 0b10,
            WordTy::Word8 => 0b11,
        };
        self.bitmaps.set(i, val);
    }
    pub(crate) fn write_annotated(&self, name: &str, wr: &mut impl Writer) -> Result<()> {
        self.bitmaps.write_annotated("word_tys", wr)
    }
}
