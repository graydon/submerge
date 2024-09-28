// Column chunks have <= 256 rows, various primitive forms:
//
// - Bit (dense, 256-bit / 32-byte unit, a bitmap) -- 1 code when > 32 bits set
// - Int (direct, 1,2,4,8 byte units shifted 1..8 bytes)
// - Int (sliced, 1 byte units, 1..=8 times, shifted 1..8 bytes)
// - Flo (direct, 8 byte units)
// - Bin (subcols: lens, prefixes, optionally hashes and offsets if any len >8)
//
// All int forms are FOR-encoded based on the chunk min val.
//
// A sequence of up to 256 column chunks is then placed into a _track_,
// which can have added multi-column _encoding_ substructure but add
// no _logical_ substructure (at the language level). A track is also
// local to a block, so that any bins in its chunks use offsets inside
// the block's heap. Dictionaries are also formed at this level which
// means a dict can only have 64k entries and the definitions are local
// to the containing block as well. This is a tradeoff against having to
// load a different block to do dictionary comparisons.
//
//   - Prim (no subcols)
//   - Runs (subcols: values, run-ends)
//   - Dict (subcols: codes, values)
//   - Virt (no subcols, val is min+(row*max) if pos, min+(row/-max) if neg:
//           mainy used for fixed-child-count-per-parent structure definition)
//
// Finally, a heterogeneous sequence of basic column chunks are
// arranged into _logical_ columns, which have both a length in basic
// column chunks and a width of some number of basic-columns making up
// a logical substructure (surfaced at the PL level)
//
//   - Basic (no subcols)
//   - Multi (subcols: 1 parent-to-child offsets (often pos-virt), 1 child-to-parent offsets (often neg-virt), 1 child column)
//   - AllOf (&, subcols: N child columns)
//   - OneOf (|, subcols: 1 selector, 1 offsets, N child columns)
//
// Every logical column has, at the layer (file) level, a unique-in-its-parent
// _label_ and a major/minor/role type-triple. This is the layer's logical
// column catalogue, which there is one of per layer. The column catalogue has a
// layer-level heap and a nested set of "multi"-columns of dictionary-coded
// bins.
//
// Note: a _decoded_ bin is _5_ words: prefix, hash, offset, size, _block ID_. These IDs can basically never be exhausted.
// Note: iterators for all int-types yield i64 values regardless of track and chunk encoding.
// Note: this is the same yielded-type between rowdb and coldb interfaces.
// Note: only _predicate pushdown_ allows pruning sliced ints before reassembly.

// Then a 64k-row block contains tracks (per column) and each track has up to 256 chunks.
// The chunk bitmap has 256 bits / 32 bytes.

// Layer file contains
// Block of columns, each contains
// Track per column, contains
// Chunk sequence

#![allow(dead_code, unused_variables)]

use std::{ops::Range,io::Write};

#[cfg(test)]
mod test;

mod ioutil;
use ioutil::{Reader, Writer, RangeExt};
use submerge_base::{Result,Error,err};


// A 32-byte / 256-bit bitmap, used both for the payload of a chunk when
// the chunk is logical type Bit, and for the chunk bitmap in a track.
struct Bitmap256 {
    bits: [u64; 4],
}
impl Bitmap256 {
    fn new() -> Self {
        Bitmap256 { bits: [0; 4] }
    }
    fn set(&mut self, i: usize) {
        self.bits[i / 64] |= 1 << (i % 64);
    }
    fn get(&self, i: usize) -> bool {
        (self.bits[i / 64] & (1 << (i % 64))) != 0
    }
    fn clear(&mut self) {
        self.bits = [0; 4];
    }
    fn count(&self) -> u32 {
        self.bits.iter().map(|x| x.count_ones()).sum()
    }
}

struct LayerMeta {
    rows: u64,
    cols: u64,
    block_ranges: Vec<Range<i64>>,
}
struct LayerWriter<W: Writer> {
    wr: Box<W>,
    range: Range<i64>,
    meta: LayerMeta,
}
struct BlockMeta {
    track_ranges: Vec<Range<i64>>, // lo/hi pair for each track
    track_encodings: Vec<TrackEncoding>, // encoding for each track
    track_rows: Vec<u16>, // row count for each track; may vary across substructure tracks
}
struct BlockWriter<W: Writer> {
    lyr: Box<LayerWriter<W>>,
    block_num: usize,
    range: Range<i64>,
    meta: BlockMeta,
}

// TrackMeta is nonempty only when track encoding is not Virt
struct TrackMeta {
    chunk_ranges: Vec<Range<i64>>, // (optional) lo/hi pair for each int/bin/flo chunk
    chunk_int_forms0: Vec<IntForm>, // (optional) prim int data, prim bin prefix, run end, dict code
    chunk_int_forms1: Vec<IntForm>, // (optional) prim bin length, run or dict value
    chunk_int_forms2: Vec<IntForm>, // (optional) form of prim/run/dict bin hash when any length > 8
    chunk_int_forms3: Vec<IntForm>, // (optional) form of prim/run/dict bin offset when any length > 8
    chunk_bins_large: Bitmap256, // (optional) if prim and logical type bin, 1 bit per chunk, 1 if any bin in chunk > 8 bytes
}
struct TrackWriter<W: Writer> {
    blk: Box<BlockWriter<W>>,
    track_num: usize,
    range: Range<i64>,
    meta: TrackMeta,
}

struct ChunkMeta {
    range: Range<i64>,
    int_form_0: IntForm,
    int_form_1: IntForm,
    int_form_2: IntForm,
    int_form_3: IntForm,
    bin_large: bool,
}

struct ChunkWriter<W: Writer> {
    trk: Box<TrackWriter<W>>,
    chunk_num: usize,
    chunk_meta: ChunkMeta,
}

/// [IntForm] is a 6-bit code that describes how an int chunk is laid out on
/// disk. It is logically a 3-tuple of (`byte_width`, `byte_shift`, `sliced`).
/// The `byte_width` is the number of bytes used to store each value, the
/// `byte_shift` is the number of bytes to shift the value left to get the final
/// value, and `sliced` is true if the chunk is written as a sequence of
/// `byte_width` 1-byte slices, with each slice the length in bytes of the
/// chunk.
///
/// Only certain combinations of this 3-tuple are valid. The [IntForm] enum covers
/// all valid combinations. The [IntFormDesc] struct is used as an intermediate
/// type that can hold all possible combinations whether valid or invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct IntFormDesc {
    byte_width: u8,
    byte_shift: u8,
    sliced: bool,
}

impl TryFrom<IntFormDesc> for IntForm {
    type Error = Error;

    fn try_from(value: IntFormDesc) -> std::result::Result<Self, Self::Error> {
        match (value.byte_width, value.byte_shift, value.sliced) {
            (1, 0, true) => Ok(Self::SlicedInt1_0),
            (1, 1, true) => Ok(Self::SlicedInt1_1),
            (1, 2, true) => Ok(Self::SlicedInt1_2),
            (1, 3, true) => Ok(Self::SlicedInt1_3),
            (1, 4, true) => Ok(Self::SlicedInt1_4),
            (1, 5, true) => Ok(Self::SlicedInt1_5),
            (1, 6, true) => Ok(Self::SlicedInt1_6),
            (1, 7, true) => Ok(Self::SlicedInt1_7),

            (2, 0, true) => Ok(Self::SlicedInt2_0),
            (2, 1, true) => Ok(Self::SlicedInt2_1),
            (2, 2, true) => Ok(Self::SlicedInt2_2),
            (2, 3, true) => Ok(Self::SlicedInt2_3),
            (2, 4, true) => Ok(Self::SlicedInt2_4),
            (2, 5, true) => Ok(Self::SlicedInt2_5),
            (2, 6, true) => Ok(Self::SlicedInt2_6),

            (3, 0, true) => Ok(Self::SlicedInt3_0),
            (3, 1, true) => Ok(Self::SlicedInt3_1),
            (3, 2, true) => Ok(Self::SlicedInt3_2),
            (3, 3, true) => Ok(Self::SlicedInt3_3),
            (3, 4, true) => Ok(Self::SlicedInt3_4),
            (3, 5, true) => Ok(Self::SlicedInt3_5),

            (4, 0, true) => Ok(Self::SlicedInt4_0),
            (4, 1, true) => Ok(Self::SlicedInt4_1),
            (4, 2, true) => Ok(Self::SlicedInt4_2),
            (4, 3, true) => Ok(Self::SlicedInt4_3),

            (5, 0, true) => Ok(Self::SlicedInt5_0),
            (5, 1, true) => Ok(Self::SlicedInt5_1),
            (5, 2, true) => Ok(Self::SlicedInt5_2),
            (5, 3, true) => Ok(Self::SlicedInt5_3),

            (6, 0, true) => Ok(Self::SlicedInt6_0),
            (6, 1, true) => Ok(Self::SlicedInt6_1),
            (6, 2, true) => Ok(Self::SlicedInt6_2),

            (7, 0, true) => Ok(Self::SlicedInt7_0),
            (7, 1, true) => Ok(Self::SlicedInt7_1),

            (8, 0, true) => Ok(Self::SlicedInt8_0),

            (2, 0, false) => Ok(Self::DirectInt2_0),
            (2, 1, false) => Ok(Self::DirectInt2_1),
            (2, 2, false) => Ok(Self::DirectInt2_2),
            (2, 3, false) => Ok(Self::DirectInt2_3),
            (2, 4, false) => Ok(Self::DirectInt2_4),
            (2, 5, false) => Ok(Self::DirectInt2_5),
            (2, 6, false) => Ok(Self::DirectInt2_6),

            (4, 0, false) => Ok(Self::DirectInt4_0),
            (4, 1, false) => Ok(Self::DirectInt4_1),
            (4, 2, false) => Ok(Self::DirectInt4_2),
            (4, 3, false) => Ok(Self::DirectInt4_3),
            (4, 4, false) => Ok(Self::DirectInt4_4),

            (8, 0, false) => Ok(Self::DirectInt8_0),

            _ => Err(err("invalid InfFormDesc")),
        }
    }
}

impl From<IntForm> for IntFormDesc {

    fn from(value: IntForm) -> Self {
        let (byte_width, byte_shift, sliced) = match value {
            IntForm::SlicedInt1_0 => (1, 0, true),
            IntForm::SlicedInt1_1 => (1, 1, true),
            IntForm::SlicedInt1_2 => (1, 2, true),
            IntForm::SlicedInt1_3 => (1, 3, true),
            IntForm::SlicedInt1_4 => (1, 4, true),
            IntForm::SlicedInt1_5 => (1, 5, true),
            IntForm::SlicedInt1_6 => (1, 6, true),
            IntForm::SlicedInt1_7 => (1, 7, true),

            IntForm::SlicedInt2_0 => (2, 0, true),
            IntForm::SlicedInt2_1 => (2, 1, true),
            IntForm::SlicedInt2_2 => (2, 2, true),
            IntForm::SlicedInt2_3 => (2, 3, true),
            IntForm::SlicedInt2_4 => (2, 4, true),
            IntForm::SlicedInt2_5 => (2, 5, true),
            IntForm::SlicedInt2_6 => (2, 6, true),

            IntForm::SlicedInt3_0 => (3, 0, true),
            IntForm::SlicedInt3_1 => (3, 1, true),
            IntForm::SlicedInt3_2 => (3, 2, true),
            IntForm::SlicedInt3_3 => (3, 3, true),
            IntForm::SlicedInt3_4 => (3, 4, true),
            IntForm::SlicedInt3_5 => (3, 5, true),

            IntForm::SlicedInt4_0 => (4, 0, true),
            IntForm::SlicedInt4_1 => (4, 1, true),
            IntForm::SlicedInt4_2 => (4, 2, true),
            IntForm::SlicedInt4_3 => (4, 3, true),
            IntForm::SlicedInt4_4 => (4, 4, true),

            IntForm::SlicedInt5_0 => (5, 0, true),
            IntForm::SlicedInt5_1 => (5, 1, true),
            IntForm::SlicedInt5_2 => (5, 2, true),
            IntForm::SlicedInt5_3 => (5, 3, true),

            IntForm::SlicedInt6_0 => (6, 0, true),
            IntForm::SlicedInt6_1 => (6, 1, true),
            IntForm::SlicedInt6_2 => (6, 2, true),

            IntForm::SlicedInt7_0 => (7, 0, true),
            IntForm::SlicedInt7_1 => (7, 1, true),

            IntForm::SlicedInt8_0 => (8, 0, true),

            IntForm::DirectInt2_0 => (2, 0, false),
            IntForm::DirectInt2_1 => (2, 1, false),
            IntForm::DirectInt2_2 => (2, 2, false),
            IntForm::DirectInt2_3 => (2, 3, false),
            IntForm::DirectInt2_4 => (2, 4, false),
            IntForm::DirectInt2_5 => (2, 5, false),
            IntForm::DirectInt2_6 => (2, 6, false),

            IntForm::DirectInt4_0 => (4, 0, false),
            IntForm::DirectInt4_1 => (4, 1, false),
            IntForm::DirectInt4_2 => (4, 2, false),
            IntForm::DirectInt4_3 => (4, 3, false),
            IntForm::DirectInt4_4 => (4, 4, false),

            IntForm::DirectInt8_0 => (8, 0, false),
        };
        IntFormDesc { byte_width, byte_shift, sliced }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
enum IntForm {
    SlicedInt1_0 = 0, // 0x00000000_000000ff
    SlicedInt1_1 = 1, // 0x00000000_0000ff00
    SlicedInt1_2 = 2, // 0x00000000_00ff0000
    SlicedInt1_3 = 3, // 0x00000000_ff000000
    SlicedInt1_4 = 4, // 0x000000ff_00000000
    SlicedInt1_5 = 5, // 0x0000ff00_00000000
    SlicedInt1_6 = 6, // 0x00ff0000_00000000
    SlicedInt1_7 = 7, // 0xff000000_00000000

    SlicedInt2_0 = 8,   // 0x00000000_0000ffff
    SlicedInt2_1 = 9,   // 0x00000000_00ffff00
    SlicedInt2_2 = 0xa, // 0x00000000_ffff0000
    SlicedInt2_3 = 0xb, // 0x000000ff_ff000000
    SlicedInt2_4 = 0xc, // 0x0000ffff_00000000
    SlicedInt2_5 = 0xd, // 0x00ffff00_00000000
    SlicedInt2_6 = 0xe, // 0xffff0000_00000000

    SlicedInt3_0 = 0xf,  // 0x00000000_00ffffff
    SlicedInt3_1 = 0x10, // 0x00000000_ffffff00
    SlicedInt3_2 = 0x11, // 0x000000ff_ffff0000
    SlicedInt3_3 = 0x12, // 0x0000ffff_ff000000
    SlicedInt3_4 = 0x13, // 0x00ffffff_00000000
    SlicedInt3_5 = 0x14, // 0xffffff00_00000000

    SlicedInt4_0 = 0x15, // 0x00000000_ffffffff
    SlicedInt4_1 = 0x16, // 0x000000ff_ffffff00
    SlicedInt4_2 = 0x17, // 0x0000ffff_ffff0000
    SlicedInt4_3 = 0x18, // 0x00ffffff_ff000000
    SlicedInt4_4 = 0x19, // 0xffffffff_00000000

    SlicedInt5_0 = 0x1a, // 0x000000ff_ffffffff
    SlicedInt5_1 = 0x1b, // 0x0000ffff_ffffff00
    SlicedInt5_2 = 0x1c, // 0x00ffffff_ffff0000
    SlicedInt5_3 = 0x1d, // 0xffffffff_ff000000

    SlicedInt6_0 = 0x1e, // 0x0000ffff_ffffffff
    SlicedInt6_1 = 0x1f, // 0x00ffffff_ffffff00
    SlicedInt6_2 = 0x20, // 0xffffffff_ffff0000

    SlicedInt7_0 = 0x21, // 0x00ffffff_ffffffff
    SlicedInt7_1 = 0x22, // 0xffffffff_ffffff00

    SlicedInt8_0 = 0x23, // 0xffffffff_ffffffff

    // No DirectInt1_*, it's same as SlicedInt1_*

    DirectInt2_0 = 0x24, // 0x00000000_0000ffff
    DirectInt2_1 = 0x25, // 0x00000000_00ffff00
    DirectInt2_2 = 0x26, // 0x00000000_ffff0000
    DirectInt2_3 = 0x27, // 0x000000ff_ff000000
    DirectInt2_4 = 0x28, // 0x0000ffff_00000000
    DirectInt2_5 = 0x29, // 0x00ffff00_00000000
    DirectInt2_6 = 0x2a, // 0xffff0000_00000000

    DirectInt4_0 = 0x2b, // 0x00000000_ffffffff
    DirectInt4_1 = 0x2c, // 0x000000ff_ffffff00
    DirectInt4_2 = 0x2d, // 0x0000ffff_ffff0000
    DirectInt4_3 = 0x2e, // 0x00ffffff_ff000000
    DirectInt4_4 = 0x2f, // 0xffffffff_00000000

    DirectInt8_0 = 0x30, // 0xffffffff_ffffffff
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum LogicalType {
    Bit = 0,
    Int = 1,
    Flo = 2,
    Bin = 3,
}
impl LogicalType {
    fn from_u8_low_2_bits(u: u8) -> Self {
        match u & 0b11 {
            0 => LogicalType::Bit,
            1 => LogicalType::Int,
            2 => LogicalType::Flo,
            3 => LogicalType::Bin,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TrackEncoding {
    Prim = 0,
    Runs = 1,
    Dict = 2,
    Virt = 3,
}
impl TrackEncoding {
    fn from_u8_low_2_bits(u: u8) -> Self {
        match u & 0b11 {
            0 => TrackEncoding::Prim,
            1 => TrackEncoding::Runs,
            2 => TrackEncoding::Dict,
            3 => TrackEncoding::Virt,
            _ => unreachable!(),
        }
    }
}



impl<W: Writer> ChunkWriter<W> {
}

impl<W: Writer> TrackWriter<W> {
    // A track should use positive-virt encoding if every value is
    // row*n for some n. We should notice this is _not_ the case after
    // the second iteration of looking.
    fn pos_virt_base_and_factor(vals: &[i64]) -> Option<(i64, i64)> {
        // For 0 or 1 value, we prefer encoding as prim.
        if vals.len() < 2 {
            return None;
        }
        let mut base: i64 = 0;
        let mut prev: i64 = 0;
        let mut diff: i64 = 0;
        for (i, val) in vals.iter().enumerate() {
            if i == 0 {
                base = *val;
                prev = *val;
            } else if i == 1 {
                diff = *val - prev;
                prev = *val;
            } else if diff == *val - prev {
                prev = *val;
            } else {
                // Pattern does not hold.
                return None;
            }
        }
        Some((base, diff))
    }

    // A track should use negative-virt encoding if every value is
    // row/n for some n, which is true exactly when it's a sequence
    // of n-length runs of values that ascend by 1 after each run.
    fn neg_virt_base_and_factor(vals: &[i64]) -> Option<(i64, i64)> {
        // For 0 or 1 value, we prefer encoding as prim.
        // eprintln!("examining {:?}", vals);
        if vals.len() < 2 {
            // eprintln!("too few vals");
            return None;
        }
        let mut base = 0;
        let mut prev = 0;
        let mut run = 0;
        let mut curr_run_len = 0;
        let mut prev_run_len = 0;
        for (i, val) in vals.iter().enumerate() {
            if i == 0 {
                base = *val;
                prev = *val;
                curr_run_len = 1;
            } else if prev == *val {
                // Run coninues.
                prev = *val;
                curr_run_len += 1;
            } else if prev + 1 != *val {
                // Run transition that is too big.
                // eprintln!("run transition too big at vals[{}] = {}: prev={}", i, *val, prev);
                return None;
            } else {
                // Possibly-valid run transition.
                if run != 0 && prev_run_len != curr_run_len {
                    // Run lengths differ.
                    // eprintln!("run lengths differ at vals[{}] = {}: prev={}, run={},
                    //           prev_run_len={}, curr_run_len={}", i, *val, prev, run,
                    //           prev_run_len, curr_run_len);
                    return None;
                }
                // Start new run.
                prev = *val;
                prev_run_len = curr_run_len;
                curr_run_len = 1;
                run += 1;
            }
        }
        if run != 0 && curr_run_len <= prev_run_len {
            // Allow final run to be short
            Some((base, -prev_run_len))
        } else {
            // eprintln!("no runs or final run is overlong");
            None
        }
    }

    fn run_end_encode<T:Eq>(vals: &[T]) -> Result<Vec<(&T,u16)>> {
        let len = vals.len();
        if len == 0 {
            return Ok(Vec::new());
        } else if len == 1 {
            return Ok(vec![(&vals[0], 0)]);
        } else if len > 0xffff {
            return Err(err("track longer than 64k rows"));
        }
        let mut encoded = Vec::new();
        let mut prev = &vals[0];
        let last = len - 1; 
        for (i, val) in vals.iter().enumerate() {
            if i > 0 && (*prev != *val || i == last) {
                encoded.push((prev, i as u16));
            }
            prev = val;
        }
        Ok(encoded)
    }

    fn dict_encode<T:Ord+Eq>(vals: &[T]) -> Result<(Vec<&T>,Vec<u16>)> {
        if vals.len() > 0xffff {
            return Err(err("track longer than 64k rows"));
        }
        let mut dict = vals.iter().map(|x| (x, 0_u16)).collect::<std::collections::BTreeMap<&T,u16>>();
        let values = dict.keys().cloned().collect::<Vec<&T>>();
        for (i, val) in dict.iter_mut().enumerate() {
            *val.1 = i as u16;
        }
        let mut codes = Vec::new();
        for val in vals.iter() {
            let code = dict.get(val).ok_or_else(|| err("dict value not found"))?;
            codes.push(*code);
        }
        Ok((values, codes))
    }

    fn select_track_encoding_and_vals(vals: &[i64]) -> Result<(TrackEncoding, i64, i64)> {
        if let Some((base, diff)) = Self::pos_virt_base_and_factor(vals) {
            // eprintln!("pos_virt: base={}, diff={}", base, diff);
           Ok((TrackEncoding::Virt, base, diff))
        } else if let Some((base, diff)) = Self::neg_virt_base_and_factor(vals) {
            // eprintln!("neg_virt: base={}, diff={}", base, diff);
            Ok((TrackEncoding::Virt, base, diff))
        } else {
            let ree = Self::run_end_encode(vals)?;
            let dict = Self::dict_encode(vals)?;
            // TODO decide which of ree, dict and prim to use.
            // eprintln!("run_savings={}", run_savings);
            // eprintln!("prim");
            let min = vals.iter().cloned().min().unwrap_or(0);
            let max = vals.iter().cloned().max().unwrap_or(0);
            Ok((TrackEncoding::Prim, min, max))
        }
    }
}

impl<W: Writer> ChunkWriter<W> {
    /// Returns the number of bytes, and the left shift, necessary to reconstruct
    /// a given column of i64 values. Note that all the i64 values should be
    /// positive (offsets from a FOR base) otherwise the result will spend bytes
    /// storing the extended sign bits.
    fn byte_width_and_shift(vals: &[i64]) -> (u8, u8) {
        let mut accum: u64 = 0;
        let mut shift: u8 = 0;
        let mut width: u8 = 0;
        for v in vals.iter() {
            accum |= *v as u64;
        }
        while accum != 0 && accum & 0xff == 0 {
            shift += 1;
            accum >>= 8;
        }
        while accum != 0 {
            width += 1;
            accum >>= 8;
        }
        (width, shift)
    }
    fn select_int_form(vals: &[i64], sliced: bool) -> IntForm {
        let (mut byte_width, byte_shift) = Self::byte_width_and_shift(vals);
        if ! sliced {
            // Direct int forms only exist in 2, 4, and 8 byte widths.
            if byte_width == 1 {
                byte_width = 2;
            } else if byte_width == 3 {
                byte_width = 4;
            } else if byte_width > 4 {
                byte_width = 8;
            }
        }
        let desc = IntFormDesc { byte_width, byte_shift, sliced };
        if let Ok(form) = IntForm::try_from(desc) {
            form
        } else if sliced {
            // This should never happen, but just in case.
            IntForm::SlicedInt8_0
        } else {
            // This should never happen, but just in case.
            IntForm::DirectInt8_0
        }
    }

}

struct LayerReader {}
struct BlockReader {
    lyr: Box<LayerReader>,
}
struct TrackReader {
    blk: Box<BlockReader>,
}
struct ChunkReader {
    trk: Box<TrackReader>,
}

impl ChunkReader {
    fn next(&self, buf: &mut [u8; 32]) {}
}
