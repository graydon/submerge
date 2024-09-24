// Column chunks have <= 256 rows, 16 primitive forms:
//
// - Bit (sparse, 1 byte units) -- 1 code when <= 32 bits set
// - Bit (dense, 256-bit / 32-byte unit, a bitmap) -- 1 code when > 32 bits set
// - Int (direct, 1,2,4,8 byte units) -- 4 codes
// - Int (sliced, 1 byte units, 1..=8 times) -- 8 codes
// - Flo (direct, 8 byte units) -- 1 code, 1 word body
// - Bin (subcols: prefixes, hashes, offsets, sizes)
//
// All int forms are FOR-encoded based on the chunk min val.
//
// A sequence of up to 256 column chunks is then placed into a _track_,
// which can have multi-column _encoding_ substructure but add
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
// Every logical column has, at the file level, a unique-in-its-parent
// _label_ and a major/minor/role type-triple. This is the file's
// logical column catalogue, which there is one of per file. The
// column catalogue has a table-level heap and a nested set of
// "multi"-columns of dictionary-coded bins.
//
// Note: a _decoded_ bin is _5_ words: prefix, hash, offset, size, _heap ID_. These IDs can basically never be exhausted.
// Note: iterators for all int-types yield i64 values regardless of basic and primitive encoding.
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
use submerge_base::Result;


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

    // No DirectInt8, it's same as SlicedInt1_0
    DirectInt16 = 0x24,
    DirectInt32 = 0x25,
    DirectInt64 = 0x26,
}

impl IntForm {

    fn is_sliced(&self) -> bool {
        (*self as u8) <= (Self::SlicedInt8_0 as u8)
    }

    fn byte_count_and_shift(&self) -> (u8,u8) {
        match self {
            Self::SlicedInt1_0 => (1,0),
            Self::SlicedInt1_1 => (1,8),
            Self::SlicedInt1_2 => (1,16),
            Self::SlicedInt1_3 => (1,24),
            Self::SlicedInt1_4 => (1,32),
            Self::SlicedInt1_5 => (1,40),
            Self::SlicedInt1_6 => (1,48),
            Self::SlicedInt1_7 => (1,56),

            Self::SlicedInt2_0 => (2,0),
            Self::SlicedInt2_1 => (2,8),
            Self::SlicedInt2_2 => (2,16),
            Self::SlicedInt2_3 => (2,24),
            Self::SlicedInt2_4 => (2,32),
            Self::SlicedInt2_5 => (2,40),
            Self::SlicedInt2_6 => (2,48),

            Self::SlicedInt3_0 => (3,0),
            Self::SlicedInt3_1 => (3,8),
            Self::SlicedInt3_2 => (3,16),
            Self::SlicedInt3_3 => (3,24),
            Self::SlicedInt3_4 => (3,32),
            Self::SlicedInt3_5 => (3,40),

            Self::SlicedInt4_0 => (4,0),
            Self::SlicedInt4_1 => (4,8),
            Self::SlicedInt4_2 => (4,16),
            Self::SlicedInt4_3 => (4,24),
            Self::SlicedInt4_4 => (4,32),

            Self::SlicedInt5_0 => (5,0),
            Self::SlicedInt5_1 => (5,8),
            Self::SlicedInt5_2 => (5,16),
            Self::SlicedInt5_3 => (5,24),

            Self::SlicedInt6_0 => (6,0),
            Self::SlicedInt6_1 => (6,8),
            Self::SlicedInt6_2 => (6,16),

            Self::SlicedInt7_0 => (7,0),
            Self::SlicedInt7_1 => (7,8),

            Self::SlicedInt8_0 => (8,0),

            Self::DirectInt16 => (2,0),
            Self::DirectInt32 => (4,0),
            Self::DirectInt64 => (8,0),
        }
    }

    fn byte_length_for_rows(&self, rows: u8) -> i64 {
        let (byte_count, _) = self.byte_count_and_shift();
        (byte_count as i64) * (rows as i64)
    }
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
    // A chunk should use positive-virt encoding if every value is
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

    // A chunk should use negative-virt encoding if every value is
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

    // Returns the number of bytes, and the left shift, necessary to
    // reconstruct a given column of u64 values.
    fn byte_width_and_shift(vals: &[u64]) -> (u8, u8) {
        let mut accum = 0;
        let mut shift = 0;
        let mut width = 0;
        for v in vals.iter() {
            accum |= *v;
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

    fn select_encoding(vals: &[i64]) {}
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
