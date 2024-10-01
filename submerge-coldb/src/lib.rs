//! The columnar storage format for submerge.
//!
//! A table is spread across multiple files, each called a "layer". As the
//! system transfers data from the row store to the column store it forms new
//! layers and periodically consolidates existing layers. A table may have quite
//! a lot of layers (theoretical max is like 2^39) as each layer can span at
//! most 2^24 rows and tables can be 2^63 rows in total. In practice most tables
//! will have a smallish number of layers.
//!
//! Each layer (file) has a catalogue of columns of one of 4 "normal" types:
//! Bit, bin, int and flo. A column may also be of type "offset" in which case
//! its content is always integral, and it encodes _structural_ relationships
//! between parent and child columns rather than directly-queryable values.
//!
//! The data in a layer is divided into up to 256 "blocks" of 64k rows each.
//! Within each block, a "track" is the max-64k-row-subset of each column
//! corresponding to the rows in the block. Tracks are further decomposed into
//! up to 256 "chunks", where each chunk covers up to 256 rows with a particular
//! choice of adaptive encoding, chosen to minimize empty space.
//!
//! A track may be either implicit or explicit.
//!
//! Implicit tracks have no content beyond their metadata descriptor, which
//! consists of two numbers A and B and implies row values based on A+(row*B).
//! Implicit offset tracks are used to describe fixed parent-child structures.
//!
//! Explicit bit-typed tracks store their values as bitmaps.
//!
//! Explicit non-bit tracks store their values uniquely and sorted in a sequence
//! of dictionary chunks and run-end-encoded code chunks. These allow by-value
//! point loads and efficient range scans.
//!
//! Bin-typed dictionaries vary their chunk content depending on whether the
//! chunk contains any "long" values -- those longer than 8 bytes. If so, the
//! value part of the chunk is _just_ the prefix/collator of the bin and there
//! are additional parts encoding a hash value of the entire bin as well as an
//! offset of the bin in the block's heap.
//!
//! Finally, columns are arranged into structures which have one of 4 types:
//!
//!   - Basic (no subcols)
//!   - Multi (subcols: 1 parent-to-child offsets (often pos-virt), 1
//!     child-to-parent offsets (often neg-virt), 1 child column)
//!   - AllOf (&, subcols: N child columns)
//!   - OneOf (|, subcols: 1 selector, 1 offsets, N child columns)
//!
//! Every column has a unique-in-its-parent-structure _label_ and a
//! major/minor/role type-triple.
//!
//! Note: a _decoded_ bin is _5_ words: prefix, hash, offset, size, _block ID_.
//! These IDs can basically never be exhausted. Note: iterators for all
//! int-types yield i64 values regardless of track and chunk encoding. Note:
//! this is the same yielded-type between rowdb and coldb interfaces. Note: only
//! _predicate pushdown_ allows pruning sliced ints before reassembly.

// Layer file contains
// Block of columns, each contains
// Track per column, contains
// Chunk sequence

#![allow(dead_code, unused_variables)]

use std::{any::type_name, io::Write, ops::Range};

#[cfg(test)]
mod test;

mod ioutil;
use ioutil::{RangeExt, Reader, Writer};
use ordered_float::OrderedFloat;
use submerge_base::{err, Error, Result};

// A 32-byte / 256-bit bitmap, used both for the payload of a chunk when
// the chunk is logical type Bit, and for the chunk bitmap in a track.
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
struct Bitmap256 {
    bits: [u64; 4],
}
impl Bitmap256 {
    fn new() -> Self {
        Bitmap256 { bits: [0; 4] }
    }
    fn set(&mut self, i: usize, val: bool) {
        if val {
            self.bits[i / 64] |= 1 << (i % 64);
        } else {
            self.bits[i / 64] &= !(1 << (i % 64));
        }
    }
    fn get(&self, i: usize) -> bool {
        (self.bits[i / 64] & (1 << (i % 64))) != 0
    }
    fn set_all(&mut self) {
        self.bits = [u64::MAX; 4];
    }
    fn clear_all(&mut self) {
        self.bits = [0; 4];
    }
    fn count(&self) -> u32 {
        self.bits.iter().map(|x| x.count_ones()).sum()
    }
    fn is_empty(&self) -> bool {
        self.bits.iter().all(|x| *x == 0)
    }
    fn is_full(&self) -> bool {
        self.bits.iter().all(|x| *x == u64::MAX)
    }
    fn union(&mut self, other: &Self) {
        for i in 0..4 {
            self.bits[i] |= other.bits[i];
        }
    }
    fn intersect(&mut self, other: &Self) {
        for i in 0..4 {
            self.bits[i] &= other.bits[i];
        }
    }
    fn subtract(&mut self, other: &Self) {
        for i in 0..4 {
            self.bits[i] &= !other.bits[i];
        }
    }
    fn write_annotated(&self, name: &str, wr: &mut impl Writer) -> Result<()> {
        wr.push_context(name);
        wr.write_annotated_num_slice::<8, u64, &str>("bitmap", &self.bits)?;
        wr.pop_context();
        Ok(())
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
struct LayerMeta {
    vers: i64,
    rows: i64,
    cols: i64,
    block_offsets: Vec<i64>,
    block_lengths: Vec<i64>,
}

impl LayerMeta {
    pub(crate) fn write_magic_header(&self, wr: &mut impl Writer) -> Result<()> {
        wr.write_annotated_byte_slice("magic", b"columnar")
    }

    pub(crate) fn write(&self, wr: &mut impl Writer) -> Result<()> {
        wr.push_context("meta");
        let pos = wr.pos()?;
        wr.write_annotated_num("rows", self.rows)?;
        wr.write_annotated_num("cols", self.cols)?;
        wr.write_annotated_num_slice("block_offsets", &self.block_offsets)?;
        wr.write_annotated_num_slice("block_lengths", &self.block_lengths)?;
        let pos2 = wr.pos()?;
        wr.write_annotated_num("self_len", (pos2 - pos) as i64)?;
        wr.pop_context();
        Ok(())
    }
}

struct LayerWriter {
    meta: LayerMeta,
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
struct BlockMeta {
    track_lo_vals: Vec<i64>,
    track_hi_vals: Vec<i64>,
    track_implicit: Bitmap256,
    track_rows: Vec<u16>, // row count for each track; may vary across substructure tracks
}

impl BlockMeta {
    pub(crate) fn write(&self, wr: &mut impl Writer) -> Result<()> {
        wr.push_context("meta");
        let pos = wr.pos()?;
        wr.write_annotated_num_slice("track_lo_vals", &self.track_lo_vals)?;
        wr.write_annotated_num_slice("track_hi_vals", &self.track_hi_vals)?;
        self.track_implicit.write_annotated("track_implicit", wr)?;
        wr.write_annotated_num_slice("track_rows", &self.track_rows)?;
        let pos2 = wr.pos()?;
        wr.write_annotated_num("self_len", (pos2 - pos) as i64)?;
        wr.pop_context();
        Ok(())
    }
}

// What if for physical coding we go even simpler:
//
//  - Every non-bit track is dict-encoded, always. We need to for f64 and bin
//    anyways, and if we also do so for ints then we're always _scanning_ 1 or
//    2-byte columns after a dict lookup; the dict is sorted and contents are
//    probed with binary search anyways so it doesn't benefit from SIMD, doesn't
//    need slicing. We can shift and size-class the dict content words at least.
//
//       - Dict means a point-binsearch that fails => value not in track (even
//         if the value is inside the _range_ of the track).
//
//       - So this is a partial solution to the point-query question. And if
//         there is a _hit_ then the value returned is a dict code, and if
//         there's a unique chunk holding that code (i.e. truly a point lookup)
//         then only one chunk range will hold it, but determining that will
//         only require looking at 256 chunk ranges and then finding the row
//         inside the chunk: 3 256-byte reads + 1 final-byte read to confirm the
//         second byte of the row. It's not nothing but not super expensive.
//
//       - Dict also lets you do a range query by 2 binsearches + a 1-or-2 byte
//         scan, limited to those chunk-ranges that intersect. This is
//         potentially cheaper then a multi-stripe scan.
//
//       - A 64k-entry binsearch is <= 16 iterations, which is not bad.
//
//  - Further, we can run-end-encode the dict codes, and the run-ends are all 2
//    byte words also. The dict codes themselves may benefit from slicing, but
//    _only_ them. We can have 1 bitmap that says whether the dict codes are
//    1-byte or 2-byte (chunk by chunk), and another bitmap that indicates
//    whether each chunk is accompanied by a chunk of run-end numbers, or if all
//    runs are implicitly 1-row long.
//
//  - The chunk ranges / FOR anchors of each chunk are then only 2-byte words,
//    since they're dict codes too.
//
//  - So our physical track forms are:
//    - direct (maybe shifted, maybe 1, 2, 4 or 8-byte) cols for bin/int/flo
//      dict contents
//    - direct 2-byte cols for run-end numbers or multi-sructure numbers
//    - sliced 1-byte or 2-byte cols for dict-code chunks
//
//
// Ok so a chunk has:
//
//   - 1 or 2 byte dict codes (striped)
//   - 0 or 2 byte run-ends (unstriped)
//
// And a track dict has:
//
//   - 1, 2, 4, or 8 byte values (unstriped)
//   - 2 * 2 byte row-ranges (unstriped)

#[derive(Clone, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
enum WordTy {
    Word1,
    Word2,
    Word4,
    Word8,
}

// Wrapper for storing two bitmaps to encode
// an array of 4-case WordTy values.
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
struct WordTy256 {
    hi: Bitmap256,
    lo: Bitmap256,
}

impl WordTy256 {
    fn get_word_ty(&self, i: usize) -> WordTy {
        match (self.hi.get(i), self.lo.get(i)) {
            (false, false) => WordTy::Word1,
            (false, true) => WordTy::Word2,
            (true, false) => WordTy::Word4,
            (true, true) => WordTy::Word8,
        }
    }
    fn set_word_ty(&mut self, i: usize, ty: WordTy) {
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
    fn write_annotated(&self, name: &str, wr: &mut impl Writer) -> Result<()> {
        wr.annotate(name, |w| {
            for &v in self.lo.bits.iter().chain(self.hi.bits.iter()) {
                w.write_all(&v.to_le_bytes())?;
            }
            Ok(())
        })
    }
}

// TrackMeta is nonempty only when track encoding is not Virt
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
struct TrackMeta {
    // If the track is virtual, there is _no_ track metadata read or written.
    chunk_populated: Bitmap256, // 1 bit per chunk, 1 if any row in chunk is populated

    // All remaining trackmeta fields are optional depending on datatype and encoding.
    // If the track is of type `bit`, or is virtual, then all other fields are empty
    // and not read/written.
    chunk_two_bytes: Bitmap256, // 1 bit per chunk, 1 if any dict code > 0xff
    chunk_has_recol: Bitmap256, // 1 bit per chunk, 1 if any run > 1 row (chunk has extra 2-byte run-end column)

    dict_val_chunk_tys: WordTy256, // dict value: types of chunks storing int/flo data or bin collator/prefix
    dict_bin_len_chunk_tys: WordTy256, // (optional) if bin: types of chunks of lengths

    dict_bin_large: Bitmap256, // (optional) if bin, 1 if any bin in chunk > 8 bytes

    dict_chunk_tys_2: WordTy256, // (optional) if large bin: types of chunks of hashes
    dict_chunk_tys_3: WordTy256, // (optional) if large bin: types of chunks of heap offsets

    // 256 * 4 bytes = 1k bytes
    chunk_dict_lo_codes: Vec<u16>, // lo dict code for each populated chunk
    chunk_dict_hi_codes: Vec<u16>, // hi dict code for each populated chunk
}

impl TrackMeta {
    pub(crate) fn write(&self, wr: &mut impl Writer) -> Result<()> {
        wr.push_context("meta");
        let pos = wr.pos()?;
        self.chunk_populated
            .write_annotated("chunk_populated", wr)?;
        self.chunk_two_bytes
            .write_annotated("chunk_two_bytes", wr)?;
        self.chunk_has_recol
            .write_annotated("chunk_has_recol", wr)?;
        self.dict_val_chunk_tys
            .write_annotated("dict_val_chunk_tys", wr)?;
        self.dict_bin_len_chunk_tys
            .write_annotated("dict_bin_len_chunk_tys", wr)?;
        self.dict_bin_large.write_annotated("dict_bin_large", wr)?;
        self.dict_chunk_tys_2
            .write_annotated("dict_chunk_tys_2", wr)?;
        self.dict_chunk_tys_3
            .write_annotated("dict_chunk_tys_3", wr)?;
        wr.write_annotated_num_slice("chunk_dict_lo_codes", &self.chunk_dict_lo_codes)?;
        wr.write_annotated_num_slice("chunk_dict_hi_codes", &self.chunk_dict_hi_codes)?;
        let pos2 = wr.pos()?;
        wr.write_annotated_num("self_len", (pos2 - pos) as i64)?;
        wr.pop_context();
        Ok(())
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

// A track should use positive-virt encoding if every value is
// row*n for some n. We should notice this is _not_ the case after
// the second iteration of looking.
pub(crate) fn pos_virt_base_and_factor(vals: &[i64]) -> Option<(i64, i64)> {
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
pub(crate) fn neg_virt_base_and_factor(vals: &[i64]) -> Option<(i64, i64)> {
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

pub(crate) fn run_end_encode<T: Eq>(vals: &[T]) -> Result<(Vec<&T>, Vec<u16>)> {
    let mut run_vals = Vec::new();
    let mut run_ends = Vec::new();
    let len = vals.len();
    if len == 0 {
        return Ok((run_vals, run_ends));
    } else if len == 1 {
        run_vals.push(&vals[0]);
        run_ends.push(0);
        return Ok((run_vals, run_ends));
    } else if len > 0xffff {
        return Err(err("track longer than 64k rows"));
    }
    let mut prev = &vals[0];
    let last = len - 1;
    for (i, val) in vals.iter().enumerate() {
        if i > 0 && (*prev != *val || i == last) {
            run_vals.push(prev);
            run_ends.push(i as u16);
        }
        prev = val;
    }
    Ok((run_vals, run_ends))
}

pub(crate) fn dict_encode<T: Ord + Eq>(vals: &[T]) -> Result<(Vec<&T>, Vec<u16>)> {
    if vals.len() > 0xffff {
        return Err(err("track longer than 64k rows"));
    }
    let mut dict = vals
        .iter()
        .map(|x| (x, 0_u16))
        .collect::<std::collections::BTreeMap<&T, u16>>();
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

fn write_one_or_two_byte_dict_code_chunk(
    vals: &[u16],
    any_two_bytes: bool,
    wr: &mut impl Writer,
) -> Result<()> {
    wr.push_context("code_lanes");
    if any_two_bytes {
        wr.write_lane_of_annotated_num_slice("hi_lane", 1, vals)?;
    }
    wr.write_lane_of_annotated_num_slice("lo_lane", 0, vals)?;
    wr.pop_context();
    Ok(())
}

pub(crate) trait DictEntry: Eq + Ord {

    fn get_component_count(&self) -> usize;
    fn get_component_name(i: usize) -> &'static str;
    fn get_component_as_int(&self, component: usize) -> i64;

    fn write_entries(vals: &[&Self], wr: &mut impl Writer) -> Result<()> {
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
                    .map(|x| x.get_component_as_int(component))
                    .collect::<Vec<i64>>();
                let (wordty, shift) = select_word_ty_and_shift(&vals);
                match wordty {
                    WordTy::Word1 => {
                        let vals = vals.iter().map(|x| *x as u8).collect::<Vec<u8>>();
                        wr.write_annotated_byte_slice("word1s", &vals)?;
                    }
                    WordTy::Word2 => {
                        let vals = vals.iter().map(|x| *x as u16).collect::<Vec<u16>>();
                        wr.write_annotated_num_slice("word2s", &vals)?;
                    }
                    WordTy::Word4 => {
                        let vals = vals.iter().map(|x| *x as u32).collect::<Vec<u32>>();
                        wr.write_annotated_num_slice("word4s", &vals)?;
                    }
                    WordTy::Word8 => {
                        wr.write_annotated_num_slice("word8s", &vals)?;
                    }
                }
                if n_components > 1 {
                    wr.pop_context();
                }
            }
            wr.pop_context();
        }
        wr.pop_context();
        Ok(())
    }
}

impl DictEntry for i64 {
    fn get_component_count(&self) -> usize {
        1
    }
    fn get_component_name(i: usize) -> &'static str {
        "int"
    }
    fn get_component_as_int(&self, _component: usize) -> i64 {
        *self
    }
}

impl DictEntry for OrderedFloat<f64> {
    fn get_component_count(&self) -> usize {
        1
    }
    fn get_component_name(i: usize) -> &'static str {
        "flo"
    }
    fn get_component_as_int(&self, _component: usize) -> i64 {
        self.0 as i64
    }
}


pub(crate) fn encode_track<T: DictEntry, W: Writer>(vals: &[T], wr: &mut W) -> Result<TrackMeta> {
    let mut tm = TrackMeta::default();

    let (dict, codes) = dict_encode(vals)?;
    if dict.len() > 0xffff {
        return Err(err("track longer than 64k rows"));
    }
    if dict.len() == 0 {
        // Empty track, nothing to do.
        return Ok(tm);
    }
    let max_dict_code = (dict.len() - 1) as u16;
    wr.push_context("dict");
    wr.write_annotated_num("len", dict.len() as u16)?;
    T::write_entries(&dict, wr)?;
    wr.pop_context(); // dict

    wr.push_context("code_chunks");
    for (c, chunk) in codes.chunks(256).enumerate() {
        wr.push_context(c); // chunk_num
        // First decide whether the codes in this chunk need 2 bytes.
        let mut chunk_two_bytes = false;
        let mut chunk_min_code = max_dict_code;
        let mut chunk_max_code = 0;

        for &code in chunk {
            if code > 0xff {
                chunk_two_bytes = true;
            }
            if code < chunk_min_code {
                chunk_min_code = code;
            }
            if code > chunk_max_code {
                chunk_max_code = code;
            }
        }
        tm.chunk_dict_lo_codes.push(chunk_min_code);
        tm.chunk_dict_hi_codes.push(chunk_max_code);

        tm.chunk_two_bytes.set(c, chunk_two_bytes);

        // Then decide whether to row-end-encode this chunk.
        let (run_vals, run_ends) = run_end_encode(&chunk)?;
        let chunk_code_width = if chunk_two_bytes { 2 } else { 1 };
        let run_end_encoded_len = run_ends.len() * (chunk_code_width + 2);
        let simple_encoded_len = chunk.len() * chunk_code_width;
        if run_end_encoded_len < simple_encoded_len {
            // Yes, REE is a savings, use it.
            tm.chunk_has_recol.set(c, true);
            let run_vals = run_vals.iter().map(|x| **x).collect::<Vec<u16>>();
            write_one_or_two_byte_dict_code_chunk(&run_vals, chunk_two_bytes, wr)?;
            wr.write_annotated_num_slice("run_ends", &run_ends)?;
        } else {
            // No point, REE actually takes more space.
            write_one_or_two_byte_dict_code_chunk(chunk, chunk_two_bytes, wr)?;
        }
        wr.pop_context(); // chunk_num
    }
    wr.pop_context(); // code_chunks
    Ok(tm)
}

struct ChunkMeta {
    range: Range<u16>,
    bin_large: bool,
}

/// Returns the number of bytes, and the left shift, necessary to reconstruct
/// a given column of i64 values. Note that all the i64 values should be
/// positive (offsets from a FOR base) otherwise the result will spend bytes
/// storing the extended sign bits.
pub(crate) fn byte_width_and_shift(vals: &[i64]) -> (u8, u8) {
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
fn select_word_ty_and_shift(vals: &[i64]) -> (WordTy, u8) {
    let (byte_width, byte_shift) = byte_width_and_shift(vals);
    let wordty = match byte_width {
        1 => WordTy::Word1,
        2 => WordTy::Word2,
        3 | 4 => WordTy::Word4,
        _ => WordTy::Word8,
    };
    (wordty, byte_shift)
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
