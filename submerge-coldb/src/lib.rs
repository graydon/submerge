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
//! of dictionary chunks and optionally-run-end-encoded and byte-sliced
//! dict-code chunks. These allow by-value point loads (by binary search in the
//! track dictionary) and efficient range scans (by bytewise-SIMD-scanning the
//! dict-code chunks).
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
//! Note: a _decoded_ bin is _5_ words: prefix, len, hash, off, _block ID_.
//! These IDs can basically never be exhausted. Note: iterators for all
//! int-types yield i64 values regardless of track and chunk encoding.
//!
//! Note: this is the same yielded-type between rowdb and coldb interfaces.
//!
//! Note: only _predicate pushdown_ allows pruning sliced ints before
//! reassembly. No sliced ints escape this module.

// Layer file contains
// Block of columns, each contains
// Track per column, contains
// Chunk sequence

#![allow(dead_code, unused_variables)]

use ordered_float::OrderedFloat;
use std::{any::type_name, io::Write, ops::Range};
use submerge_base::{err, Bitmap256, Error, Result};

#[cfg(test)]
mod test;

mod ioutil;
use ioutil::{Bitmap256IoExt, RangeExt, Reader, Writer};

mod wordty;
use wordty::{WordTy, WordTy256};

mod dict;
use dict::DictEncodable;
mod heap;

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
struct LayerMeta {
    vers: i64,
    rows: i64,
    cols: i64,
    block_end_offsets: Vec<i64>,
}

impl LayerMeta {

    pub const MAGIC: &[u8;8] = b"columnar";
    pub const VERS: i64 = 0;

    pub(crate) fn write_magic_header(&self, wr: &mut impl Writer) -> Result<()> {
        wr.rewind()?;
        wr.write_annotated_byte_slice("magic", Self::MAGIC)
    }

    pub(crate) fn write(&self, wr: &mut impl Writer) -> Result<()> {
        wr.push_context("meta");
        let start_pos = wr.pos()?;
        wr.write_annotated_le_num("vers", Self::VERS)?;
        wr.write_annotated_le_num("rows", self.rows)?;
        wr.write_annotated_le_num("cols", self.cols)?;
        let ublocks = self.block_end_offsets.len();
        let blocks = ublocks as i64;
        if blocks != ublocks as i64 {
            return Err(err("bad block count"));
        }
        wr.write_annotated_le_num("blocks", blocks)?;
        wr.write_annotated_le_num_slice("block_end_offsets", &self.block_end_offsets)?;
        wr.write_len_of_footer_starting_at(start_pos)?;
        wr.pop_context();
        Ok(())
    }

    pub(crate) fn read(rd: &mut impl Reader) -> Result<Self> {
        rd.rewind()?;
        let mut buf: [u8;8] = [0;8];
        rd.read_exact(&mut buf)?;
        if buf != *Self::MAGIC {
            return Err(err("bad magic number"))
        }
        let vers: i64 = rd.read_le_num()?;
        if vers > Self::VERS {
            return Err(err("unsupported future version number"))
        }
        rd.seek(std::io::SeekFrom::End(0))?;
        rd.read_footer_len_and_rewind_to_start()?;
        let rows: i64 = rd.read_le_num()?;
        let cols: i64 = rd.read_le_num()?;
        let blocks: i64 = rd.read_le_num()?;
        let ublocks = blocks as usize;
        if ublocks as i64 != blocks {
            return Err(err("bad block count"));
        }
        let mut block_end_offsets = vec![0_i64; ublocks];
        rd.read_le_num_slice(&mut block_end_offsets)?;
        Ok(Self {
            vers, rows, cols, block_end_offsets
        })
    }
}

struct LayerWriter {
    meta: LayerMeta,
}

impl LayerWriter {
    pub(crate) fn new(wr: &mut impl Writer) -> Result<Self> {
        wr.push_context("layer");
        let meta = LayerMeta::default();
        meta.write_magic_header(wr)?;
        Ok(LayerWriter {
            meta,
        })
    }

    pub(crate) fn begin_block(self, wr: &mut impl Writer) -> BlockWriter {
        BlockWriter::new(self, wr)
    }

    pub(crate) fn finish_layer(self, wr: &mut impl Writer) -> Result<()> {
        self.meta.write(wr)?;
        wr.pop_context();
        Ok(())
    }
}

struct BlockWriter {
    layer_writer: LayerWriter,
    meta: BlockMeta,
}

impl BlockWriter {
    pub(crate) fn new(layer_writer: LayerWriter, wr: &mut impl Writer) -> Self {
        wr.push_context("block");
        wr.push_context(layer_writer.meta.block_end_offsets.len());
        BlockWriter {
            layer_writer,
            meta: BlockMeta::default(),
        }
    }

    pub(crate) fn begin_track(self, wr: &mut impl Writer) -> Result<TrackWriter> {
        TrackWriter::new(self, wr)
    }

    pub(crate) fn finish_block(mut self, wr: &mut impl Writer) -> Result<LayerWriter> {
        self.meta.write(wr)?;
        wr.pop_context();
        wr.pop_context();
        let pos = wr.pos()?;
        self.layer_writer.meta.block_end_offsets.push(pos);
        Ok(self.layer_writer)
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
struct BlockMeta {
    track_lo_vals: Vec<i64>,
    track_hi_vals: Vec<i64>,
    track_implicit: Bitmap256,
    track_rows: Vec<u16>, // row count for each track; may vary across substructure tracks
    track_end_offsets: Vec<i64>,
}

impl BlockMeta {
    pub(crate) fn write(&self, wr: &mut impl Writer) -> Result<()> {
        let ntracks = self.track_lo_vals.len();
        if ntracks != self.track_hi_vals.len() {
            return Err(err("track_lo_vals and track_hi_vals length mismatch"));
        }
        if ntracks > 256 {
            return Err(err("track count > 256"));
        }
        if ntracks != self.track_rows.len() as usize {
            return Err(err("track_lo_vals and track_rows length mismatch"));
        }
        if ntracks != self.track_end_offsets.len() as usize {
            return Err(err("track_lo_vals and track_end_offsets length mismatch"));
        }
        wr.push_context("meta");
        let start_pos = wr.pos()?;
        wr.write_annotated_le_num_slice("track_lo_vals", &self.track_lo_vals)?;
        wr.write_annotated_le_num_slice("track_hi_vals", &self.track_hi_vals)?;
        self.track_implicit.write_annotated("track_implicit", wr)?;
        wr.write_annotated_le_num_slice("track_rows", &self.track_rows)?;
        wr.write_annotated_le_num_slice("track_end_offsets", &self.track_end_offsets)?;
        wr.write_len_of_footer_starting_at(start_pos)?;
        wr.pop_context();
        Ok(())
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
    dict_bin_off_tys: WordTy256, // (optional) if any large bin: types of chunks of heap offsets

    // 256 * 4 bytes = 1k bytes
    chunk_min_dict_codes: Vec<u16>, // min dict code for each populated chunk
    chunk_max_dict_codes: Vec<u16>, // max dict code for each populated chunk
}

impl TrackMeta {
    pub(crate) fn write(&self, wr: &mut impl Writer) -> Result<()> {
        wr.push_context("meta");
        let start_pos = wr.pos()?;
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
        if self.dict_bin_large.any() {
            self.dict_bin_off_tys.write_annotated("dict_off_tys", wr)?;
        }
        wr.write_annotated_le_num_slice("chunk_min_dict_codes", &self.chunk_min_dict_codes)?;
        wr.write_annotated_le_num_slice("chunk_max_dict_codes", &self.chunk_max_dict_codes)?;
        wr.write_len_of_footer_starting_at(start_pos)?;
        wr.pop_context();
        Ok(())
    }
}

struct TrackWriter {
    block_writer: BlockWriter,
    meta: TrackMeta,
}

impl TrackWriter {
    pub(crate) fn new(block_writer: BlockWriter, wr: &mut impl Writer) -> Result<Self> {
        wr.push_context("track");
        wr.push_context(block_writer.meta.track_lo_vals.len());
        Ok(TrackWriter {
            block_writer,
            meta: TrackMeta::default(),
        })
    }

    pub(crate) fn finish_track(mut self, wr: &mut impl Writer) -> Result<BlockWriter> {
        self.meta.write(wr)?;
        wr.pop_context();
        wr.pop_context();
        let pos = wr.pos()?;
        self.block_writer.meta.track_end_offsets.push(pos);
        Ok(self.block_writer)
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
        wr.write_be_lane_of_annotated_num_slice("hi_lane", 0, vals)?;
    }
    wr.write_be_lane_of_annotated_num_slice("lo_lane", 1, vals)?;
    wr.pop_context();
    Ok(())
}

pub(crate) fn encode_track<T: DictEncodable, W: Writer>(
    vals: &[T],
    wr: &mut W,
) -> Result<TrackMeta> {
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
    wr.write_annotated_le_num("len", dict.len() as u16)?;
    T::write_dict_entries(&dict, wr)?;
    wr.pop_context(); // dict

    wr.push_context("code_chunks");
    for (c, chunk) in codes.chunks(256).enumerate() {
        wr.push_context(c); // chunk_num
                            // First decide whether the codes in this chunk need 2 bytes.
        let mut chunk_two_bytes = false;
        let mut chunk_min_dict_code = max_dict_code;
        let mut chunk_max_dict_code = 0;

        for &code in chunk {
            if code > 0xff {
                chunk_two_bytes = true;
            }
            if code < chunk_min_dict_code {
                chunk_min_dict_code = code;
            }
            if code > chunk_max_dict_code {
                chunk_max_dict_code = code;
            }
        }
        tm.chunk_min_dict_codes.push(chunk_min_dict_code);
        tm.chunk_max_dict_codes.push(chunk_max_dict_code);

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
            wr.write_annotated_le_num_slice("run_ends", &run_ends)?;
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
