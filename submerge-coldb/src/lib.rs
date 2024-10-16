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

mod block;
mod chunk;
mod dict;
mod heap;
mod ioutil;
mod layer;
mod track;
mod wordty;

#[cfg(test)]
mod test;

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
