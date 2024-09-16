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
// Primitive columns chunks are then arranged into _basic_ column
// chunks, which can have multi-column _encoding_ substructure but add
// no _logical_ substructure (at the language level). Also the basic
// column chunk structure may vary chunk-to-chunk in the same column.
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

#![allow(dead_code,unused_variables)]

struct LayerWriter {
}
struct BlockWriter {
    lyr: Box<LayerWriter>
}
struct TrackWriter {
    blk: Box<BlockWriter>
}
struct ChunkWriter {
    trk: Box<TrackWriter>
}

impl ChunkWriter {

    // A chunk should use positive-virt encoding if every value is
    // row*n for some n. We should notice this is _not_ the case after
    // the second iteration of looking.
    fn pos_virt_base_and_factor(vals: &[i64]) -> Option<(i64,i64)> {
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
    fn neg_virt_base_and_factor(vals: &[i64]) -> Option<(i64,i64)> {
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
	    Some((base,-prev_run_len))
	} else {
	    // eprintln!("no runs or final run is overlong");
	    None
	}
    }

    fn select_encoding(vals: &[i64]) {	
    }
}

#[test]
fn test_pos_virt_base_and_factor() {
    assert_eq!(ChunkWriter::pos_virt_base_and_factor(&[2,6,10,14,18]), Some((2,4)));
}

#[test]
fn test_neg_virt_base_and_factor() {
    assert_eq!(ChunkWriter::neg_virt_base_and_factor(&[2,2,3,3,3]), None);
    assert_eq!(ChunkWriter::neg_virt_base_and_factor(&[2,2,2,3,3,3,4,4,4,5,5]), Some((2,-3)));
}

    

struct LayerReader {
}
struct BlockReader {
    lyr: Box<LayerReader>
}
struct TrackReader {
    blk: Box<BlockReader>
}
struct ChunkReader {
    trk: Box<TrackReader>
}

impl ChunkReader {
    fn next(&self, buf: &mut [u8;32]) {}
}
