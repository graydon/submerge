// GUH all this is bad and wrong and not sufficient and over-engineered.
//
// Look at arrow. Take a subset of it.

// Columns have 16 primitive forms:
//
// - Err -- 1 code no body
// - Nil -- 1 code no body
// - Bit -- 1 code bitmap body
// - Flo (direct, 8 bytes) -- 1 code, 1 word body
// - Int (direct, 1,2,4,8 bytes) -- 4 codes
// - Int (sliced, 1..=8 bytes) -- 8 codes
// 
// All int forms are FOR-encoded based on the chunk min val.
//
// Primitive columns are then arranged into _basic_ columns, which can
// have multi-column _encoding_ substructure but add no _logical_
// substructure (at the language level):
//
//   - Prim (no subcols)
//   - Bins (subcols: prefixes, hashes, offsets, sizes)
//   - Runs (subcols: values, offsets, sizes)
//   - Dict (subcols: codes, values)
//
// Finally, basic columns are arranged into _logical_ columns, which
// have logical substructure (surfaced at the PL level)
//
//   - Basic (no subcols, 6 sub-cases: err, nil, bit, flo, int, bin)
//   - Lists (subcols: 1 offsets, 1 sizes, 1 child column)
//   - Table (&, subcols: N child columns)
//   - Union (|, subcols: 1 selector, 1 offsets, N child columns)
//   - Boxed (>, subcols: 1 table, 1 code)
//
// Every logical column has, at the file level, a unique-in-its-parent _label_ and a major/minor/role type-triple.
// This is the file's logical column catalogue, which there is one of per file.
// The column catalogue has a table-level heap and a nested set of "multi"-columns of dictionary-coded bins.
//
// Note: a _decoded_ bin is _4_ words: prefix, offset, size, _heap ID_. These IDs can basically never be exhausted.
// Note: iterators for all int-types yield i64 values regardless of basic and primitive encoding.
// Note: this is the same yielded-type between rowdb and coldb interfaces.
// Note: only _predicate pushdown_ allows pruning sliced ints before reassembly.

struct LayerWriter {
}

struct BlockWriter {
}


struct FileReader {
    
}

struct BlockReader {
    
}

struct ChunkReader {
    blk: Box<BlockReader>;
    col: i64,
    row: i64,
}

impl ChunkReader {
    fn next(&self, buf: &mut [u8;32]) {}
}


// A file contains a single table: a description of a set of columns, then a set of blocks for those columns.
// A layer is a self-contained set of blocks across all columns.
// A block is a max-64k set of rows from one column.
// A block contains a max-16MiB (24-bit) heap of bytes values, and 1 max-64k dict pointing into it.
// A block header specifies a min and max value for the column.
// A block's column is each subdivided into 256 chunks.
// A chunk covers max-256 rows of 1 column, with a specific encoding; it's the unit of adaptive compression.
// A chunk is also the unit of dispatch and value-production.
// That is: every operator consumes a stream of chunks, and produces a stream of chunks.
//
// Every chunk has a min and max value (8 + 8 bytes). The dictionary for all chunks of a column is stored at the block level.
// Every chunk has a byte giving its row count. The max size of a chunk (storing 256 8 byte values) is 2KiB.
// Every chunk has an 8 byte file offset.
// Every chunk also has a control byte:
//
//    - 1 bit has-a-bitmap
//    - 2 bits chunk kind
//    - if explicit:
//      - 3 bits width
//      - 1 bit split
//      - 1 bit dict
//
// There are 4 kinds of chunks: 3 implicit, 1 explicit.
//
//  - Empty (implicit: nothing)
//  - Const (implicit: min val)
//  - Range (implicit: min+(row*max)) -- is this actually useful? Defines regular structures, eg. 1 parent => 4 children is just range(0,4)
//  - Coded (explicit)
//
// Coded chunks are themselves organized into 32-byte / 256-bit
// _lines_. Loops are unrolled to operate line-at-a-time and chunks
// are always a multiple of line size. Depending on SIMD hardware this
// might get 1, 2, 4 or more operations per line.
//
// If the has-a-bitmap bit is set, the first line of a coded chunk is
// a bitmap indicating row occupancy.
//
// If the column is bit-typed, then there is only 1 further line: a
// bitmap, of the bits!
//
// Otherwise for int, flo or bin typed columns:
//
//   The width bits tell you how wide the data is _logically_ (1..=8 bytes).
//   The split bit tells you if the data is composed or decomposed into sub-word 1-byte columns.
//   (If composed, only widths of 1, 2, 4 or 8 are legal).
//   The dict bit tells you if the encoded values are direct values or dictionary indices.
//
// Logically an int, flo or bin-typed column _can_ be a 64-bit word,
// though they are often dict-coded. An int is an int64. A flo is an
// f64. Int-typed columns are always FOR-encoded (unsigned positive
// deltas from the chunk min).
//
// A bin describes up to 255 bytes of binary data in an 8 byte
// word. It is 4 bytes of bin-prefix followed by 1 byte of length and
// then either
//
//   - 3 more bytes of payload, if len <= 7
//   - 3 bytes / 24 bits of heap offset, if len in 8..=255
