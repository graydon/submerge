use std::sync::Arc;

use crate::{block::{BlockReader, BlockWriter}, chunk::{DictCodeChunkMeta, DictCodeChunkWriter, DictEntryChunkMeta, DictEntryChunkWriter}, dict::DictEncodable, heap::Heap, ioutil::{Bitmap256IoExt, Writer}, wordty::WordTy256};
use submerge_base::{err, Bitmap256, Result};

// TrackMeta is nonempty only when track encoding is not Virt
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct TrackMeta {

    // If the track is virtual, there is _no_ track metadata read or written.
    chunk_populated: Bitmap256, // 1 bit per chunk, 1 if any row in chunk is populated

    // All remaining trackmeta fields are optional depending on datatype and encoding.
    // If the track is of type `bit`, or is virtual, then all other fields are empty
    // and not read/written.
    chunk_two_bytes: Bitmap256, // 1 bit per chunk, 1 if any dict code > 0xff
    chunk_run_coded: Bitmap256, // 1 bit per chunk, 1 if any run > 1 row (chunk has extra 2-byte run-end column)

    dict_val_chunk_tys: WordTy256, // dict value: types of chunks storing int/flo data or bin collator/prefix
    dict_bin_len_chunk_tys: WordTy256, // (optional) if bin: types of chunks of lengths

    dict_bin_large: Bitmap256, // (optional) if bin, 1 if any bin in chunk > 8 bytes
    dict_bin_off_tys: WordTy256, // (optional) if any large bin: types of chunks of heap offsets

    // 256 * 4 bytes = 1k bytes
    chunk_min_dict_codes: Vec<u16>, // min dict code for each populated chunk
    chunk_max_dict_codes: Vec<u16>, // max dict code for each populated chunk
}

// This structure is not serialized; it collects information about a track while it's
// being written and conveys it back to the block writer when the track is finished.
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct TrackInfoForBlock {
    pub(crate) track_num: u8,
    pub(crate) lo_val: i64,
    pub(crate) hi_val: i64,
    pub(crate) implicit: bool,
    pub(crate) rows: u16,
    pub(crate) end_pos: i64,
}

impl TrackMeta {
    pub(crate) fn write(&mut self, wr: &mut impl Writer) -> Result<()> {
        wr.push_context("meta");
        let start_pos = wr.pos()?;
        self.chunk_populated
            .write_annotated("chunk_populated", wr)?;
        self.chunk_two_bytes
            .write_annotated("chunk_two_bytes", wr)?;
        self.chunk_run_coded
            .write_annotated("chunk_run_coded", wr)?;
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

pub(crate) struct TrackWriter {
    block_writer: BlockWriter,
    meta: TrackMeta,
    info: TrackInfoForBlock,
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


impl TrackWriter {
    pub(crate) fn new(block_writer: BlockWriter, track_num: usize,  wr: &mut impl Writer) -> Result<Self> {
        if track_num > 255 {
            return Err(err("track count > 255"));
        }
        let track_num = track_num as u8;
        wr.push_context("track");
        wr.push_context(track_num);
        let meta = TrackMeta::default();
        let info = TrackInfoForBlock {
            track_num,
            lo_val: 0,
            hi_val: 0,
            implicit: false,
            rows: 0,
            end_pos: 0,
        };
        Ok(TrackWriter {
            block_writer,
            meta,
            info,
        })
    }

    pub(crate) fn note_dict_entry_chunk_finished(&mut self, wr: &mut impl Writer, meta: &DictEntryChunkMeta) -> Result<()> {
        Ok(())
    }

    pub(crate) fn note_dict_code_chunk_finished(&mut self, wr: &mut impl Writer, meta: &DictCodeChunkMeta) -> Result<()> {
        Ok(())
    }

    pub(crate) fn write_dict_encoded<T: DictEncodable>(mut self, vals: &[T], wr: &mut impl Writer) -> Result<Self> {

        if vals.len() > 0xffff {
            return Err(err("track longer than 64k rows"));
        }
        self.info.rows = vals.len() as u16;
        self.info.implicit = false;
        if vals.len() == 0 {
            return Ok(self)
        }

        let (dict, codes) = dict_encode(vals)?;
        let max_dict_code = (dict.len() - 1) as u16;
        self.info.lo_val = dict.first().ok_or_else(|| err("dict empty"))?.get_value_as_int();
        self.info.hi_val = dict.last().ok_or_else(|| err("dict empty"))?.get_value_as_int();

        let mut heap = Heap::default();

        wr.push_context("dict_entry_chunks");
        wr.write_annotated_le_num("len", dict.len() as u16)?;
        for (chunk_num, chunk) in dict.chunks(256).enumerate() {
            let mut chunk_writer = DictEntryChunkWriter::new(self, chunk_num, wr);
            chunk_writer.write_dict_encoded(chunk, wr, &mut heap)?;
            self = chunk_writer.finish_chunk(wr)?;
        }
        wr.pop_context(); // dict_entry_chunks

        wr.push_context("dict_code_chunks");
        for (chunk_num, chunk) in codes.chunks(256).enumerate() {
            let mut chunk_writer = DictCodeChunkWriter::new(self, chunk_num, wr);
            chunk_writer.write_dict_codes(chunk, wr)?;
            self = chunk_writer.finish_chunk(wr)?;
        }
        wr.pop_context(); // dict_code_chunks

        if heap.data.len() > 0 {
            wr.push_context("heap");
            wr.write_annotated_le_num("len", heap.data.len())?;
            wr.write_annotated_byte_slice("data", &heap.data)?;
            wr.pop_context();
        }

        Ok(self)
    }

    pub(crate) fn finish_track(mut self, wr: &mut impl Writer) -> Result<BlockWriter> {
        self.meta.write(wr)?;
        self.info.end_pos = wr.pos()?;
        wr.pop_context();
        wr.pop_context();
        self.block_writer.note_track_finished(wr, &self.info)?;
        Ok(self.block_writer)
    }
}

pub(crate) struct TrackReader {
    block_reader: Arc<BlockReader>,
    track_num: usize,
    meta: TrackMeta,
}