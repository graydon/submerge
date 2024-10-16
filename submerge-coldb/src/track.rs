use std::sync::Arc;

use crate::{
    block::{BlockReader, BlockWriter},
    chunk::{DictCodeChunkMeta, DictCodeChunkWriter, DictEntryChunkMeta, DictEntryChunkWriter},
    dict::DictEncodable,
    heap::Heap,
    ioutil::{Bitmap256IoExt, Reader, Writer},
    wordty::WordTy256,
};
use submerge_base::{err, Bitmap256, Result};

// TrackMeta is nonempty only when track encoding is not Virt
#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct TrackMeta {
    code_chunk_populated: Bitmap256, // 1 bit per chunk, 1 if any row in chunk is populated

    // All remaining trackmeta fields are optional depending on datatype and encoding.
    // If the track is of type `bit`, or is implicit, then all other fields are empty
    // and not read/written.
    dict_entry_count: u16, // Dicts are dense so we just need a count of entries.
    dict_val_chunk_tys: WordTy256, // dict value: word-tys of chunks storing int/flo data or bin collator/prefix
    dict_bin_len_chunk_tys: WordTy256, // (optional) if bin: word-tys of chunks of lengths

    dict_bin_large: Bitmap256, // (optional) if bin, 1 if any bin in chunk > 8 bytes
    dict_bin_off_tys: WordTy256, // (optional) if any large bin: word-tys of chunks of heap offsets

    code_chunk_two_bytes: Bitmap256, // 1 bit per chunk, 1 if any dict code > 0xff
    code_chunk_run_coded: Bitmap256, // 1 bit per chunk, 1 if any run > 1 row (chunk has extra 2-byte run-end column)

    // 256 * 4 bytes = 1k bytes
    code_chunk_mins: Vec<u16>, // min dict code for each populated code chunk
    code_chunk_maxs: Vec<u16>, // max dict code for each populated code chunk
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

// A TrackMap is an expansion of information that is densely encoded in the TrackMeta
// but a little difficult to compute incrementally. It is used to quickly find the
// offset of a particular dictionary, code chunk or heap entry.
struct TrackMap {
    dict_chunk_offsets: Vec<i64>,
    code_chunk_offsets: Vec<Option<i64>>,
    heap_offset: i64,
}

impl TrackMap {
    fn new(meta: &TrackMeta, is_bin: bool) -> Self {
        let mut off = 0;
        let mut dict_chunk_offsets = Vec::new();
        let mut dict_entry_count = meta.dict_entry_count;
        for i in 0..=255 {
            let n_chunk_entries = dict_entry_count.min(256) as i64;
            dict_chunk_offsets.push(off);
            let mut chunk_len = 0;
            chunk_len += n_chunk_entries * (meta.dict_val_chunk_tys.get_word_ty(i).len() as i64);
            if is_bin {
                chunk_len +=
                    n_chunk_entries * (meta.dict_bin_len_chunk_tys.get_word_ty(i).len() as i64);
                if meta.dict_bin_large.get(i) {
                    chunk_len +=
                        n_chunk_entries * (meta.dict_bin_off_tys.get_word_ty(i).len() as i64);
                }
            }
            off += chunk_len;
            if n_chunk_entries < 256 {
                break;
            }
            dict_entry_count -= 256;
        }

        let mut code_chunk_offsets = Vec::new();
        let mut total_codes = meta.code_chunk_populated.count() as i64;
        for i in 0..=255 {
            let n_chunk_codes = total_codes.min(256) as i64;
            if !meta.code_chunk_populated.get(i) {
                code_chunk_offsets.push(None);
                continue;
            }
            code_chunk_offsets.push(Some(off));
            let mut chunk_len = 0;
            if meta.code_chunk_two_bytes.get(i) {
                chunk_len += n_chunk_codes; // 2-byte codes
            }
            if meta.code_chunk_run_coded.get(i) {
                chunk_len += n_chunk_codes; // run-coded
            }
            off += chunk_len;
            if n_chunk_codes < 256 {
                break;
            }
            total_codes -= 256;
        }

        TrackMap {
            dict_chunk_offsets,
            code_chunk_offsets,
            heap_offset: off,
        }
    }
}

impl TrackMeta {
    pub(crate) fn write(&mut self, wr: &mut impl Writer) -> Result<()> {
        if self.code_chunk_mins.len() != self.code_chunk_maxs.len() {
            return Err(err("min/max dict code mismatch"));
        }
        if self.code_chunk_mins.len() != self.code_chunk_populated.count() as usize {
            return Err(err("dict code populated-bitset count mismatch"));
        }

        wr.push_context("meta");
        let start_pos = wr.pos()?;
        self.code_chunk_populated
            .write_annotated("code_chunk_populated", wr)?;

        wr.write_annotated_le_num("dict_entry_count", self.dict_entry_count)?;
        self.dict_val_chunk_tys
            .write_annotated("dict_val_chunk_tys", wr)?;
        self.dict_bin_len_chunk_tys
            .write_annotated("dict_bin_len_chunk_tys", wr)?;
        self.dict_bin_large.write_annotated("dict_bin_large", wr)?;
        if self.dict_bin_large.any() {
            self.dict_bin_off_tys.write_annotated("dict_off_tys", wr)?;
        }

        self.code_chunk_two_bytes
            .write_annotated("code_chunk_two_bytes", wr)?;
        self.code_chunk_run_coded
            .write_annotated("code_chunk_run_coded", wr)?;

        wr.write_annotated_le_num_slice("chunk_min_dict_codes", &self.code_chunk_mins)?;
        wr.write_annotated_le_num_slice("chunk_max_dict_codes", &self.code_chunk_maxs)?;
        wr.write_len_of_footer_starting_at(start_pos)?;
        wr.pop_context();
        Ok(())
    }

    pub(crate) fn read_from_footer_end(rd: &mut impl Reader, end_pos: i64) -> Result<Self> {
        rd.read_footer_len_ending_at_pos_and_rewind_to_start(end_pos)?;
        let mut meta = TrackMeta::default();
        meta.code_chunk_populated = Bitmap256::read(rd)?;

        meta.dict_val_chunk_tys = WordTy256::read(rd)?;
        meta.dict_bin_len_chunk_tys = WordTy256::read(rd)?;
        meta.dict_bin_large = Bitmap256::read(rd)?;
        if meta.dict_bin_large.any() {
            meta.dict_bin_off_tys = WordTy256::read(rd)?;
        }

        meta.code_chunk_two_bytes = Bitmap256::read(rd)?;
        meta.code_chunk_run_coded = Bitmap256::read(rd)?;

        let n_code_chunks = meta.code_chunk_populated.count() as usize;
        meta.code_chunk_mins = rd.read_le_num_vec(n_code_chunks)?;
        meta.code_chunk_maxs = rd.read_le_num_vec(n_code_chunks)?;
        Ok(meta)
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
    pub(crate) fn new(
        block_writer: BlockWriter,
        track_num: usize,
        wr: &mut impl Writer,
    ) -> Result<Self> {
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

    pub(crate) fn note_dict_entry_chunk_finished(
        &mut self,
        wr: &mut impl Writer,
        meta: &DictEntryChunkMeta,
    ) -> Result<()> {
        let chunk_num = self.meta.dict_entry_count / 256;
        if chunk_num > 255 {
            return Err(err("code chunk num > 255"));
        }
        let chunk_num = chunk_num as u8;
        self.meta.dict_entry_count += 256;
        if let Some(ty) = &meta.val_ty {
            self.meta.dict_val_chunk_tys.set_word_ty(chunk_num, *ty);
        }
        if let Some(ty) = &meta.bin_len_ty {
            self.meta.dict_bin_len_chunk_tys.set_word_ty(chunk_num, *ty);
        }
        if let Some(ty) = &meta.bin_off_ty {
            self.meta.dict_bin_off_tys.set_word_ty(chunk_num, *ty);
        }
        if meta.any_bin_large {
            self.meta.dict_bin_large.set(chunk_num, true);
        }
        Ok(())
    }

    pub(crate) fn note_dict_code_chunk_finished(
        &mut self,
        wr: &mut impl Writer,
        meta: &DictCodeChunkMeta,
    ) -> Result<()> {
        let chunk_num = self.meta.code_chunk_maxs.len();
        if chunk_num > 255 {
            return Err(err("code chunk num > 255"));
        }
        self.meta.code_chunk_populated.set(chunk_num as u8, true);
        self.meta
            .code_chunk_two_bytes
            .set(chunk_num as u8, meta.two_bytes);
        self.meta
            .code_chunk_run_coded
            .set(chunk_num as u8, meta.run_coded);
        self.meta.code_chunk_mins.push(meta.min_dict_code);
        self.meta.code_chunk_maxs.push(meta.max_dict_code);
        Ok(())
    }

    pub(crate) fn write_dict_encoded<T: DictEncodable>(
        mut self,
        vals: &[T],
        wr: &mut impl Writer,
    ) -> Result<Self> {
        if vals.len() > 0xffff {
            return Err(err("track longer than 64k rows"));
        }
        self.info.rows = vals.len() as u16;
        self.info.implicit = false;
        if vals.len() == 0 {
            return Ok(self);
        }

        let (dict, codes) = dict_encode(vals)?;
        let max_dict_code = (dict.len() - 1) as u16;
        self.info.lo_val = dict
            .first()
            .ok_or_else(|| err("dict empty"))?
            .get_value_as_int();
        self.info.hi_val = dict
            .last()
            .ok_or_else(|| err("dict empty"))?
            .get_value_as_int();

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
    map: TrackMap,
}

impl TrackReader {
    pub(crate) fn new(
        block_reader: &Arc<BlockReader>,
        track_num: usize,
        end_pos: i64,
        rd: &mut impl Reader,
    ) -> Result<Arc<Self>> {
        let block_reader = block_reader.clone();
        if track_num > 255 {
            return Err(err("track count > 255"));
        }
        let meta = TrackMeta::read_from_footer_end(rd, end_pos)?;
        // FIXME: fetch bin-ness from column catalogue in block meta?
        let is_bin = false;
        let map = TrackMap::new(&meta, is_bin);
        Ok(Arc::new(TrackReader {
            block_reader,
            track_num,
            meta,
            map,
        }))
    }
}
