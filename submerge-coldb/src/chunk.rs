use crate::{
    dict::{self, DictEncodable, BIN_COMPONENT_LEN, BIN_COMPONENT_OFFSET, COMPONENT_VALUE},
    heap::Heap,
    ioutil::Writer,
    track::{TrackReader, TrackWriter},
    wordty::WordTy,
};
use std::sync::Arc;
use submerge_base::{err, Result};

// There are Two flavours of chunks: dict-entry and dict-code.

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct DictEntryChunkMeta {
    pub(crate) any_bin_large: bool,
    pub(crate) val_ty: Option<WordTy>,
    pub(crate) bin_len_ty: Option<WordTy>,
    pub(crate) bin_off_ty: Option<WordTy>,
}

pub(crate) struct DictEntryChunkWriter {
    track_writer: TrackWriter,
    meta: DictEntryChunkMeta,
}

impl DictEntryChunkWriter {
    pub(crate) fn new(track_writer: TrackWriter, chunk_num: usize, wr: &mut impl Writer) -> Self {
        wr.push_context(chunk_num);
        DictEntryChunkWriter {
            track_writer,
            meta: DictEntryChunkMeta::default(),
        }
    }

    pub(crate) fn write_dict_encoded<T: DictEncodable>(
        &mut self,
        vals: &[&T],
        wr: &mut impl Writer,
        heap: &mut Heap,
    ) -> Result<()> {
        let n_components = vals
            .iter()
            .map(|x| x.get_component_count())
            .max()
            .unwrap_or(1);
        if n_components == dict::LARGE_BIN_COMPONENT_COUNT {
            self.meta.any_bin_large = true;
        }
        for component in 0..n_components {
            if n_components > 1 {
                wr.push_context(T::get_component_name(component));
            }
            let vals = vals
                .iter()
                .map(|x| x.get_component_as_int(component, heap))
                .collect::<Vec<i64>>();
            let (min, wordty) = WordTy::select_min_and_ty(&vals);
            wr.write_annotated_le_wordty_slice(&vals, wordty.clone())?;
            if component == COMPONENT_VALUE {
                self.meta.val_ty = Some(wordty);
            } else if component == BIN_COMPONENT_LEN {
                self.meta.bin_len_ty = Some(wordty);
            } else if component == BIN_COMPONENT_OFFSET {
                self.meta.bin_off_ty = Some(wordty);
            }
            if n_components > 1 {
                wr.pop_context();
            }
        }
        Ok(())
    }

    pub(crate) fn finish_chunk(mut self, wr: &mut impl Writer) -> Result<TrackWriter> {
        self.track_writer
            .note_dict_entry_chunk_finished(wr, &self.meta)?;
        wr.pop_context();
        Ok(self.track_writer)
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct DictCodeChunkMeta {
    pub(crate) two_bytes: bool,
    pub(crate) run_coded: bool,
    pub(crate) min_dict_code: u16,
    pub(crate) max_dict_code: u16,
}

pub(crate) struct DictCodeChunkWriter {
    track_writer: TrackWriter,
    meta: DictCodeChunkMeta,
}

impl DictCodeChunkWriter {
    pub(crate) fn new(track_writer: TrackWriter, chunk_num: usize, wr: &mut impl Writer) -> Self {
        wr.push_context(chunk_num);
        DictCodeChunkWriter {
            track_writer,
            meta: DictCodeChunkMeta::default(),
        }
    }

    pub(crate) fn write_dict_codes(&mut self, vals: &[u16], wr: &mut impl Writer) -> Result<()> {
        self.meta.min_dict_code = 0xffff;
        self.meta.max_dict_code = 0;
        for &code in vals {
            if code > 0xff {
                self.meta.two_bytes = true;
            }
            self.meta.min_dict_code = self.meta.min_dict_code.min(code);
            self.meta.max_dict_code = self.meta.max_dict_code.max(code);
        }

        // Then decide whether to row-end-encode this chunk.
        let (run_vals, run_ends) = run_end_encode(&vals)?;
        let chunk_code_width = if self.meta.two_bytes { 2 } else { 1 };
        let run_end_encoded_len = run_ends.len() * (chunk_code_width + 2);
        let simple_encoded_len = vals.len() * chunk_code_width;
        if run_end_encoded_len < simple_encoded_len {
            // Yes, REE is a savings, use it.
            self.meta.run_coded = true;
            let run_vals = run_vals.iter().map(|x| **x).collect::<Vec<u16>>();
            write_one_or_two_byte_dict_code_chunk(&run_vals, self.meta.two_bytes, wr)?;
            wr.write_annotated_le_num_slice("run_ends", &run_ends)?;
        } else {
            // No point, REE actually takes more space.
            write_one_or_two_byte_dict_code_chunk(vals, self.meta.two_bytes, wr)?;
        }
        Ok(())
    }

    pub(crate) fn finish_chunk(mut self, wr: &mut impl Writer) -> Result<TrackWriter> {
        self.track_writer
            .note_dict_code_chunk_finished(wr, &self.meta)?;
        wr.pop_context();
        Ok(self.track_writer)
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

pub(crate) struct DictEntryChunkReader {
    track_reader: Arc<TrackReader>,
    dict_chunk_num: usize,
    meta: DictEntryChunkMeta,
}

impl DictEntryChunkReader {
    pub(crate) fn new(track_reader: &Arc<TrackReader>, dict_chunk_num: usize) -> Self {
        let track_reader = track_reader.clone();
        DictEntryChunkReader {
            track_reader,
            dict_chunk_num,
            meta: DictEntryChunkMeta::default(),
        }
    }
}
