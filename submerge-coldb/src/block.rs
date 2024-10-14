use std::sync::Arc;

use crate::{ioutil::{Bitmap256IoExt, Writer}, layer::{LayerReader, LayerWriter}, track::{TrackInfoForBlock, TrackWriter}};
use submerge_base::{err, Bitmap256, Result};


pub(crate) struct BlockWriter {
    layer_writer: LayerWriter,
    meta: BlockMeta,
    info: BlockInfoForLayer,
}

impl BlockWriter {
    pub(crate) fn new(layer_writer: LayerWriter, block_num: usize, wr: &mut impl Writer) -> Result<Self> {
        wr.push_context("block");
        wr.push_context(block_num);
        let info = BlockInfoForLayer {
            block_num,
            end_pos: 0,
        };
        let meta = BlockMeta::default();
        Ok(BlockWriter {
            layer_writer,
            meta,
            info
        })
    }

    pub(crate) fn begin_track(self, wr: &mut impl Writer) -> Result<TrackWriter> {
        let track_num = self.meta.track_end_offsets.len();
        TrackWriter::new(self, track_num, wr)
    }

    pub(crate) fn note_track_finished(&mut self, wr: &mut impl Writer, info: &TrackInfoForBlock) -> Result<()> {
        self.meta.track_lo_vals.push(info.lo_val);
        self.meta.track_hi_vals.push(info.hi_val);
        self.meta.track_implicit.set(info.track_num, info.implicit);
        self.meta.track_rows.push(info.rows);
        self.meta.track_end_offsets.push(info.end_pos);
        Ok(())
    }

    pub fn finish_block(mut self, wr: &mut impl Writer) -> Result<LayerWriter> {
        self.meta.write(wr)?;
        wr.pop_context();
        wr.pop_context();
        self.layer_writer.note_block_finished(wr, &self.info)?;
        Ok(self.layer_writer)
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct BlockMeta {
    track_lo_vals: Vec<i64>,
    track_hi_vals: Vec<i64>,
    track_implicit: Bitmap256,
    track_rows: Vec<u16>, // row count for each track; may vary across substructure tracks
    track_end_offsets: Vec<i64>,
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct BlockInfoForLayer {
    pub(crate) block_num: usize,
    pub(crate) end_pos: i64,
}

impl BlockMeta {
    pub(crate) fn write(&mut self, wr: &mut impl Writer) -> Result<()> {
        let ntracks = self.track_lo_vals.len();
        if ntracks != self.track_hi_vals.len() {
            return Err(err("track_lo_vals and track_hi_vals length mismatch"));
        }
        if ntracks > 255 {
            return Err(err("track count > 255"));
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

pub(crate) struct BlockReader {
    layer_reader: Arc<LayerReader>,
    block_num: usize,
    meta: BlockMeta,
}

