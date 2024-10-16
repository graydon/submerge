use std::sync::Arc;

use crate::{
    block::{self, BlockInfoForLayer, BlockReader, BlockWriter},
    ioutil::{Reader, Writer},
};
use submerge_base::{err, Result};

#[derive(Clone, Default, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub(crate) struct LayerMeta {
    vers: i64,
    rows: i64,
    cols: i64,
    block_end_offsets: Vec<i64>,
}

impl LayerMeta {
    pub const MAGIC: &[u8; 8] = b"submerge";
    pub const VERS: i64 = 0;

    pub(crate) fn write_magic_header(&self, wr: &mut impl Writer) -> Result<()> {
        wr.rewind()?;
        wr.write_annotated_byte_slice("magic", Self::MAGIC)
    }

    pub(crate) fn read_and_check_magic_header(rd: &mut impl Reader) -> Result<()> {
        rd.rewind()?;
        let mut buf: [u8; 8] = [0; 8];
        rd.read_exact(&mut buf)?;
        if buf != *Self::MAGIC {
            return Err(err("bad magic number"));
        }
        Ok(())
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
        let vers: i64 = rd.read_le_num()?;
        if vers > Self::VERS {
            return Err(err("unsupported future version number"));
        }
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
            vers,
            rows,
            cols,
            block_end_offsets,
        })
    }
}

pub(crate) struct LayerWriter {
    meta: LayerMeta,
}

impl LayerWriter {
    pub fn new(wr: &mut impl Writer) -> Result<Self> {
        wr.push_context("layer");
        let meta = LayerMeta::default();
        meta.write_magic_header(wr)?;
        Ok(LayerWriter { meta })
    }

    pub(crate) fn begin_block(self, wr: &mut impl Writer) -> Result<BlockWriter> {
        let block_num = self.meta.block_end_offsets.len();
        BlockWriter::new(self, block_num, wr)
    }

    pub(crate) fn note_block_finished(
        &mut self,
        wr: &mut impl Writer,
        info: &BlockInfoForLayer,
    ) -> Result<()> {
        self.meta.block_end_offsets.push(info.end_pos);
        Ok(())
    }

    pub fn finish_layer(self, wr: &mut impl Writer) -> Result<()> {
        self.meta.write(wr)?;
        wr.pop_context();
        Ok(())
    }
}

pub(crate) struct LayerReader {
    meta: LayerMeta,
}

impl LayerReader {
    pub fn new(rd: &mut impl Reader) -> Result<Arc<Self>> {
        LayerMeta::read_and_check_magic_header(rd)?;
        rd.seek(std::io::SeekFrom::End(0))?;
        let end_pos = rd.pos()?;
        rd.read_footer_len_ending_at_pos_and_rewind_to_start(end_pos)?;
        let meta = LayerMeta::read(rd)?;
        Ok(Arc::new(LayerReader { meta }))
    }

    pub fn new_block_reader(
        self: &Arc<Self>,
        block_num: usize,
        rd: &mut impl Reader,
    ) -> Result<Arc<BlockReader>> {
        if let Some(&end_pos) = self.meta.block_end_offsets.get(block_num) {
            if end_pos < 0 {
                return Err(err("negative block end offset"));
            }
            BlockReader::new(self, block_num, end_pos, rd)
        } else {
            Err(err("block number out of range"))
        }
    }
}
