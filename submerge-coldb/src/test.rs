use crate::{TrackWriter, ChunkWriter, ioutil::{Writer, MemWriter}, Result};
use test_log::test;

pub(crate) mod annotations;

type MemTrackWriter = TrackWriter<MemWriter>;
type MemChunkWriter = TrackWriter<MemWriter>;

#[test]
fn test_pos_virt_base_and_factor() {
    assert_eq!(
        MemTrackWriter::pos_virt_base_and_factor(&[2, 6, 10, 14, 18]),
        Some((2, 4))
    );
}

#[test]
fn test_neg_virt_base_and_factor() {
    assert_eq!(
        MemTrackWriter::neg_virt_base_and_factor(&[2, 2, 3, 3, 3]),
        None
    );
    assert_eq!(
        MemTrackWriter::neg_virt_base_and_factor(&[2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5]),
        Some((2, -3))
    );
}

#[test]
fn test_byte_width_and_shift() {
    assert_eq!(MemChunkWriter::byte_width_and_shift(&[]), (0, 0));
    assert_eq!(MemChunkWriter::byte_width_and_shift(&[0]), (0, 0));
    assert_eq!(MemChunkWriter::byte_width_and_shift(&[1]), (1, 0));
    assert_eq!(MemChunkWriter::byte_width_and_shift(&[0xfff]), (2, 0));
    assert_eq!(MemChunkWriter::byte_width_and_shift(&[0xff00]), (1, 1));
    assert_eq!(MemChunkWriter::byte_width_and_shift(&[0xff00ff00]), (3, 1));
    assert_eq!(
        MemChunkWriter::byte_width_and_shift(&[0xff00, 0x00ff]),
        (2, 0)
    );
}

#[test]
fn test_annotations() -> Result<()> {
    let mut w = MemWriter::new();
    w.write_annotated_byte_slice("header", &"columnar".as_bytes())?;
    w.write_annotated_num("version", 1i64)?;
    eprintln!("dump:\n{}", w.render_annotations()?);
    Ok(())
}
