use crate::{
    byte_width_and_shift, encode_track,
    ioutil::{MemWriter, Writer},
    neg_virt_base_and_factor, pos_virt_base_and_factor, BlockMeta, LayerMeta, Result,
};
use test_log::test;

pub(crate) mod annotations;

#[test]
fn test_pos_virt_base_and_factor() {
    assert_eq!(pos_virt_base_and_factor(&[2, 6, 10, 14, 18]), Some((2, 4)));
}

#[test]
fn test_neg_virt_base_and_factor() {
    assert_eq!(neg_virt_base_and_factor(&[2, 2, 3, 3, 3]), None);
    assert_eq!(
        neg_virt_base_and_factor(&[2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5]),
        Some((2, -3))
    );
}

#[test]
fn test_byte_width_and_shift() {
    assert_eq!(byte_width_and_shift(&[]), (0, 0));
    assert_eq!(byte_width_and_shift(&[0]), (0, 0));
    assert_eq!(byte_width_and_shift(&[1]), (1, 0));
    assert_eq!(byte_width_and_shift(&[0xfff]), (2, 0));
    assert_eq!(byte_width_and_shift(&[0xff00]), (1, 1));
    assert_eq!(byte_width_and_shift(&[0xff00ff00]), (3, 1));
    assert_eq!(byte_width_and_shift(&[0xff00, 0x00ff]), (2, 0));
}

#[test]
fn test_annotations() -> Result<()> {
    let mut w = MemWriter::new();
    let lm = LayerMeta::default();
    lm.write_magic_header(&mut w)?;
    let bm = BlockMeta::default();
    let tm = encode_track(&[5, 5, 5, 6, 6, 6, 5, 6, 5, 3, 4, 2_i64], &mut w)?;
    tm.write(&mut w)?;
    bm.write(&mut w)?;
    lm.write(&mut w)?;
    eprintln!("dump:\n{}", w.render_annotations()?);
    Ok(())
}
