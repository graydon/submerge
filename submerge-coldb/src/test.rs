use crate::{
    encode_track,
    ioutil::{MemWriter, Writer},
    neg_virt_base_and_factor, pos_virt_base_and_factor,
    wordty::WordTy,
    BlockMeta, LayerMeta, Result,
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
fn test_wordty() {
    assert_eq!(WordTy::select_min_and_ty(&[]), (0, WordTy::Word1));
    assert_eq!(WordTy::select_min_and_ty(&[0]), (0, WordTy::Word1));
    assert_eq!(WordTy::select_min_and_ty(&[1]), (1, WordTy::Word1));
    assert_eq!(WordTy::select_min_and_ty(&[0xfff]), (0xfff, WordTy::Word1));
    assert_eq!(
        WordTy::select_min_and_ty(&[0xff00, 0x00ff]),
        (0xff, WordTy::Word2)
    );
}

#[test]
fn test_annotations() -> Result<()> {
    let mut w = MemWriter::new();
    w.push_context("layer");
    let lm = LayerMeta::default();
    lm.write_magic_header(&mut w)?;
    w.push_context("block");
    let bm = BlockMeta::default();
    w.push_context("track");

    w.push_context(0);
    let tm = encode_track(
        &[0xaa55, 0xaa55, 0xaa55, 6, 6, 6, 5, 6, 5, 3, 4, 2_i64],
        &mut w,
    )?;
    tm.write(&mut w)?;
    w.pop_context();

    w.push_context(1);
    let tm = encode_track(
        &[
            "hi there silly!".as_bytes(),
            "can see no way".as_bytes(),
            "no".as_bytes(),
        ],
        &mut w,
    )?;
    tm.write(&mut w)?;
    w.pop_context();

    bm.write(&mut w)?;
    w.pop_context();
    lm.write(&mut w)?;
    w.pop_context();
    eprintln!("dump:\n{}", w.render_annotations()?);
    Ok(())
}
