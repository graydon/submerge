use crate::{
    ioutil::MemWriter, layer::LayerWriter, neg_virt_base_and_factor, pos_virt_base_and_factor,
    wordty::WordTy,
};
use submerge_base::Result;
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

    LayerWriter::new(&mut w)?
        .begin_block(&mut w)?
        .begin_track(&mut w)?
        .write_dict_encoded(
            &[0xaa55, 0xaa55, 0xaa55, 6, 6, 6, 5, 6, 5, 3, 4, 2_i64],
            &mut w,
        )?
        .finish_track(&mut w)?
        .begin_track(&mut w)?
        .write_dict_encoded(
            &[
                "hi there silly!".as_bytes(),
                "can see no way".as_bytes(),
                "no".as_bytes(),
            ],
            &mut w,
        )?
        .finish_track(&mut w)?
        .begin_track(&mut w)?
        .write_dict_encoded(&[0xffff_ffff_i64; 1024], &mut w)?
        .finish_track(&mut w)?
        .finish_block(&mut w)?
        .finish_layer(&mut w)?;

    eprintln!("dump:\n{}", w.render_annotations()?);
    Ok(())
}
